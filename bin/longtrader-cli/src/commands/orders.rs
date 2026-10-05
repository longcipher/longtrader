use std::sync::{LazyLock, Mutex};

use color_eyre::{Report, Result, eyre::eyre};
use longtrader_contract::ext::decimal_to_common;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, trading::v1 as utrading},
};
use rust_decimal::Decimal;

use crate::output::Renderer;

/// The money-literal grammar [`parse_dec`] accepts, as a diagnostic spells it.
const GRAMMAR: &str = "[+-]?digits[.digits]";

/// `true` for a non-empty run of ASCII digits.
fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Why `s` falls outside [`GRAMMAR`], or `None` when it is inside it.
///
/// This is the single place the grammar is enforced, so the accepted shape and
/// the diagnostic that describes it cannot drift apart. The reasons are checked
/// most-specific first, so a message names the thing actually wrong with the
/// input rather than a generic rejection.
fn grammar_violation(s: &str) -> Option<&'static str> {
    // `rust_decimal` follows Rust literal syntax and reads `_` as a digit
    // separator, so `--quantity 1_000` would quietly become 1000.
    if s.contains('_') {
        return Some("digit separators are not accepted");
    }
    // Any `e`/`E` sends `rust_decimal` down its scientific-notation fallback,
    // whose exponent is parsed with `str::parse::<u32>()` and so accepts a `+`.
    if s.contains(['e', 'E']) {
        return Some("exponents are not accepted");
    }
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    if !is_digits(whole) {
        return Some("expected one or more digits before the decimal point");
    }
    if body.contains('.') && !is_digits(fraction) {
        return Some("expected one or more digits after the decimal point");
    }
    // `rust_decimal` keeps only 28 fractional digits and ROUNDS the rest, so a
    // 29-digit quantity would be accepted and silently altered. For a money
    // field that is the one failure mode a grammar check must not allow.
    if fraction.len() > MAX_FRACTION_DIGITS {
        return Some("too many fractional digits");
    }
    None
}

/// `rust_decimal` represents at most 28 fractional digits (`Decimal::MAX_SCALE`).
const MAX_FRACTION_DIGITS: usize = 28;

/// Parse a money literal for `field`, whose name is a CLI flag echoed verbatim.
///
/// The accepted grammar is exactly [`GRAMMAR`]: an optional sign, then digits,
/// then optionally a decimal point and at least one more digit. The lenient
/// spellings `rust_decimal` would also take are refused on purpose, because each
/// one turns a plausible typo into a valid order instead of an error:
///
/// * `_` digit separators — `1_000` would send 1000.
/// * any exponent — `1e3` and `1e+3` would both send 1000, and a scale the operator never typed is
///   exactly the guess a money field must not make.
/// * a leading or trailing decimal point (`.5`, `1.`) — a shell-quoting habit, not an intent.
///
/// An in-grammar literal can still fail, because `rust_decimal` caps the
/// fraction at 28 digits and the mantissa at 96 bits. Those failures carry the
/// parser's own reason as the report's cause, so the CLI's own message stays
/// recoverable from the field name and the echoed input alone.
fn parse_dec(s: &str, field: &'static str) -> Result<Decimal> {
    if let Some(reason) = grammar_violation(s) {
        return Err(eyre!("bad decimal for {field}: {s:?} (want {GRAMMAR}: {reason})"));
    }
    let message = format!("bad decimal for {field}: {s:?}");
    s.parse::<Decimal>().map_err(|source| Report::new(source).wrap_err(message))
}

/// Parse a money literal that has to be strictly greater than zero.
///
/// `--quantity 0` is not an order, and `place_order` turns *any* supplied
/// `--price` into a limit order, so `--price 0` would put a live order on the
/// book at a price no venue can ever match. Both are refused before the request
/// is built, so neither can reach a venue.
fn parse_positive_dec(s: &str, field: &'static str) -> Result<Decimal> {
    let value = parse_dec(s, field)?;
    if value <= Decimal::ZERO {
        return Err(eyre!("{field} must be greater than zero, got {s:?}"));
    }
    Ok(value)
}

/// Process-wide monotonic ULID generator behind [`client_order_id`].
///
/// `Ulid::generate()` draws 80 fresh random bits per call and its own docs
/// disclaim any ordering, so two ids minted in the same millisecond sort
/// arbitrarily and only collide with probability 2^-80. A `client_order_id` is
/// the venue's idempotency key, so it has to be strictly increasing: retries and
/// log correlations then read in submission order.
static ORDER_IDS: LazyLock<Mutex<ulid::Generator>> =
    LazyLock::new(|| Mutex::new(ulid::Generator::new()));

/// Next `client_order_id`: the CLI namespace plus a strictly increasing ULID.
///
/// The generator is behind a mutex, and the guard is taken and released entirely
/// inside this function, so no guard is ever held across an `.await`. A poisoned
/// lock still yields a usable generator: the state it protects is a single
/// `Ulid`, which a panic cannot have left half-updated.
fn client_order_id() -> String {
    let mut generator = ORDER_IDS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    match generator.generate() {
        Ok(id) => format!("cli-{id}"),
        // Overflow needs 2^80 ids inside one millisecond. Incrementing into the
        // next millisecond keeps the sequence strictly increasing.
        Err(overflow) => format!("cli-{}", overflow.commit_overflow_increment()),
    }
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn place_order(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    side: utrading::OrderSide,
    quantity: &str,
    price: Option<&str>,
    take_profit: Option<&str>,
    stop_loss: Option<&str>,
    output: &Renderer,
) -> Result<()> {
    // A price turns the order into a limit; without one it is a market order.
    let order_type =
        if price.is_some() { utrading::OrderType::Limit } else { utrading::OrderType::Market };

    // The money fields are parsed in the order the wire fields are written, so a
    // malformed quantity is always the first thing reported. Only the quantity
    // and the limit price have to be positive: an exit bracket keeps its sign,
    // because `take_profit` on a short is a price below the entry.
    let amount = parse_positive_dec(quantity, "quantity")?;
    // The wire type is `MessageField<Decimal, Inline<Decimal>>`; let the struct
    // literal below drive inference rather than naming the representation.
    let limit_price = price
        .map(|p| parse_positive_dec(p, "price"))
        .transpose()?
        .map_or_default(|d| decimal_to_common(d).into());

    let bracket = |v: Option<&str>, field: &'static str| -> Result<_> {
        Ok(v.map(|p| parse_dec(p, field).map(|d| decimal_to_common(d).into()))
            .transpose()?
            .unwrap_or_default())
    };

    let order = utrading::OrderRequest {
        client_order_id: client_order_id(),
        symbol: symbol.to_string(),
        r#type: buffa::EnumValue::Known(order_type),
        side: buffa::EnumValue::Known(side),
        amount: decimal_to_common(amount).into(),
        price: limit_price,
        take_profit: bracket(take_profit, "take_profit")?,
        stop_loss: bracket(stop_loss, "stop_loss")?,
        ..Default::default()
    };

    let req = utrading::CreateOrderRequest {
        exchange_id: exchange.clone().into(),
        order: order.into(),
        ..Default::default()
    };

    match client.create_order(req).await {
        Ok(order) => {
            output.render_orders(&[order]);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use buffa::Message;
    use longtrader_contract::ext::common_to_decimal;
    use proptest::prelude::*;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::output::OutputFormat;

    /// A loopback port nothing listens on, so every request dies in the
    /// transport. The decimal parses are the only things `place_order` decides
    /// before it touches the client, and they are the only things these tests
    /// read.
    const DEAD_ENDPOINT: &str = "http://127.0.0.1:1";

    /// Ceiling on how long a *parseable* order may spend failing to connect. A
    /// rejected decimal returns before any `await`, so it always answers on the
    /// first poll and never spends this budget.
    const TRANSPORT_DEADLINE: Duration = Duration::from_millis(250);

    /// Loopback address the capture listener binds. Port 0 lets the OS pick a
    /// free one, so concurrent tests cannot collide.
    const LOOPBACK: &str = "127.0.0.1:0";

    /// How long the listener waits for a connection. A safety net only: a
    /// parseable order always reaches the socket.
    const LISTEN_DEADLINE: Duration = Duration::from_secs(2);

    /// How long one socket read may block before the capture gives up.
    const READ_DEADLINE: Duration = Duration::from_secs(2);

    /// Ceiling on the captured `place_order` call. The capture hangs up, so a
    /// healthy call always answers long before this.
    const RUN_DEADLINE: Duration = Duration::from_secs(5);

    /// Every decimal field `place_order` parses, in the order the struct
    /// literal evaluates them.
    const FIELDS: [&str; 4] = ["quantity", "price", "take_profit", "stop_loss"];

    /// A 31-digit whole number: inside the CLI's grammar (one or more digits, no
    /// point, no separator, no exponent) but far past the 96-bit mantissa
    /// `rust_decimal` can hold, so it is the input that reaches the *parser*
    /// rather than the grammar check.
    const OVERFLOWED: &str = "1234567890123456789012345678901";

    /// The message `parse_dec` produces for an input outside the grammar.
    ///
    /// Mirrors the one `eyre!` in `parse_dec`; the in-grammar failures carry a
    /// shorter message plus a chained cause instead.
    fn shape_error(field: &str, input: &str, reason: &str) -> String {
        format!("bad decimal for {field}: {input:?} (want {GRAMMAR}: {reason})")
    }

    /// The leading `bad decimal for {field}: {input:?}` every rejection opens
    /// with, whichever of the two shapes it takes.
    fn error_prefix(field: &str, input: &str) -> String {
        format!("bad decimal for {field}: {input:?}")
    }

    // ---- the loopback capture ----
    //
    // The bracket closure and the Limit/Market choice are the whole contract of
    // this module, and neither is observable from the outside: an absent bracket
    // must stay *absent* on the wire rather than encoding a zero price, and only
    // the request `place_order` encodes can say so. So the tests bind a loopback
    // listener, capture that one request, and hang up.

    /// Read one HTTP/1.1 request off `socket` and return its body.
    ///
    /// `hpx` frames a byte-slice body with `Content-Length` (its size hint is
    /// exact), so the body is whatever follows the header terminator once the
    /// declared number of bytes has arrived.
    async fn read_one_body(socket: &mut TcpStream) -> Vec<u8> {
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];

        let header_end = loop {
            if let Some(at) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                break at + 4;
            }
            if !read_more(socket, &mut raw, &mut buf).await {
                return Vec::new();
            }
        };

        let headers = String::from_utf8_lossy(&raw[..header_end]).to_lowercase();
        let declared: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        while raw.len() < header_end + declared {
            if !read_more(socket, &mut raw, &mut buf).await {
                break;
            }
        }
        raw[header_end..].to_vec()
    }

    /// Append one socket read to `raw`; `false` means the peer hung up or the
    /// read stalled past `READ_DEADLINE`.
    ///
    /// The read is bound to a local before it is matched on: a future built in a
    /// `match` scrutinee stays alive until the end of the match, which would keep
    /// the borrow of `buf` alive across the arm that reads it.
    async fn read_more(socket: &mut TcpStream, raw: &mut Vec<u8>, buf: &mut [u8]) -> bool {
        let outcome = tokio::time::timeout(READ_DEADLINE, socket.read(buf)).await;
        match outcome {
            Ok(Ok(read)) => {
                raw.extend_from_slice(&buf[..read]);
                true
            }
            _ => false,
        }
    }

    /// Accept one request and hand back its body.
    async fn capture_one_body(listener: TcpListener) -> Option<Vec<u8>> {
        match tokio::time::timeout(LISTEN_DEADLINE, listener.accept()).await {
            Ok(Ok((mut socket, _))) => Some(read_one_body(&mut socket).await),
            _ => None,
        }
    }

    /// Run `place_order` against a loopback listener and return the
    /// `OrderRequest` it actually put on the wire.
    async fn captured_order(
        quantity: &str,
        price: Option<&str>,
        take_profit: Option<&str>,
        stop_loss: Option<&str>,
    ) -> utrading::OrderRequest {
        let listener =
            TcpListener::bind(LOOPBACK).await.expect("a loopback port is always bindable");
        let addr = listener.local_addr().expect("a bound listener always has an address");
        let capture = tokio::spawn(capture_one_body(listener));

        let client = TerminalClient::new(&format!("http://{addr}"));
        let output = Renderer::new(OutputFormat::Table);
        let exchange = common::ExchangeId::default();
        let call = super::place_order(
            &client,
            &exchange,
            "BTCUSDT",
            utrading::OrderSide::Buy,
            quantity,
            price,
            take_profit,
            stop_loss,
            &output,
        );
        // The capture hangs up, so the transport error this produces is the
        // expected outcome rather than the thing under test.
        let _ = tokio::time::timeout(RUN_DEADLINE, call).await;

        let body = capture
            .await
            .expect("the capture task never panics")
            .expect("a parseable order always sends its request");
        let request = utrading::CreateOrderRequest::decode_from_slice(&body)
            .expect("the CLI encodes a decodable CreateOrderRequest");
        request.order.into_option().expect("place_order always populates the order")
    }

    /// What `place_order` decided before it opened a socket: `Err` carries the
    /// user-facing message verbatim, `Ok(())` means the parse let the call
    /// through.
    ///
    /// Spelled `core::result::Result` because `use super::*` also brings
    /// `color_eyre::Result` into this module.
    async fn preflight(
        quantity: &str,
        price: Option<&str>,
        take_profit: Option<&str>,
        stop_loss: Option<&str>,
    ) -> core::result::Result<(), String> {
        let client = TerminalClient::new(DEAD_ENDPOINT);
        let output = Renderer::new(OutputFormat::Table);
        let exchange = common::ExchangeId::default();
        let call = super::place_order(
            &client,
            &exchange,
            "BTCUSDT",
            utrading::OrderSide::Buy,
            quantity,
            price,
            take_profit,
            stop_loss,
            &output,
        );
        match tokio::time::timeout(TRANSPORT_DEADLINE, call).await {
            Err(_elapsed) => Err("transport still pending".to_string()),
            Ok(Ok(())) => Ok(()),
            Ok(Err(report)) => Err(report.to_string()),
        }
    }

    // ---- parse_dec: accepted forms ----

    #[test]
    fn a_bare_integer_is_the_whole_number_it_spells() {
        assert_eq!(super::parse_dec("1", "quantity").expect("valid"), Decimal::from(1));
    }

    #[test]
    fn a_decimal_point_is_honoured_as_a_scale_not_a_thousands_separator() {
        assert_eq!(super::parse_dec("1.5", "quantity").expect("valid"), Decimal::new(15, 1));
    }

    #[test]
    fn a_leading_minus_is_accepted_and_keeps_the_sign() {
        assert_eq!(super::parse_dec("-2", "quantity").expect("valid"), Decimal::from(-2));
    }

    #[test]
    fn a_small_fraction_survives_at_full_scale() {
        assert_eq!(super::parse_dec("0.001", "quantity").expect("valid"), Decimal::new(1, 3));
    }

    #[test]
    fn a_leading_plus_is_accepted_because_the_grammar_allows_a_sign() {
        assert_eq!(super::parse_dec("+1", "quantity").expect("valid"), Decimal::from(1));
    }

    #[test]
    fn a_sign_is_only_accepted_as_the_very_first_character() {
        assert!(super::parse_dec("+-1", "quantity").is_err());
        assert!(super::parse_dec("1-", "quantity").is_err());
        assert!(super::parse_dec("--1", "quantity").is_err());
    }

    // ---- parse_dec: a trailing or leading decimal point is refused ----

    #[test]
    fn a_trailing_decimal_point_is_rejected_because_the_fraction_is_empty() {
        let message = super::parse_dec("1.", "quantity")
            .expect_err("`1.` is not a money literal")
            .to_string();
        assert_eq!(
            message,
            shape_error("quantity", "1.", "expected one or more digits after the decimal point")
        );
    }

    #[test]
    fn a_leading_decimal_point_is_rejected_because_the_whole_part_is_empty() {
        let message = super::parse_dec(".5", "quantity")
            .expect_err("`.5` is not a money literal")
            .to_string();
        assert_eq!(
            message,
            shape_error("quantity", ".5", "expected one or more digits before the decimal point")
        );
    }

    // ---- parse_dec: the scientific-notation fallback is never reached ----
    //
    // `FromStr` tries the plain radix-10 path first and, on failure, retries
    // anything containing an `e`/`E` through `Decimal::from_scientific_lossy`.
    // The grammar check runs first, so that fallback is dead code here.

    #[test]
    fn scientific_notation_is_rejected_rather_than_reaching_the_lossy_fallback() {
        for input in ["1e3", "1E3", "1.5e3", "-1e3"] {
            let rejected = super::parse_dec(input, "quantity");
            let message = rejected.expect_err("an exponent is not accepted").to_string();
            assert_eq!(message, shape_error("quantity", input, "exponents are not accepted"));
        }
    }

    #[test]
    fn an_explicitly_positive_exponent_is_rejected_so_a_typed_plus_cannot_scale_an_order() {
        // `rust_decimal`'s fallback parses the exponent with
        // `str::parse::<u32>()`, which accepts a leading `+`, so `1e+3` would
        // send 1000. The grammar check runs before the parse, so it never does.
        let message = super::parse_dec("1e+3", "quantity")
            .expect_err("an exponent is not accepted")
            .to_string();
        assert_eq!(message, shape_error("quantity", "1e+3", "exponents are not accepted"));
    }

    #[test]
    fn a_negative_exponent_is_rejected_too_so_no_scale_is_ever_implicit() {
        let message = super::parse_dec("1e-3", "quantity")
            .expect_err("an exponent is not accepted")
            .to_string();
        assert_eq!(message, shape_error("quantity", "1e-3", "exponents are not accepted"));
    }

    // ---- parse_dec: underscore separators are refused ----

    #[test]
    fn a_thousands_separated_integer_is_rejected_before_it_can_send_1000() {
        // `rust_decimal` follows Rust literal syntax and treats `_` as a digit
        // separator, so `--quantity 1_000` would send 1000 rather than failing.
        for input in ["1_000", "1_000.5", "1_0_0_0"] {
            let message = super::parse_dec(input, "quantity")
                .expect_err("a digit separator is not accepted")
                .to_string();
            assert_eq!(
                message,
                shape_error("quantity", input, "digit separators are not accepted")
            );
        }
    }

    #[test]
    fn an_empty_run_of_separators_is_rejected_too() {
        // `rust_decimal`'s separator arm only requires that a digit came first, so
        // a trailing separator is as legal to it as an interior one.
        for input in ["1_", "1._5", "_1"] {
            let message = super::parse_dec(input, "quantity")
                .expect_err("a digit separator is not accepted")
                .to_string();
            assert_eq!(
                message,
                shape_error("quantity", input, "digit separators are not accepted")
            );
        }
    }

    // ---- parse_dec: rejected forms ----

    #[test]
    fn an_empty_string_is_rejected() {
        assert!(super::parse_dec("", "quantity").is_err());
    }

    #[test]
    fn surrounding_whitespace_is_rejected_because_the_input_is_never_trimmed() {
        // A quoted `--quantity " 1 "` is a hard error: the space is not a legal
        // digit and no trim runs first.
        assert!(super::parse_dec(" 1 ", "quantity").is_err());
        assert!(super::parse_dec("\t1", "quantity").is_err());
        assert!(super::parse_dec("1\n", "quantity").is_err());
    }

    #[test]
    fn nan_is_rejected() {
        // `Decimal` has no NaN representation, so no spelling of the token is in
        // the grammar.
        assert!(super::parse_dec("NaN", "quantity").is_err());
        assert!(super::parse_dec("nan", "quantity").is_err());
    }

    #[test]
    fn infinity_is_rejected() {
        // Same story as NaN: no special case anywhere, so no spelling is in the
        // grammar.
        assert!(super::parse_dec("inf", "quantity").is_err());
        assert!(super::parse_dec("Infinity", "quantity").is_err());
        assert!(super::parse_dec("-inf", "quantity").is_err());
    }

    #[test]
    fn a_bare_sign_with_no_digits_is_rejected() {
        assert!(super::parse_dec("-", "quantity").is_err());
        assert!(super::parse_dec("+", "quantity").is_err());
        assert!(super::parse_dec(".", "quantity").is_err());
    }

    #[test]
    fn two_decimal_points_are_rejected() {
        for input in ["1.2.3", "0.1."] {
            let message = super::parse_dec(input, "quantity")
                .expect_err("a second decimal point is not accepted")
                .to_string();
            assert_eq!(
                message,
                shape_error(
                    "quantity",
                    input,
                    "expected one or more digits after the decimal point"
                )
            );
        }
    }

    #[test]
    fn a_comma_decimal_separator_is_rejected() {
        assert!(super::parse_dec("1,5", "quantity").is_err());
    }

    #[test]
    fn a_currency_prefix_or_suffix_is_rejected() {
        for input in ["$1", "1$", "BTC", "1btc", "0x10"] {
            assert!(super::parse_dec(input, "quantity").is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn an_exponent_marker_anywhere_is_rejected() {
        for input in ["1e", "e3", "1e3e3", "1E", "E3"] {
            assert!(super::parse_dec(input, "quantity").is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn twenty_eight_fractional_digits_is_the_largest_exactly_representable_input() {
        // 28 is `rust_decimal`'s maximum scale, so this is the smallest positive
        // value the CLI can accept at all. This is the boundary the contract
        // grammar enforces on the wire payload on the other side.
        let smallest = "0.0000000000000000000000000001";
        assert_eq!(
            super::parse_dec(smallest, "quantity").expect("the maximum scale is representable"),
            Decimal::new(1, 28)
        );
    }

    // ---- parse_dec: the error message ----

    #[test]
    fn a_rejection_names_the_field_it_came_from() {
        // `OVERFLOWED` is inside the grammar, so this is the parser's own
        // rejection and the message carries no grammar clause.
        let message = super::parse_dec(OVERFLOWED, "quantity")
            .expect_err("31 digits overflow a 96-bit mantissa")
            .to_string();
        assert_eq!(message, error_prefix("quantity", OVERFLOWED));
    }

    #[test]
    fn a_rejection_echoes_the_input_with_debug_quoting() {
        let message =
            super::parse_dec(" 1 ", "price").expect_err("a space is not a decimal").to_string();
        assert_eq!(
            message,
            shape_error("price", " 1 ", "expected one or more digits before the decimal point")
        );
    }

    #[test]
    fn every_field_name_survives_into_the_message_verbatim() {
        // The underscore in `take_profit`/`stop_loss` is what makes this worth
        // pinning: a `Debug` of the value would print `take_profit` but a
        // hand-written field list would have to match the flag names exactly.
        for field in FIELDS {
            let report =
                super::parse_dec(OVERFLOWED, field).expect_err("31 digits overflow the mantissa");
            assert_eq!(report.to_string(), error_prefix(field, OVERFLOWED), "{field}");
        }
    }

    #[test]
    fn a_parser_rejection_keeps_the_rust_decimal_reason_as_the_reports_cause() {
        // `parse_dec` wraps the parse error rather than replacing it, so the
        // display stays the CLI's own one-line diagnostic while the cause chain
        // still carries "Invalid decimal: overflow from too many digits" — which
        // is what `color-eyre`'s handler prints when the CLI exits non-zero.
        let report =
            super::parse_dec(OVERFLOWED, "quantity").expect_err("31 digits overflow the mantissa");
        assert_eq!(report.to_string(), error_prefix("quantity", OVERFLOWED));

        // Exactly one cause, and it is the parser's own reason.
        let mut causes = String::new();
        for cause in report.chain().skip(1) {
            causes.push_str(&cause.to_string());
        }
        assert_eq!(causes, "Invalid decimal: overflow from too many digits");
    }

    #[test]
    fn the_error_message_never_names_the_parser_crate() {
        // The cause is preserved, but the CLI's own line stays free of
        // implementation detail: an operator is told which flag and which input,
        // not which crate rejected it.
        for input in ["abc", OVERFLOWED, "1_000", "1e3", " 1 "] {
            let message = super::parse_dec(input, "quantity")
                .expect_err("none of these is an accepted money literal")
                .to_string();
            assert!(!message.contains("rust_decimal"), "{input:?}: {message}");
        }
    }

    // ---- which field gets named, and in what order ----

    #[tokio::test]
    async fn a_malformed_quantity_is_reported_before_any_bracket_is_parsed() {
        // The quantity is parsed first, so its diagnostic wins even when every
        // bracket is also garbage.
        let Err(message) = preflight("abc", Some("xyz"), Some("!"), Some("?")).await else {
            unreachable!("`abc` is never a valid quantity");
        };
        assert_eq!(
            message,
            shape_error("quantity", "abc", "expected one or more digits before the decimal point")
        );
    }

    #[tokio::test]
    async fn the_bracket_fields_are_parsed_in_declaration_order() {
        let Err(price) = preflight("1", Some("x"), Some("y"), Some("z")).await else {
            unreachable!("`x` is never a valid price");
        };
        assert_eq!(
            price,
            shape_error("price", "x", "expected one or more digits before the decimal point")
        );

        let Err(take_profit) = preflight("1", Some("1"), Some("y"), Some("z")).await else {
            unreachable!("`y` is never a valid take_profit");
        };
        assert_eq!(
            take_profit,
            shape_error("take_profit", "y", "expected one or more digits before the decimal point")
        );

        let Err(stop_loss) = preflight("1", Some("1"), Some("1"), Some("z")).await else {
            unreachable!("`z` is never a valid stop_loss");
        };
        assert_eq!(
            stop_loss,
            shape_error("stop_loss", "z", "expected one or more digits before the decimal point")
        );
    }

    #[tokio::test]
    async fn a_parseable_order_reaches_the_transport_so_the_parse_is_not_the_gate() {
        // The complement of the two tests above: when every field parses, the call
        // gets past the parse and fails on the dead endpoint instead.
        assert!(preflight("1", Some("1"), Some("2"), Some("3")).await.is_err());
    }

    // ---- the captured request: bracket presence ----

    #[tokio::test]
    async fn an_absent_price_stays_absent_rather_than_encoding_a_zero() {
        // A zero price on a market order is not a harmless default: it is a limit
        // order at nothing as far as any consumer of the field can tell.
        let order = captured_order("1", None, None, None).await;
        assert!(order.price.is_unset(), "an absent price must stay absent");
        assert!(order.price.as_option().is_none());
    }

    #[tokio::test]
    async fn absent_brackets_stay_absent_rather_than_encoding_zeros() {
        let order = captured_order("1", None, None, None).await;
        assert!(order.take_profit.is_unset());
        assert!(order.stop_loss.is_unset());
    }

    #[tokio::test]
    async fn a_supplied_price_is_present_and_encodes_the_typed_decimal() {
        let order = captured_order("1", Some("1500.25"), None, None).await;
        let price = order.price.as_option().expect("a supplied price must be present");
        assert_eq!(price.value, "1500.25");
    }

    #[tokio::test]
    async fn both_brackets_can_be_armed_independently() {
        let only_take_profit = captured_order("1", None, Some("1600"), None).await;
        assert!(only_take_profit.take_profit.is_set());
        assert!(only_take_profit.stop_loss.is_unset());

        let only_stop_loss = captured_order("1", None, None, Some("1400")).await;
        assert!(only_stop_loss.take_profit.is_unset());
        assert!(only_stop_loss.stop_loss.is_set());
    }

    #[tokio::test]
    async fn a_negative_bracket_price_keeps_its_sign() {
        let order = captured_order("1", None, Some("-2"), None).await;
        let take_profit = order.take_profit.as_option().expect("a supplied bracket is present");
        assert_eq!(take_profit.value, "-2");
    }

    /// `79228162514264337593543950335` is `Decimal::MAX`, so a bracket at the top
    /// of the range used to need the string representation because the numeric
    /// pair could not hold it. The payload is that text now, and it reaches the
    /// wire and comes back as the very same decimal.
    #[tokio::test]
    async fn an_arm_bracket_at_the_widest_representable_value_survives_unchanged() {
        let order = captured_order("1", None, Some("79228162514264337593543950335"), None).await;
        let take_profit = order.take_profit.as_option().expect("a supplied bracket is present");
        assert_eq!(take_profit.value, "79228162514264337593543950335");
        assert_eq!(
            common_to_decimal(take_profit).expect("the CLI writes only decodable payloads"),
            Decimal::MAX
        );
    }

    // ---- the captured request: Limit versus Market ----

    #[tokio::test]
    async fn an_order_without_a_price_is_a_market_order() {
        let order = captured_order("1", None, None, None).await;
        assert_eq!(order.r#type, buffa::EnumValue::Known(utrading::OrderType::Market));
    }

    #[tokio::test]
    async fn an_order_with_a_price_is_a_limit_order() {
        let order = captured_order("1", Some("1500.25"), None, None).await;
        assert_eq!(order.r#type, buffa::EnumValue::Known(utrading::OrderType::Limit));
    }

    #[tokio::test]
    async fn a_bracket_alone_never_promotes_a_market_order_to_a_limit() {
        // Only `price` is consulted, so arming an exit cannot silently change the
        // order's execution semantics.
        let order = captured_order("1", None, Some("1600"), Some("1400")).await;
        assert_eq!(order.r#type, buffa::EnumValue::Known(utrading::OrderType::Market));
        assert!(order.take_profit.is_set());
        assert!(order.stop_loss.is_set());
    }

    // ---- the captured request: the non-positive risk check ----
    //
    // `place_order` turns any supplied `--price` into a limit order, so a zero
    // or negative price is a live order no venue can fill. Both the price and
    // the quantity are refused before the request is built.

    #[tokio::test]
    async fn a_zero_price_is_refused_before_a_limit_order_can_reach_a_venue() {
        for price in ["0", "0.0", "0.000"] {
            let Err(message) = preflight("1", Some(price), None, None).await else {
                unreachable!("a zero price must not be sent as a limit order");
            };
            assert_eq!(message, format!("price must be greater than zero, got {price:?}"));
        }
    }

    #[tokio::test]
    async fn a_negative_price_is_refused_for_the_same_reason() {
        for price in ["-1", "-0.5", "-1500"] {
            let Err(message) = preflight("1", Some(price), None, None).await else {
                unreachable!("a negative limit price must not be sent");
            };
            assert_eq!(message, format!("price must be greater than zero, got {price:?}"));
        }
    }

    #[tokio::test]
    async fn a_non_positive_quantity_is_refused_before_the_request_is_built() {
        for quantity in ["0", "0.000", "-1", "-0.5"] {
            let Err(message) = preflight(quantity, Some("1500"), None, None).await else {
                unreachable!("a zero or negative quantity is not an order");
            };
            assert_eq!(message, format!("quantity must be greater than zero, got {quantity:?}"));
        }
    }

    #[tokio::test]
    async fn the_quantity_risk_check_wins_over_a_later_bad_price() {
        // The quantity is parsed first, so `--quantity 0 --price abc` reports the
        // quantity rather than the unparsable price.
        let Err(message) = preflight("0", Some("abc"), None, None).await else {
            unreachable!("a zero quantity must not be sent");
        };
        assert_eq!(message, "quantity must be greater than zero, got \"0\"");
    }

    #[tokio::test]
    async fn a_non_positive_exit_bracket_is_still_accepted_because_brackets_keep_their_sign() {
        // Only the quantity and the limit price are risk-checked: `take_profit` on
        // a short sits below the entry, and `stop_loss` on a long does too. Either
        // outcome of the call proves it — a refusal would name a flag.
        let Err(message) = preflight("1", None, Some("-2"), Some("-1")).await else {
            unreachable!("the request must reach the dead transport");
        };
        assert!(!message.contains("must be greater than zero"), "{message}");
    }

    #[tokio::test]
    async fn the_smallest_accepted_price_still_survives_the_risk_check() {
        // 28 fractional digits is exactly representable and strictly positive, so
        // the risk check must not shadow the scale boundary.
        let typed = "0.0000000000000000000000000001";
        let order = captured_order("1", Some(typed), None, None).await;
        assert_eq!(order.r#type, buffa::EnumValue::Known(utrading::OrderType::Limit));
        let price = order.price.as_option().expect("an accepted price is present");
        assert_eq!(price.value, typed);
    }

    // ---- the captured request: the rest of the envelope ----

    #[tokio::test]
    async fn the_side_and_symbol_are_copied_onto_the_wire_unchanged() {
        let order = captured_order("2.5", Some("1500"), None, None).await;
        let amount = order.amount.as_option().expect("the amount is always set");
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(order.side, buffa::EnumValue::Known(utrading::OrderSide::Buy));
        assert_eq!(amount.value, "2.5");
    }

    #[tokio::test]
    async fn the_client_order_id_carries_the_cli_namespace_prefix() {
        let order = captured_order("1", None, None, None).await;
        assert!(
            order.client_order_id.starts_with("cli-"),
            "the venue scopes ids by prefix, got {:?}",
            order.client_order_id
        );
        assert_eq!(order.client_order_id.len(), "cli-".len() + 26, "a ULID is 26 base32 chars");
    }

    #[tokio::test]
    async fn two_orders_get_two_distinct_client_order_ids() {
        // Distinctness now comes from the shared monotonic `ulid::Generator`, so
        // this is a guarantee rather than a 2^-80 bet on the thread RNG. The two
        // orders cannot be compared for *order*, because the test harness runs
        // other `place_order` tests on other threads against the same generator;
        // the sequencing itself is pinned by the `client_order_id` test below.
        let first = captured_order("1", None, None, None).await;
        let second = captured_order("1", None, None, None).await;
        assert_ne!(first.client_order_id, second.client_order_id);
    }

    // ---- client_order_id: the monotonic venue idempotency key ----

    #[test]
    fn successive_client_order_ids_are_strictly_increasing() {
        // `client_order_id` mints from one process-wide generator, so two calls
        // in a row on one thread are ordered. ULID's 26-character base32 form
        // sorts the same as its 128-bit value, so plain string ordering is the
        // property a log or a retry cares about.
        let ids: Vec<String> = (0..16).map(|_| super::client_order_id()).collect();
        for pair in ids.windows(2) {
            assert!(pair[0] < pair[1], "{} is not below {}", pair[0], pair[1]);
        }
    }

    #[test]
    fn every_client_order_id_is_prefixed_and_therefore_uniform_in_length() {
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let id = super::client_order_id();
            assert!(id.starts_with("cli-"), "{id}");
            assert_eq!(id.len(), "cli-".len() + 26, "{id}");
            assert!(seen.insert(id.clone()), "{id} was minted twice");
        }
    }

    #[tokio::test]
    async fn ids_minted_from_concurrent_tasks_are_distinct() {
        // The generator is process-wide and shared across runtime threads, so the
        // uniqueness guarantee has to survive contention. The mutex is taken and
        // released inside `client_order_id`, so no worker thread is parked on it
        // across an `.await` and the tasks really do run in parallel.
        let mut handles = Vec::new();
        for _ in 0..8 {
            handles.push(tokio::task::spawn_blocking(super::client_order_id));
        }
        let mut ids = Vec::with_capacity(handles.len());
        for handle in handles {
            ids.push(handle.await.expect("a blocking task never panics"));
        }
        let mut unique = ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), ids.len(), "two tasks minted the same id: {ids:?}");
    }

    #[tokio::test]
    async fn the_exchange_selector_is_carried_onto_the_wire() {
        let listener =
            TcpListener::bind(LOOPBACK).await.expect("a loopback port is always bindable");
        let addr = listener.local_addr().expect("a bound listener always has an address");
        let capture = tokio::spawn(capture_one_body(listener));

        let client = TerminalClient::new(&format!("http://{addr}"));
        let output = Renderer::new(OutputFormat::Table);
        let exchange = crate::commands::exchange_id("binance");
        let call = super::place_order(
            &client,
            &exchange,
            "ETHUSDT",
            utrading::OrderSide::Sell,
            "1",
            None,
            None,
            None,
            &output,
        );
        let _ = tokio::time::timeout(RUN_DEADLINE, call).await;

        let body = capture
            .await
            .expect("the capture task never panics")
            .expect("a parseable order always sends its request");
        let request = utrading::CreateOrderRequest::decode_from_slice(&body)
            .expect("the CLI encodes a decodable CreateOrderRequest");
        assert_eq!(
            request.exchange_id.as_option().expect("the selector is always set").id,
            "binance"
        );
        let order = request.order.into_option().expect("the order is always set");
        assert_eq!(order.symbol, "ETHUSDT");
        assert_eq!(order.side, buffa::EnumValue::Known(utrading::OrderSide::Sell));
    }

    // ---- proptest: the parse contract ----

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The error contract holds for every input: a rejection opens with the
        /// field it came from and the input with `Debug` quoting, so a typo is
        /// recoverable from the message alone. The two rejection shapes (a
        /// grammar violation and a parser rejection) differ only after that
        /// prefix.
        #[test]
        fn a_rejection_always_names_the_field_and_echoes_the_input(
            input in ".{0,10}",
            field in proptest::sample::select(FIELDS.to_vec()),
        ) {
            if let Err(report) = super::parse_dec(&input, field) {
                prop_assert!(
                    report.to_string().starts_with(&error_prefix(field, &input)),
                    "{}",
                    report
                );
            }
        }

        /// The grammar is a property of the bytes alone, never of the field: the
        /// same input is accepted or refused for every money flag.
        #[test]
        fn the_accepted_grammar_does_not_depend_on_the_field(
            input in ".{0,10}",
        ) {
            let outcomes: Vec<bool> =
                FIELDS.iter().map(|field| super::parse_dec(&input, field).is_ok()).collect();
            prop_assert!(
                outcomes.windows(2).all(|pair| pair[0] == pair[1]),
                "input={:?} outcomes={:?}",
                input,
                outcomes
            );
        }

        /// Every literal the documented grammar admits really is accepted: an
        /// optional sign, at least one whole digit, and an optional point with at
        /// least one more digit. This is the constructive half of the contract;
        /// the examples and `no_digit_separator_or_exponent_ever_reaches_the_parser`
        /// cover the refusals.
        #[test]
        fn the_grammar_is_exactly_the_documented_shape(
            sign in proptest::sample::select(&["", "+", "-"][..]),
            whole in "[0-9]{1,9}",
            dot in proptest::sample::select(&["", "."][..]),
            frac in "[0-9]{1,9}",
        ) {
            let literal = format!("{sign}{whole}{dot}{frac}");
            prop_assert!(
                super::parse_dec(&literal, "quantity").is_ok(),
                "literal={:?} was refused",
                literal
            );
        }

        /// Whatever `parse_dec` accepts, the contract encoder hands the very same
        /// value back on decode, so a quantity can never be altered between the
        /// shell and the wire. The fraction is at least one digit because `1.` is
        /// deliberately outside the grammar.
        #[test]
        fn an_accepted_decimal_survives_the_contract_round_trip(
            whole in "[0-9]{1,9}",
            frac in "[0-9]{1,9}",
        ) {
            let input = format!("{whole}.{frac}");
            let parsed = super::parse_dec(&input, "quantity")
                .expect("a plain decimal literal always parses");
            let wire = decimal_to_common(parsed);
            prop_assert_eq!(
                common_to_decimal(&wire).expect("the encoder always emits a decodable decimal"),
                parsed,
                "input={:?}",
                input
            );
        }

        /// The grammar check is the only thing standing between a Rust-literal
        /// spelling and the wire, so it must refuse every input `rust_decimal`
        /// would reinterpret: digit separators and any exponent marker. A sign is
        /// in the noise too, because it is only legal as the very first byte.
        #[test]
        fn no_digit_separator_or_exponent_ever_reaches_the_parser(
            noise in "[-_eE+]{1,4}",
        ) {
            let literal = format!("1{noise}000");
            let refused = super::parse_dec(&literal, "quantity");
            prop_assert!(
                refused.is_err(),
                "literal={:?} was accepted as {:?}",
                literal,
                refused.map(|value| value.to_string())
            );
        }

        /// Whatever the encoder writes is the base-10 text of the very same
        /// decimal, at every scale a decimal can hold: the CLI never has to
        /// choose between two representations, so there is nothing on the wire
        /// to disagree with what the operator typed.
        #[test]
        fn an_encoded_decimal_carries_its_own_value(
            mantissa in proptest::num::i64::ANY,
            scale in 0u32..=28,
        ) {
            let value = Decimal::new(mantissa, scale);
            let wire = decimal_to_common(value);
            // Borrow, do not move: `common_to_decimal` needs `&wire` below.
            prop_assert_eq!(&wire.value, &value.to_string());
            prop_assert_eq!(
                common_to_decimal(&wire).expect("the encoder writes only decodable payloads"),
                value,
                "mantissa={} scale={}",
                mantissa,
                scale
            );
        }
    }
}
