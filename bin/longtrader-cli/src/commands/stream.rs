use color_eyre::Result;
use futures_util::StreamExt;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, stream::v1 as stream},
};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    topics: &[String],
    output: &Renderer,
) -> Result<()> {
    let topic_classes: Vec<stream::TopicClass> = if topics.is_empty() {
        vec![
            stream::TopicClass::MarketLite,
            stream::TopicClass::MarketHeavy,
            stream::TopicClass::Trading,
        ]
    } else {
        let mut classes = Vec::new();
        let mut unknown = Vec::new();
        for t in topics {
            match t.to_uppercase().as_str() {
                "MARKET_LITE" => classes.push(stream::TopicClass::MarketLite),
                "MARKET_HEAVY" => classes.push(stream::TopicClass::MarketHeavy),
                "TRADING" => classes.push(stream::TopicClass::Trading),
                "RUNTIME" => classes.push(stream::TopicClass::Runtime),
                "FUNDING" => classes.push(stream::TopicClass::Funding),
                "DERIVATIVES" => classes.push(stream::TopicClass::Derivatives),
                // The unmatched arm echoes the *raw* token, not the folded
                // scrutinee the table was matched against: `--topics nope` must
                // say `nope`. Same contract as the timeframe parse in
                // `candles.rs`.
                _ => unknown.push(t.clone()),
            }
        }
        if !unknown.is_empty() {
            return Err(color_eyre::Report::msg(format!(
                "Unknown topic class(es): {}",
                unknown.join(", ")
            )));
        }
        classes
    };

    let mut stream = client.stream_updates(exchange, &[], &topic_classes).await?;
    output.render_msg("Streaming updates (Ctrl+C to stop)...");
    loop {
        tokio::select! {
            maybe = stream.next() => {
                match maybe {
                    Some(Ok(env)) => {
                        let payload_desc = match &env.payload {
                            Some(p) => describe_payload(p),
                            None => "empty".to_string(),
                        };
                        output.render_msg(&format!(
                            "[{}] seq={} {}",
                            topic_name(env.topic_class),
                            env.seq,
                            payload_desc
                        ));
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                output.render_msg("Stopped by user (Ctrl+C)");
                break;
            }
        }
    }
    Ok(())
}

fn topic_name(topic: buffa::EnumValue<stream::TopicClass>) -> &'static str {
    match topic {
        buffa::EnumValue::Known(stream::TopicClass::MarketLite) => "MARKET_LITE",
        buffa::EnumValue::Known(stream::TopicClass::MarketHeavy) => "MARKET_HEAVY",
        buffa::EnumValue::Known(stream::TopicClass::Trading) => "TRADING",
        buffa::EnumValue::Known(stream::TopicClass::Runtime) => "RUNTIME",
        buffa::EnumValue::Known(stream::TopicClass::Funding) => "FUNDING",
        buffa::EnumValue::Known(stream::TopicClass::Derivatives) => "DERIVATIVES",
        _ => "UNKNOWN",
    }
}

/// One-line description of an `UpdateEnvelope.payload`.
///
/// Every variant of the fourteen-arm oneof has its own arm, so a new contract
/// payload cannot silently start printing `other`: adding a variant is a compile
/// error until it is described. Each arm leads with the variant name so a stream
/// line stays greppable, then the identifying field (a symbol, an order or
/// position id) plus whatever else the bucket is read for.
fn describe_payload(payload: &stream::update_envelope::Payload) -> String {
    use stream::update_envelope::Payload;

    use crate::output::fmt_dec;
    match payload {
        Payload::Tick(t) => format!("Tick {} @ {}", t.symbol, fmt_dec(&t.last)),
        Payload::Book(b) => {
            format!("Book {} ({} bids, {} asks)", b.symbol, b.bids.len(), b.asks.len())
        }
        Payload::Trade(t) => {
            format!("Trade {} {} @ {}", t.symbol, fmt_dec(&t.amount), fmt_dec(&t.price))
        }
        // The derivatives bucket is opt-in, so its payloads are the ones a
        // subscriber most wants to read; a liquidation carries the size and the
        // price it printed at, exactly like a trade.
        Payload::Liquidation(l) => {
            format!("Liquidation {} {} @ {}", l.symbol, fmt_dec(&l.amount), fmt_dec(&l.price))
        }
        // `seq` is on the line because a gap in it is a data discontinuity the
        // consumer has to resync for.
        Payload::BookDelta(d) => format!(
            "BookDelta {} seq={} ({} bids, {} asks)",
            d.symbol,
            d.seq,
            d.bids.len(),
            d.asks.len()
        ),
        Payload::MyOrder(o) => format!(
            "MyOrder {} {} queue={}/{}",
            o.order_id, o.symbol, o.queue_position, o.total_in_queue
        ),
        Payload::Position(p) => format!("Position {} {}", p.id, p.symbol),
        Payload::PositionClosed(p) => {
            format!("PositionClosed {} {} pnl={}", p.id, p.symbol, fmt_dec(&p.realized_pnl))
        }
        Payload::Order(o) => format!("Order {} {}", o.id, o.symbol),
        Payload::Account(a) => format!("Account balance={}", fmt_dec(&a.balance)),
        Payload::Ticker(t) => format!("Ticker {} last={}", t.symbol, fmt_dec(&t.last)),
        Payload::FundingRate(f) => {
            format!("FundingRate {} rate={}", f.symbol, fmt_dec(&f.rate))
        }
        Payload::Execution(e) => format!("Execution {} {}", e.order_id, e.symbol),
        Payload::RuntimeStatus(r) => format!("RuntimeStatus connected={}", r.connected),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use buffa::Message;
    use longtrader_contract::ext::decimal_to_common;
    use longtrader_proto::proto::longtrader::{common::v1 as common, market::v1 as umarket};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::output::OutputFormat;

    /// Loopback address the capture listener binds. Port 0 lets the OS pick a
    /// free one, so concurrent tests cannot collide.
    const LOOPBACK: &str = "127.0.0.1:0";

    /// How long the listener waits for a connection. A pure safety net: the
    /// rejected branch aborts the task instead of waiting for this to expire.
    const LISTEN_DEADLINE: Duration = Duration::from_secs(2);

    /// How long one socket read may block before the capture gives up.
    const READ_DEADLINE: Duration = Duration::from_secs(2);

    /// Ceiling on a `run` that got past the parse. The capture hangs up, so a
    /// healthy run always answers long before this.
    const RUN_DEADLINE: Duration = Duration::from_secs(5);

    /// The exact prefix `run` uses to report unrecognised topic tokens.
    const REJECTED_PREFIX: &str = "Unknown topic class(es): ";

    /// Every topic token `run` accepts, with the bucket it maps to.
    const ACCEPTED: [(&str, stream::TopicClass); 6] = [
        ("MARKET_LITE", stream::TopicClass::MarketLite),
        ("MARKET_HEAVY", stream::TopicClass::MarketHeavy),
        ("TRADING", stream::TopicClass::Trading),
        ("RUNTIME", stream::TopicClass::Runtime),
        ("FUNDING", stream::TopicClass::Funding),
        ("DERIVATIVES", stream::TopicClass::Derivatives),
    ];

    /// The three buckets `run` subscribes to when the caller names none.
    const DEFAULTS: [stream::TopicClass; 3] = [
        stream::TopicClass::MarketLite,
        stream::TopicClass::MarketHeavy,
        stream::TopicClass::Trading,
    ];

    /// The buckets the default set deliberately leaves out.
    const OPT_IN_ONLY: [stream::TopicClass; 3] =
        [stream::TopicClass::Runtime, stream::TopicClass::Funding, stream::TopicClass::Derivatives];

    /// Every bucket `topic_name` names, with the label it must produce.
    const NAMED: [(stream::TopicClass, &str); 6] = [
        (stream::TopicClass::MarketLite, "MARKET_LITE"),
        (stream::TopicClass::MarketHeavy, "MARKET_HEAVY"),
        (stream::TopicClass::Trading, "TRADING"),
        (stream::TopicClass::Runtime, "RUNTIME"),
        (stream::TopicClass::Funding, "FUNDING"),
        (stream::TopicClass::Derivatives, "DERIVATIVES"),
    ];

    /// A wire decimal for `mantissa * 10^-scale`, spelled the way every other
    /// fixture in this file spells it. Going through the encoder keeps the
    /// fixtures identical to what a real writer emits.
    fn dec(mantissa: i64, scale: i32) -> common::Decimal {
        let scale = u32::try_from(scale).expect("these fixtures never use a negative scale");
        longtrader_contract::ext::decimal_to_common(rust_decimal::Decimal::new(mantissa, scale))
    }

    /// The message `run` produces for the given joined offending tokens.
    fn unknown(joined: &str) -> String {
        format!("{REJECTED_PREFIX}{joined}")
    }

    /// The `Verdict` a token list made only of offenders produces.
    fn rejected(joined: &str) -> Verdict {
        Verdict::Rejected(format!("{REJECTED_PREFIX}{joined}"))
    }

    // ---- loopback capture ----
    //
    // `run`'s topic table and its default bucket set are the only things it
    // decides before it touches the client, and the buckets themselves are only
    // visible in the `StreamUpdatesRequest` it puts on the wire. So the tests
    // bind a loopback listener, capture that one request, and hang up.

    /// What `run` decided about the topic strings.
    #[derive(Debug, PartialEq, Eq)]
    enum Verdict {
        /// `run` refused the tokens, carrying its message verbatim.
        Rejected(String),
        /// `run` asked the terminal for exactly these buckets.
        Subscribed(Vec<stream::TopicClass>),
    }

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

    /// Accept one request and hand back its body. `None` means nothing ever
    /// connected, which is how a topic set `run` rejected before the client
    /// call looks from the listener's side.
    async fn capture_one_body(listener: TcpListener) -> Option<Vec<u8>> {
        match tokio::time::timeout(LISTEN_DEADLINE, listener.accept()).await {
            Ok(Ok((mut socket, _))) => Some(read_one_body(&mut socket).await),
            _ => None,
        }
    }

    /// The bucket list inside a captured request body.
    ///
    /// `stream_updates` prefixes the protobuf payload with the Connect envelope
    /// (one flag byte plus a big-endian `u32` length).
    fn subscribed_topics(body: Vec<u8>) -> Vec<stream::TopicClass> {
        let payload = body.get(5..).expect("the Connect envelope is five bytes wide");
        let request = stream::StreamUpdatesRequest::decode_from_slice(payload)
            .expect("the CLI encodes a decodable StreamUpdatesRequest");
        request
            .topics
            .into_iter()
            .map(|topic| match topic {
                buffa::EnumValue::Known(known) => known,
                // `stream_updates` wraps every bucket it is handed in `Known`,
                // so an unknown wire value here would mean the client changed.
                buffa::EnumValue::Unknown(wire) => {
                    unreachable!("stream_updates only sends Known buckets, saw {wire}")
                }
            })
            .collect()
    }

    /// Drive `run` with `topics` against a listener that captures the single
    /// `StreamUpdatesRequest`, and report what the topic strings decided.
    async fn verdict(topics: &[&str]) -> Verdict {
        let listener =
            TcpListener::bind(LOOPBACK).await.expect("a loopback port is always bindable");
        let addr = listener.local_addr().expect("a bound listener always has an address");
        let capture = tokio::spawn(capture_one_body(listener));

        let owned: Vec<String> = topics.iter().map(|topic| (*topic).to_string()).collect();
        let client = TerminalClient::new(&format!("http://{addr}"));
        let output = Renderer::new(OutputFormat::Table);
        let exchange = common::ExchangeId::default();
        let observed =
            tokio::time::timeout(RUN_DEADLINE, super::run(&client, &exchange, &owned, &output))
                .await;

        let message = match observed {
            Ok(Err(report)) => Some(report.to_string()),
            // A timeout and a clean exit both mean the parse let the call through.
            Ok(Ok(())) | Err(_) => None,
        };
        match message {
            Some(message) if message.starts_with(REJECTED_PREFIX) => {
                // The parse refused before any socket was opened, so the
                // listener will never see a connection: drop it rather than
                // waiting out `LISTEN_DEADLINE`.
                capture.abort();
                Verdict::Rejected(message)
            }
            _ => {
                let body = capture
                    .await
                    .expect("the capture task never panics")
                    .expect("an accepted topic list always sends its request");
                Verdict::Subscribed(subscribed_topics(body))
            }
        }
    }

    /// Block on a future from inside a synchronous `proptest!` body.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime is always constructible")
            .block_on(future)
    }

    // ---- the topic table ----

    #[tokio::test]
    async fn every_accepted_topic_token_maps_to_its_own_bucket() {
        for (token, class) in ACCEPTED {
            assert_eq!(verdict(&[token]).await, Verdict::Subscribed(vec![class]), "{token}");
        }
    }

    #[tokio::test]
    async fn a_lower_case_topic_token_is_mapped_because_the_input_is_upper_cased() {
        for (token, class) in ACCEPTED {
            let lower = token.to_lowercase();
            let expected = Verdict::Subscribed(vec![class]);
            assert_eq!(verdict(&[lower.as_str()]).await, expected, "{lower}");
        }
    }

    #[tokio::test]
    async fn a_topic_token_surrounded_by_whitespace_is_rejected_because_it_is_never_trimmed() {
        // `to_uppercase()` is applied but no `trim()` is, so quoting a token on
        // the command line breaks it.
        assert_eq!(verdict(&[" MARKET_LITE "]).await, rejected(" MARKET_LITE "));
        assert_eq!(verdict(&["\ttrading"]).await, rejected("\ttrading"));
    }

    #[tokio::test]
    async fn several_tokens_subscribe_to_several_buckets_in_input_order() {
        assert_eq!(
            verdict(&["derivatives", "funding", "runtime"]).await,
            Verdict::Subscribed(vec![
                stream::TopicClass::Derivatives,
                stream::TopicClass::Funding,
                stream::TopicClass::Runtime,
            ])
        );
    }

    #[tokio::test]
    async fn a_repeated_token_is_subscribed_twice_because_nothing_deduplicates() {
        // The backend treats `topics` as a filter list; sending the same bucket
        // twice is the CLI's business, not something `run` silently repairs.
        assert_eq!(
            verdict(&["trading", "TRADING", "Trading"]).await,
            Verdict::Subscribed(vec![stream::TopicClass::Trading; 3])
        );
    }

    // ---- the default bucket set ----

    #[tokio::test]
    async fn naming_no_topics_subscribes_to_market_lite_market_heavy_and_trading() {
        assert_eq!(verdict(&[]).await, Verdict::Subscribed(DEFAULTS.to_vec()));
    }

    #[tokio::test]
    async fn the_default_set_excludes_runtime_funding_and_derivatives() {
        // The three low-volume buckets are opt-in even though nothing in the
        // command line asks for them by omission; pinning the exclusion keeps a
        // widened default from looking like a deliberate choice later.
        let Verdict::Subscribed(buckets) = verdict(&[]).await else {
            unreachable!("an empty topic list is never rejected");
        };
        for absent in OPT_IN_ONLY {
            assert!(!buckets.contains(&absent), "{absent:?} must not be a default bucket");
        }
    }

    // ---- the rejection path ----

    #[tokio::test]
    async fn an_unrecognised_topic_token_is_rejected_before_the_client_is_touched() {
        assert_eq!(verdict(&["nope"]).await, rejected("nope"));
    }

    #[tokio::test]
    async fn the_rejection_message_echoes_the_raw_token_not_what_the_table_matched() {
        // The unmatched arm pushes the caller's own `&String`, not the folded
        // scrutinee, so a user who typed `NoPe` is shown `NoPe`. Compare the
        // timeframe parse in `candles.rs`, which follows the same contract.
        assert_eq!(verdict(&["NoPe"]).await, rejected("NoPe"));
    }

    #[tokio::test]
    async fn the_rejection_message_lists_every_offending_token_in_input_order() {
        assert_eq!(
            verdict(&["trading", "alpha", "MARKET_LITE", "  ", "beta"]).await,
            rejected("alpha,   , beta")
        );
    }

    #[tokio::test]
    async fn an_empty_topic_token_is_rejected_with_nothing_after_the_colon() {
        // `--topics ""` yields one empty element, not an empty list: the message
        // ends in the separator with no offender behind it.
        assert_eq!(verdict(&[""]).await, rejected(""));
    }

    #[tokio::test]
    async fn an_empty_string_inside_a_list_contributes_an_empty_segment_to_the_join() {
        assert_eq!(verdict(&["", "nope"]).await, rejected(", nope"));
    }

    // ---- proptest: the token table against a live capture ----

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        /// Differential check of the whole table: for an arbitrary token, `run`
        /// either subscribes to exactly the bucket this mirror predicts, or
        /// rejects it with the caller's raw token echoed. The mirror is a copy of
        /// `run`'s match, so any disagreement between the two is a failure.
        #[test]
        fn an_arbitrary_token_lands_on_the_bucket_the_table_predicts(
            token in "[A-Za-z0-9_. ,-]{0,14}",
        ) {
            let folded = token.to_uppercase();
            let expected = match folded.as_str() {
                "MARKET_LITE" => Verdict::Subscribed(vec![stream::TopicClass::MarketLite]),
                "MARKET_HEAVY" => Verdict::Subscribed(vec![stream::TopicClass::MarketHeavy]),
                "TRADING" => Verdict::Subscribed(vec![stream::TopicClass::Trading]),
                "RUNTIME" => Verdict::Subscribed(vec![stream::TopicClass::Runtime]),
                "FUNDING" => Verdict::Subscribed(vec![stream::TopicClass::Funding]),
                "DERIVATIVES" => Verdict::Subscribed(vec![stream::TopicClass::Derivatives]),
                // The fold decides *whether* the token is known; the rejection
                // echoes the raw input, exactly as `run` does.
                _ => Verdict::Rejected(unknown(&token)),
            };
            prop_assert_eq!(block_on(verdict(&[token.as_str()])), expected, "token={:?}", token);
        }

        /// A recognised token dressed up with padding can never collide with the
        /// table (which never starts with those bytes), so it must come back
        /// rejected with the raw text echoed.
        #[test]
        fn a_padded_token_is_rejected_with_the_raw_input(
            base in proptest::sample::select(vec![
                "MARKET_LITE", "MARKET_HEAVY", "TRADING", "RUNTIME", "FUNDING", "DERIVATIVES",
            ]),
            pad in "[ _-]{1,3}",
        ) {
            let token = format!("{pad}{base}{pad}");
            prop_assert_eq!(
                block_on(verdict(&[token.as_str()])),
                rejected(&token),
                "token={:?}",
                token
            );
        }
    }

    // ---- topic_name ----

    #[test]
    fn every_named_bucket_renders_as_its_proto_enum_suffix() {
        for (class, label) in NAMED {
            assert_eq!(super::topic_name(buffa::EnumValue::Known(class)), label, "{class:?}");
        }
    }

    #[test]
    fn the_unspecified_bucket_reaches_the_catch_all_because_it_has_no_name() {
        // `Unspecified` is a real variant the topic table never subscribes to, so
        // it shares the catch-all arm with a genuinely unrecognised wire value.
        assert_eq!(
            super::topic_name(buffa::EnumValue::Known(stream::TopicClass::Unspecified)),
            "UNKNOWN"
        );
    }

    #[test]
    fn an_unrecognised_wire_bucket_renders_as_unknown() {
        for wire in [0, 7, -1, i32::MAX, i32::MIN] {
            let named = super::topic_name(buffa::EnumValue::Unknown(wire));
            assert_eq!(named, "UNKNOWN", "wire {wire}");
        }
    }

    #[test]
    fn no_subscribed_bucket_ever_renders_as_the_catch_all_label() {
        // The buckets `run` can request are exactly the six named ones, so a
        // stream line can never show `UNKNOWN` for a bucket the CLI asked for.
        for class in DEFAULTS {
            assert_ne!(super::topic_name(buffa::EnumValue::Known(class)), "UNKNOWN", "{class:?}");
        }
    }

    // ---- describe_payload ----
    //
    // The `UpdateEnvelope.payload` oneof has fourteen variants and every one of
    // them has a named arm, so there is no catch-all: a newly added contract
    // payload is a compile error until it is described rather than a silent
    // `other`.

    #[test]
    fn a_tick_is_described_by_its_symbol_and_last_price() {
        let payload = stream::update_envelope::Payload::Tick(Box::new(stream::Tick {
            symbol: "BTCUSDT".to_string(),
            last: dec(101_325, 2).into(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Tick BTCUSDT @ 1013.25");
    }

    #[test]
    fn a_book_snapshot_is_described_by_its_level_counts() {
        let payload = stream::update_envelope::Payload::Book(Box::new(stream::BookSnapshot {
            symbol: "ETHUSDT".to_string(),
            bids: vec![stream::BookLevel::default(); 2],
            asks: vec![stream::BookLevel::default(); 5],
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Book ETHUSDT (2 bids, 5 asks)");
    }

    #[test]
    fn an_empty_book_snapshot_reports_zero_on_both_sides() {
        let payload = stream::update_envelope::Payload::Book(Box::new(stream::BookSnapshot {
            symbol: "ETHUSDT".to_string(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Book ETHUSDT (0 bids, 0 asks)");
    }

    #[test]
    fn a_trade_is_described_by_symbol_then_amount_then_price() {
        let payload = stream::update_envelope::Payload::Trade(Box::new(stream::Trade {
            symbol: "ETHUSDT".to_string(),
            price: dec(15_005, 1).into(),
            amount: dec(3, 0).into(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Trade ETHUSDT 3 @ 1500.5");
    }

    #[test]
    fn an_order_projection_is_described_by_its_id_then_symbol() {
        let payload = stream::update_envelope::Payload::Order(Box::new(stream::Order {
            id: "order-7".to_string(),
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Order order-7 BTCUSDT");
    }

    #[test]
    fn a_position_projection_is_described_by_its_id_then_symbol() {
        let payload = stream::update_envelope::Payload::Position(Box::new(stream::Position {
            id: "pos-3".to_string(),
            symbol: "SOLUSDT".to_string(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Position pos-3 SOLUSDT");
    }

    #[test]
    fn an_account_snapshot_is_described_by_its_balance_only() {
        // The other four balances are dropped on the floor: a stream line has
        // room for one number.
        let payload = stream::update_envelope::Payload::Account(Box::new(stream::Account {
            balance: dec(12_345, 2).into(),
            equity: dec(9_999, 2).into(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Account balance=123.45");
    }

    #[test]
    fn a_borrowed_market_ticker_is_described_by_its_symbol_and_last_price() {
        let payload = stream::update_envelope::Payload::Ticker(Box::new(umarket::Ticker {
            symbol: "SOLUSDT".to_string(),
            last: dec(99, 1).into(),
            ..Default::default()
        }));
        assert_eq!(super::describe_payload(&payload), "Ticker SOLUSDT last=9.9");
    }

    #[test]
    fn a_funding_rate_is_described_by_its_symbol_and_rate() {
        let payload =
            stream::update_envelope::Payload::FundingRate(Box::new(umarket::FundingRate {
                symbol: "BTCUSDT".to_string(),
                rate: dec(1, 4).into(),
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "FundingRate BTCUSDT rate=0.0001");
    }

    #[test]
    fn an_execution_report_is_described_by_order_id_then_symbol() {
        let payload =
            stream::update_envelope::Payload::Execution(Box::new(stream::ExecutionReport {
                order_id: "order-9".to_string(),
                symbol: "BTCUSDT".to_string(),
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "Execution order-9 BTCUSDT");
    }

    #[test]
    fn a_runtime_status_reports_the_connection_flag_only() {
        for connected in [true, false] {
            let payload =
                stream::update_envelope::Payload::RuntimeStatus(Box::new(stream::RuntimeStatus {
                    connected,
                    ..Default::default()
                }));
            assert_eq!(
                super::describe_payload(&payload),
                format!("RuntimeStatus connected={connected}")
            );
        }
    }

    #[test]
    fn a_liquidation_is_described_like_a_trade_because_that_is_what_it_prints() {
        let payload =
            stream::update_envelope::Payload::Liquidation(Box::new(stream::Liquidation {
                symbol: "BTCUSDT".to_string(),
                amount: dec(25, 0).into(),
                price: dec(955, 1).into(),
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "Liquidation BTCUSDT 25 @ 95.5");
    }

    #[test]
    fn a_book_delta_is_described_by_its_symbol_seq_and_level_counts() {
        // `seq` is on the line because a gap in it means the consumer has to
        // resync; the level counts say whether the update moved the book at all.
        let payload =
            stream::update_envelope::Payload::BookDelta(Box::new(stream::OrderbookDelta {
                symbol: "ETHUSDT".to_string(),
                seq: 41,
                bids: vec![stream::BookLevel::default(); 3],
                asks: vec![stream::BookLevel::default(); 1],
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "BookDelta ETHUSDT seq=41 (3 bids, 1 asks)");
    }

    #[test]
    fn an_own_order_projection_is_described_by_its_id_symbol_and_queue_position() {
        let payload =
            stream::update_envelope::Payload::MyOrder(Box::new(stream::MyOrderPosition {
                order_id: "order-11".to_string(),
                symbol: "BTCUSDT".to_string(),
                queue_position: 2,
                total_in_queue: 5,
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "MyOrder order-11 BTCUSDT queue=2/5");
    }

    #[test]
    fn a_closed_position_is_described_by_its_id_symbol_and_realized_pnl() {
        let payload =
            stream::update_envelope::Payload::PositionClosed(Box::new(stream::ClosedPosition {
                id: "pos-4".to_string(),
                symbol: "SOLUSDT".to_string(),
                realized_pnl: dec(-1_250, 2).into(),
                ..Default::default()
            }));
        assert_eq!(super::describe_payload(&payload), "PositionClosed pos-4 SOLUSDT pnl=-12.50");
    }

    /// An all-defaults message still has to print something parseable, so the
    /// shape is kept and the values are blank.
    ///
    /// The price used to render as `0`. It no longer does, and that is the point:
    /// under the old dual representation an unpopulated `Decimal`
    /// (`unscaled = 0, scale = 0, raw_str = ""`) read as a zero, so a message
    /// that carried no price was indistinguishable from one that carried a price
    /// of zero. A blank payload is now its own condition and the renderer says
    /// so, instead of printing a number the sender never wrote.
    #[test]
    fn an_all_default_tick_renders_an_empty_symbol_rather_than_going_missing() {
        let rendered =
            super::describe_payload(&stream::update_envelope::Payload::Tick(Box::default()));
        assert!(
            rendered.starts_with("Tick  @ "),
            "the shape must survive an all-default message, got {rendered:?}"
        );
        assert!(
            rendered.contains("undecodable"),
            "an unpopulated price must not print as a number, got {rendered:?}"
        );
    }

    /// The other half of that distinction: a price the sender really did set to
    /// zero still renders as zero.
    #[test]
    fn a_zero_price_still_renders_as_zero() {
        let tick =
            stream::Tick { last: decimal_to_common(Decimal::ZERO).into(), ..Default::default() };
        assert_eq!(
            super::describe_payload(&stream::update_envelope::Payload::Tick(Box::new(tick))),
            "Tick  @ 0"
        );
    }

    /// The two must be different on the wire, not merely different in how they
    /// render — otherwise the distinction is cosmetic.
    #[test]
    fn a_zero_price_and_an_unpopulated_one_are_different_messages() {
        let zero =
            stream::Tick { last: decimal_to_common(Decimal::ZERO).into(), ..Default::default() };
        let unpopulated = stream::Tick::default();
        assert_ne!(
            zero.encode_to_vec(),
            unpopulated.encode_to_vec(),
            "a zero price and no price at all must not encode identically"
        );
        assert_eq!(common::Decimal::default().value, "", "the default carries no value");
    }

    #[test]
    fn every_payload_variant_renders_its_own_variant_name_and_no_catch_all_exists() {
        use stream::update_envelope::Payload;
        // One representative per arm of the fourteen-variant oneof, paired with
        // the label that arm must lead with. A variant that fell through to a
        // catch-all would fail here rather than print an anonymous `other`.
        let cases: [(Payload, &str); 14] = [
            (Payload::Tick(Box::default()), "Tick"),
            (Payload::Book(Box::default()), "Book"),
            (Payload::Trade(Box::default()), "Trade"),
            (Payload::Liquidation(Box::default()), "Liquidation"),
            (Payload::BookDelta(Box::default()), "BookDelta"),
            (Payload::MyOrder(Box::default()), "MyOrder"),
            (Payload::Position(Box::default()), "Position"),
            (Payload::PositionClosed(Box::default()), "PositionClosed"),
            (Payload::Order(Box::default()), "Order"),
            (Payload::Account(Box::default()), "Account"),
            (Payload::Ticker(Box::default()), "Ticker"),
            (Payload::FundingRate(Box::default()), "FundingRate"),
            (Payload::Execution(Box::default()), "Execution"),
            (Payload::RuntimeStatus(Box::default()), "RuntimeStatus"),
        ];
        for (payload, label) in &cases {
            let described = super::describe_payload(payload);
            assert!(described.starts_with(*label), "expected {label:?} in {described:?}");
            assert_ne!(described, "other", "{payload:?} reached the catch-all label");
        }
    }

    #[test]
    fn a_liquidation_and_a_book_delta_keep_the_symbol_the_catch_all_used_to_drop() {
        use stream::update_envelope::Payload;
        let liquidation = Payload::Liquidation(Box::new(stream::Liquidation {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        }));
        let delta = Payload::BookDelta(Box::new(stream::OrderbookDelta {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        }));
        for payload in [&liquidation, &delta] {
            let described = super::describe_payload(payload);
            assert!(described.contains("BTCUSDT"), "the symbol was discarded: {described}");
        }
    }

    // ---- proptest: describe_payload cannot fail on any payload shape ----

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Every symbol the CLI can be handed survives `describe_payload`
        /// unchanged, and the result always names the variant it rendered. This
        /// is what makes a stream line greppable even when the decimals around
        /// it are nonsense.
        #[test]
        fn describe_payload_renders_any_symbol_and_names_its_variant(
            symbol in "[A-Za-z0-9:/._-]{0,20}",
        ) {
            let tick = stream::update_envelope::Payload::Tick(Box::new(stream::Tick {
                symbol: symbol.clone(),
                last: dec(1, 0).into(),
                ..Default::default()
            }));
            prop_assert_eq!(super::describe_payload(&tick), format!("Tick {symbol} @ 1"));
        }

        /// An out-of-range decimal pair renders as the empty string (the
        /// `fmt_dec` defect pinned in `output.rs`), so `describe_payload` must
        /// still produce a well-formed line rather than panicking or losing the
        /// symbol.
        #[test]
        fn describe_payload_survives_an_undecodable_decimal(
            payload in ".{0,40}",
        ) {
            let wire = common::Decimal { value: payload, ..Default::default() };
            let account = stream::update_envelope::Payload::Account(Box::new(stream::Account {
                balance: wire.into(),
                ..Default::default()
            }));
            let described = super::describe_payload(&account);
            prop_assert!(described.starts_with("Account balance="), "{}", described);
            prop_assert!(!described.contains("Decimal"), "{}", described);
        }
    }
}
