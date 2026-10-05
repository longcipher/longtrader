use color_eyre::Result;
use longtrader_proto::{client::TerminalClient, proto::longtrader::common::v1 as common};

use crate::output::{Renderer, fmt_dec};

/// Nanoseconds in one second, the scale the timestamp pair is normalised at.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Render a `google.protobuf.Timestamp` as `seconds.nanoseconds`: exactly one
/// dot, exactly nine fractional digits, and a fraction that is never negative
/// and never reaches a full second.
///
/// The contract is protobuf's own: `nanos` is a non-negative fraction that
/// counts *forward* from `seconds`, including when `seconds` is negative. So
/// `seconds: -5, nanos: 500_000_000` is -4.5s and prints `-5.500000000` — read
/// the fraction as counting forward, not as a signed offset.
///
/// Nothing obliges a backend to keep that invariant (`nanos` is documented as
/// 0..=999,999,999 but nothing rejects it otherwise), and the two fields are
/// otherwise rendered as ten-plus digits or with the sign inside the fraction.
/// The pair is therefore re-derived as one nanosecond count and split with a
/// floor division, which puts the sign on the seconds half alone and lands the
/// fraction in 0..=999,999,999 for *every* input. `seconds: -4, nanos:
/// -500_000_000` is the same instant as `seconds: -5, nanos: 500_000_000` and
/// renders identically.
fn timestamp_text(seconds: i64, nanos: i32) -> String {
    // `i128` because the widest `seconds` times a billion overflows `i64`; the
    // floor division below is exact at that width.
    let total = i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos);
    let whole = total.div_euclid(NANOS_PER_SECOND);
    let fraction = total.rem_euclid(NANOS_PER_SECOND);
    format!("{whole}.{fraction:09}")
}

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    depth: u32,
    output: &Renderer,
) -> Result<()> {
    match client.fetch_order_book(exchange, symbol, depth).await {
        Ok(book) => {
            if let Renderer { format: crate::output::OutputFormat::Json, .. } = output {
                println!("{}", serde_json::to_string_pretty(&book).unwrap_or_default());
            } else {
                println!("Symbol: {}", book.symbol);
                if let Some(ts) = book.timestamp.as_option() {
                    println!("Timestamp: {}", timestamp_text(ts.seconds, ts.nanos));
                }
                println!();
                println!("{:<15} {:<15}", "BID PRICE", "BID AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.bids {
                    println!("{:<15} {:<15}", fmt_dec(&level.price), fmt_dec(&level.amount));
                }
                println!();
                println!("{:<15} {:<15}", "ASK PRICE", "ASK AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.asks {
                    println!("{:<15} {:<15}", fmt_dec(&level.price), fmt_dec(&level.amount));
                }
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// This file's own source, read at compile time.
    ///
    /// The tests below exercise the very `timestamp_text` `run` prints with, so
    /// the source check is not about keeping a mirror honest — it is what stops
    /// `run` from growing a second, inline rendering the tests would not cover.
    const SOURCE: &str = include_str!("book.rs");

    /// The call `run` makes into the shared renderer.
    const TIMESTAMP_CALL: &str = "timestamp_text(ts.seconds, ts.nanos)";

    /// How many times [`TIMESTAMP_CALL`] must appear in this file: once in the
    /// constant itself and once at the `println!` in `run`. Counting is what stops
    /// the constant from satisfying its own assertion.
    const TIMESTAMP_CALL_SITES: usize = 2;

    #[test]
    fn run_renders_its_timestamp_through_the_shared_helper() {
        let sites = SOURCE.matches(TIMESTAMP_CALL).count();
        assert!(
            sites >= TIMESTAMP_CALL_SITES,
            "the timestamp rendering moved: found {sites} call sites, expected at least \
             {TIMESTAMP_CALL_SITES}. Update `TIMESTAMP_CALL` to match and re-derive what \
             these tests assert"
        );
    }

    // ---- the shape every rendering has ----

    #[test]
    fn a_sub_second_timestamp_is_rendered_with_nine_fractional_digits() {
        assert_eq!(timestamp_text(1_700_000_000, 0), "1700000000.000000000");
        assert_eq!(timestamp_text(1_700_000_000, 1), "1700000000.000000001");
        assert_eq!(timestamp_text(0, 500_000_000), "0.500000000");
    }

    #[test]
    fn a_fraction_of_exactly_nine_digits_is_not_padded_further() {
        assert_eq!(timestamp_text(0, 999_999_999), "0.999999999");
    }

    #[test]
    fn fractional_digits_below_the_width_are_left_padded_with_zeros() {
        assert_eq!(timestamp_text(7, 42), "7.000000042");
    }

    #[test]
    fn the_widest_legal_seconds_still_render_without_a_panic() {
        assert_eq!(timestamp_text(i64::MAX, 0), format!("{}.000000000", i64::MAX));
        assert_eq!(timestamp_text(i64::MIN, 0), format!("{}.000000000", i64::MIN));
    }

    // ---- nanos outside the legal range are normalised, not printed raw ----
    //
    // The old renderer used `{}.{:09}`, a *minimum* width, so an out-of-range
    // `nanos` widened the field past nine digits and a negative one put the sign
    // where the fraction should start. `timestamp_text` re-derives the pair from
    // a single nanosecond count, so neither shape can survive.

    #[test]
    fn a_nano_field_of_a_whole_second_carries_into_the_seconds_half() {
        // 1e9 nanos is one whole second, so it belongs in `seconds`, not in a
        // ten-digit fraction.
        assert_eq!(timestamp_text(0, 1_000_000_000), "1.000000000");
        assert_eq!(timestamp_text(0, 1_000_000_000).split('.').nth(1).map(str::len), Some(9));
    }

    #[test]
    fn an_extreme_nano_field_is_normalised_into_a_legal_pair() {
        assert_eq!(timestamp_text(0, i32::MAX), "2.147483647");
        assert_eq!(timestamp_text(0, i32::MIN), "-3.852516352");
    }

    #[test]
    fn a_negative_nano_field_never_puts_a_sign_inside_the_fraction() {
        // The instant one nanosecond before the epoch has the canonical pair
        // (-1s, 999_999_999ns): the fraction counts forward from the negative
        // seconds half, so the line means -0.000000001s rather than -1.999999999s.
        let rendered = timestamp_text(0, -1);
        assert_eq!(rendered, "-1.999999999");
        assert_eq!(rendered.split('.').nth(1).map(str::len), Some(9));
        assert!(!rendered.split_once('.').expect("one dot").1.contains('-'), "{rendered}");
    }

    // ---- a pre-epoch timestamp follows the protobuf forward-counting rule ----

    #[test]
    fn a_pre_epoch_timestamp_keeps_the_fraction_non_negative() {
        assert_eq!(timestamp_text(-5, 0), "-5.000000000");
        // -5s + 0.5s is -4.5s, and the canonical pair for -4.5s is (-5, 0.5s):
        // the fraction counts *forward* from a negative seconds half.
        assert_eq!(timestamp_text(-5, 500_000_000), "-5.500000000");
    }

    #[test]
    fn both_spellings_of_a_pre_epoch_instant_render_identically() {
        // The two conventions a backend might use for "half a second before -4.5s"
        // describe the same instant, so they must produce the same line.
        assert_eq!(timestamp_text(-5, 500_000_000), timestamp_text(-4, -500_000_000));
        assert_eq!(timestamp_text(0, 250_000_000), timestamp_text(1, -750_000_000));
    }

    #[test]
    fn the_whole_number_boundary_rolls_into_the_next_second_not_the_previous_one() {
        // 0.5s and -0.5s straddle zero; floor division must keep each on its own
        // side of it.
        assert_eq!(timestamp_text(0, 500_000_000), "0.500000000");
        assert_eq!(timestamp_text(-1, 500_000_000), "-1.500000000");
        assert_eq!(timestamp_text(0, -500_000_000), "-1.500000000");
    }

    #[test]
    fn a_rendered_line_round_trips_back_to_the_same_instant() {
        // Parsing the text under the documented contract
        // (`instant = seconds + fraction / 1e9`) must recover the original pair.
        let cases = [(0, 0), (1_700_000_000, 1), (-5, 500_000_000), (-4, -500_000_000)];
        for (seconds, nanos) in cases {
            let rendered = timestamp_text(seconds, nanos);
            let (whole, fraction) = rendered.split_once('.').expect("exactly one dot");
            let recovered = i128::from(whole.parse::<i64>().expect("whole seconds")) *
                NANOS_PER_SECOND +
                i128::from(fraction.parse::<i32>().expect("nine fractional digits"));
            let original = i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos);
            assert_eq!(recovered, original, "seconds={seconds} nanos={nanos} -> {rendered}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The nine-digit contract holds for *every* pair, in-range or not: one
        /// dot, nine fractional digits, always.
        #[test]
        fn a_timestamp_always_renders_nine_fractional_digits(
            seconds in proptest::num::i64::ANY,
            nanos in proptest::num::i32::ANY,
        ) {
            let rendered = timestamp_text(seconds, nanos);
            let fraction = rendered.split('.').nth(1).unwrap_or_default();
            prop_assert_eq!(fraction.len(), 9, "{}", rendered);
            prop_assert_eq!(rendered.matches('.').count(), 1, "{}", rendered);
        }

        /// The normalised fraction is always a legal `nanos`: non-negative and
        /// under a whole second, with the sign carried by the seconds half alone.
        #[test]
        fn the_normalised_fraction_is_always_in_the_legal_range(
            seconds in proptest::num::i64::ANY,
            nanos in proptest::num::i32::ANY,
        ) {
            let rendered = timestamp_text(seconds, nanos);
            let (whole, fraction) = rendered.split_once('.').expect("exactly one dot");
            let fraction: i64 = fraction.parse().expect("nine digits, no sign");
            prop_assert!((0..1_000_000_000).contains(&fraction), "{}", rendered);
            prop_assert!(
                whole.parse::<i64>().is_ok(),
                "the seconds half must stay an i64, got {:?}",
                whole
            );
        }

        /// Whatever the wire hands it, the renderer must never panic and must
        /// always keep the seconds field and the fraction on one line.
        #[test]
        fn the_timestamp_render_never_panics(
            seconds in proptest::num::i64::ANY,
            nanos in proptest::num::i32::ANY,
        ) {
            let rendered = timestamp_text(seconds, nanos);
            prop_assert!(rendered.contains('.'), "{}", rendered);
        }
    }
}
