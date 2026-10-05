use color_eyre::Result;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, market::v1 as umarket},
};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    timeframe: &str,
    limit: u32,
    output: &Renderer,
) -> Result<()> {
    let tf = match timeframe.to_uppercase().as_str() {
        "S100" => umarket::Timeframe::S100,
        "S1" => umarket::Timeframe::S1,
        "M1" => umarket::Timeframe::M1,
        "M5" => umarket::Timeframe::M5,
        "M15" => umarket::Timeframe::M15,
        "M30" => umarket::Timeframe::M30,
        "H1" => umarket::Timeframe::H1,
        "H4" => umarket::Timeframe::H4,
        "D1" => umarket::Timeframe::D1,
        "W1" => umarket::Timeframe::W1,
        _ => return Err(color_eyre::Report::msg(format!("Unsupported timeframe: {timeframe}"))),
    };

    match client.get_candles(exchange, symbol, tf, limit).await {
        Ok(candles) => {
            output.render_candles(&candles);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;
    use crate::output::OutputFormat;

    /// A loopback port nothing listens on, so every request dies in the
    /// transport instead of reaching a terminal. The timeframe parse is the
    /// only thing these tests care about, and it is the only thing `run`
    /// decides before it touches the client.
    const DEAD_ENDPOINT: &str = "http://127.0.0.1:1";

    /// Ceiling on how long a *valid* timeframe may spend failing to connect. A
    /// rejected timeframe returns before any `await`, so it always answers on
    /// the first poll and never spends this budget.
    const TRANSPORT_DEADLINE: Duration = Duration::from_millis(250);

    /// The exact prefix `run` uses to report an unrecognised timeframe.
    const REJECTED_PREFIX: &str = "Unsupported timeframe: ";

    /// Every timeframe token the `run` match accepts, verbatim.
    const SUPPORTED: [&str; 10] = ["S100", "S1", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1"];

    /// What the timeframe parse decided, independent of the transport outcome
    /// that follows it.
    #[derive(Debug, PartialEq, Eq)]
    enum Verdict {
        /// `run` bailed out on the parse, carrying its message verbatim.
        Rejected(String),
        /// `run` got past the parse and failed (or was cut off) downstream.
        Accepted,
    }

    /// The `Verdict` a rejected token produces: the prefix plus the caller's
    /// exact, untrimmed, unmodified input.
    fn rejected(timeframe: &str) -> Verdict {
        Verdict::Rejected(format!("{REJECTED_PREFIX}{timeframe}"))
    }

    /// Drive `run` far enough to observe the timeframe decision.
    async fn verdict(timeframe: &str) -> Verdict {
        let client = TerminalClient::new(DEAD_ENDPOINT);
        let output = Renderer::new(OutputFormat::Table);
        let exchange = common::ExchangeId::default();
        let call = super::run(&client, &exchange, "BTCUSDT", timeframe, 1, &output);
        match tokio::time::timeout(TRANSPORT_DEADLINE, call).await {
            // Nothing came back in time, so the parse clearly let the call
            // through and the transport is simply slow.
            Err(_elapsed) => Verdict::Accepted,
            Ok(Ok(())) => Verdict::Accepted,
            Ok(Err(report)) => {
                let message = report.to_string();
                if message.starts_with(REJECTED_PREFIX) {
                    Verdict::Rejected(message)
                } else {
                    Verdict::Accepted
                }
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

    // ---- accepted tokens ----

    #[tokio::test]
    async fn every_documented_timeframe_is_accepted() {
        for timeframe in SUPPORTED {
            assert_eq!(verdict(timeframe).await, Verdict::Accepted, "timeframe {timeframe:?}");
        }
    }

    #[tokio::test]
    async fn a_lowercase_timeframe_is_accepted_via_uppercasing() {
        for timeframe in SUPPORTED {
            let lower = timeframe.to_lowercase();
            assert_eq!(verdict(&lower).await, Verdict::Accepted, "timeframe {lower:?}");
        }
    }

    #[tokio::test]
    async fn both_sub_minute_tokens_are_accepted_so_the_leading_s_is_not_a_prefix_match() {
        // "S100" and "S1" share a first character; neither may shadow the other.
        assert_eq!(verdict("S100").await, Verdict::Accepted);
        assert_eq!(verdict("S1").await, Verdict::Accepted);
    }

    // ---- rejected tokens ----

    #[tokio::test]
    async fn an_empty_timeframe_is_rejected() {
        assert_eq!(verdict("").await, rejected(""));
    }

    #[tokio::test]
    async fn a_surrounded_timeframe_is_rejected_because_the_input_is_never_trimmed() {
        // `to_uppercase()` is applied but no `trim()` is, so quoting the token
        // on the command line breaks it.
        assert_eq!(verdict(" M1 ").await, rejected(" M1 "));
        assert_eq!(verdict("\tM1").await, rejected("\tM1"));
    }

    #[tokio::test]
    async fn an_inner_space_timeframe_is_rejected() {
        assert_eq!(verdict("M 1").await, rejected("M 1"));
    }

    #[tokio::test]
    async fn a_zero_padded_timeframe_is_rejected() {
        assert_eq!(verdict("M01").await, rejected("M01"));
        assert_eq!(verdict("H01").await, rejected("H01"));
    }

    #[tokio::test]
    async fn a_bare_number_timeframe_is_rejected() {
        assert_eq!(verdict("60").await, rejected("60"));
        assert_eq!(verdict("0").await, rejected("0"));
    }

    #[tokio::test]
    async fn a_neighbouring_interval_is_rejected() {
        // 30s and 2h are plausible-looking but simply are not in the table.
        assert_eq!(verdict("S30").await, rejected("S30"));
        assert_eq!(verdict("H2").await, rejected("H2"));
    }

    #[tokio::test]
    async fn the_underscore_free_token_2h_is_rejected_because_the_table_says_h4() {
        // Guard against a "looks close enough" reading of the match: the table
        // has H1 and H4 only, so a user typing 2h is told so.
        assert_eq!(verdict("2H").await, rejected("2H"));
    }

    // ---- the rejection message ----

    #[tokio::test]
    async fn the_rejection_message_echoes_the_raw_input_not_the_uppercased_form() {
        // The message interpolates the original `timeframe`, so a user who typed
        // `m2` sees `m2` back, never the `M2` the parser normalised to.
        assert_eq!(verdict("m2").await, rejected("m2"));
    }

    #[tokio::test]
    async fn the_rejection_message_echoes_quotes_and_whitespace_verbatim() {
        assert_eq!(verdict("  \"m2\"\t").await, rejected("  \"m2\"\t"));
    }

    #[tokio::test]
    async fn the_rejection_message_names_no_alternative_token() {
        // The message is a fixed prefix plus the input: it does not enumerate
        // the supported set, which is the whole of the user-facing diagnostic.
        let Verdict::Rejected(message) = verdict("M2").await else {
            unreachable!("M2 is not in the documented alphabet");
        };
        assert_eq!(message, "Unsupported timeframe: M2");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Letter case is the only normalisation: any casing of a documented
        /// token is accepted.
        #[test]
        fn letter_case_is_the_only_thing_normalised(
            base in proptest::sample::select(vec![
                "S100", "S1", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1",
            ]),
            upper in proptest::bool::ANY,
        ) {
            let timeframe = if upper { base.to_string() } else { base.to_lowercase() };
            prop_assert_eq!(block_on(verdict(&timeframe)), Verdict::Accepted);
        }

        /// A supported token dressed up with any padding or decoration can never
        /// collide with the table (which never starts with those bytes) and so
        /// must be rejected with the caller's exact text.
        #[test]
        fn a_decorated_token_is_rejected_with_the_exact_input(
            base in proptest::sample::select(vec!["M1", "M15", "H4", "W1"]),
            pad in "[ _-]{1,3}",
        ) {
            let token = format!("{pad}{base}{pad}");
            let expected = Verdict::Rejected(format!("{REJECTED_PREFIX}{token}"));
            prop_assert_eq!(block_on(verdict(&token)), expected);
        }

        /// Arbitrary user input is classified deterministically: the verdict is
        /// always exactly one of the two outcomes, and it never panics.
        #[test]
        fn an_arbitrary_token_always_yields_a_verdict(token in ".{0,12}") {
            let observed = block_on(verdict(&token));
            match observed {
                Verdict::Accepted => prop_assert!(true),
                Verdict::Rejected(message) => {
                    prop_assert!(message.starts_with(REJECTED_PREFIX));
                    prop_assert_eq!(message, format!("{REJECTED_PREFIX}{token}"));
                }
            }
        }
    }
}
