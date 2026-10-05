//! Predicted-return-rate entry strategy.
//!
//! Reads an externally produced signal file containing the predicted return
//! for the next horizon (one decimal number per line; the last line wins)
//! and trades its sign against `entry_threshold`. This keeps model
//! inference out of the trading process: any external system can write the
//! file and the strategy reacts on the next poll.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::TradingGateway,
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the IRR strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct IrrConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Path to the signal file (last non-empty line = predicted return).
    pub signal_file: String,
    /// Entry threshold on the predicted return.
    #[serde(default = "default_threshold")]
    pub entry_threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_threshold() -> Decimal {
    Decimal::new(1, 2)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl IrrConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Reads the latest predicted return from a signal file.
pub fn read_signal(path: &str) -> Option<Decimal> {
    std::fs::read_to_string(path).ok()?.lines().rev().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return None;
        }
        trimmed.parse::<Decimal>().ok()
    })
}

/// IRR strategy.
pub struct Irr {
    config: IrrConfig,
    gateway: Arc<dyn TradingGateway>,
    held: Mutex<bool>,
}

impl Irr {
    pub fn new(config: IrrConfig, gateway: Arc<dyn TradingGateway>) -> Self {
        Self { config, gateway, held: Mutex::new(false) }
    }

    /// One decision cycle.
    ///
    /// The `held` flag is advanced only *after* `create_order` succeeds, and
    /// that ordering is load-bearing: the strategy is edge-triggered on `held`,
    /// so latching it before the venue confirms would make a transient rejection
    /// look like an open position and the entry would never be retried. The
    /// flag is therefore written on the far side of the `create_order` await,
    /// and a rejected exit likewise leaves `held` set so the exit is retried.
    pub async fn tick(&self) -> Result<()> {
        let Some(signal) = read_signal(&self.config.signal_file) else {
            tracing::debug!("irr: no signal available");
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        // Entering is a buy and exiting is a sell, so the new `held` value is
        // also the side of the order. Decided under the lock, which is released
        // across the RPC.
        let is_held = {
            let held = self.held.lock().await;
            if !*held && signal > self.config.entry_threshold {
                true
            } else if *held && signal < -self.config.entry_threshold {
                false
            } else {
                return Ok(());
            }
        };
        let coid = format!("irr-{}", ulid::Ulid::generate());
        self.gateway
            .create_order(market_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_held,
                self.config.qty,
            ))
            .await?;
        *self.held.lock().await = is_held;
        Ok(())
    }
}

#[async_trait]
impl Strategy for Irr {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "irr tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use buffa::EnumValue;
    // Imported item by item rather than as `prelude::*`: the prelude also exports
    // a `Strategy` trait, which collides with `crate::strategies::Strategy` here.
    use proptest::prelude::{ProptestConfig, prop, prop_assert, prop_assert_eq, proptest};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, proto::trading};

    #[test]
    fn reads_last_signal_line() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("signal.txt");
        std::fs::write(&path, "0.01\n0.05\n").expect("write");
        assert_eq!(read_signal(path.to_str().expect("utf8")), Some(Decimal::new(5, 2)));
        std::fs::write(&path, "not-a-number\n-0.02\n").expect("write");
        assert_eq!(read_signal(path.to_str().expect("utf8")), Some(Decimal::new(-2, 2)));
    }

    #[test]
    fn missing_file_yields_none() {
        assert!(read_signal("/nonexistent/signal.txt").is_none());
    }

    // -----------------------------------------------------------------------
    // read_signal: the "last parsable line wins" scan
    // -----------------------------------------------------------------------

    /// Writes `contents` into a fresh temp file and reads it back.
    fn signal_in(contents: &str) -> Option<Decimal> {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("signal.txt");
        std::fs::write(&path, contents).expect("write");
        read_signal(path.to_str().expect("utf8"))
    }

    #[test]
    fn an_empty_file_has_no_signal() {
        assert!(signal_in("").is_none());
    }

    /// A file of blank lines carries no number, so there is nothing to trade on.
    #[test]
    fn an_all_blank_file_has_no_signal() {
        assert!(signal_in("\n\n   \n\t\n").is_none());
    }

    /// Unparsable lines are skipped, not treated as zero: a zero signal would
    /// sit exactly between the entry and exit thresholds and be acted on as if a
    /// model had spoken.
    #[test]
    fn a_file_of_unparsable_lines_has_no_signal() {
        assert!(signal_in("abc\nn/a\n1.2.3\n0x10\n--\n").is_none());
    }

    /// The last *parsable* line wins even when the final line is junk.
    #[test]
    fn the_last_parsable_line_wins_over_trailing_garbage() {
        assert_eq!(signal_in("0.05\nnot-a-number\n"), Some(dec!(0.05)));
        assert_eq!(signal_in("0.05\n   \n"), Some(dec!(0.05)));
    }

    #[test]
    fn a_trailing_newline_does_not_hide_the_last_value() {
        assert_eq!(signal_in("0.05"), Some(dec!(0.05)));
        assert_eq!(signal_in("0.05\n"), Some(dec!(0.05)));
        assert_eq!(signal_in("0.05\n\n"), Some(dec!(0.05)));
    }

    #[test]
    fn surrounding_whitespace_does_not_prevent_a_parse() {
        assert_eq!(signal_in("  0.05  \n"), Some(dec!(0.05)));
        assert_eq!(signal_in("\t-1.25\t"), Some(dec!(-1.25)));
    }

    #[test]
    fn a_signed_or_zero_signal_round_trips() {
        assert_eq!(signal_in("-0.02"), Some(dec!(-0.02)));
        assert_eq!(signal_in("0"), Some(Decimal::ZERO));
    }

    /// An unreadable path is indistinguishable from "no model opinion yet": both
    /// mean the strategy must sit out the cycle.
    #[test]
    fn a_directory_that_does_not_exist_yields_none() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("no-such-dir").join("signal.txt");
        assert!(read_signal(path.to_str().expect("utf8")).is_none());
    }

    // -----------------------------------------------------------------------
    // The entry / exit decision
    // -----------------------------------------------------------------------

    /// The contract amount the mock echoed back from an order request.
    fn order_amount(order: &trading::Order) -> Decimal {
        order
            .amount
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()
            .expect("the contract decimal decodes")
            .expect("the amount field is set")
    }

    fn irr_for(adapter: &StdArc<MockAdapter>, signal_file: &str, doc: &str) -> Irr {
        let full = format!("symbol = \"BTCUSDT\"\nsignal_file = {signal_file:?}\n{doc}");
        let config = IrrConfig::from_params(&toml::from_str(&full).expect("toml")).expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        Irr::new(config, gateway)
    }

    /// A file the strategy can re-read across ticks; the `TempDir` outlives it.
    struct SignalFile {
        _dir: tempfile::TempDir,
        path: std::path::PathBuf,
    }

    impl SignalFile {
        fn starting_with(contents: &str) -> Self {
            let dir = tempfile::tempdir().expect("tmp");
            let path = dir.path().join("signal.txt");
            std::fs::write(&path, contents).expect("write");
            Self { _dir: dir, path }
        }

        fn write(&self, contents: &str) {
            std::fs::write(&self.path, contents).expect("write");
        }

        /// The raw path. `irr_for` quotes it when it builds the TOML, so
        /// quoting it here as well would embed literal quotes in the value and
        /// the file would never be found.
        fn as_param(&self) -> String {
            self.path.to_str().expect("utf8").to_string()
        }
    }

    #[tokio::test]
    async fn a_signal_above_the_threshold_buys_and_records_the_position() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"\nqty = \"0.5\"");

        s.tick().await.expect("tick");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 1, "a positive signal buys");
        assert!(matches!(orders[0].side, EnumValue::Known(trading::OrderSide::Buy)));
        assert_eq!(order_amount(&orders[0]), dec!(0.5));
        assert!(*s.held.lock().await, "the position must be recorded");
    }

    /// The entry comparison is a strict `>`: a signal sitting exactly on the
    /// threshold is not an opinion worth paying the spread for.
    #[tokio::test]
    async fn a_signal_exactly_at_the_threshold_does_not_buy() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.1");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");

        s.tick().await.expect("tick");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert!(orders.is_empty());
        assert!(!*s.held.lock().await);
    }

    #[tokio::test]
    async fn a_repeated_entry_signal_does_not_buy_twice() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");

        s.tick().await.expect("tick");
        s.tick().await.expect("tick");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 1, "the held flag must gate the entry");
    }

    #[tokio::test]
    async fn a_signal_below_the_negative_threshold_sells_and_clears_the_position() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"\nqty = \"0.5\"");

        s.tick().await.expect("entered");
        file.write("-0.5");
        s.tick().await.expect("exited");

        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 2);
        assert!(matches!(orders[1].side, EnumValue::Known(trading::OrderSide::Sell)));
        assert_eq!(order_amount(&orders[1]), dec!(0.5));
        assert!(!*s.held.lock().await, "the position flag must be cleared");
    }

    /// The exit comparison is a strict `<` on `-entry_threshold`.
    #[tokio::test]
    async fn a_signal_exactly_at_the_negative_threshold_does_not_sell() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");

        s.tick().await.expect("entered");
        file.write("-0.1");
        s.tick().await.expect("no exit at the threshold");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 1, "the exit comparison is a strict `<` on the threshold");
        assert!(*s.held.lock().await);
    }

    #[tokio::test]
    async fn a_signal_between_the_thresholds_holds_the_position() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");

        s.tick().await.expect("entered");
        file.write("-0.05");
        s.tick().await.expect("still holding");
        assert_eq!(
            adapter
                .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
                .await
                .expect("orders")
                .len(),
            1
        );
        assert!(*s.held.lock().await);
    }

    /// No signal is a hold, not a flat position: the strategy must not invent an
    /// exit for a model that never spoke.
    #[tokio::test]
    async fn a_missing_signal_file_places_no_order() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let dir = tempfile::tempdir().expect("tmp");
        let missing = dir.path().join("absent.txt");
        let s = irr_for(&adapter, &format!("{:?}", missing.to_str().expect("utf8")), "");

        s.tick().await.expect("a missing signal is not an error");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert!(orders.is_empty());
        assert!(!*s.held.lock().await);
    }

    #[tokio::test]
    async fn an_unparsable_signal_file_places_no_order() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("garbage\nalso-garbage");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");

        s.tick().await.expect("garbage is not an error");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert!(orders.is_empty());
    }

    /// A rejected entry must not latch. The strategy is edge-triggered on `held`,
    /// so believing it is long after a rejection would drop the signal for good;
    /// leaving it flat makes the very next poll re-send the same entry.
    #[tokio::test]
    async fn a_rejected_entry_leaves_the_strategy_flat_so_the_next_poll_retries() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");
        adapter.fail_next_creates(1).await;

        assert!(s.tick().await.is_err(), "the venue rejection must surface");
        assert!(!*s.held.lock().await, "the flag must not latch before the venue confirms");

        s.tick().await.expect("the second cycle does not error");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 1, "the retried signal is re-sent");
        assert!(*s.held.lock().await, "the retried entry latches");
    }

    /// The mirror: a rejected exit leaves `held` set, so the position is still
    /// believed to be open and the next poll retries the exit instead of the
    /// strategy forgetting it is long.
    #[tokio::test]
    async fn a_rejected_exit_keeps_the_position_held_so_the_next_poll_retries() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let file = SignalFile::starting_with("0.5");
        let s = irr_for(&adapter, &file.as_param(), "entry_threshold = \"0.1\"");
        s.tick().await.expect("entered");

        file.write("-0.5");
        adapter.fail_next_creates(1).await;
        assert!(s.tick().await.is_err(), "the venue rejection must surface");
        assert!(*s.held.lock().await, "a failed exit must not write off the position");

        s.tick().await.expect("the second cycle does not error");
        let orders = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(orders.len(), 2, "the retried exit is re-sent");
        assert!(!*s.held.lock().await, "the retried exit clears the position");
    }

    /// The default threshold and quantity decide when the strategy trades.
    #[test]
    fn the_default_threshold_and_quantity_are_pinned() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nsignal_file = \"/tmp/sig\"").expect("toml");
        let cfg = IrrConfig::from_params(&table).expect("config");
        assert_eq!(cfg.entry_threshold, dec!(0.01));
        assert_eq!(cfg.qty, dec!(0.001));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Nothing but blank lines can never yield a signal, whatever the shape
        /// of the file.
        #[test]
        fn a_blank_only_file_never_carries_a_signal(
            lines in prop::collection::vec(
                prop::sample::select(&["", " ", "\t", "  ", "\r"]),
                0..8,
            ),
        ) {
            let dir = tempfile::tempdir().expect("tmp");
            let path = dir.path().join("signal.txt");
            std::fs::write(&path, lines.join("\n")).expect("write");
            prop_assert!(read_signal(path.to_str().expect("utf8")).is_none());
        }

        /// A token with no digits and no exponent can never parse, so junk
        /// interleaved with one good line cannot displace the good line.
        #[test]
        fn junk_never_displaces_the_last_parsable_line(
            good in -1_000_000i64..1_000_000,
            junk in prop::collection::vec("[a-zA-Z!_]{1,6}", 0..5),
        ) {
            let dir = tempfile::tempdir().expect("tmp");
            let path = dir.path().join("signal.txt");
            std::fs::write(&path, format!("{}\n{good}", junk.join("\n"))).expect("write");
            prop_assert_eq!(
                read_signal(path.to_str().expect("utf8")),
                Some(Decimal::from(good))
            );
        }
    }
}
