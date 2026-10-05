//! Connectivity / health sentinel.
//!
//! Periodically probes the backend (ticker + account reads). After
//! `max_consecutive_failures` consecutive failures it trips a kill switch
//! (cancel all orders) so an unhealthy link cannot leave resting orders
//! unmanaged. Recovery resets the counter.
//!
//! The kill switch fires **once per outage**: it is armed on the transition past
//! the threshold and disarmed on recovery, so an outage of `n` polls issues one
//! `cancel_all_orders` instead of `n` calls against a book that is already
//! cancelled. A cancel the venue refuses leaves the switch armed, because the
//! safety net did not deploy.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the sentinel.
#[derive(Debug, Clone, Deserialize)]
pub struct SentinelConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_max_failures")]
    pub max_consecutive_failures: u32,
}

const fn default_max_failures() -> u32 {
    3
}

impl SentinelConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors, and rejects
    /// `max_consecutive_failures = 0`: a threshold of zero is meaningless as a
    /// *tolerance* — it means "cancel on the very first failed probe" and it
    /// makes the recovery report vacuous (`0 >= 0` would log a recovery on
    /// every healthy probe). The runtime still handles a hand-built zero, since
    /// the field is public, but a config file must state a real tolerance.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        let config: Self = params_from_table(table)?;
        if config.max_consecutive_failures == 0 {
            color_eyre::eyre::bail!("max_consecutive_failures must be at least 1");
        }
        Ok(config)
    }
}

/// Failure counter plus the kill switch's arming, kept in one value so the two
/// transitions are updated under a single lock.
#[derive(Debug, Default)]
struct Health {
    /// Consecutive failed probes.
    failures: u32,
    /// Whether the kill switch still owes a cancel for the current outage.
    armed: bool,
}

/// Health sentinel strategy.
pub struct Sentinel {
    config: SentinelConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    health: Mutex<Health>,
}

impl Sentinel {
    pub fn new(
        config: SentinelConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, health: Mutex::new(Health::default()) }
    }

    /// One probe cycle; returns `true` when healthy.
    #[allow(clippy::cognitive_complexity)]
    pub async fn tick(&self) -> Result<bool> {
        let exchange = self.config.common.proto_exchange_id();
        let probe = async {
            self.market
                .fetch_ticker(crate::proto::market::FetchTickerRequest {
                    exchange_id: buffa::MessageField::some(exchange.clone()),
                    symbol: self.config.common.symbol.clone(),
                    ..Default::default()
                })
                .await?;
            self.gateway.get_account(crate::proto::trading::GetAccountRequest::default()).await?;
            Ok::<(), crate::ports::PortError>(())
        }
        .await;

        let mut health = self.health.lock().await;
        match probe {
            Ok(()) => {
                // A recovery is only news if something actually failed. The
                // `> 0` half matters for a threshold of zero, where `0 >= 0`
                // would otherwise log a recovery on every healthy probe.
                if health.failures > 0 && health.failures >= self.config.max_consecutive_failures {
                    tracing::warn!(
                        failures = health.failures,
                        "sentinel: link recovered after failures"
                    );
                }
                // Disarms the kill switch: the next outage has to cross the
                // threshold again before orders are cancelled.
                *health = Health::default();
                Ok(true)
            }
            Err(err) => {
                health.failures += 1;
                let n = health.failures;
                // Arm on the transition *past* the threshold, not on every poll
                // past it: re-firing per cycle meant an outage of `n` cycles
                // issued `n` `cancel_all_orders` calls against a book the first
                // one had already cleared. `armed` must only be set once the
                // threshold is actually crossed, or the crossing itself would
                // find the switch already marked and stand down.
                let fire = n >= self.config.max_consecutive_failures && !health.armed;
                if fire {
                    health.armed = true;
                }
                drop(health);
                tracing::error!(error = %err, failures = n, "sentinel: probe failed");
                if !fire {
                    return Ok(false);
                }
                if let Err(err) = self
                    .gateway
                    .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                        exchange_id: buffa::MessageField::some(exchange),
                        symbol: String::new(),
                        ..Default::default()
                    })
                    .await
                {
                    // The safety net did not deploy, so leave the switch armed:
                    // the next failed probe has to try again.
                    let mut health = self.health.lock().await;
                    health.armed = false;
                    return Err(err.into());
                }
                tracing::error!("sentinel: kill switch tripped, all orders cancelled");
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl Strategy for Sentinel {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            let _ = self.tick().await.inspect_err(|err| {
                tracing::error!(error = %err, "sentinel cycle error");
            });
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };

    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketEventStream, OverflowPolicy, PortError},
        proto::{common, market, trading, worker},
    };

    #[tokio::test]
    async fn healthy_probe_resets_counter() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let config = SentinelConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nmax_consecutive_failures = 2").expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let s = Sentinel::new(config, gateway, market);
        assert!(s.tick().await.expect("tick"));
        assert!(s.tick().await.expect("tick"));
    }

    #[test]
    fn config_parses() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nmax_consecutive_failures = 5").expect("toml");
        let cfg = SentinelConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.max_consecutive_failures, 5);
    }

    // -----------------------------------------------------------------------
    // Failure counter / kill switch
    //
    // `MockAdapter` only injects failures into `create_order`, so every health
    // path below needs a link whose probes and kill switch are scriptable.
    // -----------------------------------------------------------------------

    /// Consumes one scripted failure, reporting whether this call was it.
    fn consume(counter: &AtomicU32) -> bool {
        counter.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
    }

    /// A backend link whose ticker/account probes fail on demand and whose
    /// kill-switch cancel is observable.
    struct Link {
        fail_ticker: AtomicU32,
        fail_account: AtomicU32,
        account_probes: AtomicU32,
        cancels: AtomicU32,
        /// Whether the cancel-all was addressed to every symbol rather than one.
        cancel_all_symbols: AtomicBool,
        fail_cancel: bool,
    }

    impl Link {
        fn healthy() -> Self {
            Self {
                fail_ticker: AtomicU32::new(0),
                fail_account: AtomicU32::new(0),
                account_probes: AtomicU32::new(0),
                cancels: AtomicU32::new(0),
                cancel_all_symbols: AtomicBool::new(false),
                fail_cancel: false,
            }
        }

        /// The next `n` probes fail at both ends.
        fn failing(n: u32) -> Self {
            Self {
                fail_ticker: AtomicU32::new(n),
                fail_account: AtomicU32::new(n),
                ..Self::healthy()
            }
        }

        /// A link whose kill-switch cancel is refused by the venue.
        fn refusing_cancels(self) -> Self {
            Self { fail_cancel: true, ..self }
        }

        /// Replaces the failure script for both halves of the probe.
        fn script_failures(&self, n: u32) {
            self.fail_ticker.store(n, Ordering::SeqCst);
            self.fail_account.store(n, Ordering::SeqCst);
        }

        /// Fails only the market-data half, leaving the account read reachable.
        fn script_ticker_failures(&self, n: u32) {
            self.fail_ticker.store(n, Ordering::SeqCst);
        }

        fn cancels(&self) -> u32 {
            self.cancels.load(Ordering::SeqCst)
        }

        fn account_probes(&self) -> u32 {
            self.account_probes.load(Ordering::SeqCst)
        }

        fn cancel_covers_every_symbol(&self) -> bool {
            self.cancel_all_symbols.load(Ordering::SeqCst)
        }
    }

    /// Every port method the health probe never calls. They must exist (the
    /// trait is the whole boundary) but a probe must not reach them.
    fn unused<T>(op: &str) -> Result<T, PortError> {
        Err(PortError::Unsupported(op.to_string()))
    }

    #[async_trait]
    impl TradingGateway for Link {
        async fn create_order(
            &self,
            _req: trading::CreateOrderRequest,
        ) -> Result<trading::Order, PortError> {
            unused("create_order")
        }

        async fn batch_create_orders(
            &self,
            _req: trading::CreateOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            unused("batch_create_orders")
        }

        async fn cancel_order(
            &self,
            _req: trading::CancelOrderRequest,
        ) -> Result<trading::Order, PortError> {
            unused("cancel_order")
        }

        /// The kill switch: counted, scope-checked, and scriptable to fail.
        async fn cancel_all_orders(
            &self,
            req: trading::CancelAllOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            self.cancel_all_symbols.store(req.symbol.is_empty(), Ordering::SeqCst);
            if self.fail_cancel {
                return Err(PortError::Transport("cancel-all refused".into()));
            }
            Ok(Vec::new())
        }

        async fn fetch_open_orders(
            &self,
            _req: trading::FetchOpenOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            unused("fetch_open_orders")
        }

        async fn get_account(
            &self,
            _req: trading::GetAccountRequest,
        ) -> Result<trading::GetAccountResponse, PortError> {
            self.account_probes.fetch_add(1, Ordering::SeqCst);
            if consume(&self.fail_account) {
                return Err(PortError::Rpc { code: 503, message: "account read failed".into() });
            }
            Ok(trading::GetAccountResponse::default())
        }

        async fn get_positions(
            &self,
            _req: trading::GetPositionsRequest,
        ) -> Result<trading::GetPositionsResponse, PortError> {
            unused("get_positions")
        }

        async fn get_order_history(
            &self,
            _req: trading::GetOrderHistoryRequest,
        ) -> Result<trading::GetOrderHistoryResponse, PortError> {
            unused("get_order_history")
        }

        async fn get_closed_positions(
            &self,
            _req: trading::GetClosedPositionsRequest,
        ) -> Result<trading::GetClosedPositionsResponse, PortError> {
            unused("get_closed_positions")
        }

        async fn close_position(
            &self,
            _req: trading::ClosePositionRequest,
        ) -> Result<trading::ClosePositionResponse, PortError> {
            unused("close_position")
        }

        async fn close_all_positions(
            &self,
            _req: trading::CloseAllPositionsRequest,
        ) -> Result<trading::CloseAllPositionsResponse, PortError> {
            unused("close_all_positions")
        }

        async fn modify_position(
            &self,
            _req: trading::ModifyPositionRequest,
        ) -> Result<trading::ModifyPositionResponse, PortError> {
            unused("modify_position")
        }

        async fn sync_state(
            &self,
            _exchange_id: &common::ExchangeId,
        ) -> Result<worker::ReconcileStateResponse, PortError> {
            unused("sync_state")
        }
    }

    #[async_trait]
    impl MarketDataSource for Link {
        async fn fetch_ticker(
            &self,
            req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            if consume(&self.fail_ticker) {
                return Err(PortError::Transport(format!("ticker read failed for {}", req.symbol)));
            }
            Ok(market::Ticker { symbol: req.symbol, ..Default::default() })
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            unused("fetch_order_book")
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            unused("get_candles")
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            unused("list_symbols")
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            unused("search_symbols")
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            unused("list_tickers")
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            unused("subscribe_market_data")
        }
    }

    fn sentinel(link: &StdArc<Link>, max_consecutive_failures: u32) -> Sentinel {
        let doc =
            format!("symbol = \"BTCUSDT\"\nmax_consecutive_failures = {max_consecutive_failures}");
        let config =
            SentinelConfig::from_params(&toml::from_str(&doc).expect("toml")).expect("config");
        let gateway: StdArc<dyn TradingGateway> = link.clone();
        let market: StdArc<dyn MarketDataSource> = link.clone();
        Sentinel::new(config, gateway, market)
    }

    /// A sentinel built from a hand-written config, bypassing the `from_params`
    /// validation that rejects a zero tolerance. Only the zero-threshold test
    /// below needs it.
    fn sentinel_with_threshold(link: &StdArc<Link>, max_consecutive_failures: u32) -> Sentinel {
        let config = SentinelConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "5m".to_string(),
                poll_secs: 30,
            },
            max_consecutive_failures,
        };
        let gateway: StdArc<dyn TradingGateway> = link.clone();
        let market: StdArc<dyn MarketDataSource> = link.clone();
        Sentinel::new(config, gateway, market)
    }

    /// The kill switch is the whole point of the counter: an unhealthy link must
    /// not leave resting orders unmanaged. Failures below the threshold must
    /// leave them alone.
    #[tokio::test]
    async fn failures_below_the_threshold_leave_orders_alone() {
        let link = StdArc::new(Link::failing(2));
        let s = sentinel(&link, 3);
        assert!(!s.tick().await.expect("first failure"));
        assert!(!s.tick().await.expect("second failure"));
        assert_eq!(link.cancels(), 0, "two failures under a threshold of three must not cancel");
    }

    #[tokio::test]
    async fn reaching_the_threshold_cancels_every_order() {
        let link = StdArc::new(Link::failing(3));
        let s = sentinel(&link, 3);
        assert!(!s.tick().await.expect("failure 1"));
        assert!(!s.tick().await.expect("failure 2"));
        assert!(!s.tick().await.expect("failure 3 reaches the threshold"));
        assert_eq!(link.cancels(), 1);
        assert!(
            link.cancel_covers_every_symbol(),
            "the kill switch must not be narrowed to one symbol"
        );
    }

    /// The kill switch arms on the transition past the threshold and stays armed
    /// until recovery, so an outage of five polls is one `cancel_all_orders` —
    /// not four more calls against a book the first one already cleared.
    #[tokio::test]
    async fn the_kill_switch_fires_once_per_outage() {
        let link = StdArc::new(Link::failing(5));
        let s = sentinel(&link, 2);
        for _ in 0..5 {
            assert!(!s.tick().await.expect("the link is down"));
        }
        assert_eq!(link.cancels(), 1, "only the transition past the threshold fires");
    }

    /// Recovery stands the switch down, so a later outage has to cross the
    /// threshold again before orders are cancelled.
    #[tokio::test]
    async fn a_second_outage_trips_the_kill_switch_again() {
        let link = StdArc::new(Link::failing(2));
        let s = sentinel(&link, 2);
        assert!(!s.tick().await.expect("first failure"));
        assert!(!s.tick().await.expect("the threshold trips the kill switch"));
        assert_eq!(link.cancels(), 1);

        link.script_failures(0);
        assert!(s.tick().await.expect("the link recovered"));

        link.script_failures(2);
        assert!(!s.tick().await.expect("first failure of the second outage"));
        assert!(!s.tick().await.expect("the second outage reaches the threshold"));
        assert_eq!(link.cancels(), 2, "recovery must re-arm the kill switch");
    }

    /// A recovered probe must reset the counter, not merely report healthy:
    /// otherwise the very next failure would trip the kill switch again.
    #[tokio::test]
    async fn a_recovered_probe_reports_healthy_and_resets_the_counter() {
        let link = StdArc::new(Link::healthy());
        let s = sentinel(&link, 2);

        link.script_failures(1);
        assert!(!s.tick().await.expect("one scripted failure"));
        assert_eq!(link.cancels(), 0);

        link.script_failures(0);
        assert!(s.tick().await.expect("the probe recovered"));

        link.script_failures(1);
        assert!(!s.tick().await.expect("one failure after the reset"));
        assert_eq!(link.cancels(), 0, "recovery must zero the failure counter");
    }

    /// The kill switch failing is not "unhealthy but fine": the orders were not
    /// cancelled, so the caller has to learn that the safety net did not deploy.
    #[tokio::test]
    async fn a_refused_kill_switch_cancel_propagates_to_the_caller() {
        let link = StdArc::new(Link::failing(1).refusing_cancels());
        let s = sentinel(&link, 1);
        let err = s.tick().await.expect_err("the cancel error must not be swallowed");
        assert!(err.to_string().contains("cancel-all refused"), "{err}");
        assert_eq!(link.cancels(), 1, "the cancel was attempted");
    }

    /// Arming the kill switch once per outage must not turn a *refused* cancel
    /// into "already handled": the switch stays armed so the next failed probe
    /// tries again.
    #[tokio::test]
    async fn a_refused_kill_switch_cancel_is_retried_on_the_next_failure() {
        let link = StdArc::new(Link::failing(2).refusing_cancels());
        let s = sentinel(&link, 1);
        assert!(s.tick().await.is_err(), "the first cancel is refused");
        assert!(s.tick().await.is_err(), "the second cancel is refused");
        assert_eq!(link.cancels(), 2, "a refused cancel must not disarm the kill switch");
    }

    /// The probe is `fetch_ticker` then `get_account`: one failing read is one
    /// failure, not two, so a single broken endpoint cannot reach the threshold
    /// in half the polls.
    #[tokio::test]
    async fn a_failed_ticker_read_skips_the_account_read() {
        let link = StdArc::new(Link::healthy());
        let s = sentinel(&link, 3);
        link.script_ticker_failures(1);
        assert!(!s.tick().await.expect("the ticker read failed"));
        assert_eq!(link.account_probes(), 0, "the account read must not follow a failed probe");
        assert_eq!(link.cancels(), 0);
    }

    #[tokio::test]
    async fn a_healthy_probe_reads_the_account_and_stays_healthy() {
        let link = StdArc::new(Link::healthy());
        let s = sentinel(&link, 3);
        assert!(s.tick().await.expect("healthy"));
        assert_eq!(link.account_probes(), 1, "both halves of the probe must run");
        assert_eq!(link.cancels(), 0);
    }

    /// A zero tolerance is not a config value: it says "cancel on the very first
    /// failed probe" and would make the recovery report vacuous.
    #[test]
    fn a_zero_failure_threshold_is_rejected_by_the_config() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nmax_consecutive_failures = 0").expect("toml");
        let err = SentinelConfig::from_params(&table).expect_err("zero is not a tolerance");
        assert!(err.to_string().contains("max_consecutive_failures"), "{err}");
    }

    /// Hand-built (the config parser rejects zero), a zero threshold still
    /// cancels on the first failure — the strictest possible reading — while the
    /// healthy branch's recovery report is suppressed by the `> 0` guard, since
    /// `0 >= 0` describes every healthy probe.
    #[tokio::test]
    async fn a_hand_built_zero_threshold_cancels_on_the_first_failure() {
        let link = StdArc::new(Link::failing(1));
        let s = sentinel_with_threshold(&link, 0);
        assert!(!s.tick().await.expect("the probe failed"));
        assert_eq!(link.cancels(), 1);
    }

    /// `max_consecutive_failures` defaults to three; pin it, because the
    /// threshold is what decides when live orders get cancelled.
    #[test]
    fn the_default_failure_threshold_is_three() {
        let table: toml::Table = toml::from_str("symbol = \"BTCUSDT\"").expect("toml");
        let cfg = SentinelConfig::from_params(&table).expect("config");
        assert_eq!(cfg.max_consecutive_failures, 3);
    }
}
