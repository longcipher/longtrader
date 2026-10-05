//! EMA crossover strategy over the worker candle-poll model.
//!
//! Polls candles, computes fast/slow EMAs, and submits a market order when
//! the pair crosses. Cross state is edge-triggered in-memory: after a
//! restart the strategy waits for the next fresh cross instead of replaying
//! the last one, and the latch only advances once the venue has accepted the
//! order, so a rejected cross is retried on the next poll.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_closes_of, market_order, params_from_table},
};

/// Config for the worker EMA-cross strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct EmaCrossConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_fast")]
    pub fast_window: usize,
    #[serde(default = "default_slow")]
    pub slow_window: usize,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_fast() -> usize {
    9
}
const fn default_slow() -> usize {
    21
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl EmaCrossConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrossState {
    Unknown,
    FastAbove,
    FastBelow,
}

/// Worker EMA-cross strategy.
pub struct EmaCross {
    config: EmaCrossConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    cross_state: Mutex<CrossState>,
}

impl EmaCross {
    pub fn new(
        config: EmaCrossConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, cross_state: Mutex::new(CrossState::Unknown) }
    }

    /// One poll cycle: fetch candles, detect a fresh cross, trade it.
    pub async fn tick(&self) -> Result<()> {
        let candles = fetch_closes_of(
            self.market.as_ref(),
            &self.config.common.proto_exchange_id(),
            &self.config.common.symbol,
            &self.config.common.timeframe,
        )
        .await?;
        let mut fast = Ema::new(self.config.fast_window);
        let mut slow = Ema::new(self.config.slow_window);
        for close in &candles {
            fast.push(*close);
            slow.push(*close);
        }
        let (Some(f), Some(s)) = (fast.value(), slow.value()) else {
            tracing::debug!("ema-cross warmup incomplete");
            return Ok(());
        };
        let new_state = match f.cmp(&s) {
            std::cmp::Ordering::Greater => CrossState::FastAbove,
            std::cmp::Ordering::Less => CrossState::FastBelow,
            std::cmp::Ordering::Equal => return Ok(()),
        };

        let state = self.cross_state.lock().await;
        let crossed_up = *state != CrossState::FastAbove && new_state == CrossState::FastAbove;
        let crossed_down = *state != CrossState::FastBelow && new_state == CrossState::FastBelow;
        let is_buy = if crossed_up {
            true
        } else if crossed_down {
            false
        } else {
            return Ok(());
        };
        // The latch is written after the venue confirms (see below), so the guard
        // is released before the request is sent.
        drop(state);

        let coid = format!("emacross-{}", ulid::Ulid::generate());
        let request = market_order(
            &self.config.common.proto_exchange_id(),
            &self.config.common.symbol,
            coid,
            is_buy,
            self.config.qty,
        );
        self.gateway.create_order(request).await?;
        // Latched *after* the venue has accepted the order. `cross_state` is this
        // strategy's edge trigger: advancing it before the order existed would
        // make a rejected cross indistinguishable from a traded one and the entry
        // would never be retried.
        *self.cross_state.lock().await = new_state;
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, "ema-cross signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for EmaCross {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "ema-cross tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use longtrader_contract::ext::{common_to_decimal, decimal_to_common};
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
        proto::{market, trading},
    };

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table = toml::from_str(
            r#"
            exchange_id = "binance"
            symbol = "BTCUSDT"
            timeframe = "5m"
            poll_secs = 15
            fast_window = 5
            slow_window = 20
            qty = "0.002"
            "#,
        )
        .expect("valid toml");
        let cfg = EmaCrossConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.common.symbol, "BTCUSDT");
        assert_eq!(cfg.fast_window, 5);
        assert_eq!(cfg.qty.to_string(), "0.002");
        assert_eq!(cfg.common.poll_secs, 15);
    }

    #[test]
    fn missing_symbol_fails_config() {
        let table: toml::Table = toml::from_str("qty = \"1\"").expect("valid toml");
        assert!(EmaCrossConfig::from_params(&table).is_err());
    }

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    fn config() -> EmaCrossConfig {
        EmaCrossConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "5m".to_string(),
                poll_secs: 30,
            },
            fast_window: 2,
            slow_window: 4,
            qty: dec!(0.5),
        }
    }

    /// With windows 2 and 4, an accelerating ramp leaves the fast EMA above the
    /// slow one; a decaying ramp leaves it below; a flat one ties.
    fn rising() -> Vec<Decimal> {
        vec![dec!(1), dec!(2), dec!(3), dec!(4)]
    }

    fn falling() -> Vec<Decimal> {
        vec![dec!(4), dec!(3), dec!(2), dec!(1)]
    }

    fn flat() -> Vec<Decimal> {
        vec![dec!(5), dec!(5), dec!(5), dec!(5)]
    }

    fn strategy_with(window: &[Decimal]) -> (Arc<MockAdapter>, Arc<ScriptedCloses>, EmaCross) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedCloses::of(window));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        let strategy = EmaCross::new(config(), gateway, market);
        (adapter, feed, strategy)
    }

    async fn placed(adapter: &MockAdapter) -> Vec<trading::Order> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup")
    }

    async fn state_of(strategy: &EmaCross) -> CrossState {
        *strategy.cross_state.lock().await
    }

    fn amount_of(order: &trading::Order) -> Decimal {
        let raw = order.amount.as_option().expect("the order carries an amount");
        common_to_decimal(raw).expect("a representable amount")
    }

    // -----------------------------------------------------------------------
    // Cross state machine
    // -----------------------------------------------------------------------

    /// `Unknown` to `FastAbove` is a fresh cross, so the first resolved window
    /// opens a long outright.
    #[tokio::test]
    async fn a_first_window_above_the_slow_ema_opens_a_long() {
        let (adapter, _feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastAbove);
        let orders = placed(&adapter).await;
        assert_eq!(orders.len(), 1);
        let order = orders.first().expect("one order");
        assert_eq!(order.side, buffa::EnumValue::Known(trading::OrderSide::Buy));
        assert_eq!(order.symbol, "BTC/USDT");
        assert_eq!(order.r#type, buffa::EnumValue::Known(trading::OrderType::Market));
    }

    /// The mirror arm: `Unknown` to `FastBelow` opens a short.
    #[tokio::test]
    async fn a_first_window_below_the_slow_ema_opens_a_short() {
        let (adapter, _feed, strategy) = strategy_with(&falling());
        strategy.tick().await.expect("tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastBelow);
        let orders = placed(&adapter).await;
        assert_eq!(orders.len(), 1);
        assert_eq!(
            orders.first().expect("one order").side,
            buffa::EnumValue::Known(trading::OrderSide::Sell)
        );
    }

    /// The trigger is edge-based, so re-reading the same trend is the no-op arm:
    /// a long is not re-opened on every poll.
    #[tokio::test]
    async fn staying_above_the_slow_ema_does_not_reopen_the_long() {
        let (adapter, _feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("first tick");
        strategy.tick().await.expect("second tick");
        strategy.tick().await.expect("third tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastAbove);
        assert_eq!(placed(&adapter).await.len(), 1, "the edge fired exactly once");
    }

    /// The same no-op arm on the short side.
    #[tokio::test]
    async fn staying_below_the_slow_ema_does_not_reopen_the_short() {
        let (adapter, _feed, strategy) = strategy_with(&falling());
        strategy.tick().await.expect("first tick");
        strategy.tick().await.expect("second tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastBelow);
        assert_eq!(placed(&adapter).await.len(), 1, "the edge fired exactly once");
    }

    /// A reversal is itself an edge: the single market order closes one side
    /// and opens the other, so the state follows the new direction.
    #[tokio::test]
    async fn a_cross_from_above_to_below_reverses_to_a_short() {
        let (adapter, feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("first tick");
        feed.set(&falling());
        strategy.tick().await.expect("second tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastBelow);
        let sides: Vec<_> = placed(&adapter).await.iter().map(|o| o.side).collect();
        assert_eq!(
            sides,
            vec![
                buffa::EnumValue::Known(trading::OrderSide::Buy),
                buffa::EnumValue::Known(trading::OrderSide::Sell),
            ]
        );
    }

    /// And the other way round.
    #[tokio::test]
    async fn a_cross_from_below_to_above_reverses_to_a_long() {
        let (adapter, feed, strategy) = strategy_with(&falling());
        strategy.tick().await.expect("first tick");
        feed.set(&rising());
        strategy.tick().await.expect("second tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastAbove);
        let sides: Vec<_> = placed(&adapter).await.iter().map(|o| o.side).collect();
        assert_eq!(
            sides,
            vec![
                buffa::EnumValue::Known(trading::OrderSide::Sell),
                buffa::EnumValue::Known(trading::OrderSide::Buy),
            ]
        );
    }

    /// `Equal` returns before the latch, so a tied window must neither trade
    /// nor overwrite the remembered direction. If it did overwrite, the very
    /// next real cross would be measured against a fabricated prior.
    #[tokio::test]
    async fn a_tied_window_neither_trades_nor_forgets_the_direction() {
        let (adapter, feed, strategy) = strategy_with(&falling());
        strategy.tick().await.expect("first tick");
        assert_eq!(state_of(&strategy).await, CrossState::FastBelow);

        feed.set(&flat());
        strategy.tick().await.expect("a tied window is not a failure");
        assert_eq!(
            state_of(&strategy).await,
            CrossState::FastBelow,
            "a tie must leave the remembered direction alone"
        );
        assert_eq!(placed(&adapter).await.len(), 1, "a tie is not a cross");

        // The next real cross is still measured against the last real direction.
        feed.set(&rising());
        strategy.tick().await.expect("third tick");
        assert_eq!(placed(&adapter).await.len(), 2);
    }

    /// Warmup is silent. Too few closes resolves neither leg, so the tick is a
    /// no-op that leaves the latch exactly where it was.
    #[tokio::test]
    async fn an_incomplete_window_trades_nothing_and_keeps_the_state() {
        let (adapter, feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("first tick");
        feed.set(&[dec!(1)]);
        strategy.tick().await.expect("a warmup-incomplete tick is not a failure");
        assert_eq!(state_of(&strategy).await, CrossState::FastAbove, "state must survive");
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    #[tokio::test]
    async fn an_empty_window_trades_nothing() {
        let (adapter, _feed, strategy) = strategy_with(&[]);
        strategy.tick().await.expect("an empty window is not a failure");
        assert_eq!(state_of(&strategy).await, CrossState::Unknown);
        assert!(placed(&adapter).await.is_empty());
    }

    /// The configured size has to reach the venue unchanged: this strategy is a
    /// signal generator, not a sizing policy.
    #[tokio::test]
    async fn the_configured_size_reaches_the_order() {
        let (adapter, _feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("tick");
        let orders = placed(&adapter).await;
        assert_eq!(amount_of(orders.first().expect("one order")), dec!(0.5));
    }

    /// Each cross mints its own client order id, so a reversal cannot be
    /// deduplicated against the entry it is replacing.
    #[tokio::test]
    async fn each_cross_order_carries_a_fresh_client_order_id() {
        let (adapter, feed, strategy) = strategy_with(&rising());
        strategy.tick().await.expect("first tick");
        feed.set(&falling());
        strategy.tick().await.expect("second tick");
        let orders = placed(&adapter).await;
        assert_eq!(orders.len(), 2);
        let first = orders.first().expect("the opening order");
        let second = orders.get(1).expect("the reversing order");
        assert!(first.client_order_id.starts_with("emacross-"));
        assert!(second.client_order_id.starts_with("emacross-"));
        assert_ne!(first.client_order_id, second.client_order_id);
    }

    /// A rejected order has to surface: the strategy must not report a quiet
    /// tick for a signal it could not deliver.
    #[tokio::test]
    async fn a_rejected_order_propagates_out_of_the_tick() {
        let (adapter, _feed, strategy) = strategy_with(&rising());
        adapter.fail_next_creates(1).await;
        let err = strategy.tick().await.expect_err("a rejected order must not be swallowed");
        assert!(err.to_string().contains("scripted failure"), "{err}");
        assert!(placed(&adapter).await.is_empty());
    }

    /// The latch is written only after the venue confirms, so a rejected order
    /// leaves the strategy still remembering the previous direction and the next
    /// poll re-sends the missed cross. Advancing it before the order existed
    /// would drop the entry for good, since nothing else re-arms the edge.
    #[tokio::test]
    async fn a_rejected_order_leaves_the_cross_state_unlatched_for_a_retry() {
        let (adapter, _feed, strategy) = strategy_with(&rising());
        adapter.fail_next_creates(1).await;
        assert!(strategy.tick().await.is_err(), "a rejected order must surface");
        assert_eq!(
            state_of(&strategy).await,
            CrossState::Unknown,
            "the latch must not advance before the venue confirms"
        );
        assert!(placed(&adapter).await.is_empty());

        strategy.tick().await.expect("the retry is a real attempt");
        assert_eq!(state_of(&strategy).await, CrossState::FastAbove);
        let orders = placed(&adapter).await;
        assert_eq!(orders.len(), 1, "the missed cross is re-sent");
        assert_eq!(
            orders.first().expect("one order").side,
            buffa::EnumValue::Known(trading::OrderSide::Buy)
        );
    }

    /// A broken candle feed is fatal to the tick and must not latch anything.
    #[tokio::test]
    async fn a_candle_feed_failure_propagates_out_of_the_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedCloses::failing());
        let strategy = EmaCross::new(config(), gateway, market);
        let err = strategy.tick().await.expect_err("a broken feed must not read as a quiet tick");
        assert!(err.to_string().contains("candle feed down"), "{err}");
        assert_eq!(state_of(&strategy).await, CrossState::Unknown);
        assert!(placed(&adapter).await.is_empty());
    }

    /// Market source that replays a close window the test can rewrite between
    /// ticks, or fails outright.
    struct ScriptedCloses {
        closes: StdArc<StdMutex<Vec<Decimal>>>,
        error: bool,
    }

    impl ScriptedCloses {
        fn of(window: &[Decimal]) -> Self {
            Self { closes: StdArc::new(StdMutex::new(window.to_vec())), error: false }
        }

        fn failing() -> Self {
            Self { closes: StdArc::new(StdMutex::new(Vec::new())), error: true }
        }

        fn set(&self, window: &[Decimal]) {
            *self.closes.lock().expect("closes mutex") = window.to_vec();
        }
    }

    #[async_trait]
    impl MarketDataSource for ScriptedCloses {
        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Err(PortError::Transport("ticker feed down".into()))
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Transport("book feed down".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            if self.error {
                return Err(PortError::Transport("candle feed down".into()));
            }
            let window = self.closes.lock().expect("closes mutex").clone();
            Ok(market::GetCandlesResponse {
                candles: window
                    .iter()
                    .map(|close| market::Candle {
                        close: buffa::MessageField::some(decimal_to_common(*close)),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Unsupported("no tickers".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Err(PortError::Unsupported("no stream".into()))
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Across any sequence of windows, the strategy places exactly one order
        /// per change of resolved direction: a tie latches nothing, and a
        /// repeated direction is the no-op arm.
        #[test]
        fn one_order_is_placed_per_change_of_direction(
            windows in prop::collection::vec(
                prop::collection::vec(0i64..10_000, 0..24),
                1..8,
            ),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime");
            let adapter = Arc::new(MockAdapter::new(dec!(100)));
            let gateway: Arc<dyn TradingGateway> = adapter.clone();
            let feed = Arc::new(ScriptedCloses::of(&[]));
            let market: Arc<dyn MarketDataSource> = feed.clone();
            let strategy = EmaCross::new(config(), gateway, market);
            let mut changes = 0usize;
            let mut previous = CrossState::Unknown;
            for window in &windows {
                let closes: Vec<Decimal> =
                    window.iter().copied().map(Decimal::from).collect();
                feed.set(&closes);
                runtime.block_on(strategy.tick()).expect("tick");
                let now = runtime.block_on(state_of(&strategy));
                if now != previous {
                    changes += 1;
                }
                previous = now;
            }
            let orders = runtime.block_on(placed(&adapter));
            prop_assert_eq!(orders.len(), changes, "one order per direction change");
            for order in orders {
                prop_assert!(order.client_order_id.starts_with("emacross-"));
            }
        }
    }
}
