//! Darvas Box breakout over the worker candle-poll model.
//!
//! Tracks new highs (or new lows) over a `period` window and then looks for
//! `box_bars` consolidation bars where price trades inside a tight range (the
//! "box"). A breakout from that box trades in the direction of the break.
//!
//! The box is defined once price makes a new N-day high and then stays within
//! the high and low of the following `box_bars` bars. Entry on the bar that
//! exits the box; the phase exits when a new box forms against the position.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the Darvas Box strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct DarvasBoxConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_box_bars")]
    pub box_bars: usize,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    20
}
const fn default_box_bars() -> usize {
    3
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl DarvasBoxConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Flat,
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    Enter(bool),
}

/// Darvas Box breakout strategy.
pub struct DarvasBox {
    config: DarvasBoxConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl DarvasBox {
    pub fn new(
        config: DarvasBoxConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let need = self.config.period + self.config.box_bars + 5;
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            need as u32,
        )
        .await?;

        let n_raw = candles.len();
        if n_raw < self.config.period + self.config.box_bars {
            tracing::debug!("darvas_box warmup incomplete");
            return Ok(());
        }

        // Decode bars.
        let mut highs = Vec::with_capacity(n_raw);
        let mut lows = Vec::with_capacity(n_raw);
        let mut closes = Vec::with_capacity(n_raw);
        for candle in &candles {
            let (Some(h), Some(l), Some(c)) =
                (candle.high.as_option(), candle.low.as_option(), candle.close.as_option())
            else {
                continue;
            };
            let (Ok(h), Ok(l), Ok(c)) = (
                longtrader_contract::ext::common_to_decimal(h),
                longtrader_contract::ext::common_to_decimal(l),
                longtrader_contract::ext::common_to_decimal(c),
            ) else {
                continue;
            };
            highs.push(h);
            lows.push(l);
            closes.push(c);
        }

        let n = highs.len();
        if n < self.config.period + self.config.box_bars {
            return Ok(());
        }

        let last_idx = n - 1;
        let last_close = closes[last_idx];

        // Held: check for the opposite-direction box as an exit signal.
        let held = *self.phase.lock().await;
        if matches!(held, Phase::Long) {
            // Look for a lower-high pattern forming.
            let range = highs[last_idx] - lows[last_idx];
            let mid = (highs[last_idx] + lows[last_idx]) / Decimal::from(2);
            if last_close < mid - range / Decimal::from(2) {
                // Price has crushed through the lower band of the recent bar.
                let is_buy = false;
                let coid = format!("darvas-{}", ulid::Ulid::generate());
                self.gateway
                    .create_order(market_order(
                        &exchange,
                        &self.config.common.symbol,
                        coid,
                        is_buy,
                        self.config.qty,
                    ))
                    .await?;
                *self.phase.lock().await = Phase::Flat;
                tracing::info!("darvas_box long exit");
            }
            return Ok(());
        } else if matches!(held, Phase::Short) {
            let range = highs[last_idx] - lows[last_idx];
            let mid = (highs[last_idx] + lows[last_idx]) / Decimal::from(2);
            if last_close > mid + range / Decimal::from(2) {
                let is_buy = true;
                let coid = format!("darvas-{}", ulid::Ulid::generate());
                self.gateway
                    .create_order(market_order(
                        &exchange,
                        &self.config.common.symbol,
                        coid,
                        is_buy,
                        self.config.qty,
                    ))
                    .await?;
                *self.phase.lock().await = Phase::Flat;
                tracing::info!("darvas_box short exit");
            }
            return Ok(());
        }

        // Scanning for a new N-day high in the window before the box.
        // The box occupies exactly `box_bars` bars immediately before the
        // last bar; the last bar is the potential breakout bar and is
        // therefore excluded from the box bounds.
        let scan_end = last_idx - self.config.box_bars;
        if scan_end < self.config.period {
            return Ok(());
        }
        let window_high = highs[..scan_end].iter().copied().max().expect("non-empty");
        let box_high = highs[scan_end..last_idx].iter().copied().max().expect("non-empty");
        let box_low = lows[scan_end..last_idx].iter().copied().min().expect("non-empty");

        // Was there a genuine new high before the box?
        let new_high = highs[scan_end - 1] >= window_high;
        // And is the box range meaningful (not degenerate)?
        let box_range = box_high - box_low;

        let signal = if new_high && last_close > box_high && box_range > Decimal::ZERO {
            Signal::Enter(true)
        } else if !new_high && last_close < box_low && box_range > Decimal::ZERO {
            Signal::Enter(false)
        } else {
            return Ok(());
        };

        let Signal::Enter(is_buy) = signal;

        let coid = format!("darvas-{}", ulid::Ulid::generate());
        self.gateway
            .create_order(market_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                self.config.qty,
            ))
            .await?;
        *self.phase.lock().await = match signal {
            Signal::Enter(true) => Phase::Long,
            Signal::Enter(false) => Phase::Short,
        };
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, %box_high, %box_low, "darvas_box signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for DarvasBox {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "darvas_box tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use proptest::prelude::*;
    use rust_decimal::Decimal;
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
            symbol = "SOLUSDT"
            period = 10
            box_bars = 3
            qty = "0.1"
            "#,
        )
        .expect("valid toml");
        let cfg = DarvasBoxConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 10);
        assert_eq!(cfg.box_bars, 3);
    }

    fn config() -> DarvasBoxConfig {
        DarvasBoxConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            period: 5,
            box_bars: 2,
            qty: dec!(0.5),
        }
    }

    fn bar(high: Decimal, low: Decimal, close: Decimal) -> market::Candle {
        market::Candle {
            high: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(high)),
            low: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(low)),
            close: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(close)),
            ..Default::default()
        }
    }

    struct ScriptedBars {
        candles: StdArc<StdMutex<Vec<market::Candle>>>,
    }

    impl ScriptedBars {
        fn of(candles: Vec<market::Candle>) -> Self {
            Self { candles: StdArc::new(StdMutex::new(candles)) }
        }
    }

    #[async_trait]
    impl MarketDataSource for ScriptedBars {
        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Err(PortError::Transport("no ticker".into()))
        }
        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Transport("no book".into()))
        }
        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            let candles = self.candles.lock().expect("candles mutex").clone();
            Ok(market::GetCandlesResponse { candles, ..Default::default() })
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, DarvasBox) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, DarvasBox::new(config(), gateway, market))
    }

    async fn sides(adapter: &MockAdapter) -> Vec<buffa::EnumValue<trading::OrderSide>> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup")
            .iter()
            .map(|o| o.side)
            .collect()
    }

    #[tokio::test]
    async fn breakout_above_box_top_goes_long() {
        // period=5, box_bars=2.
        // Bars 0-3: baseline uptrend (high 100, 101, 102, 100).
        // Bar 4 (scan_end - 1): new high at 106 ≥ window_high (= 102).
        // Bars 5-6 (box): consolidation, box_high = 104, box_low = 100.
        // Bar 7 (last): breakout close at 130 > box_high (104).
        //
        // scan_end = 7 - 2 = 5, period = 5, so scan_end < period is false ✓
        // window_high = max(highs[0..5]) = max(100,101,102,100,106) = 106
        // new_high = highs[4] >= 106 → 106 >= 106 = true ✓
        // box_high = max(highs[5..7]) = max(104,103) = 104
        // last_close = 130 > 104 ✓
        let (adapter, strategy) = run_strategy(vec![
            bar(dec!(100), dec!(90), dec!(95)),
            bar(dec!(101), dec!(91), dec!(96)),
            bar(dec!(102), dec!(92), dec!(97)),
            bar(dec!(100), dec!(93), dec!(98)),
            bar(dec!(106), dec!(94), dec!(99)), // new high ≥ window_high
            bar(dec!(104), dec!(100), dec!(102)), // box bar 1
            bar(dec!(103), dec!(101), dec!(103)), // box bar 2
            bar(dec!(130), dec!(102), dec!(130)), // breakout close > box_high
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Buy)),
            "should go long on breakout, got sides: {s:?}"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn a_poll_places_at_most_one_order(
            closes in prop::collection::vec(1i64..10_000, 0..32),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime");
            let candles: Vec<market::Candle> = closes
                .into_iter()
                .map(|c| {
                    let c = Decimal::from(c);
                    bar(c + Decimal::ONE, c - Decimal::ONE, c)
                })
                .collect();
            let (adapter, strategy) = runtime.block_on(run_strategy(candles));
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(sides(&adapter)).len() <= 1);
        }
    }
}
