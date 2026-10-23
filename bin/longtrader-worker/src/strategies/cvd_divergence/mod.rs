//! Cumulative Volume Delta divergence overlay over the worker candle-poll model.
//!
//! Approximates a CVD line using each candle's buy/sell volume, then compares
//! the direction of the CVD against the rolling price move. A Z-score on the
//! difference flags divergence:
//!   - price rising + CVD diverging downward  → short mean-reversion.
//!   - price falling + CVD diverging upward    → long mean-reversion.
//!
//! Phase state clears on the CVD reverting to within band (no divergence
//! means exit the contrarian position).

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::ZScore,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the CVD divergence overlay.
#[derive(Debug, Clone, Deserialize)]
pub struct CvdDivergenceConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_threshold")]
    pub z_threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    20
}
fn default_threshold() -> Decimal {
    Decimal::new(15, 1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl CvdDivergenceConfig {
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
    Exit,
}

impl Phase {
    const fn exit_side(self) -> bool {
        matches!(self, Self::Short)
    }
}

/// CVD divergence overlay.
pub struct CvdDivergence {
    config: CvdDivergenceConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl CvdDivergence {
    pub fn new(
        config: CvdDivergenceConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// Candle body-direction proxy for buy/sell volume. A close above the
    /// midpoint of the candle body adds the body range; below subtracts it.
    /// Approximate but noiseless.
    fn bar_delta(high: Decimal, low: Decimal, close: Decimal) -> Decimal {
        let mid = (high + low) / Decimal::from(2);
        let range = (high - low).max(Decimal::ZERO);
        match close.cmp(&mid) {
            std::cmp::Ordering::Greater => range,
            std::cmp::Ordering::Less => -range,
            std::cmp::Ordering::Equal => Decimal::ZERO,
        }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let need = self.config.period * 2 + 5;
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            need as u32,
        )
        .await?;

        let mut deltas = Vec::with_capacity(candles.len());
        let mut closes = Vec::with_capacity(candles.len());
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
            deltas.push(Self::bar_delta(h, l, c));
            closes.push(c);
        }

        if deltas.len() < self.config.period {
            tracing::debug!("cvd_divergence warmup incomplete");
            return Ok(());
        }

        // Keep a rolling Z-score of the CVD residuals.
        // Raw CVD = rolling sum of delta; change measures momentum.
        let mut zs = ZScore::new(self.config.period);
        let mut acc = Decimal::ZERO;
        for &d in &deltas {
            acc += d;
            zs.push(acc);
        }
        let z = zs.value();

        // Price trend (first versus last close in the window).
        let price_move = closes.last().copied().unwrap_or(Decimal::ZERO) -
            closes[closes.len() - self.config.period];

        // Divergence is when price up / CVD zscore down OR price down / CVD zscore up.
        let divergence_long =
            price_move < Decimal::ZERO && z.unwrap_or(Decimal::ZERO) > self.config.z_threshold;
        let divergence_short =
            price_move > Decimal::ZERO && z.unwrap_or(Decimal::ZERO) < -self.config.z_threshold;

        let held = *self.phase.lock().await;
        let signal = match held {
            Phase::Flat => {
                if divergence_long {
                    Signal::Enter(true)
                } else if divergence_short {
                    Signal::Enter(false)
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit when divergence has returned to normal.
                if divergence_long {
                    return Ok(());
                }
                Signal::Exit
            }
            Phase::Short => {
                if divergence_short {
                    return Ok(());
                }
                Signal::Exit
            }
        };

        let is_buy = match signal {
            Signal::Enter(b) => b,
            Signal::Exit => held.exit_side(),
        };

        let coid = format!("cvd-{}", ulid::Ulid::generate());
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
            Signal::Exit => Phase::Flat,
        };
        tracing::info!(
            side = if is_buy { "buy" } else { "sell" },
            z = %z.unwrap_or(Decimal::ZERO),
            "cvd_divergence signal traded"
        );
        Ok(())
    }
}

#[async_trait]
impl Strategy for CvdDivergence {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "cvd_divergence tick failed");
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
            z_threshold = "2.0"
            qty = "0.05"
            "#,
        )
        .expect("valid toml");
        let cfg = CvdDivergenceConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 10);
        assert_eq!(cfg.z_threshold.to_string(), "2.0");
    }

    fn config() -> CvdDivergenceConfig {
        CvdDivergenceConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "15m".to_string(),
                poll_secs: 60,
            },
            period: 5,
            z_threshold: dec!(1),
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, CvdDivergence) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, CvdDivergence::new(config(), gateway, market))
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
    async fn incomplete_warmup_places_nothing() {
        let (adapter, strategy) = run_strategy(vec![
            bar(dec!(100), dec!(90), dec!(95)),
            bar(dec!(95), dec!(85), dec!(90)),
        ])
        .await;
        strategy.tick().await.expect("tick");
        assert!(sides(&adapter).await.is_empty());
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
