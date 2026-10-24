//! ADX trend-strength filter with DI crossover over the worker candle-poll model.
//!
//! Computes the Average Directional Index (ADX) and the directional indicators
//! +DI / -DI. Enters long when ADX > threshold (strong trend) and +DI crosses
//! above -DI; enters short when ADX > threshold and -DI crosses above +DI.
//! Exits when ADX falls below the threshold (trend weakening).
//!
//! This is the classic Wilder trend-strength gate: it only trades when the
//! market is actually directional and stands aside in ranges.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::Adx,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the ADX trend strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AdxTrendConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_threshold")]
    pub threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    14
}
fn default_threshold() -> Decimal {
    Decimal::from(25)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl AdxTrendConfig {
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

impl Signal {
    const fn target(self) -> Phase {
        match self {
            Self::Enter(true) => Phase::Long,
            Self::Enter(false) => Phase::Short,
            Self::Exit => Phase::Flat,
        }
    }
}

/// ADX trend-strength strategy with DI crossover direction.
pub struct AdxTrend {
    config: AdxTrendConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl AdxTrend {
    pub fn new(
        config: AdxTrendConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    ///
    /// Returns early on warmup / data errors so it polls again next tick
    /// rather than committing capital on an uncertain read.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let need = (self.config.period * 3 + 10) as u32;
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            need,
        )
        .await?;
        let mut adx = Adx::new(self.config.period);
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
            adx.push(h, l, c);
        }

        let (Some(adx_val), Some(plus_di), Some(minus_di)) =
            (adx.value(), adx.plus_di(), adx.minus_di())
        else {
            tracing::debug!("adx_trend warmup incomplete");
            return Ok(());
        };

        let held = *self.phase.lock().await;
        let signal = match held {
            Phase::Flat => {
                // Strong trend: ADX must exceed the threshold.
                if adx_val > self.config.threshold {
                    match plus_di.cmp(&minus_di) {
                        std::cmp::Ordering::Greater => Signal::Enter(true),
                        std::cmp::Ordering::Less => Signal::Enter(false),
                        std::cmp::Ordering::Equal => return Ok(()),
                    }
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit when trend weakens or bears take the DI lead.
                if adx_val < self.config.threshold || minus_di > plus_di {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
            Phase::Short => {
                if adx_val < self.config.threshold || plus_di > minus_di {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
        };

        let is_buy = match signal {
            Signal::Enter(is_buy) => is_buy,
            Signal::Exit => held.exit_side(),
        };

        let coid = format!("adx-{}", ulid::Ulid::generate());
        self.gateway
            .create_order(market_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                self.config.qty,
            ))
            .await?;
        *self.phase.lock().await = signal.target();
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, %adx_val, "adx signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for AdxTrend {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "adx_trend tick failed");
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
            symbol = "ETHUSDT"
            period = 7
            threshold = "30"
            qty = "0.1"
            "#,
        )
        .expect("valid toml");
        let cfg = AdxTrendConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 7);
        assert_eq!(cfg.threshold.to_string(), "30");
        assert_eq!(cfg.qty.to_string(), "0.1");
    }

    fn config() -> AdxTrendConfig {
        AdxTrendConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            period: 3,
            threshold: dec!(20),
            qty: dec!(0.5),
        }
    }

    fn candle(high: Decimal, low: Decimal, close: Decimal) -> market::Candle {
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, AdxTrend) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, AdxTrend::new(config(), gateway, market))
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
    async fn strong_uptrend_with_plus_di_lead_goes_long() {
        // A series of steadily rising bars builds +DI well above -DI and pushes ADX up.
        let (adapter, strategy) = run_strategy(vec![
            candle(dec!(100), dec!(90), dec!(95)),
            candle(dec!(110), dec!(100), dec!(105)),
            candle(dec!(120), dec!(110), dec!(115)),
            candle(dec!(130), dec!(120), dec!(125)),
            candle(dec!(140), dec!(130), dec!(135)),
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Buy)),
            "strong uptrend should go long"
        );
    }

    #[tokio::test]
    async fn strong_downtrend_with_minus_di_lead_goes_short() {
        let (adapter, strategy) = run_strategy(vec![
            candle(dec!(100), dec!(90), dec!(95)),
            candle(dec!(95), dec!(80), dec!(85)),
            candle(dec!(85), dec!(70), dec!(75)),
            candle(dec!(75), dec!(60), dec!(65)),
            candle(dec!(65), dec!(50), dec!(55)),
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Sell)),
            "strong downtrend should go short"
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
                .expect("runtime creation");
            let candles: Vec<market::Candle> = closes
                .into_iter()
                .map(|c| {
                    let c = Decimal::from(c);
                    candle(c + Decimal::ONE, c - Decimal::ONE, c)
                })
                .collect();
            let (adapter, strategy) = runtime.block_on(run_strategy(candles));
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(sides(&adapter)).len() <= 1);
        }
    }
}
