//! Composite momentum factor over the worker candle-poll model.
//!
//! Builds a momentum score from two components:
//!   - Raw return over a rolling `period` (trend strength),
//!   - RSI filtered to confirm direction (velocity).
//!
//! Enters long when return > threshold AND RSI > 50 + velocity band; enters short when return <
//! -threshold AND RSI < 50 - velocity band.
//!
//! Exits when momentum recedes below the neutral zone.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::Rsi,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the momentum factor strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct MomentumFactorConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_return_threshold")]
    pub return_threshold: Decimal,
    #[serde(default = "default_velocity_band")]
    pub velocity_band: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    14
}
fn default_return_threshold() -> Decimal {
    Decimal::new(2, 2)
}
fn default_velocity_band() -> Decimal {
    Decimal::from(5)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl MomentumFactorConfig {
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

/// Composite momentum factor strategy.
pub struct MomentumFactor {
    config: MomentumFactorConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl MomentumFactor {
    pub fn new(
        config: MomentumFactorConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let need = self.config.period * 3 + 10;
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            need as u32,
        )
        .await?;

        let mut rsi = Rsi::new(self.config.period);
        let mut closes = Vec::with_capacity(candles.len());
        for candle in &candles {
            let Some(c) = candle.close.as_option() else { continue };
            let Ok(c) = longtrader_contract::ext::common_to_decimal(c) else { continue };
            rsi.push(c);
            closes.push(c);
        }

        let (Some(rsi_val), Some(old_close)) =
            (rsi.value(), closes.get(closes.len().saturating_sub(self.config.period + 1)).copied())
        else {
            tracing::debug!("momentum_factor warmup incomplete");
            return Ok(());
        };

        let last_close = *closes.last().expect("non-empty");
        let raw_return = if old_close > Decimal::ZERO {
            (last_close - old_close) / old_close
        } else {
            Decimal::ZERO
        };
        let mid = Decimal::from(50);

        let held = *self.phase.lock().await;
        let signal = match held {
            Phase::Flat => {
                // Require both aligned:
                // 1. return exceeds the threshold (momentum confirmation),
                // 2. RSI sits above (or below) the neutral zone by the band.
                if raw_return > self.config.return_threshold &&
                    rsi_val > mid + self.config.velocity_band
                {
                    Signal::Enter(true)
                } else if raw_return < -self.config.return_threshold &&
                    rsi_val < mid - self.config.velocity_band
                {
                    Signal::Enter(false)
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit once return drops below a smaller threshold (half entry).
                if raw_return < self.config.return_threshold / Decimal::from(2) {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
            Phase::Short => {
                if raw_return > -self.config.return_threshold / Decimal::from(2) {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
        };

        let is_buy = match signal {
            Signal::Enter(b) => b,
            Signal::Exit => held.exit_side(),
        };

        let coid = format!("mom-{}", ulid::Ulid::generate());
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
            %raw_return,
            %rsi_val,
            "momentum_factor signal traded"
        );
        Ok(())
    }
}

#[async_trait]
impl Strategy for MomentumFactor {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "momentum_factor tick failed");
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
            period = 7
            return_threshold = "0.05"
            velocity_band = "8"
            qty = "0.1"
            "#,
        )
        .expect("valid toml");
        let cfg = MomentumFactorConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 7);
        assert_eq!(cfg.return_threshold.to_string(), "0.05");
        assert_eq!(cfg.velocity_band.to_string(), "8");
    }

    fn config() -> MomentumFactorConfig {
        MomentumFactorConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            period: 3,
            return_threshold: dec!(0.05),
            velocity_band: dec!(5),
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, MomentumFactor) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, MomentumFactor::new(config(), gateway, market))
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
    async fn strong_rally_goes_long() {
        let (adapter, strategy) = run_strategy(vec![
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(103), dec!(100), dec!(103)),
            bar(dec!(106), dec!(103), dec!(106)),
            bar(dec!(110), dec!(106), dec!(110)),
            bar(dec!(150), dec!(140), dec!(150)),
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Buy)),
            "strong rally should go long"
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
                    bar(c + Decimal::ONE, c - Decimal::ONE, c)
                })
                .collect();
            let (adapter, strategy) = runtime.block_on(run_strategy(candles));
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(sides(&adapter)).len() <= 1);
        }
    }
}
