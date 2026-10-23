//! Mean-reversion Z-score strategy over the worker candle-poll model.
//!
//! Computes the rolling Z-score of log-style returns: a Z-score exceeding the
//! entry threshold signals a likely snap back to the mean, and triggers a
//! contrarian trade. The position is unwound once the Z-score retraces
//! inside the exit band, which is smaller than the exit threshold so
//! positions do not ping-pong open and closed.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::ZScore as ZsIndicator,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the mean-reversion strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct MeanReversionConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_entry_threshold")]
    pub entry_threshold: Decimal,
    #[serde(default = "default_exit_band")]
    pub exit_band: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    20
}
fn default_entry_threshold() -> Decimal {
    Decimal::from(2)
}
fn default_exit_band() -> Decimal {
    Decimal::new(5, 1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl MeanReversionConfig {
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

/// Mean-reversion Z-score strategy.
pub struct MeanReversion {
    config: MeanReversionConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl MeanReversion {
    pub fn new(
        config: MeanReversionConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
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

        let mut closes = Vec::with_capacity(candles.len());
        for candle in &candles {
            let Some(c) = candle.close.as_option() else { continue };
            let Ok(c) = longtrader_contract::ext::common_to_decimal(c) else { continue };
            closes.push(c);
        }

        if closes.len() < self.config.period {
            tracing::debug!("mean_reversion warmup incomplete");
            return Ok(());
        }

        // Z-score over the raw close series — when the windowed mean is the fair
        // value, a |Z| > threshold is a snap-back candidate.
        let mut zs = ZsIndicator::new(self.config.period);
        for &c in &closes {
            zs.push(c);
        }
        let z = zs.value();
        let held = *self.phase.lock().await;

        let signal = match held {
            Phase::Flat => {
                if let Some(zv) = z {
                    if zv > self.config.entry_threshold {
                        Signal::Enter(false) // overbought → short
                    } else if zv < -self.config.entry_threshold {
                        Signal::Enter(true) // oversold → long
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit when Z has retraced toward zero (within the band).
                if z.unwrap_or(Decimal::ZERO).abs() < self.config.exit_band {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
            Phase::Short => {
                if z.unwrap_or(Decimal::ZERO).abs() < self.config.exit_band {
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

        let coid = format!("mr-{}", ulid::Ulid::generate());
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
            "mean_reversion signal traded"
        );
        Ok(())
    }
}

#[async_trait]
impl Strategy for MeanReversion {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "mean_reversion tick failed");
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
            entry_threshold = "1.5"
            exit_band = "0.3"
            qty = "0.01"
            "#,
        )
        .expect("valid toml");
        let cfg = MeanReversionConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 10);
        assert_eq!(cfg.entry_threshold.to_string(), "1.5");
        assert_eq!(cfg.exit_band.to_string(), "0.3");
    }

    fn config() -> MeanReversionConfig {
        MeanReversionConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            period: 5,
            entry_threshold: dec!(1),
            exit_band: dec!(0.5),
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, MeanReversion) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, MeanReversion::new(config(), gateway, market))
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
    async fn extreme_positive_z_short() {
        let (adapter, strategy) = run_strategy(vec![
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(100), dec!(90), dec!(100)),
            bar(dec!(200), dec!(100), dec!(200)), // extreme out-of-band close
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Sell)),
            "extreme Z → short"
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
