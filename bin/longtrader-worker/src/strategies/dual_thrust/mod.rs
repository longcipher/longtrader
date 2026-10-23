//! Dual Thrust opening-range breakout over the worker candle-poll model.
//!
//! Computes the highest-high / lowest-low / open over a rolling `period`-bar
//! window, then derives upper and lower trigger bands:
//!   - Upper = Open(last) + K1 * (HH - LL)   (buy trigger)
//!   - Lower = Open(last) - K2 * (HH - LL)   (sell trigger)
//!
//! A close above the upper band goes long; a close below the lower opens short.
//!
//! Phase state is edge-triggered in memory.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::DonchianChannel,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the Dual Thrust strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct DualThrustConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_period")]
    pub period: usize,
    #[serde(default = "default_k1")]
    pub k1: Decimal,
    #[serde(default = "default_k2")]
    pub k2: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_period() -> usize {
    5
}
fn default_k1() -> Decimal {
    Decimal::new(5, 1)
}
fn default_k2() -> Decimal {
    Decimal::new(5, 1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl DualThrustConfig {
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

/// Dual Thrust opening-range breakout.
pub struct DualThrust {
    config: DualThrustConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl DualThrust {
    pub fn new(
        config: DualThrustConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            (self.config.period + 5) as u32,
        )
        .await?;

        // Donchian covers HH / LL over the last `period` bars.
        let mut dc = DonchianChannel::new(self.config.period);
        let mut opens = Vec::with_capacity(candles.len());
        let mut last_close = None;
        for candle in &candles {
            let (Some(h), Some(l), Some(c), Some(o)) = (
                candle.high.as_option(),
                candle.low.as_option(),
                candle.close.as_option(),
                candle.open.as_option(),
            ) else {
                continue;
            };
            let (Ok(h), Ok(l), Ok(c), Ok(o)) = (
                longtrader_contract::ext::common_to_decimal(h),
                longtrader_contract::ext::common_to_decimal(l),
                longtrader_contract::ext::common_to_decimal(c),
                longtrader_contract::ext::common_to_decimal(o),
            ) else {
                continue;
            };
            dc.push(h, l);
            opens.push(o);
            last_close = Some(c);
        }

        let (Some(upper), Some(lower), Some(open)) =
            (dc.upper(), dc.lower(), opens.last().copied())
        else {
            tracing::debug!("dual_thrust warmup incomplete");
            return Ok(());
        };
        let range = upper - lower;
        let buy_trigger = open + self.config.k1 * range;
        let sell_trigger = open - self.config.k2 * range;

        let Some(close) = last_close else {
            return Ok(());
        };

        let held = *self.phase.lock().await;
        let signal = match held {
            Phase::Flat => {
                if close > buy_trigger {
                    Signal::Enter(true)
                } else if close < sell_trigger {
                    Signal::Enter(false)
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit when price crosses back below the midpoint of the range.
                if close < open {
                    Signal::Exit
                } else {
                    return Ok(());
                }
            }
            Phase::Short => {
                if close > open {
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

        let coid = format!("dt-{}", ulid::Ulid::generate());
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
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, %buy_trigger, %sell_trigger, "dual_thrust signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for DualThrust {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "dual_thrust tick failed");
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
            k1 = "0.4"
            k2 = "0.6"
            qty = "0.5"
            "#,
        )
        .expect("valid toml");
        let cfg = DualThrustConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.period, 7);
        assert_eq!(cfg.k1.to_string(), "0.4");
        assert_eq!(cfg.k2.to_string(), "0.6");
    }

    fn config() -> DualThrustConfig {
        DualThrustConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            period: 3,
            k1: dec!(1),
            k2: dec!(1),
            qty: dec!(0.5),
        }
    }

    fn candle(open: Decimal, high: Decimal, low: Decimal, close: Decimal) -> market::Candle {
        market::Candle {
            open: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(open)),
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, DualThrust) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, DualThrust::new(config(), gateway, market))
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
    async fn breakout_above_upper_threshold_goes_long() {
        // Window=3 so Donchian reads the last 3 bars. All bars share the same
        // high/low, so the breakout bar keeps the range pinned at 15 while its
        // close sails above the trigger.
        let (adapter, strategy) = run_strategy(vec![
            candle(dec!(100), dec!(110), dec!(95), dec!(100)),
            candle(dec!(100), dec!(110), dec!(95), dec!(100)),
            candle(dec!(100), dec!(110), dec!(95), dec!(100)),
            candle(dec!(100), dec!(110), dec!(95), dec!(100)),
            candle(dec!(100), dec!(110), dec!(95), dec!(100)),
            candle(dec!(100), dec!(110), dec!(100), dec!(120)), // close > open + K1 * range = 115
        ])
        .await;
        strategy.tick().await.expect("tick");
        assert!(sides(&adapter).await.contains(&buffa::EnumValue::Known(trading::OrderSide::Buy)));
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
                    candle(c, c + Decimal::ONE, c - Decimal::ONE, c)
                })
                .collect();
            let (adapter, strategy) = runtime.block_on(run_strategy(candles));
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(sides(&adapter)).len() <= 1);
        }
    }
}
