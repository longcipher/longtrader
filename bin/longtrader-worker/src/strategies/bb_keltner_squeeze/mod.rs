//! Bollinger-Keltner squeeze breakout (TTM Squeeze) over the worker candle-poll model.
//!
//! Constructs both Bollinger Bands (SMA ± 2σ) and Keltner Channel
//! (EMA ± 1.5 ATR). A "squeeze" fires when both Bollinger bands contract
//! completely inside the Keltner Channel — a low-volatility coil that
//! historically precedes a directional expansion.
//!
//! On squeeze release, the direction is set by the Bollinger breakout:
//! a close above the upper Bollinger goes long; a close below the lower
//! Bollinger goes short. The squeeze flag must re-arm (price must re-coil)
//! before another entry, so consecutive release bars do not churn the book.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::{BollingerBands, KeltnerChannel as KcIndicator},
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the Bollinger-Keltner squeeze strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct BbKeltnerSqueezeConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_window")]
    pub window: usize,
    #[serde(default = "default_bb_mult")]
    pub bb_mult: Decimal,
    #[serde(default = "default_kc_mult")]
    pub kc_mult: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_window() -> usize {
    20
}
fn default_bb_mult() -> Decimal {
    Decimal::from(2)
}
fn default_kc_mult() -> Decimal {
    Decimal::new(15, 1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl BbKeltnerSqueezeConfig {
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

/// Bollinger-Keltner squeeze breakout strategy.
pub struct BbKeltnerSqueeze {
    config: BbKeltnerSqueezeConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl BbKeltnerSqueeze {
    pub fn new(
        config: BbKeltnerSqueezeConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let need = (self.config.window * 2 + 10) as u32;
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            need,
        )
        .await?;

        // Collect closes for Bollinger + feed both indicators across the window.
        let mut closes = Vec::with_capacity(candles.len());
        let mut kc = KcIndicator::new(self.config.window, self.config.window, self.config.kc_mult);
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
            kc.push(h, l, c);
            closes.push(c);
        }

        let Some(bands) = BollingerBands::compute(&closes, self.config.bb_mult) else {
            tracing::debug!("bb_keltner_squeeze: bollinger warmup incomplete");
            return Ok(());
        };
        let (Some(kc_upper), Some(kc_lower)) = (kc.upper(), kc.lower()) else {
            tracing::debug!("bb_keltner_squeeze: keltner warmup incomplete");
            return Ok(());
        };
        let last_close = *closes.last().expect("non-empty after bands");

        // Is the Bollinger band fully inside the Keltner Channel?
        let squeezed = bands.upper < kc_upper && bands.lower > kc_lower;

        let held = *self.phase.lock().await;
        let signal = match held {
            Phase::Flat => {
                // Only enter when we are in a squeeze AND price breaks a Bollinger.
                if squeezed && last_close > bands.upper {
                    Signal::Enter(true)
                } else if squeezed && last_close < bands.lower {
                    Signal::Enter(false)
                } else {
                    return Ok(());
                }
            }
            Phase::Long => {
                // Exit when squeeze releases (Bollinger re-expands outside Keltner).
                if squeezed {
                    return Ok(());
                }
                Signal::Exit
            }
            Phase::Short => {
                if squeezed {
                    return Ok(());
                }
                Signal::Exit
            }
        };

        let is_buy = match signal {
            Signal::Enter(is_buy) => is_buy,
            Signal::Exit => held.exit_side(),
        };

        let coid = format!("squeeze-{}", ulid::Ulid::generate());
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
        tracing::info!(
            side = if is_buy { "buy" } else { "sell" },
            squeezed,
            "squeeze signal traded"
        );
        Ok(())
    }
}

#[async_trait]
impl Strategy for BbKeltnerSqueeze {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "bb_keltner_squeeze tick failed");
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
            symbol = "BTCUSDT"
            window = 10
            bb_mult = "2.0"
            kc_mult = "1.5"
            qty = "0.05"
            "#,
        )
        .expect("valid toml");
        let cfg = BbKeltnerSqueezeConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.window, 10);
        assert_eq!(cfg.bb_mult.to_string(), "2.0");
        assert_eq!(cfg.qty.to_string(), "0.05");
    }

    fn config() -> BbKeltnerSqueezeConfig {
        BbKeltnerSqueezeConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "1h".to_string(),
                poll_secs: 60,
            },
            window: 3,
            bb_mult: dec!(1),
            kc_mult: dec!(5),
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

    async fn run_strategy(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, BbKeltnerSqueeze) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(candles));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, BbKeltnerSqueeze::new(config(), gateway, market))
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
    async fn squeeze_breakout_goes_long() {
        // Flat bars coil (BB collapses toward the single close), then a breakout
        // close breaks above the upper Bollinger. With kc_mult=5 the Keltner
        // stays wide enough to hold the BB inside (the squeeze).
        let (adapter, strategy) = run_strategy(vec![
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(101), dec!(99), dec!(100)),
            candle(dec!(150), dec!(100), dec!(150)), // squeeze release upward
        ])
        .await;
        strategy.tick().await.expect("tick");
        let s = sides(&adapter).await;
        assert!(
            s.contains(&buffa::EnumValue::Known(trading::OrderSide::Buy)),
            "should go long on squeeze release"
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
                    candle(c + Decimal::ONE, c - Decimal::ONE, c)
                })
                .collect();
            let (adapter, strategy) = runtime.block_on(run_strategy(candles));
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(sides(&adapter)).len() <= 1);
        }
    }
}
