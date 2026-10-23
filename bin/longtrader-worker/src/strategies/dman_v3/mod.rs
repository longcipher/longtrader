//! DMAN v3 — Dual Moving Average Nested directional strategy.
//!
//! A directional strategy using nested moving averages across multiple
//! timeframes. It computes fast and slow EMAs on the primary timeframe,
//! and a trend filter EMA on a higher timeframe. A long signal requires
//! the fast EMA to cross above the slow EMA while price is above the
//! trend filter. A short signal is the inverse.
//!
//! ```text
//!   trend = close > ema_trend
//!   if ema_fast > ema_slow && trend → buy
//!   if ema_fast < ema_slow && !trend → sell
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/directional-macd-ema/> (DMAN family)

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, TradingGateway},
    strategies::{
        CommonParams, Strategy, closes_of, fetch_candles, market_order, params_from_table,
    },
};

/// Configuration for the DMAN v3 strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct DmanV3Config {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Fast EMA window.
    #[serde(default = "default_fast")]
    pub fast_window: usize,
    /// Slow EMA window.
    #[serde(default = "default_slow")]
    pub slow_window: usize,
    /// Trend-filter EMA window (higher timeframe).
    #[serde(default = "default_trend")]
    pub trend_window: usize,
    /// Position size in base units.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_fast() -> usize {
    9
}
fn default_slow() -> usize {
    21
}
fn default_trend() -> usize {
    50
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl DmanV3Config {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// DMAN v3 signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmanSignal {
    Buy,
    Sell,
    NoTrade,
}

/// Generate a signal from the nested MA configuration.
#[must_use]
pub fn dman_signal(closes: &[Decimal], fast: usize, slow: usize, trend: usize) -> DmanSignal {
    if closes.len() < trend.max(slow).max(fast) {
        return DmanSignal::NoTrade;
    }
    let mut ema_fast = Ema::new(fast);
    let mut ema_slow = Ema::new(slow);
    let mut ema_trend = Ema::new(trend);
    for c in closes {
        ema_fast.push(*c);
        ema_slow.push(*c);
        ema_trend.push(*c);
    }
    let (Some(f), Some(s), Some(t)) = (ema_fast.value(), ema_slow.value(), ema_trend.value())
    else {
        return DmanSignal::NoTrade;
    };
    let last = match closes.last() {
        Some(c) => *c,
        None => return DmanSignal::NoTrade,
    };
    let trend_bullish = last > t;
    if f > s && trend_bullish {
        DmanSignal::Buy
    } else if f < s && !trend_bullish {
        DmanSignal::Sell
    } else {
        DmanSignal::NoTrade
    }
}

/// DMAN v3 strategy.
pub struct DmanV3 {
    config: DmanV3Config,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    has_position: tokio::sync::Mutex<bool>,
}

impl DmanV3 {
    pub fn new(
        config: DmanV3Config,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, has_position: tokio::sync::Mutex::new(false) }
    }

    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            u32::try_from(self.config.trend_window).unwrap_or(200),
        )
        .await?;
        let closes = closes_of(&candles);
        let signal = dman_signal(
            &closes,
            self.config.fast_window,
            self.config.slow_window,
            self.config.trend_window,
        );
        let mut has_pos = self.has_position.lock().await;

        match signal {
            DmanSignal::Buy if !*has_pos => {
                let coid = format!("dmanv3-{}", ulid::Ulid::generate());
                self.gateway
                    .create_order(market_order(
                        &exchange,
                        &self.config.common.symbol,
                        coid,
                        true,
                        self.config.qty,
                    ))
                    .await?;
                *has_pos = true;
            }
            DmanSignal::Sell if *has_pos => {
                let coid = format!("dmanv3-{}", ulid::Ulid::generate());
                self.gateway
                    .create_order(market_order(
                        &exchange,
                        &self.config.common.symbol,
                        coid,
                        false,
                        self.config.qty,
                    ))
                    .await?;
                *has_pos = false;
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for DmanV3 {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            fast = %self.config.fast_window,
            slow = %self.config.slow_window,
            trend = %self.config.trend_window,
            "starting DMAN v3"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "DMAN v3 tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn dman_signal_buy_on_uptrend_with_cross() {
        // Rising prices: fast EMA > slow EMA, close > trend EMA.
        let closes: Vec<Decimal> = (1..=60).map(|i| dec!(100) + Decimal::from(i)).collect();
        assert_eq!(dman_signal(&closes, 9, 21, 50), DmanSignal::Buy);
    }

    #[test]
    fn dman_signal_sell_on_downtrend() {
        let closes: Vec<Decimal> = (1..=60).map(|i| dec!(200) - Decimal::from(i)).collect();
        assert_eq!(dman_signal(&closes, 9, 21, 50), DmanSignal::Sell);
    }

    #[test]
    fn dman_signal_no_trade_warmup() {
        let closes = vec![dec!(100); 10];
        assert_eq!(dman_signal(&closes, 9, 21, 50), DmanSignal::NoTrade);
    }

    #[test]
    fn dman_signal_no_trade_mixed() {
        // Flat prices: no clear trend.
        let closes = vec![dec!(100); 60];
        assert_eq!(dman_signal(&closes, 9, 21, 50), DmanSignal::NoTrade);
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nfast_window = 12\nslow_window = 26\ntrend_window = 100",
        )
        .expect("toml");
        let cfg = DmanV3Config::from_params(&table).expect("parse");
        assert_eq!(cfg.fast_window, 12);
        assert_eq!(cfg.slow_window, 26);
        assert_eq!(cfg.trend_window, 100);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table =
            toml::from_str("symbol = \"ETH/USDT\"\nqty = \"0.5\"").expect("toml");
        let cfg = DmanV3Config::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.5));
    }
}
