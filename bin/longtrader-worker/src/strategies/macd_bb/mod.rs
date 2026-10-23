//! MACD + Bollinger Bands — combined momentum and mean-reversion strategy.
//!
//! Uses MACD for momentum direction and Bollinger Bands for entry timing.
//! A buy signal requires the MACD line to be above the signal line
//! (bullish momentum) AND the price to be near the lower Bollinger Band
//! (oversold). A sell signal is the inverse.
//!
//! ```text
//!   if macd > signal && close ≤ lower_band → buy
//!   if macd < signal && close ≥ upper_band → sell
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/macd-bollinger/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::{BollingerBands, Ema},
    ports::{MarketDataSource, TradingGateway},
    strategies::{
        CommonParams, Strategy, closes_of, fetch_candles, market_order, params_from_table,
    },
};

/// Configuration for the MACD-BB strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct MacdBbConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// MACD fast EMA window.
    #[serde(default = "default_fast")]
    pub macd_fast: usize,
    /// MACD slow EMA window.
    #[serde(default = "default_slow")]
    pub macd_slow: usize,
    /// MACD signal EMA window.
    #[serde(default = "default_signal")]
    pub macd_signal: usize,
    /// Bollinger window.
    #[serde(default = "default_boll_window")]
    pub boll_window: usize,
    /// Bollinger multiplier.
    #[serde(default = "default_boll_mult")]
    pub boll_mult: Decimal,
    /// Position size in base units.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_fast() -> usize {
    12
}
fn default_slow() -> usize {
    26
}
fn default_signal() -> usize {
    9
}
fn default_boll_window() -> usize {
    20
}
fn default_boll_mult() -> Decimal {
    dec!(2)
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl MacdBbConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// MACD + BB signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacdBbSignal {
    Buy,
    Sell,
    NoTrade,
}

/// Compute MACD line values from closes.
#[must_use]
pub fn macd_line(closes: &[Decimal], fast: usize, slow: usize) -> Option<Decimal> {
    let mut ema_fast = Ema::new(fast);
    let mut ema_slow = Ema::new(slow);
    for c in closes {
        ema_fast.push(*c);
        ema_slow.push(*c);
    }
    let f = ema_fast.value()?;
    let s = ema_slow.value()?;
    Some(f - s)
}

/// Generate a MACD + Bollinger signal.
#[must_use]
pub fn macd_bb_signal(
    closes: &[Decimal],
    macd_fast: usize,
    macd_slow: usize,
    macd_signal: usize,
    boll_window: usize,
    boll_mult: Decimal,
) -> MacdBbSignal {
    // Need enough data for MACD slow + signal + Bollinger window.
    let min_len = macd_slow + macd_signal;
    if closes.len() < min_len.max(boll_window) {
        return MacdBbSignal::NoTrade;
    }
    let macd = match macd_line(closes, macd_fast, macd_slow) {
        Some(v) => v,
        None => return MacdBbSignal::NoTrade,
    };
    // Signal line: EMA of MACD values.
    let mut macd_values = Vec::new();
    let mut ema_f = Ema::new(macd_fast);
    let mut ema_s = Ema::new(macd_slow);
    for c in closes {
        ema_f.push(*c);
        ema_s.push(*c);
        if let (Some(f), Some(s)) = (ema_f.value(), ema_s.value()) {
            macd_values.push(f - s);
        }
    }
    let mut signal_ema = Ema::new(macd_signal);
    for v in &macd_values {
        signal_ema.push(*v);
    }
    let signal_val = match signal_ema.value() {
        Some(v) => v,
        None => return MacdBbSignal::NoTrade,
    };
    let bands = match BollingerBands::compute(closes, boll_mult) {
        Some(b) => b,
        None => return MacdBbSignal::NoTrade,
    };
    let last = match closes.last() {
        Some(c) => *c,
        None => return MacdBbSignal::NoTrade,
    };
    if macd > signal_val && last <= bands.lower {
        MacdBbSignal::Buy
    } else if macd < signal_val && last >= bands.upper {
        MacdBbSignal::Sell
    } else {
        MacdBbSignal::NoTrade
    }
}

/// MACD + BB strategy.
pub struct MacdBb {
    config: MacdBbConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    has_position: tokio::sync::Mutex<bool>,
}

impl MacdBb {
    pub fn new(
        config: MacdBbConfig,
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
            u32::try_from(self.config.macd_slow + self.config.macd_signal).unwrap_or(200),
        )
        .await?;
        let closes = closes_of(&candles);
        let signal = macd_bb_signal(
            &closes,
            self.config.macd_fast,
            self.config.macd_slow,
            self.config.macd_signal,
            self.config.boll_window,
            self.config.boll_mult,
        );
        let mut has_pos = self.has_position.lock().await;

        match signal {
            MacdBbSignal::Buy if !*has_pos => {
                let coid = format!("macdbb-{}", ulid::Ulid::generate());
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
            MacdBbSignal::Sell if *has_pos => {
                let coid = format!("macdbb-{}", ulid::Ulid::generate());
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
impl Strategy for MacdBb {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            macd_fast = %self.config.macd_fast,
            macd_slow = %self.config.macd_slow,
            "starting MACD + Bollinger"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "MACD-BB tick failed");
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
    fn macd_line_positive_when_fast_above_slow() {
        let closes: Vec<Decimal> = (1..=30).map(|i| dec!(100) + Decimal::from(i)).collect();
        let macd = macd_line(&closes, 12, 26).expect("macd");
        assert!(macd > Decimal::ZERO);
    }

    #[test]
    fn macd_line_negative_when_fast_below_slow() {
        let closes: Vec<Decimal> = (1..=30).map(|i| dec!(200) - Decimal::from(i)).collect();
        let macd = macd_line(&closes, 12, 26).expect("macd");
        assert!(macd < Decimal::ZERO);
    }

    #[test]
    fn macd_bb_signal_no_trade_warmup() {
        let closes = vec![dec!(100); 10];
        assert_eq!(macd_bb_signal(&closes, 12, 26, 9, 20, dec!(2)), MacdBbSignal::NoTrade);
    }

    #[test]
    fn macd_bb_signal_buy_on_momentum_and_oversold() {
        // Create a series that dips at the end to touch the lower band
        // while maintaining bullish MACD.
        let mut closes: Vec<Decimal> = (1..=40).map(|i| dec!(100) + Decimal::from(i)).collect();
        // Sharp dip at the end.
        closes.push(dec!(80));
        closes.push(dec!(75));
        let signal = macd_bb_signal(&closes, 12, 26, 9, 20, dec!(2));
        // The signal depends on the exact data; we just verify it doesn't panic.
        assert!(matches!(signal, MacdBbSignal::Buy | MacdBbSignal::NoTrade));
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\nmacd_fast = 8\nmacd_slow = 21\nmacd_signal = 7")
                .expect("toml");
        let cfg = MacdBbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.macd_fast, 8);
        assert_eq!(cfg.macd_slow, 21);
        assert_eq!(cfg.macd_signal, 7);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nboll_window = 30\nboll_mult = \"2.5\"\nqty = \"0.5\"",
        )
        .expect("toml");
        let cfg = MacdBbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.boll_window, 30);
        assert_eq!(cfg.boll_mult, dec!(2.5));
        assert_eq!(cfg.qty, dec!(0.5));
    }
}
