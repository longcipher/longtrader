//! Bollinger Bands v2 — directional mean-reversion strategy.
//!
//! A directional strategy that uses Bollinger Bands to identify overbought
//! and oversold conditions. When the price touches the lower band, it goes
//! long (mean reversion to the middle). When it touches the upper band, it
//! goes short. A confirmation filter using the band width (squeeze) avoids
//! trading in low-volatility regimes.
//!
//! ```text
//!   if close ≤ lower_band → buy (mean reversion up)
//!   if close ≥ upper_band → sell (mean reversion down)
//!   if band_width < squeeze_threshold → no trade (squeeze)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/bollinger-bands-v2/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::BollingerBands,
    ports::{MarketDataSource, TradingGateway},
    strategies::{
        CommonParams, Strategy, closes_of, fetch_candles, market_order, params_from_table,
    },
};

/// Configuration for the Bollinger v2 strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct BollingerV2Config {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Bollinger window length.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Bollinger standard deviation multiplier.
    #[serde(default = "default_mult")]
    pub mult: Decimal,
    /// Minimum band width as fraction of middle to allow trades (squeeze filter).
    #[serde(default = "default_squeeze")]
    pub squeeze_threshold: Decimal,
    /// Position size in base units.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_window() -> usize {
    20
}
fn default_mult() -> Decimal {
    dec!(2)
}
fn default_squeeze() -> Decimal {
    dec!(0.01)
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl BollingerV2Config {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Bollinger v2 signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BollingerSignal {
    Buy,
    Sell,
    NoTrade,
}

/// Generate a signal from Bollinger bands and the latest close.
#[must_use]
pub fn bollinger_signal(
    close: Decimal,
    bands: &crate::indicators::Bands,
    squeeze_threshold: Decimal,
) -> BollingerSignal {
    let width = (bands.upper - bands.lower) / bands.middle;
    if width < squeeze_threshold {
        return BollingerSignal::NoTrade;
    }
    if close <= bands.lower {
        BollingerSignal::Buy
    } else if close >= bands.upper {
        BollingerSignal::Sell
    } else {
        BollingerSignal::NoTrade
    }
}

/// Bollinger v2 strategy.
pub struct BollingerV2 {
    config: BollingerV2Config,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    has_position: tokio::sync::Mutex<bool>,
}

impl BollingerV2 {
    pub fn new(
        config: BollingerV2Config,
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
            u32::try_from(self.config.window).unwrap_or(200),
        )
        .await?;
        let closes = closes_of(&candles);
        let Some(bands) = BollingerBands::compute(&closes, self.config.mult) else {
            return Ok(());
        };
        let Some(close) = closes.last().copied() else {
            return Ok(());
        };
        let signal = bollinger_signal(close, &bands, self.config.squeeze_threshold);
        let mut has_pos = self.has_position.lock().await;

        match signal {
            BollingerSignal::Buy if !*has_pos => {
                let coid = format!("bollv2-{}", ulid::Ulid::generate());
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
            BollingerSignal::Sell if *has_pos => {
                let coid = format!("bollv2-{}", ulid::Ulid::generate());
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
impl Strategy for BollingerV2 {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            window = %self.config.window,
            mult = %self.config.mult,
            "starting Bollinger v2"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "Bollinger v2 tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::indicators::Bands;

    #[test]
    fn bollinger_signal_buy_at_lower() {
        let bands = Bands { middle: dec!(100), upper: dec!(110), lower: dec!(90) };
        assert_eq!(bollinger_signal(dec!(89), &bands, dec!(0.01)), BollingerSignal::Buy);
    }

    #[test]
    fn bollinger_signal_sell_at_upper() {
        let bands = Bands { middle: dec!(100), upper: dec!(110), lower: dec!(90) };
        assert_eq!(bollinger_signal(dec!(111), &bands, dec!(0.01)), BollingerSignal::Sell);
    }

    #[test]
    fn bollinger_signal_no_trade_in_middle() {
        let bands = Bands { middle: dec!(100), upper: dec!(110), lower: dec!(90) };
        assert_eq!(bollinger_signal(dec!(100), &bands, dec!(0.01)), BollingerSignal::NoTrade);
    }

    #[test]
    fn bollinger_signal_squeeze_filter() {
        // Width = 0.005 < 0.01 squeeze threshold.
        let bands = Bands {
            middle: dec!(100),
            upper: dec!(100) + dec!(0.25),
            lower: dec!(100) - dec!(0.25),
        };
        assert_eq!(bollinger_signal(dec!(99), &bands, dec!(0.01)), BollingerSignal::NoTrade);
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nwindow = 30\nmult = \"2.5\"\nsqueeze_threshold = \"0.02\"",
        )
        .expect("toml");
        let cfg = BollingerV2Config::from_params(&table).expect("parse");
        assert_eq!(cfg.window, 30);
        assert_eq!(cfg.mult, dec!(2.5));
        assert_eq!(cfg.squeeze_threshold, dec!(0.02));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table =
            toml::from_str("symbol = \"ETH/USDT\"\nqty = \"0.5\"").expect("toml");
        let cfg = BollingerV2Config::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.5));
    }
}
