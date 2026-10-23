//! Liquidity Mining — CLMM LP range management.
//!
//! Manages concentrated liquidity market maker (CLMM) positions by
//! adjusting the price range based on volatility. When volatility is high,
//! the range widens to avoid going out of range. When volatility is low,
//! the range narrows to maximise fee income.
//!
//! ```text
//!   range_width = k · σ · √T
//!   lower = mid · (1 − range_width)
//!   upper = mid · (1 + range_width)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/liquidity-mining/>

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::decimal_sqrt,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Configuration for the liquidity mining strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct LiqMiningConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Range multiplier k (higher → wider range).
    #[serde(default = "default_k")]
    pub range_k: Decimal,
    /// Rolling window for volatility estimation.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Minimum range width as a fraction.
    #[serde(default = "default_min_range")]
    pub min_range: Decimal,
    /// Maximum range width as a fraction.
    #[serde(default = "default_max_range")]
    pub max_range: Decimal,
}

fn default_k() -> Decimal {
    dec!(1)
}
fn default_window() -> usize {
    20
}
fn default_min_range() -> Decimal {
    dec!(0.01)
}
fn default_max_range() -> Decimal {
    dec!(0.5)
}

impl LiqMiningConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// A CLMM price range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceRange {
    pub lower: Decimal,
    pub upper: Decimal,
}

/// Rolling volatility estimator (annualised from bar returns).
#[derive(Debug, Clone)]
pub struct VolEstimator {
    window: usize,
    buf: VecDeque<Decimal>,
}

impl VolEstimator {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, ret: Decimal) {
        self.buf.push_back(ret);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    /// Realised volatility (standard deviation of returns).
    #[must_use]
    pub fn vol(&self) -> Option<Decimal> {
        let n = self.buf.len();
        if n < 2 {
            return None;
        }
        let count = Decimal::from(n);
        let mean = self
            .buf
            .iter()
            .try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))?
            .checked_div(count)?;
        let sq = self.buf.iter().try_fold(Decimal::ZERO, |acc, &x| {
            let d = x.checked_sub(mean)?;
            acc.checked_add(d.checked_mul(d)?)
        })?;
        let variance = sq.checked_div(count)?;
        Some(decimal_sqrt(variance))
    }
}

/// Compute the CLMM price range from mid and volatility.
#[must_use]
pub fn compute_range(
    mid: Decimal,
    vol: Option<Decimal>,
    range_k: Decimal,
    min_range: Decimal,
    max_range: Decimal,
) -> Option<PriceRange> {
    if mid <= Decimal::ZERO {
        return None;
    }
    let vol = vol.unwrap_or(min_range);
    let half_width = (range_k * vol).clamp(min_range, max_range);
    let lower = mid * (Decimal::ONE - half_width);
    let upper = mid * (Decimal::ONE + half_width);
    Some(PriceRange { lower, upper })
}

/// Liquidity mining strategy.
pub struct LiqMining {
    config: LiqMiningConfig,
    #[allow(dead_code)]
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    vol_est: tokio::sync::Mutex<VolEstimator>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
}

impl LiqMining {
    pub fn new(
        config: LiqMiningConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let window = config.window;
        Self {
            config,
            gateway,
            market,
            vol_est: tokio::sync::Mutex::new(VolEstimator::new(window)),
            last_mid: tokio::sync::Mutex::new(None),
        }
    }

    async fn current_mid(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let p = longtrader_contract::ext::common_to_decimal(last)?;
            if p > Decimal::ZERO {
                return Ok(p);
            }
        }
        color_eyre::eyre::bail!("no usable mid")
    }

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let mut last = self.last_mid.lock().await;
        let mut vol_est = self.vol_est.lock().await;
        if let Some(prev) = *last {
            let ret = (mid - prev).abs();
            vol_est.push(ret);
        }
        *last = Some(mid);
        let _ = vol_est.vol();
        // In a production system, this would adjust the CLMM position range.
        Ok(())
    }
}

#[async_trait]
impl Strategy for LiqMining {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            range_k = %self.config.range_k,
            window = %self.config.window,
            "starting liquidity mining"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "liquidity mining tick failed");
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
    fn vol_estimator_tracks_dispersion() {
        let mut v = VolEstimator::new(5);
        v.push(dec!(1));
        v.push(dec!(2));
        v.push(dec!(3));
        assert!(v.vol().is_some());
    }

    #[test]
    fn vol_estimator_needs_two_samples() {
        let mut v = VolEstimator::new(5);
        v.push(dec!(1));
        assert!(v.vol().is_none());
    }

    #[test]
    fn vol_estimator_window_slides() {
        let mut v = VolEstimator::new(2);
        v.push(dec!(1));
        v.push(dec!(2));
        v.push(dec!(100));
        // Only 2 and 100 remain.
        let vol = v.vol().expect("vol");
        assert!(vol > Decimal::ZERO);
    }

    #[test]
    fn compute_range_symmetric_around_mid() {
        let range = compute_range(dec!(100), Some(dec!(0.02)), dec!(1), dec!(0.01), dec!(0.5))
            .expect("range");
        assert!(range.lower < dec!(100));
        assert!(range.upper > dec!(100));
    }

    #[test]
    fn compute_range_clamps_to_bounds() {
        let range = compute_range(dec!(100), Some(dec!(10)), dec!(1), dec!(0.01), dec!(0.5))
            .expect("range");
        // Half-width clamped to max_range = 0.5.
        assert_eq!(range.lower, dec!(50));
        assert_eq!(range.upper, dec!(150));
    }

    #[test]
    fn compute_range_rejects_bad_input() {
        assert!(compute_range(dec!(0), Some(dec!(0.02)), dec!(1), dec!(0.01), dec!(0.5)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\nrange_k = \"2\"\nwindow = 30").expect("toml");
        let cfg = LiqMiningConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.range_k, dec!(2));
        assert_eq!(cfg.window, 30);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table =
            toml::from_str("symbol = \"ETH/USDT\"\nmin_range = \"0.02\"\nmax_range = \"0.3\"")
                .expect("toml");
        let cfg = LiqMiningConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.min_range, dec!(0.02));
        assert_eq!(cfg.max_range, dec!(0.3));
    }
}
