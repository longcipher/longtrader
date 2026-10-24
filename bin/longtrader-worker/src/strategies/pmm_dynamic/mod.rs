//! PMM Dynamic — Pure Market Making with dynamic spread adjustment.
//!
//! An enhanced version of Pure Market Making that dynamically adjusts
//! the bid/ask half-spreads based on real-time volatility. When volatility
//! is high, spreads widen to compensate for risk. When volatility is low,
//! spreads narrow to remain competitive.
//!
//! ```text
//!   dynamic_spread = base_spread · (current_vol / base_vol) · inventory_skew_factor
//!   bid = mid · (1 − dynamic_spread_bid)
//!   ask = mid · (1 + dynamic_spread_ask)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/pure-market-making/> (dynamic mode)

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::decimal_sqrt,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, cancel_all_orders, limit_order, params_from_table},
};

/// Configuration for the PMM Dynamic strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct PmmDynamicConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Base half-spread as fraction.
    #[serde(default = "default_base_spread")]
    pub base_spread: Decimal,
    /// Base volatility (reference for scaling).
    #[serde(default = "default_base_vol")]
    pub base_vol: Decimal,
    /// Rolling window for realised volatility.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Inventory skew factor.
    #[serde(default = "default_skew")]
    pub inventory_skew: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_base_spread() -> Decimal {
    dec!(0.005)
}
fn default_base_vol() -> Decimal {
    dec!(0.01)
}
fn default_window() -> usize {
    20
}
fn default_skew() -> Decimal {
    dec!(0.5)
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl PmmDynamicConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Rolling volatility estimator for dynamic spread.
#[derive(Debug, Clone)]
pub struct VolTracker {
    window: usize,
    buf: VecDeque<Decimal>,
}

impl VolTracker {
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
        Some(decimal_sqrt(sq.checked_div(count)?))
    }
}

/// Compute dynamic spreads given volatility.
#[must_use]
pub fn dynamic_spreads(
    base_spread: Decimal,
    base_vol: Decimal,
    current_vol: Option<Decimal>,
    inventory_skew: Decimal,
    inventory: Decimal,
) -> Option<(Decimal, Decimal)> {
    if base_spread < Decimal::ZERO || base_vol <= Decimal::ZERO {
        return None;
    }
    let vol = current_vol.unwrap_or(base_vol);
    let vol_ratio = vol / base_vol;
    let base = base_spread * vol_ratio;
    // Inventory skew: positive inventory → wider bid (less aggressive), narrower ask.
    let skew_term = inventory_skew * inventory;
    let bid_spread = (base * (Decimal::ONE + skew_term)).max(Decimal::ZERO);
    let ask_spread = (base * (Decimal::ONE - skew_term)).max(Decimal::ZERO);
    Some((bid_spread, ask_spread))
}

/// PMM Dynamic strategy.
pub struct PmmDynamic {
    config: PmmDynamicConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    inventory: tokio::sync::Mutex<Decimal>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
    vol_tracker: tokio::sync::Mutex<VolTracker>,
}

impl PmmDynamic {
    pub fn new(
        config: PmmDynamicConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let window = config.window;
        Self {
            config,
            gateway,
            market,
            inventory: tokio::sync::Mutex::new(Decimal::ZERO),
            last_mid: tokio::sync::Mutex::new(None),
            vol_tracker: tokio::sync::Mutex::new(VolTracker::new(window)),
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
        let mut vol_tracker = self.vol_tracker.lock().await;
        if let Some(prev) = *last {
            let ret = (mid - prev).abs();
            vol_tracker.push(ret);
        }
        *last = Some(mid);
        let vol = vol_tracker.vol();
        drop(vol_tracker);
        drop(last);

        let inv = *self.inventory.lock().await;
        let Some((bid_spread, ask_spread)) = dynamic_spreads(
            self.config.base_spread,
            self.config.base_vol,
            vol,
            self.config.inventory_skew,
            inv,
        ) else {
            return Ok(());
        };

        let bid = mid * (Decimal::ONE - bid_spread);
        let ask = mid * (Decimal::ONE + ask_spread);
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
            return Ok(());
        }
        let exchange = self.config.common.proto_exchange_id();
        cancel_all_orders(self.gateway.as_ref(), &exchange.clone(), &self.config.common.symbol)
            .await?;
        let bid_coid = String::from("pm");
        let ask_coid = String::from("pm");
        let _ = (bid_coid, ask_coid);
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                format!("pmmdyn-{}", ulid::Ulid::generate()),
                true,
                bid,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "PMM dynamic bid failed");
        }
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                format!("pmmdyn-{}", ulid::Ulid::generate()),
                false,
                ask,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "PMM dynamic ask failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for PmmDynamic {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            base_spread = %self.config.base_spread,
            base_vol = %self.config.base_vol,
            "starting PMM dynamic"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "PMM dynamic tick failed");
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
    fn vol_tracker_tracks_dispersion() {
        let mut v = VolTracker::new(5);
        v.push(dec!(1));
        v.push(dec!(2));
        v.push(dec!(3));
        assert!(v.vol().is_some());
    }

    #[test]
    fn vol_tracker_needs_two_samples() {
        let mut v = VolTracker::new(5);
        v.push(dec!(1));
        assert!(v.vol().is_none());
    }

    #[test]
    fn dynamic_spreads_scale_with_vol() {
        let (bid_low, ask_low) =
            dynamic_spreads(dec!(0.01), dec!(0.01), Some(dec!(0.01)), dec!(0), dec!(0))
                .expect("spreads");
        let (bid_high, ask_high) =
            dynamic_spreads(dec!(0.01), dec!(0.01), Some(dec!(0.04)), dec!(0), dec!(0))
                .expect("spreads");
        assert!(bid_high > bid_low);
        assert!(ask_high > ask_low);
    }

    #[test]
    fn dynamic_spreads_inventory_skew() {
        let (bid, ask) =
            dynamic_spreads(dec!(0.01), dec!(0.01), None, dec!(0.5), dec!(2)).expect("spreads");
        // Positive inventory → bid spread wider, ask spread narrower.
        assert!(bid > ask);
    }

    #[test]
    fn dynamic_spreads_rejects_bad_input() {
        assert!(dynamic_spreads(dec!(-0.01), dec!(0.01), None, dec!(0), dec!(0)).is_none());
        assert!(dynamic_spreads(dec!(0.01), dec!(0), None, dec!(0), dec!(0)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\nbase_spread = \"0.01\"\nbase_vol = \"0.02\"")
                .expect("toml");
        let cfg = PmmDynamicConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.base_spread, dec!(0.01));
        assert_eq!(cfg.base_vol, dec!(0.02));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nwindow = 50\ninventory_skew = \"0.8\"\nqty = \"0.5\"",
        )
        .expect("toml");
        let cfg = PmmDynamicConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.window, 50);
        assert_eq!(cfg.inventory_skew, dec!(0.8));
        assert_eq!(cfg.qty, dec!(0.5));
    }
}
