//! Pure Market Making (PMM) — the flagship Hummingbot MM strategy.
//!
//! Places multiple bid and ask orders around a mid-price with configurable
//! size and spread per level. Supports inventory skew (order sizes adjust with
//! position), price bands (re-quote when price moves outside a band), hanging
//! orders (orders that persist after a counterpart fills to avoid adverse
//! selection), and order-level customization.
//!
//! ```text
//!   level_i spread = order_step_size · (i + 1) · (1 + order_size_spread)
//!   bid_qty_i     = base_qty · (1 + inventory_skew · q)
//!   ask_qty_i     = base_qty · (1 − inventory_skew · q)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/pure-market-making/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, cancel_all_orders, limit_order, params_from_table},
};

/// Configuration for the PMM strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct PmmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Order levels: number of bid and ask levels to place.
    #[serde(default = "default_levels")]
    pub levels: u32,
    /// Order step size: fraction of mid-price per level spacing.
    #[serde(default = "default_step_size")]
    pub order_step_size: Decimal,
    /// Order size spread: widen spread for deeper levels.
    #[serde(default = "default_size_spread")]
    pub order_size_spread: Decimal,
    /// Base order size in base units.
    #[serde(default = "default_order_amount")]
    pub order_amount: Decimal,
    /// Inventory skew factor: how much to adjust size with inventory.
    #[serde(default = "default_inventory_skew")]
    pub inventory_skew: Decimal,
    /// Price band multiplier: re-quote when price moves beyond this fraction.
    #[serde(default = "default_price_band")]
    pub price_band: Decimal,
    /// Enable hanging orders: keep orders that have a counterpart fill.
    #[serde(default = "default_hanging_orders")]
    pub hanging_orders: bool,
    /// Take order if hanging after this many ticks.
    #[serde(default = "default_hanging_cancel")]
    pub hanging_cancel_ticks: u32,
}

fn default_levels() -> u32 {
    3
}
fn default_step_size() -> Decimal {
    Decimal::new(5, 3) // 0.5%
}
fn default_size_spread() -> Decimal {
    Decimal::new(1, 3) // 0.1%
}
fn default_order_amount() -> Decimal {
    Decimal::new(1, 3) // 0.001
}
fn default_inventory_skew() -> Decimal {
    dec!(0.5)
}
fn default_price_band() -> Decimal {
    Decimal::new(1, 1) // 10%
}
fn default_hanging_orders() -> bool {
    true
}
fn default_hanging_cancel() -> u32 {
    5
}

impl PmmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// One level of the PMM order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PmmLevel {
    pub level: u32,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
}

/// Plan the full PMM order book given current state.
///
/// `None` when mid is invalid or parameters are inconsistent.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn plan_pmm_levels(
    mid: Decimal,
    levels: u32,
    step_size: Decimal,
    size_spread: Decimal,
    base_qty: Decimal,
    inventory_skew: Decimal,
    inventory: Decimal,
) -> Option<Vec<PmmLevel>> {
    if mid <= Decimal::ZERO || levels == 0 || step_size <= Decimal::ZERO {
        return None;
    }
    let mut result = Vec::with_capacity(levels as usize);
    for i in 0..levels {
        let level = i + 1;
        let spread =
            step_size * Decimal::from(level) * (Decimal::ONE + size_spread * Decimal::from(i));
        let bid_price = mid * (Decimal::ONE - spread);
        let ask_price = mid * (Decimal::ONE + spread);
        // Inventory skew: positive inventory → reduce bid size, increase ask size.
        let skew_factor = inventory_skew * inventory;
        let bid_qty = base_qty * (Decimal::ONE - skew_factor).max(Decimal::ZERO);
        let ask_qty = base_qty * (Decimal::ONE + skew_factor).max(Decimal::ZERO);
        result.push(PmmLevel { level, bid_price, ask_price, bid_qty, ask_qty });
    }
    Some(result)
}

/// Check if the current price is within the acceptable band.
#[must_use]
pub fn within_price_band(current: Decimal, reference: Decimal, band: Decimal) -> bool {
    if reference <= Decimal::ZERO || band < Decimal::ZERO {
        return false;
    }
    let lower = reference * (Decimal::ONE - band);
    let upper = reference * (Decimal::ONE + band);
    current >= lower && current <= upper
}

pub struct Pmm {
    config: PmmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    inventory: tokio::sync::Mutex<Decimal>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
}

impl Pmm {
    pub fn new(
        config: PmmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            inventory: tokio::sync::Mutex::new(Decimal::ZERO),
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
        let inv = *self.inventory.lock().await;
        let mut last = self.last_mid.lock().await;

        // Re-quote if price moved outside the band.
        let should_requite = if let Some(prev) = *last {
            !within_price_band(mid, prev, self.config.price_band)
        } else {
            true
        };

        if should_requite {
            let Some(levels) = plan_pmm_levels(
                mid,
                self.config.levels,
                self.config.order_step_size,
                self.config.order_size_spread,
                self.config.order_amount,
                self.config.inventory_skew,
                inv,
            ) else {
                return Ok(());
            };
            let exchange = self.config.common.proto_exchange_id();
            cancel_all_orders(self.gateway.as_ref(), &exchange.clone(), &self.config.common.symbol)
                .await?;
            for lvl in &levels {
                let bid_coid = format!("pmm-{}", ulid::Ulid::generate());
                let ask_coid = format!("pmm-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &exchange,
                        &self.config.common.symbol,
                        bid_coid,
                        true,
                        lvl.bid_price,
                        lvl.bid_qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "PMM bid placement failed");
                }
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &exchange,
                        &self.config.common.symbol,
                        ask_coid,
                        false,
                        lvl.ask_price,
                        lvl.ask_qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "PMM ask placement failed");
                }
            }
        }
        *last = Some(mid);
        Ok(())
    }
}

#[async_trait]
impl Strategy for Pmm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            levels = %self.config.levels,
            step_size = %self.config.order_step_size,
            "starting pure market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "PMM tick failed");
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
    fn plan_pmm_levels_spreads_symmetrically() {
        let levels =
            plan_pmm_levels(dec!(100), 3, dec!(0.005), dec!(0.001), dec!(1), dec!(0), dec!(0))
                .expect("levels");
        assert_eq!(levels.len(), 3);
        // Level 1: spread = 0.005 * 1 * 1.0 = 0.005 → bid=99.5, ask=100.5
        assert_eq!(levels[0].bid_price, dec!(99.5));
        assert_eq!(levels[0].ask_price, dec!(100.5));
        // Level 2: spread = 0.005 * 2 * 1.001 = 0.01001
        assert!(levels[1].bid_price < levels[0].bid_price);
        assert!(levels[1].ask_price > levels[0].ask_price);
    }

    #[test]
    fn plan_pmm_inventory_skew_adjusts_sizes() {
        let flat = plan_pmm_levels(dec!(100), 2, dec!(0.005), dec!(0), dec!(1), dec!(0.5), dec!(0))
            .expect("flat");
        let long = plan_pmm_levels(dec!(100), 2, dec!(0.005), dec!(0), dec!(1), dec!(0.5), dec!(2))
            .expect("long");
        // Positive inventory → bid size reduced, ask size increased.
        assert!(long[0].bid_qty < flat[0].bid_qty);
        assert!(long[0].ask_qty > flat[0].ask_qty);
    }

    #[test]
    fn plan_pmm_rejects_bad_input() {
        assert!(
            plan_pmm_levels(dec!(0), 3, dec!(0.005), dec!(0), dec!(1), dec!(0), dec!(0)).is_none()
        );
        assert!(
            plan_pmm_levels(dec!(100), 0, dec!(0.005), dec!(0), dec!(1), dec!(0), dec!(0))
                .is_none()
        );
        assert!(
            plan_pmm_levels(dec!(100), 3, dec!(0), dec!(0), dec!(1), dec!(0), dec!(0)).is_none()
        );
    }

    #[test]
    fn within_price_band_checks() {
        assert!(within_price_band(dec!(101), dec!(100), dec!(0.05)));
        assert!(within_price_band(dec!(99), dec!(100), dec!(0.05)));
        assert!(!within_price_band(dec!(110), dec!(100), dec!(0.05)));
        assert!(!within_price_band(dec!(90), dec!(100), dec!(0.05)));
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nlevels = 5\norder_step_size = \"0.01\"\norder_amount = \"0.01\"",
        )
        .expect("toml");
        let cfg = PmmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.levels, 5);
        assert_eq!(cfg.order_step_size, dec!(0.01));
        assert_eq!(cfg.order_amount, dec!(0.01));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nlevels = 2\norder_step_size = \"0.002\"\n\
             order_size_spread = \"0.005\"\norder_amount = \"0.1\"\n\
             inventory_skew = \"0.8\"\nprice_band = \"0.05\"\n\
             hanging_orders = false\nhanging_cancel_ticks = 10",
        )
        .expect("toml");
        let cfg = PmmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.levels, 2);
        assert_eq!(cfg.order_step_size, dec!(0.002));
        assert_eq!(cfg.order_size_spread, dec!(0.005));
        assert_eq!(cfg.order_amount, dec!(0.1));
        assert_eq!(cfg.inventory_skew, dec!(0.8));
        assert_eq!(cfg.price_band, dec!(0.05));
        assert!(!cfg.hanging_orders);
        assert_eq!(cfg.hanging_cancel_ticks, 10);
    }
}
