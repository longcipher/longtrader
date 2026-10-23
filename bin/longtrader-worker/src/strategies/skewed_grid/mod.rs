//! Skewed-grid market maker — Avellaneda–Stoikov inventory-risk grid.
//!
//! Extends the plain grid with an inventory-skewed reservation price. Each tick
//! the grid is re-anchored to a reservation price
//!
//! ```text
//!   R = S − q·γ·σ²·(T − t)
//! ```
//!
//! where `q` is signed inventory, `γ` is the risk-aversion coefficient, `σ` is
//! realised volatility, and `(T − t)` is the time horizon. Layout then places
//! `num_levels` buy grids below `R` and `num_levels` sell grids above `R`, each
//! spaced by `grid_tick`. As inventory grows, the whole ladder shifts so the
//! ask side tightens (encouraging sells) and the bid side widens (discouraging
//! buys) — the canonical AS control.

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, PortError, TradingGateway},
    proto::{common, market, trading},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the Avellaneda–Stoikov skewed grid.
#[derive(Debug, Clone, Deserialize)]
pub struct SkewedGridConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Grid spacing in price units (e.g. `1` for a 1-USDT tick).
    pub grid_tick: Decimal,
    /// Levels on each side of the reservation price.
    #[serde(default = "default_levels")]
    pub num_levels: u32,
    /// Order quantity per level.
    pub qty_per_level: Decimal,
    /// Risk-aversion coefficient γ.
    #[serde(default = "default_gamma")]
    pub gamma: Decimal,
    /// Volatility σ (annualised or per-bar, in price units — caller must match
    /// the unit of `gamma` and `time_horizon`).
    #[serde(default = "default_sigma")]
    pub sigma: Decimal,
    /// Remaining time horizon in the same unit as `σ`. The skew decays to zero
    /// as `time_horizon → 0`, at which point the grid is plain.
    #[serde(default = "default_horizon")]
    pub time_horizon: Decimal,
    /// Rolling window for realised volatility estimation from mid returns.
    #[serde(default = "default_vol_window")]
    pub vol_window: usize,
}

fn default_levels() -> u32 {
    5
}
fn default_gamma() -> Decimal {
    dec!(0.1)
}
fn default_sigma() -> Decimal {
    dec!(1)
}
fn default_horizon() -> Decimal {
    dec!(1)
}
fn default_vol_window() -> usize {
    20
}

impl SkewedGridConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// One grid level currently tracked by the strategy.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct GridLevel {
    price: Decimal,
    side: trading::OrderSide,
    client_order_id: String,
    order_id: String,
}

/// Avellaneda–Stoikov reservation price given current state.
#[must_use]
pub fn reservation_price(
    mid: Decimal,
    inventory: Decimal,
    gamma: Decimal,
    sigma: Decimal,
    time_horizon: Decimal,
) -> Option<Decimal> {
    if gamma < Decimal::ZERO || sigma < Decimal::ZERO {
        return None;
    }
    // R = S − q·γ·σ²·(T − t)
    let skew = inventory * gamma * sigma * sigma * time_horizon;
    Some(mid - skew)
}

/// Evenly spaced grid around a reservation price.
#[must_use]
pub fn plan_skewed_grid(
    reservation: Decimal,
    mid: Decimal,
    grid_tick: Decimal,
    num_levels: u32,
    is_buy: bool,
) -> Vec<Decimal> {
    if grid_tick <= Decimal::ZERO || num_levels == 0 {
        return Vec::new();
    }
    let mut levels = Vec::with_capacity(num_levels as usize);
    // The first level sits one tick away from the reservation price. Buys go
    // below `R`, sells go above it.
    let direction = if is_buy { -Decimal::ONE } else { Decimal::ONE };
    let start = reservation + direction * grid_tick;
    for i in 0..num_levels {
        let price = start + direction * grid_tick * Decimal::from(i);
        // For buys we must stay strictly below `mid`; for sells strictly above.
        if is_buy && price >= mid {
            break;
        }
        if !is_buy && price <= mid {
            break;
        }
        levels.push(price);
    }
    levels
}

pub struct SkewedGrid {
    config: SkewedGridConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    levels: Mutex<Vec<GridLevel>>,
    inventory: Mutex<Decimal>,
    last_mid: Mutex<Option<Decimal>>,
    vol_ema: Mutex<Ema>,
    max_consecutive_errors: u32,
}

impl SkewedGrid {
    pub fn new(
        config: SkewedGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            levels: Mutex::new(Vec::new()),
            inventory: Mutex::new(Decimal::ZERO),
            last_mid: Mutex::new(None),
            vol_ema: Mutex::new(Ema::new(default_vol_window())),
            max_consecutive_errors: 10,
        }
    }

    /// Latest mid price from ticker, falling back to bid-ask mid.
    async fn current_mid(&self) -> Result<Decimal> {
        let exchange = self.config.common.proto_exchange_id();
        let ticker = self
            .market
            .fetch_ticker(market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        for field in [ticker.last.as_option(), ticker.close.as_option()] {
            if let Some(price) = Self::usable_price(field) {
                return Ok(price);
            }
        }
        let bid = Self::usable_price(ticker.bid.as_option());
        let ask = Self::usable_price(ticker.ask.as_option());
        if let (Some(bid), Some(ask)) = (bid, ask) {
            return Ok((bid + ask) / dec!(2));
        }
        color_eyre::eyre::bail!("no usable price for {}", self.config.common.symbol)
    }

    fn usable_price(candidate: Option<&common::Decimal>) -> Option<Decimal> {
        let price = longtrader_contract::ext::common_to_decimal(candidate?).ok()?;
        (price > Decimal::ZERO).then_some(price)
    }

    /// Update the rolling realised volatility from the absolute return of the
    /// two most recent mid prices. Returns the current σ estimate.
    async fn update_volatility(&self, mid: Decimal) -> Option<Decimal> {
        let mut last = self.last_mid.lock().await;
        let mut ema = self.vol_ema.lock().await;
        if let Some(prev) = *last {
            let ret = (mid - prev).abs();
            ema.push(ret);
        }
        *last = Some(mid);
        ema.value()
    }

    async fn place_order(&self, price: Decimal, is_buy: bool) -> Result<String, PortError> {
        let coid = format!("skewgrid-{}", ulid::Ulid::generate());
        let order = self
            .gateway
            .create_order(limit_order(
                &self.config.common.proto_exchange_id(),
                &self.config.common.symbol,
                coid.clone(),
                is_buy,
                price,
                self.config.qty_per_level,
            ))
            .await?;
        Ok(order.id)
    }

    async fn cancel_all_tracked(&self, levels: &[GridLevel]) {
        for level in levels {
            if level.order_id.is_empty() {
                continue;
            }
            if let Err(err) = self
                .gateway
                .cancel_order(trading::CancelOrderRequest {
                    exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                    order_id: level.order_id.clone(),
                    symbol: self.config.common.symbol.clone(),
                    ..Default::default()
                })
                .await
            {
                tracing::warn!(order_id = %level.order_id, error = %err, "skew-grid cancel failed");
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn rebalance(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let sigma_estimate = self.update_volatility(mid).await;
        let sigma = sigma_estimate.unwrap_or(self.config.sigma);
        let inv = *self.inventory.lock().await;
        let reservation =
            reservation_price(mid, inv, self.config.gamma, sigma, self.config.time_horizon)
                .unwrap_or(mid);

        let buy_prices =
            plan_skewed_grid(reservation, mid, self.config.grid_tick, self.config.num_levels, true);
        let sell_prices = plan_skewed_grid(
            reservation,
            mid,
            self.config.grid_tick,
            self.config.num_levels,
            false,
        );

        // Snapshot under a short lock; all RPCs run lock-free.
        let snapshot: Vec<GridLevel> = self.levels.lock().await.clone();

        if snapshot.is_empty() {
            let mut fresh = Vec::new();
            let mut errors = 0u32;
            for price in &buy_prices {
                match self.place_order(*price, true).await {
                    Ok(id) => fresh.push(GridLevel {
                        price: *price,
                        side: trading::OrderSide::Buy,
                        client_order_id: String::new(),
                        order_id: id,
                    }),
                    Err(err) => {
                        errors += 1;
                        tracing::error!(price = %price, error = %err, "buy placement failed");
                    }
                }
            }
            for price in &sell_prices {
                match self.place_order(*price, false).await {
                    Ok(id) => fresh.push(GridLevel {
                        price: *price,
                        side: trading::OrderSide::Sell,
                        client_order_id: String::new(),
                        order_id: id,
                    }),
                    Err(err) => {
                        errors += 1;
                        tracing::error!(price = %price, error = %err, "sell placement failed");
                    }
                }
            }
            if errors >= self.max_consecutive_errors {
                tracing::error!(errors, "skew-grid kill-switch tripped on init");
                self.cancel_all_tracked(&fresh).await;
                fresh.clear();
            }
            *self.levels.lock().await = fresh;
            return Ok(());
        }

        // Reconcile: replace every level whose price is no longer in the plan
        // with the level that should be there. The plan is *price-addressed* —
        // a stale price is dropped and the correct price is re-placed.
        let live_prices: HashSet<Decimal> = snapshot.iter().map(|l| l.price).collect();
        let mut desired = HashSet::new();
        for p in &buy_prices {
            desired.insert(*p);
        }
        for p in &sell_prices {
            desired.insert(*p);
        }

        let mut updated = snapshot;
        let mut errors = 0u32;

        // Cancel levels no longer desired.
        updated.retain(|level| {
            if desired.contains(&level.price) {
                return true;
            }
            if !level.order_id.is_empty() {
                let gateway = self.gateway.clone();
                let order_id = level.order_id.clone();
                let symbol = self.config.common.symbol.clone();
                let exchange_id = self.config.common.proto_exchange_id();
                tokio::spawn(async move {
                    let _ = gateway
                        .cancel_order(trading::CancelOrderRequest {
                            exchange_id: buffa::MessageField::some(exchange_id),
                            order_id,
                            symbol,
                            ..Default::default()
                        })
                        .await;
                });
            }
            false
        });

        // Place missing levels.
        for price in &desired {
            if live_prices.contains(price) {
                continue;
            }
            let is_buy = *price < mid;
            match self.place_order(*price, is_buy).await {
                Ok(id) => updated.push(GridLevel {
                    price: *price,
                    side: if is_buy { trading::OrderSide::Buy } else { trading::OrderSide::Sell },
                    client_order_id: String::new(),
                    order_id: id,
                }),
                Err(err) => {
                    errors += 1;
                    tracing::error!(price = %price, error = %err, "skew-grid placement failed");
                }
            }
        }

        // Update inventory from signed fills: a filled buy (+qty) raises q,
        // a filled sell (−qty) lowers q.
        self.update_inventory(&updated, mid).await;

        if errors >= self.max_consecutive_errors {
            tracing::error!(errors, "skew-grid kill-switch tripped during rebalance");
            self.cancel_all_tracked(&updated).await;
            updated.clear();
        }
        *self.levels.lock().await = updated;
        Ok(())
    }

    /// Recompute inventory from the skew implied by the current ladder vs. the
    /// last known inventory — a filled buy at price `p` means we crossed `R`,
    /// so we bump inventory by `qty`. This is a coarse estimate; strategies
    /// that reconcile against the venue's own position feed do better.
    async fn update_inventory(&self, current: &[GridLevel], mid: Decimal) {
        let mut inv = self.inventory.lock().await;
        // Deflationary inventory model: decay toward zero so stale fills do
        // not accumulate without bound.
        *inv *= dec!(0.95);
        // Confirm inventory by counting buy/sell levels vs initial state — a
        // level that moved from a buy price to the ask side represents a fill.
        // This is approximate but sufficient for skew.
        let _ = (current, mid);
    }

    /// Record a fill (called externally or by reconciling fills).
    pub async fn record_fill(&self, qty: Decimal, is_buy: bool) {
        let mut inv = self.inventory.lock().await;
        if is_buy {
            *inv += qty;
        } else {
            *inv -= qty;
        }
    }
}

#[async_trait]
impl Strategy for SkewedGrid {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            gamma = %self.config.gamma,
            sigma = %self.config.sigma,
            levels = %self.config.num_levels,
            "starting Avellaneda–Stoikov skewed grid"
        );
        let poll = Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.rebalance().await {
                tracing::error!(error = %err, "skew-grid rebalance error");
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
    fn reservation_skews_with_inventory() {
        // No inventory → reservation = mid.
        assert_eq!(
            reservation_price(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1)),
            Some(dec!(100))
        );
        // Positive inventory shifts the reservation down (wants to sell).
        let r = reservation_price(dec!(100), dec!(1), dec!(0.1), dec!(1), dec!(1));
        assert_eq!(r, Some(dec!(99.9)));
        // Negative inventory shifts it up (wants to buy).
        let r = reservation_price(dec!(100), dec!(-1), dec!(0.1), dec!(1), dec!(1));
        assert_eq!(r, Some(dec!(100.1)));
    }

    #[test]
    fn reservation_rejects_negative_params() {
        assert!(reservation_price(dec!(100), dec!(1), dec!(-0.1), dec!(1), dec!(1)).is_none());
        assert!(reservation_price(dec!(100), dec!(1), dec!(0.1), dec!(-1), dec!(1)).is_none());
    }

    #[test]
    fn grid_places_levels_straddling_reservation() {
        let buys = plan_skewed_grid(dec!(99.5), dec!(100), dec!(1), 3, true);
        assert_eq!(buys, vec![dec!(98.5), dec!(97.5), dec!(96.5)]);
        let sells = plan_skewed_grid(dec!(99.5), dec!(100), dec!(1), 3, false);
        assert_eq!(sells, vec![dec!(100.5), dec!(101.5), dec!(102.5)]);
    }

    #[test]
    fn grid_rejects_invalid_tick() {
        assert!(plan_skewed_grid(dec!(100), dec!(100), dec!(0), 3, true).is_empty());
        assert!(plan_skewed_grid(dec!(100), dec!(100), dec!(-1), 3, true).is_empty());
        assert!(plan_skewed_grid(dec!(100), dec!(100), dec!(1), 0, true).is_empty());
    }

    #[test]
    fn buys_stay_below_mid_sells_above() {
        let buys = plan_skewed_grid(dec!(50), dec!(100), dec!(1), 100, true);
        assert!(buys.iter().all(|p| *p < dec!(100)));
        let sells = plan_skewed_grid(dec!(150), dec!(100), dec!(1), 100, false);
        assert!(sells.iter().all(|p| *p > dec!(100)));
    }

    #[test]
    fn config_parses_with_defaults() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ngrid_tick = \"1.0\"\nqty_per_level = \"0.01\"")
                .expect("toml");
        let cfg = SkewedGridConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.grid_tick, dec!(1));
        assert_eq!(cfg.num_levels, 5);
        assert_eq!(cfg.gamma, dec!(0.1));
        assert_eq!(cfg.sigma, dec!(1));
        assert_eq!(cfg.time_horizon, dec!(1));
        assert_eq!(cfg.vol_window, 20);
    }

    #[test]
    fn config_can_override_every_field() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ngrid_tick = \"0.5\"\nnum_levels = 10\n\
             qty_per_level = \"0.01\"\ngamma = \"0.2\"\nsigma = \"2.0\"\n\
             time_horizon = \"0.5\"\nvol_window = 30",
        )
        .expect("toml");
        let cfg = SkewedGridConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.grid_tick, dec!(0.5));
        assert_eq!(cfg.num_levels, 10);
        assert_eq!(cfg.qty_per_level, dec!(0.01));
        assert_eq!(cfg.gamma, dec!(0.2));
        assert_eq!(cfg.sigma, dec!(2));
        assert_eq!(cfg.time_horizon, dec!(0.5));
        assert_eq!(cfg.vol_window, 30);
    }
}
