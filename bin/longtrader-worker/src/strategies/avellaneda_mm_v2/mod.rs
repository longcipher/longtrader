//! Avellaneda–Stoikov Market Making v2 — trade-intensity tracking.
//!
//! Unlike the grid-based `skewed_grid`, this is the direct Avellaneda-Stoikov
//! continuous-time solution with real-time tracking of the instantaneous trade
//! arrival intensity λ. The optimal half-spread:
//!
//! ```text
//!   δ* = γ·σ²·(T − t) + (1/γ) · ln(1 + γ/k_eff)
//! ```
//!
//! where `k_eff` is the effective decay parameter estimated from the observed
//! market order flow (trade intensity). Higher trade intensity → narrower
//! spreads because fills arrive more frequently. The reservation price adds
//! an inventory term:
//!
//! ```text
//!   R = S − q · γ · σ² · (T − t)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/avellaneda-market-making/>

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the Avellaneda MM v2 strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AvellanedaMmV2Config {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Risk-aversion coefficient γ.
    #[serde(default = "default_gamma")]
    pub gamma: Decimal,
    /// Volatility σ (per-bar, in price units).
    #[serde(default = "default_sigma")]
    pub sigma: Decimal,
    /// Time horizon (in matching units).
    #[serde(default = "default_horizon")]
    pub time_horizon: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Rolling window for trade-intensity estimation.
    #[serde(default = "default_intensity_window")]
    pub intensity_window: usize,
    /// Rolling window for realised volatility estimation.
    #[serde(default = "default_vol_window")]
    pub vol_window: usize,
    /// Minimum effective decay k_eff (floor to avoid division issues).
    #[serde(default = "default_min_decay")]
    pub min_decay: Decimal,
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
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_intensity_window() -> usize {
    20
}
fn default_vol_window() -> usize {
    20
}
fn default_min_decay() -> Decimal {
    dec!(0.1)
}

impl AvellanedaMmV2Config {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Rolling trade-intensity estimator.
///
/// Tracks the number of market orders (trades) per unit time in a rolling
/// window. The intensity λ drives the effective decay: higher intensity
/// means fills arrive faster, so the optimal spread narrows.
#[derive(Debug, Clone)]
pub struct TradeIntensity {
    window: usize,
    timestamps: VecDeque<u64>,
}

impl TradeIntensity {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, timestamps: VecDeque::with_capacity(window) }
    }

    /// Record a trade observation at the given timestamp.
    pub fn observe(&mut self, timestamp: u64) {
        self.timestamps.push_back(timestamp);
        if self.timestamps.len() > self.window {
            self.timestamps.pop_front();
        }
    }

    /// Current intensity estimate: trades per unit time.
    #[must_use]
    pub fn intensity(&self) -> Option<Decimal> {
        if self.timestamps.len() < 2 {
            return None;
        }
        let first = *self.timestamps.front()?;
        let last = *self.timestamps.back()?;
        let elapsed = last.saturating_sub(first);
        if elapsed == 0 {
            return None;
        }
        let n = Decimal::from(self.timestamps.len() - 1);
        let dt = Decimal::from(elapsed);
        Some(n / dt)
    }
}

/// Optimal half-spread from the AS closed form with trade-intensity estimate.
///
/// `None` when parameters are invalid.
#[must_use]
pub fn optimal_spread_v2(
    gamma: Decimal,
    sigma: Decimal,
    time_horizon: Decimal,
    intensity: Option<Decimal>,
    min_decay: Decimal,
) -> Option<Decimal> {
    if gamma <= Decimal::ZERO || sigma < Decimal::ZERO {
        return None;
    }
    let variance = sigma * sigma;
    let inventory_term = gamma * variance * time_horizon;
    // Effective decay: k_eff = max(λ, min_decay).
    let k_eff = intensity.unwrap_or(min_decay).max(min_decay);
    if gamma >= k_eff {
        return None;
    }
    // ln(1 + γ/k_eff) / γ via f64. Decimal has no native ln; the model is
    // robust to the precision loss, but a conversion failure must be visible
    // (fail-closed, no quote) rather than a silent skip.
    let Some(gamma_f) = gamma.to_f64() else {
        tracing::debug!(%gamma, %k_eff, "avellaneda spread skipped: gamma not representable as f64");
        return None;
    };
    let Some(k_f) = k_eff.to_f64() else {
        tracing::debug!(%gamma, %k_eff, "avellaneda spread skipped: k_eff not representable as f64");
        return None;
    };
    let Ok(log_term) = Decimal::try_from((gamma_f / k_f).ln_1p() / gamma_f) else {
        tracing::debug!(%gamma, %k_eff, "avellaneda spread skipped: f64 log term out of Decimal range");
        return None;
    };
    Some(inventory_term + log_term)
}

/// Reservation price with inventory skew.
#[must_use]
pub fn reservation_price_v2(
    mid: Decimal,
    inventory: Decimal,
    gamma: Decimal,
    sigma: Decimal,
    time_horizon: Decimal,
) -> Decimal {
    let skew = inventory * gamma * sigma * sigma * time_horizon;
    mid - skew
}

pub struct AvellanedaMmV2 {
    config: AvellanedaMmV2Config,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    inventory: tokio::sync::Mutex<Decimal>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
    vol_ema: tokio::sync::Mutex<Ema>,
    intensity: tokio::sync::Mutex<TradeIntensity>,
}

impl AvellanedaMmV2 {
    pub fn new(
        config: AvellanedaMmV2Config,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let intensity_window = config.intensity_window;
        Self {
            config,
            gateway,
            market,
            inventory: tokio::sync::Mutex::new(Decimal::ZERO),
            last_mid: tokio::sync::Mutex::new(None),
            vol_ema: tokio::sync::Mutex::new(Ema::new(default_vol_window())),
            intensity: tokio::sync::Mutex::new(TradeIntensity::new(intensity_window)),
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

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let sigma_est = self.update_volatility(mid).await;
        let sigma = sigma_est.unwrap_or(self.config.sigma);
        let inv = *self.inventory.lock().await;
        let intensity = self.intensity.lock().await.intensity();
        let Some(spread) = optimal_spread_v2(
            self.config.gamma,
            sigma,
            self.config.time_horizon,
            intensity,
            self.config.min_decay,
        ) else {
            return Ok(());
        };
        let reservation =
            reservation_price_v2(mid, inv, self.config.gamma, sigma, self.config.time_horizon);
        let bid = reservation - spread;
        let ask = reservation + spread;
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
            return Ok(());
        }
        let exchange = self.config.common.proto_exchange_id();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        let bid_coid = format!("asmm2-{}", ulid::Ulid::generate());
        let ask_coid = format!("asmm2-{}", ulid::Ulid::generate());
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                bid_coid,
                true,
                bid,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "ASv2 bid placement failed");
        }
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                ask_coid,
                false,
                ask,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "ASv2 ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for AvellanedaMmV2 {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            gamma = %self.config.gamma,
            sigma = %self.config.sigma,
            intensity_window = %self.config.intensity_window,
            "starting Avellaneda MM v2"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "ASv2 tick failed");
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
    fn trade_intensity_estimates_rate() {
        let mut ti = TradeIntensity::new(10);
        ti.observe(1000);
        ti.observe(2000);
        ti.observe(3000);
        // 2 trades over 2000 time units = 0.001 per unit.
        let intensity = ti.intensity().expect("intensity");
        assert!(intensity > Decimal::ZERO);
    }

    #[test]
    fn trade_intensity_needs_two_observations() {
        let mut ti = TradeIntensity::new(10);
        ti.observe(1000);
        assert!(ti.intensity().is_none());
    }

    #[test]
    fn trade_intensity_window_slides() {
        let mut ti = TradeIntensity::new(3);
        ti.observe(100);
        ti.observe(200);
        ti.observe(300);
        ti.observe(400);
        // Only 200, 300, 400 remain.
        let intensity = ti.intensity().expect("intensity");
        assert!(intensity > Decimal::ZERO);
    }

    #[test]
    fn optimal_spread_v2_narrower_with_higher_intensity() {
        // gamma must be strictly less than k_eff (which includes min_decay floor).
        // gamma=0.05, intensity=0.1 → k_eff=0.1, 0.05 < 0.1 → valid.
        let low = optimal_spread_v2(dec!(0.05), dec!(1), dec!(1), Some(dec!(0.1)), dec!(0.01))
            .expect("low");
        let high = optimal_spread_v2(dec!(0.05), dec!(1), dec!(1), Some(dec!(10)), dec!(0.01))
            .expect("high");
        assert!(high < low, "higher intensity → narrower spread");
    }

    #[test]
    fn optimal_spread_v2_rejects_bad_params() {
        assert!(optimal_spread_v2(dec!(0), dec!(1), dec!(1), None, dec!(0.1)).is_none());
        assert!(optimal_spread_v2(dec!(0.1), dec!(-1), dec!(1), None, dec!(0.1)).is_none());
        // γ ≥ k_eff → None.
        assert!(optimal_spread_v2(dec!(1), dec!(1), dec!(1), Some(dec!(0.5)), dec!(0.1)).is_none());
    }

    #[test]
    fn reservation_price_v2_skews_with_inventory() {
        assert_eq!(
            reservation_price_v2(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1)),
            dec!(100)
        );
        assert_eq!(
            reservation_price_v2(dec!(100), dec!(1), dec!(0.1), dec!(1), dec!(1)),
            dec!(99.9)
        );
        assert_eq!(
            reservation_price_v2(dec!(100), dec!(-1), dec!(0.1), dec!(1), dec!(1)),
            dec!(100.1)
        );
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ngamma = \"0.2\"\nsigma = \"1.5\"\nintensity_window = 30",
        )
        .expect("toml");
        let cfg = AvellanedaMmV2Config::from_params(&table).expect("parse");
        assert_eq!(cfg.gamma, dec!(0.2));
        assert_eq!(cfg.sigma, dec!(1.5));
        assert_eq!(cfg.intensity_window, 30);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\ngamma = \"0.05\"\nsigma = \"2.0\"\n\
             time_horizon = \"0.5\"\nqty = \"0.1\"\nvol_window = 50\nmin_decay = \"0.2\"",
        )
        .expect("toml");
        let cfg = AvellanedaMmV2Config::from_params(&table).expect("parse");
        assert_eq!(cfg.gamma, dec!(0.05));
        assert_eq!(cfg.sigma, dec!(2));
        assert_eq!(cfg.time_horizon, dec!(0.5));
        assert_eq!(cfg.qty, dec!(0.1));
        assert_eq!(cfg.vol_window, 50);
        assert_eq!(cfg.min_decay, dec!(0.2));
    }
}
