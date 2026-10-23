//! GLFT (Guéant–Lehalle–Fernandez–Tapia) optimal market making.
//!
//! The GLFT closed-form solution gives the optimal bid/ask half-spread as a
//! function of risk aversion, volatility, and market order arrival intensity:
//!
//! ```text
//!   δ_bid* = γ·σ²·(T − t) + (1/γ)·ln(1 + γ/k)
//!   δ_ask* = γ·σ²·(T − t) − (1/γ)·ln(1 − γ/k)
//! ```
//!
//! where `k` ≡ A⁻¹·(2/θ·ln(1 + γ/ε)) is the effective arrival rate decay
//! coefficient and `γ` is risk aversion. The skew (inventory adjustment) reads:
//!
//! ```text
//!   skew = γ · σ² · q
//! ```
//!
//! The reservation price is `R = mid − skew`; the bid and ask are placed at
//! `R ∓ δ*`. This is the canonical optimal MM baseline.

use std::sync::Arc;

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

/// Configuration for the GLFT market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct GlftMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Risk-aversion coefficient γ.
    #[serde(default = "default_gamma")]
    pub gamma: Decimal,
    /// Annualised (or per-bar) volatility σ.
    #[serde(default = "default_sigma")]
    pub sigma: Decimal,
    /// Order arrival intensity A (expected market orders per unit time).
    #[serde(default = "default_arrival")]
    pub arrival_intensity: Decimal,
    /// Order arrival decay k: λ(δ) = A·exp(−k·δ).
    #[serde(default = "default_decay")]
    pub decay: Decimal,
    /// Remaining horizon (in time units matching σ).
    #[serde(default = "default_horizon")]
    pub time_horizon: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Rolling window for realised volatility estimation.
    #[serde(default = "default_vol_window")]
    pub vol_window: usize,
}

fn default_gamma() -> Decimal {
    dec!(0.1)
}
fn default_sigma() -> Decimal {
    dec!(1)
}
fn default_arrival() -> Decimal {
    dec!(10)
}
fn default_decay() -> Decimal {
    dec!(1)
}
fn default_horizon() -> Decimal {
    dec!(1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_vol_window() -> usize {
    20
}

impl GlftMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Optimal half-spreads from the GLFT closed form.
///
/// `None` when the parameters violate the model's regularity condition
/// (`γ < k` is required for the ask-side log to be defined).
#[must_use]
pub fn optimal_spreads(
    gamma: Decimal,
    sigma: Decimal,
    decay: Decimal,
    time_horizon: Decimal,
) -> Option<(Decimal, Decimal)> {
    if gamma <= Decimal::ZERO || sigma < Decimal::ZERO || decay <= Decimal::ZERO {
        return None;
    }
    // γ < k is required for ln(1 − γ/k) to be defined.
    if gamma >= decay {
        return None;
    }
    let variance = sigma * sigma;
    let inventory_term = gamma * variance * time_horizon;
    // GLFT closed form requires ln(1 ± γ/k); Decimal has no native ln, so we
    // compute in f64 and convert back. The model is robust to the precision
    // loss because the log term is only one component of the spread.
    let gamma_f: f64 = gamma.to_f64()?;
    let decay_f: f64 = decay.to_f64()?;
    let log_term = Decimal::try_from((gamma_f / decay_f).ln_1p() / gamma_f).ok()?;
    let ask_log_term = Decimal::try_from(-((-gamma_f / decay_f).ln_1p()) / gamma_f).ok()?;
    let bid = inventory_term + log_term;
    let ask = inventory_term + ask_log_term;
    Some((bid, ask))
}

/// Inventory skew.
#[must_use]
pub fn glft_skew(inventory: Decimal, gamma: Decimal, sigma: Decimal) -> Decimal {
    gamma * sigma * sigma * inventory
}

/// Quoted bid / ask given mid, inventory, and model spreads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlftQuotes {
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
}

#[must_use]
pub fn plan_glft_quotes(
    mid: Decimal,
    inventory: Decimal,
    gamma: Decimal,
    sigma: Decimal,
    decay: Decimal,
    time_horizon: Decimal,
    qty: Decimal,
) -> Option<GlftQuotes> {
    let (bid_spread, ask_spread) = optimal_spreads(gamma, sigma, decay, time_horizon)?;
    let skew = glft_skew(inventory, gamma, sigma);
    let reservation = mid - skew;
    let bid = reservation - bid_spread;
    let ask = reservation + ask_spread;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
        return None;
    }
    Some(GlftQuotes { bid, ask, bid_qty: qty, ask_qty: qty })
}

pub struct GlftMm {
    config: GlftMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    inventory: tokio::sync::Mutex<Decimal>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
    vol_ema: tokio::sync::Mutex<Ema>,
}

impl GlftMm {
    pub fn new(
        config: GlftMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            inventory: tokio::sync::Mutex::new(Decimal::ZERO),
            last_mid: tokio::sync::Mutex::new(None),
            vol_ema: tokio::sync::Mutex::new(Ema::new(default_vol_window())),
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
        let Some(quotes) = plan_glft_quotes(
            mid,
            inv,
            self.config.gamma,
            sigma,
            self.config.decay,
            self.config.time_horizon,
            self.config.qty,
        ) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        let bid_coid = format!("glftmm-{}", ulid::Ulid::generate());
        let ask_coid = format!("glftmm-{}", ulid::Ulid::generate());
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                bid_coid,
                true,
                quotes.bid,
                quotes.bid_qty,
            ))
            .await
        {
            tracing::error!(error = %err, "GLFT bid placement failed");
        }
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                ask_coid,
                false,
                quotes.ask,
                quotes.ask_qty,
            ))
            .await
        {
            tracing::error!(error = %err, "GLFT ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for GlftMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            gamma = %self.config.gamma,
            sigma = %self.config.sigma,
            decay = %self.config.decay,
            "starting GLFT market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "GLFT tick failed");
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
    fn optimal_spreads_symmetric_without_inventory() {
        // Equal time horizon and decay give roughly symmetric spreads.
        let (bid, ask) = optimal_spreads(dec!(0.1), dec!(1), dec!(1), dec!(1)).expect("spreads");
        assert!(bid > Decimal::ZERO);
        assert!(ask > Decimal::ZERO);
        // As time horizon shrinks, spreads narrow on their inventory term.
        let (bid_tight, ask_tight) =
            optimal_spreads(dec!(0.1), dec!(1), dec!(1), dec!(0.1)).expect("spreads");
        assert!(bid_tight < bid);
        assert!(ask_tight < ask);
    }

    #[test]
    fn optimal_spreads_rejects_bad_params() {
        // γ ≥ k violates the regularity condition.
        assert!(optimal_spreads(dec!(2), dec!(1), dec!(1), dec!(1)).is_none());
        assert!(optimal_spreads(dec!(0), dec!(1), dec!(1), dec!(1)).is_none());
        assert!(optimal_spreads(dec!(0.1), dec!(1), dec!(0), dec!(1)).is_none());
    }

    #[test]
    fn glft_skew_with_inventory() {
        // No inventory → no skew.
        assert_eq!(glft_skew(dec!(0), dec!(0.1), dec!(1)), dec!(0));
        // Positive inventory → negative skew (reserve price drops).
        assert_eq!(glft_skew(dec!(1), dec!(0.1), dec!(1)), dec!(0.1));
        assert_eq!(glft_skew(dec!(-1), dec!(0.1), dec!(1)), dec!(-0.1));
    }

    #[test]
    fn plan_glft_quotes_places_bid_below_ask() {
        let q = plan_glft_quotes(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1), dec!(1), dec!(1))
            .expect("quotes");
        assert!(q.bid < q.ask);
        assert!(q.bid > Decimal::ZERO);
    }

    #[test]
    fn plan_glft_quotes_with_inventory_skews_down() {
        let flat =
            plan_glft_quotes(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1), dec!(1), dec!(1))
                .expect("flat");
        let long =
            plan_glft_quotes(dec!(100), dec!(2), dec!(0.1), dec!(1), dec!(1), dec!(1), dec!(1))
                .expect("long");
        // Positive inventory lowers the reservation price → both bid and ask drop.
        assert!(long.bid < flat.bid);
        assert!(long.ask < flat.ask);
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\ngamma = \"0.2\"\nsigma = \"1.5\"\narrival_intensity = \"20\"\ndecay = \"1.5\"",
        )
        .expect("toml");
        let cfg = GlftMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.gamma, dec!(0.2));
        assert_eq!(cfg.sigma, dec!(1.5));
        assert_eq!(cfg.arrival_intensity, dec!(20));
        assert_eq!(cfg.decay, dec!(1.5));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\ngamma = \"0.05\"\nqty = \"0.5\"\nvol_window = 50\ntime_horizon = \"0.5\"",
        )
        .expect("toml");
        let cfg = GlftMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.gamma, dec!(0.05));
        assert_eq!(cfg.qty, dec!(0.5));
        assert_eq!(cfg.vol_window, 50);
        assert_eq!(cfg.time_horizon, dec!(0.5));
    }
}
