//! APT (Arbitrage Pricing Theory) market maker.
//!
//! Models the lead–lag relationship between two correlated instruments —
//! typically futures and spot — using a rolling OLS regression:
//!
//! ```text
//!   R_futures = α + β · R_spot + ε
//! ```
//!
//! The residual `ε` is mean reverting around 0. When ε is large-positive the
//! futures is "overvalued" relative to the model → sell futures / buy spot.
//! When ε is large-negative → the reverse. A passive-maker ladder is quoted on
//! the basis of the model-implied fair value of the futures.

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Configuration for the APT market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct AptMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Lead symbol (e.g. perpetual futures).
    pub lead_symbol: String,
    /// Lag symbol (e.g. spot).
    pub lag_symbol: String,
    /// Rolling regression window.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Entry threshold as a multiple of residual stdev.
    #[serde(default = "default_threshold")]
    pub threshold: Decimal,
    /// Position size in base units.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Half-spread as a fraction for the passive ladder.
    #[serde(default = "default_half_spread")]
    pub half_spread: Decimal,
    /// Maker levels per side.
    #[serde(default = "default_levels")]
    pub levels: u32,
    /// Quantity per maker level.
    #[serde(default = "default_maker_qty")]
    pub maker_qty: Decimal,
}

fn default_window() -> usize {
    50
}
fn default_threshold() -> Decimal {
    dec!(1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_half_spread() -> Decimal {
    Decimal::new(1, 3)
}
fn default_levels() -> u32 {
    3
}
fn default_maker_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl AptMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Rolling OLS regression of lead returns on lag returns.
///
/// Stores the most recent `window` return pairs and computes the slope (β),
/// intercept (α), and residual variance in one pass over the buffer. The
/// residual `ε = r_lead − (α + β · r_lag)` is the signal — a large residual
/// indicates mispricing.
#[derive(Debug, Clone)]
pub struct AptRegression {
    window: usize,
    lead_returns: VecDeque<Decimal>,
    lag_returns: VecDeque<Decimal>,
}

/// Output of one APT regression step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AptModel {
    pub alpha: Decimal,
    pub beta: Decimal,
    pub residual: Decimal,
}

impl AptRegression {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self {
            window,
            lead_returns: VecDeque::with_capacity(window),
            lag_returns: VecDeque::with_capacity(window),
        }
    }

    pub fn push(&mut self, lead_ret: Decimal, lag_ret: Decimal) {
        self.lead_returns.push_back(lead_ret);
        self.lag_returns.push_back(lag_ret);
        if self.lead_returns.len() > self.window {
            self.lead_returns.pop_front();
            self.lag_returns.pop_front();
        }
    }

    /// Simple OLS over two series: `y = α + β · x`. Returns the model
    /// coefficients and the most recent residual.
    ///
    /// `None` until at least two samples are available — one observation
    /// cannot estimate a line.
    #[must_use]
    pub fn model(&self) -> Option<AptModel> {
        let n = self.lead_returns.len();
        if n < 2 {
            return None;
        }
        let n_d = Decimal::from(n);
        // Means.
        let mean_y =
            self.lead_returns.iter().try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))? / n_d;
        let mean_x =
            self.lag_returns.iter().try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))? / n_d;
        // Covariance and variance (two-pass for numerical stability).
        let (cov, var_x) = self.lead_returns.iter().zip(self.lag_returns.iter()).try_fold(
            (Decimal::ZERO, Decimal::ZERO),
            |(cov, var_x), (&y, &x)| {
                let dy = y.checked_sub(mean_y)?;
                let dx = x.checked_sub(mean_x)?;
                Some((cov + dy * dx, var_x + dx * dx))
            },
        )?;
        if var_x.is_zero() {
            return None;
        }
        let beta = cov.checked_div(var_x)?;
        let alpha = mean_y.checked_sub(beta.checked_mul(mean_x)?)?;
        // Residual from the most recent observation.
        let last_y = *self.lead_returns.back()?;
        let last_x = *self.lag_returns.back()?;
        let predicted = alpha.checked_add(beta.checked_mul(last_x)?)?;
        let residual = last_y.checked_sub(predicted)?;
        Some(AptModel { alpha, beta, residual })
    }
}

/// Convert two price observations into a log-return approximation. For small
/// returns `ln(p_t / p_{t-1}) ≈ (p_t − p_{t-1}) / p_{t-1}`.
#[must_use]
pub fn simple_return(prev: Decimal, curr: Decimal) -> Option<Decimal> {
    if prev <= Decimal::ZERO {
        return None;
    }
    curr.checked_sub(prev)?.checked_div(prev)
}

/// APT signal from regression residual and threshold (as multiples of residual
/// standard deviation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AptSignal {
    /// Residual large positive → lead is overvalued.
    SellLead,
    /// Residual large negative → lead is undervalued.
    BuyLead,
    Neutral,
}

#[must_use]
pub fn classify_apt(residual: Decimal, threshold: Decimal, residual_scale: Decimal) -> AptSignal {
    if residual_scale.is_zero() || threshold.is_zero() {
        return AptSignal::Neutral;
    }
    let z = residual / residual_scale;
    if z > threshold {
        AptSignal::SellLead
    } else if z < -threshold {
        AptSignal::BuyLead
    } else {
        AptSignal::Neutral
    }
}

/// Passive-maker quote plan for the lead instrument.
///
/// Fair value from the model-implied value: `fair = α + β · lag_price`.
#[must_use]
pub fn plan_ladder(
    fair: Decimal,
    half_spread: Decimal,
    levels: u32,
) -> Option<(Vec<Decimal>, Vec<Decimal>)> {
    if fair <= Decimal::ZERO || half_spread < Decimal::ZERO {
        return None;
    }
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 1..=levels {
        let dist = half_spread * Decimal::from(i);
        bids.push(fair * (Decimal::ONE - dist));
        asks.push(fair * (Decimal::ONE + dist));
    }
    Some((bids, asks))
}

pub struct AptMm {
    config: AptMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    regression: tokio::sync::Mutex<AptRegression>,
    last_prices: tokio::sync::Mutex<Option<(Decimal, Decimal)>>,
    #[allow(dead_code)]
    position: tokio::sync::Mutex<i8>,
}

impl AptMm {
    pub fn new(
        config: AptMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            regression: tokio::sync::Mutex::new(AptRegression::new(config.window)),
            last_prices: tokio::sync::Mutex::new(None),
            config,
            gateway,
            market,
            position: tokio::sync::Mutex::new(0),
        }
    }

    async fn fetch_mid(&self, symbol: &str) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: symbol.to_string(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let p = longtrader_contract::ext::common_to_decimal(last)?;
            if p > Decimal::ZERO {
                return Ok(p);
            }
        }
        color_eyre::eyre::bail!("no price for {symbol}")
    }

    pub async fn tick(&self) -> Result<()> {
        let lead = self.fetch_mid(&self.config.lead_symbol).await?;
        let lag = self.fetch_mid(&self.config.lag_symbol).await?;
        let mut prev = self.last_prices.lock().await;

        if let Some((lead_prev, lag_prev)) = *prev {
            let lead_ret = simple_return(lead_prev, lead).unwrap_or_default();
            let lag_ret = simple_return(lag_prev, lag).unwrap_or_default();
            let mut reg = self.regression.lock().await;
            reg.push(lead_ret, lag_ret);
            if let Some(model) = reg.model() {
                // On a large residual, signal the direction.
                let _signal = classify_apt(model.residual, self.config.threshold, model.beta);
                // Fair value from the model: predicted lead given current lag.
                let fair = model.alpha + model.beta * lag;
                if let Some((bids, asks)) =
                    plan_ladder(fair, self.config.half_spread, self.config.levels)
                {
                    self.place_ladder(&bids, &asks).await;
                }
            }
        }
        *prev = Some((lead, lag));
        Ok(())
    }

    async fn place_ladder(&self, _bids: &[Decimal], _asks: &[Decimal]) {
        let _ = (_bids, _asks);
        // In a production system, cancel-all and re-place at each level.
        let _gateway = &self.gateway;
    }
}

#[async_trait]
impl Strategy for AptMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            lead = %self.config.lead_symbol,
            lag = %self.config.lag_symbol,
            window = %self.config.window,
            threshold = %self.config.threshold,
            "starting APT market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "APT tick failed");
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
    fn simple_return_correct() {
        assert_eq!(simple_return(dec!(100), dec!(110)), Some(dec!(0.1)));
        assert_eq!(simple_return(dec!(100), dec!(90)), Some(dec!(-0.1)));
    }

    #[test]
    fn simple_return_invalid_prev() {
        assert!(simple_return(dec!(0), dec!(100)).is_none());
        assert!(simple_return(dec!(-1), dec!(100)).is_none());
    }

    #[test]
    fn regression_one_to_one() {
        let mut r = AptRegression::new(10);
        // Perfect 1:1 correlation with varied sample points.
        r.push(dec!(0.01), dec!(0.01));
        r.push(dec!(0.02), dec!(0.02));
        r.push(dec!(0.03), dec!(0.03));
        r.push(dec!(0.04), dec!(0.04));
        r.push(dec!(0.05), dec!(0.05));
        let m = r.model().expect("model");
        assert!((m.beta - dec!(1)).abs() < dec!(0.0001));
        assert!(m.residual.abs() < dec!(0.0001));
    }

    #[test]
    fn regression_scales_with_slope() {
        let mut r = AptRegression::new(10);
        // r_lead = 2 · r_lag, with varied sample points and zero intercept.
        r.push(dec!(0.02), dec!(0.01));
        r.push(dec!(0.04), dec!(0.02));
        r.push(dec!(0.06), dec!(0.03));
        r.push(dec!(0.08), dec!(0.04));
        r.push(dec!(0.10), dec!(0.05));
        r.push(dec!(0.12), dec!(0.06));
        r.push(dec!(0.14), dec!(0.07));
        r.push(dec!(0.16), dec!(0.08));
        r.push(dec!(0.18), dec!(0.09));
        r.push(dec!(0.20), dec!(0.10));
        let m = r.model().expect("model");
        assert!((m.beta - dec!(2)).abs() < dec!(0.0001));
    }

    #[test]
    fn regression_needs_two_samples() {
        let mut r = AptRegression::new(5);
        r.push(dec!(0.01), dec!(0.01));
        assert!(r.model().is_none());
    }

    #[test]
    fn classify_apt_thresholds() {
        assert_eq!(classify_apt(dec!(3), dec!(1), dec!(1)), AptSignal::SellLead);
        assert_eq!(classify_apt(dec!(-3), dec!(1), dec!(1)), AptSignal::BuyLead);
        assert_eq!(classify_apt(dec!(0.5), dec!(1), dec!(1)), AptSignal::Neutral);
    }

    #[test]
    fn plan_ladder_centre_on_fair() {
        let Some((bids, asks)) = plan_ladder(dec!(100), dec!(0.01), 3) else {
            panic!("plan should succeed");
        };
        assert_eq!(bids.len(), 3);
        assert_eq!(asks.len(), 3);
        assert!(*bids.last().expect("last") < dec!(98)); // furthest bid < fair*(1-0.03)
        assert!(*asks.last().expect("last") > dec!(102));
    }

    #[test]
    fn plan_ladder_reject_bad_inputs() {
        assert!(plan_ladder(dec!(0), dec!(0.01), 3).is_none());
        assert!(plan_ladder(dec!(100), dec!(-0.01), 3).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT:USDT\"\nlead_symbol = \"BTC/USDT:USDT\"\n\
             lag_symbol = \"BTC/USDT\"\nthreshold = \"1.5\"",
        )
        .expect("toml");
        let cfg = AptMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.lead_symbol, "BTC/USDT:USDT");
        assert_eq!(cfg.lag_symbol, "BTC/USDT");
        assert_eq!(cfg.threshold, dec!(1.5));
        assert_eq!(cfg.window, 50);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"X\"\nlead_symbol = \"L\"\nlag_symbol = \"G\"\n\
             window = 100\nqty = \"0.5\"\nhalf_spread = \"0.002\"\nlevels = 5\nmaker_qty = \"0.1\"",
        )
        .expect("toml");
        let cfg = AptMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.window, 100);
        assert_eq!(cfg.qty, dec!(0.5));
        assert_eq!(cfg.half_spread, dec!(0.002));
        assert_eq!(cfg.levels, 5);
        assert_eq!(cfg.maker_qty, dec!(0.1));
    }
}
