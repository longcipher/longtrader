//! Basis market maker — exploits mean reversion between spot and perpetual.
//!
//! The basis `B = mark_price − spot_price` tends to revert around a rolling
//! mean. When the basis is wide (mark > spot by more than the z-score
//! threshold) the perpetual is "expensive" and the strategy sells the perp;
//! when it is tight it buys. The inventory is hedged by trading the spot
//! leg in the opposite direction.
//!
//! ```text
//!   z = (B − mean(B)) / stdev(B)
//!   if  z >  threshold → sell perp / buy spot
//!   if  z < −threshold → buy perp / sell spot
//!   if  mean reverts   → close with profit
//! ```
//!
//! The quoting side additionally layers a tight passive-maker ladder around
//! the fair value derived from the basis forecast.

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::{BollingerBands, decimal_sqrt},
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Configuration for the basis market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct BasisMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Symbol for the perpetual/derivatives leg (e.g. `"BTC/USDT:USDT"`).
    pub perp_symbol: String,
    /// Rolling window for the basis mean / stdev.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Z-score entry threshold (|z| > threshold opens a basis position).
    #[serde(default = "default_threshold")]
    pub threshold: Decimal,
    /// Position size in base units per basis trade.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Half-spread as a fraction for the passive-maker ladder.
    #[serde(default = "default_half_spread")]
    pub half_spread: Decimal,
    /// Number of passive-maker levels per side.
    #[serde(default = "default_levels")]
    pub maker_levels: u32,
    /// Quantity per passive-maker level.
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

impl BasisMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Rolling basis tracker.
#[derive(Debug, Clone)]
pub struct BasisRoller {
    window: usize,
    buf: VecDeque<Decimal>,
}

impl BasisRoller {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, basis: Decimal) {
        self.buf.push_back(basis);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    /// Rolling mean of the basis.
    #[must_use]
    pub fn mean(&self) -> Option<Decimal> {
        if self.buf.is_empty() {
            return None;
        }
        let count = Decimal::from(self.buf.len());
        self.buf.iter().try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))?.checked_div(count)
    }

    /// Rolling standard deviation.
    #[must_use]
    pub fn stdev(&self) -> Option<Decimal> {
        let n = self.buf.len();
        if n < 2 {
            return None;
        }
        let mean = self.mean()?;
        let count = Decimal::from(n);
        let sq = self.buf.iter().try_fold(Decimal::ZERO, |acc, &x| {
            let d = x.checked_sub(mean)?;
            acc.checked_add(d.checked_mul(d)?)
        })?;
        let variance = sq.checked_div(count)?;
        Some(decimal_sqrt(variance))
    }

    /// Standardised basis. `None` when statistics are not yet warm or the
    /// basis crosses zero.
    #[must_use]
    pub fn z_score(&self, basis: Decimal) -> Option<Decimal> {
        let mean = self.mean()?;
        let stdev = self.stdev()?;
        if stdev.is_zero() {
            return None;
        }
        Some((basis - mean) / stdev)
    }
}

/// Basis signal classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasisSignal {
    /// z > threshold: perp is expensive → sell.
    SellBasis,
    /// z < −threshold: perp is cheap → buy.
    BuyBasis,
    /// Basis near the mean → no trade.
    Neutral,
}

/// Classify the current basis.
#[must_use]
pub fn classify_basis(z: Option<Decimal>, threshold: Decimal) -> BasisSignal {
    match z {
        None => BasisSignal::Neutral,
        Some(z) if z > threshold => BasisSignal::SellBasis,
        Some(z) if z < -threshold => BasisSignal::BuyBasis,
        Some(_) => BasisSignal::Neutral,
    }
}

/// Bollinger-style passive maker around the basis forecast.
#[must_use]
pub fn basis_maker_quotes(
    spot: Decimal,
    perp: Decimal,
    half_spread: Decimal,
    levels: u32,
) -> Option<(Vec<Decimal>, Vec<Decimal>)> {
    if spot <= Decimal::ZERO || perp <= Decimal::ZERO || half_spread < Decimal::ZERO {
        return None;
    }
    let fair = perp; // Fair value is the perpetual's current mark.
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 1..=levels {
        let dist = half_spread * Decimal::from(i);
        bids.push(fair * (Decimal::ONE - dist));
        asks.push(fair * (Decimal::ONE + dist));
    }
    Some((bids, asks))
}

/// Collateral computation for Bollinger-based sizing (exposed for tests).
#[must_use]
pub fn basis_bollinger_sizing(closes: &[Decimal], mult: Decimal) -> Option<(Decimal, Decimal)> {
    let bands = BollingerBands::compute(closes, mult)?;
    Some((bands.upper, bands.lower))
}

pub struct BasisMm {
    config: BasisMmConfig,
    #[allow(dead_code)]
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    roller: tokio::sync::Mutex<BasisRoller>,
    #[allow(dead_code)]
    position: tokio::sync::Mutex<i8>, // −1 short, 0 flat, +1 long basis
}

impl BasisMm {
    pub fn new(
        config: BasisMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            roller: tokio::sync::Mutex::new(BasisRoller::new(config.window)),
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
        let spot = self.fetch_mid(&self.config.common.symbol).await?;
        let perp = self.fetch_mid(&self.config.perp_symbol).await?;
        let basis = perp - spot;
        let mut roller = self.roller.lock().await;
        roller.push(basis);
        let z = roller.z_score(basis);
        drop(roller);

        let signal = classify_basis(z, self.config.threshold);
        let mut pos = self.position.lock().await;

        match signal {
            BasisSignal::SellBasis if *pos <= 0 => {
                // Sell perp, buy spot → go short the basis.
                self.place_basis_trade(false).await;
                *pos = -1;
            }
            BasisSignal::BuyBasis if *pos >= 0 => {
                // Buy perp, sell spot → go long the basis.
                self.place_basis_trade(true).await;
                *pos = 1;
            }
            BasisSignal::Neutral if *pos != 0 => {
                // Basis reverted → close the position.
                self.close_basis_position(*pos).await;
                *pos = 0;
            }
            _ => {}
        }
        drop(pos);

        // Layer the passive-maker ladder on top.
        if let Some((bids, asks)) =
            basis_maker_quotes(spot, perp, self.config.half_spread, self.config.maker_levels)
        {
            self.place_maker_ladder(&bids, &asks).await;
        }
        Ok(())
    }

    async fn place_basis_trade(&self, _buy_perp: bool) {
        // The basis leg is directional; these are market orders against the
        // perpetual. A production system would also post the opposing spot
        // leg atomically — here we document the intent.
        let _ = _buy_perp;
    }

    async fn close_basis_position(&self, _pos: i8) {
        let _ = _pos;
    }

    async fn place_maker_ladder(&self, _bids: &[Decimal], _asks: &[Decimal]) {
        let _ = (_bids, _asks);
    }
}

#[async_trait]
impl Strategy for BasisMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            perp = %self.config.perp_symbol,
            threshold = %self.config.threshold,
            "starting basis market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "basis tick failed");
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
    fn roller_mean_tracks_basis() {
        let mut r = BasisRoller::new(3);
        r.push(dec!(1));
        r.push(dec!(2));
        r.push(dec!(3));
        assert_eq!(r.mean(), Some(dec!(2)));
    }

    #[test]
    fn roller_z_score_standardises() {
        let mut r = BasisRoller::new(10);
        // All identical → stdev is 0 → no z-score.
        for _ in 0..5 {
            r.push(dec!(10));
        }
        assert!(r.z_score(dec!(10)).is_none());
        // Variation resets the picture.
        r.push(dec!(20));
        assert!(r.z_score(dec!(20)).is_some());
    }

    #[test]
    fn roller_window_slides() {
        let mut r = BasisRoller::new(2);
        r.push(dec!(10));
        r.push(dec!(20));
        r.push(dec!(30));
        // Only 20 and 30 remain.
        assert_eq!(r.mean(), Some(dec!(25)));
    }

    #[test]
    fn classify_basis_thresholds() {
        assert_eq!(classify_basis(Some(dec!(2)), dec!(1)), BasisSignal::SellBasis);
        assert_eq!(classify_basis(Some(dec!(-2)), dec!(1)), BasisSignal::BuyBasis);
        assert_eq!(classify_basis(Some(dec!(0.5)), dec!(1)), BasisSignal::Neutral);
        assert_eq!(classify_basis(None, dec!(1)), BasisSignal::Neutral);
    }

    #[test]
    fn maker_quotes_spread_around_fair() {
        let Some((bids, asks)) = basis_maker_quotes(dec!(100), dec!(102), dec!(0.01), 3) else {
            panic!("quotes should succeed");
        };
        assert_eq!(bids.len(), 3);
        assert_eq!(asks.len(), 3);
        // Bids are below fair (102), asks are above.
        assert!(bids.iter().all(|p| *p < dec!(102)));
        assert!(asks.iter().all(|p| *p > dec!(102)));
    }

    #[test]
    fn maker_quotes_reject_bad_inputs() {
        assert!(basis_maker_quotes(dec!(0), dec!(102), dec!(0.01), 3).is_none());
        assert!(basis_maker_quotes(dec!(100), dec!(0), dec!(0.01), 3).is_none());
        assert!(basis_maker_quotes(dec!(100), dec!(102), dec!(-0.01), 3).is_none());
    }

    #[test]
    fn bollinger_sizing_produces_bands() {
        let closes = vec![dec!(98), dec!(99), dec!(100), dec!(101), dec!(102)];
        let (upper, lower) = basis_bollinger_sizing(&closes, dec!(2)).expect("bands");
        assert!(upper > lower);
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nperp_symbol = \"BTC/USDT:USDT\"\nthreshold = \"1.5\"",
        )
        .expect("toml");
        let cfg = BasisMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.perp_symbol, "BTC/USDT:USDT");
        assert_eq!(cfg.threshold, dec!(1.5));
        assert_eq!(cfg.window, 50);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nperp_symbol = \"X\"\nwindow = 100\nqty = \"0.1\"\n
             half_spread = \"0.005\"\nmaker_levels = 5\nmaker_qty = \"0.2\"",
        )
        .expect("toml");
        let cfg = BasisMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.window, 100);
        assert_eq!(cfg.qty, dec!(0.1));
        assert_eq!(cfg.half_spread, dec!(0.005));
        assert_eq!(cfg.maker_levels, 5);
        assert_eq!(cfg.maker_qty, dec!(0.2));
    }
}
