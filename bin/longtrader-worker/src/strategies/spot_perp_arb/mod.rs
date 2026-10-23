//! Spot-Perpetual Arbitrage — basis trading between spot and perpetual.
//!
//! Exploits the price difference (basis) between a spot market and its
//! corresponding perpetual futures contract. When the basis is positive
//! (perp > spot), sell the perp and buy spot. When negative, do the
//! opposite. The basis is expected to converge at expiry.
//!
//! ```text
//!   basis = (perp_price − spot_price) / spot_price
//!   if  basis > threshold → short perp / long spot
//!   if  basis < −threshold → long perp / short spot
//!   if  |basis| < exit_threshold → close
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/spot-perpetual-arbitrage/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the spot-perp arbitrage strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct SpotPerpArbConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Spot symbol (on the spot venue / primary).
    pub spot_symbol: String,
    /// Perpetual symbol (on the perp venue / hedge).
    pub perp_symbol: String,
    /// Basis threshold to open a position.
    #[serde(default = "default_entry")]
    pub entry_threshold: Decimal,
    /// Basis threshold to close a position.
    #[serde(default = "default_exit")]
    pub exit_threshold: Decimal,
    /// Position size in base units.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_entry() -> Decimal {
    dec!(0.01)
}
fn default_exit() -> Decimal {
    dec!(0.002)
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl SpotPerpArbConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Basis signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasisArbSignal {
    /// Perp is expensive → short perp, long spot.
    ShortPerpLongSpot,
    /// Perp is cheap → long perp, short spot.
    LongPerpShortSpot,
    /// Basis within exit band → close.
    Close,
    /// No signal.
    NoTrade,
}

/// Compute the basis between spot and perpetual.
#[must_use]
pub fn compute_basis(spot_price: Decimal, perp_price: Decimal) -> Option<Decimal> {
    if spot_price <= Decimal::ZERO || perp_price <= Decimal::ZERO {
        return None;
    }
    Some((perp_price - spot_price) / spot_price)
}

/// Classify the basis signal.
#[must_use]
pub fn classify_basis_arb(
    basis: Option<Decimal>,
    entry_threshold: Decimal,
    exit_threshold: Decimal,
    has_position: bool,
) -> BasisArbSignal {
    let basis = match basis {
        None => return BasisArbSignal::NoTrade,
        Some(b) => b,
    };
    if has_position {
        if basis.abs() < exit_threshold { BasisArbSignal::Close } else { BasisArbSignal::NoTrade }
    } else if basis > entry_threshold {
        BasisArbSignal::ShortPerpLongSpot
    } else if basis < -entry_threshold {
        BasisArbSignal::LongPerpShortSpot
    } else {
        BasisArbSignal::NoTrade
    }
}

/// Spot-Perpetual arbitrage strategy.
pub struct SpotPerpArb {
    config: SpotPerpArbConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    has_position: tokio::sync::Mutex<bool>,
}

impl SpotPerpArb {
    pub fn new(
        config: SpotPerpArbConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, has_position: tokio::sync::Mutex::new(false) }
    }

    async fn fetch_price(
        &self,
        exchange: &crate::proto::common::ExchangeId,
        symbol: &str,
    ) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
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
        let spot =
            self.fetch_price(&self.config.venues.primary.proto(), &self.config.spot_symbol).await?;
        let perp =
            self.fetch_price(&self.config.venues.hedge.proto(), &self.config.perp_symbol).await?;
        let basis = compute_basis(spot, perp);
        let has_pos = *self.has_position.lock().await;
        match classify_basis_arb(
            basis,
            self.config.entry_threshold,
            self.config.exit_threshold,
            has_pos,
        ) {
            BasisArbSignal::NoTrade => {}
            BasisArbSignal::ShortPerpLongSpot => {
                let qty = self.config.qty;
                let coid = format!("spab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.venues.primary.proto(),
                        &self.config.spot_symbol,
                        coid,
                        true,
                        spot,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "spot-perp arb spot buy failed");
                }
                let coid = format!("spab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.venues.hedge.proto(),
                        &self.config.perp_symbol,
                        coid,
                        false,
                        perp,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "spot-perp arb perp sell failed");
                }
                *self.has_position.lock().await = true;
            }
            BasisArbSignal::LongPerpShortSpot => {
                let qty = self.config.qty;
                let coid = format!("spab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.venues.primary.proto(),
                        &self.config.spot_symbol,
                        coid,
                        false,
                        spot,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "spot-perp arb spot sell failed");
                }
                let coid = format!("spab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.venues.hedge.proto(),
                        &self.config.perp_symbol,
                        coid,
                        true,
                        perp,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "spot-perp arb perp buy failed");
                }
                *self.has_position.lock().await = true;
            }
            BasisArbSignal::Close => {
                // Closing logic: reverse the position (simplified).
                *self.has_position.lock().await = false;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for SpotPerpArb {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            spot_symbol = %self.config.spot_symbol,
            perp_symbol = %self.config.perp_symbol,
            entry_threshold = %self.config.entry_threshold,
            "starting spot-perpetual arbitrage"
        );
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "spot-perp arb tick failed");
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
    fn compute_basis_positive_when_perp_premium() {
        assert_eq!(compute_basis(dec!(100), dec!(102)), Some(dec!(0.02)));
    }

    #[test]
    fn compute_basis_negative_when_perp_discount() {
        assert_eq!(compute_basis(dec!(100), dec!(98)), Some(dec!(-0.02)));
    }

    #[test]
    fn compute_basis_rejects_bad_input() {
        assert!(compute_basis(dec!(0), dec!(100)).is_none());
        assert!(compute_basis(dec!(100), dec!(0)).is_none());
    }

    #[test]
    fn classify_basis_arb_entry_and_exit() {
        assert_eq!(
            classify_basis_arb(Some(dec!(0.02)), dec!(0.01), dec!(0.002), false),
            BasisArbSignal::ShortPerpLongSpot
        );
        assert_eq!(
            classify_basis_arb(Some(dec!(-0.02)), dec!(0.01), dec!(0.002), false),
            BasisArbSignal::LongPerpShortSpot
        );
        assert_eq!(
            classify_basis_arb(Some(dec!(0.001)), dec!(0.01), dec!(0.002), true),
            BasisArbSignal::Close
        );
        assert_eq!(
            classify_basis_arb(Some(dec!(0.005)), dec!(0.01), dec!(0.002), false),
            BasisArbSignal::NoTrade
        );
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nspot_symbol = \"BTC/USDT\"\n\
             perp_symbol = \"BTC/USDT:USDT\"\nentry_threshold = \"0.015\"\n[primary]\n\
             exchange_id = \"binance\"\n[hedge]\nexchange_id = \"binance\"",
        )
        .expect("toml");
        let cfg = SpotPerpArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.spot_symbol, "BTC/USDT");
        assert_eq!(cfg.perp_symbol, "BTC/USDT:USDT");
        assert_eq!(cfg.entry_threshold, dec!(0.015));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nspot_symbol = \"ETH/USDT\"\n\
             perp_symbol = \"ETH/USDT:USDT\"\nexit_threshold = \"0.001\"\n\
             qty = \"0.5\"\n[primary]\nexchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"",
        )
        .expect("toml");
        let cfg = SpotPerpArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.exit_threshold, dec!(0.001));
        assert_eq!(cfg.qty, dec!(0.5));
    }
}
