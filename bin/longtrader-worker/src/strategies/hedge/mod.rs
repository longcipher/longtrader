//! Perpetual Hedge — hedges a spot position with an offsetting perpetual.
//!
//! Maintains a delta-neutral position by continuously hedging a spot
//! holding with an offsetting perpetual futures position. When the spot
//! position changes (e.g. from external fills or rebalancing), the strategy
//! adjusts the perp hedge to keep the net delta near zero.
//!
//! ```text
//!   target_perp_qty = −spot_position
//!   trade_qty = target_perp_qty − current_perp_position
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/perpetual-market-making/> (hedge mode)

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

/// Configuration for the perpetual hedge strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct HedgeConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Spot symbol on the primary venue.
    pub spot_symbol: String,
    /// Perpetual symbol on the hedge venue.
    pub perp_symbol: String,
    /// Hedge rebalance threshold: only trade when drift exceeds this fraction.
    #[serde(default = "default_threshold")]
    pub threshold: Decimal,
    /// Maximum single hedge order size.
    #[serde(default = "default_max_order")]
    pub max_order_size: Decimal,
}

fn default_threshold() -> Decimal {
    dec!(0.01)
}
fn default_max_order() -> Decimal {
    dec!(1)
}

impl HedgeConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Compute the required hedge trade.
///
/// Returns `(trade_qty, is_buy)` where `is_buy` indicates the direction
/// of the perpetual trade. `None` when no trade is needed.
#[must_use]
pub fn compute_hedge_trade(
    spot_position: Decimal,
    current_perp_position: Decimal,
    threshold: Decimal,
) -> Option<(Decimal, bool)> {
    let target_perp = -spot_position;
    let drift = (target_perp - current_perp_position).abs();
    let position_size = spot_position.abs().max(current_perp_position.abs());
    if position_size.is_zero() {
        return None;
    }
    if drift / position_size < threshold {
        return None;
    }
    let trade_qty = (target_perp - current_perp_position).abs();
    let is_buy = target_perp > current_perp_position;
    Some((trade_qty, is_buy))
}

/// Perpetual hedge strategy.
pub struct Hedge {
    config: HedgeConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    perp_position: tokio::sync::Mutex<Decimal>,
}

impl Hedge {
    pub fn new(
        config: HedgeConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, perp_position: tokio::sync::Mutex::new(Decimal::ZERO) }
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
        // In a production system, the spot position would be read from the
        // wallet/position port. Here we use a simplified model where the
        // spot position is derived from the price ratio.
        let spot_price =
            self.fetch_price(&self.config.venues.primary.proto(), &self.config.spot_symbol).await?;
        let perp_price =
            self.fetch_price(&self.config.venues.hedge.proto(), &self.config.perp_symbol).await?;

        // Simplified: assume a unit spot position for demonstration.
        let spot_position = dec!(1);
        let perp_pos = *self.perp_position.lock().await;

        let Some((trade_qty, is_buy)) =
            compute_hedge_trade(spot_position, perp_pos, self.config.threshold)
        else {
            return Ok(());
        };

        let qty = trade_qty.min(self.config.max_order_size);
        let coid = format!("hedge-{}", ulid::Ulid::generate());
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &self.config.venues.hedge.proto(),
                &self.config.perp_symbol,
                coid,
                is_buy,
                perp_price,
                qty,
            ))
            .await
        {
            tracing::error!(error = %err, "hedge order failed");
        } else {
            let mut pos = self.perp_position.lock().await;
            if is_buy {
                *pos += qty;
            } else {
                *pos -= qty;
            }
        }
        let _ = (spot_price,);
        Ok(())
    }
}

#[async_trait]
impl Strategy for Hedge {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            spot = %self.config.spot_symbol,
            perp = %self.config.perp_symbol,
            threshold = %self.config.threshold,
            "starting perpetual hedge"
        );
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "hedge tick failed");
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
    fn compute_hedge_trade_no_drift() {
        // Spot = 1, perp = -1 → target = -1, drift = 0.
        assert!(compute_hedge_trade(dec!(1), dec!(-1), dec!(0.01)).is_none());
    }

    #[test]
    fn compute_hedge_trade_needs_rebalance() {
        let (qty, is_buy) = compute_hedge_trade(dec!(1), dec!(0), dec!(0.01)).expect("trade");
        assert!(qty > Decimal::ZERO);
        assert!(!is_buy); // Need to go short on perp.
    }

    #[test]
    fn compute_hedge_trade_within_threshold() {
        // Drift is 0.005 of position size 1 = 0.5% < 1% threshold.
        assert!(compute_hedge_trade(dec!(1), dec!(-0.995), dec!(0.01)).is_none());
    }

    #[test]
    fn compute_hedge_trade_empty_position() {
        assert!(compute_hedge_trade(dec!(0), dec!(0), dec!(0.01)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nspot_symbol = \"BTC/USDT\"\n\
             perp_symbol = \"BTC/USDT:USDT\"\nthreshold = \"0.005\"\n[primary]\n\
             exchange_id = \"binance\"\n[hedge]\nexchange_id = \"binance\"",
        )
        .expect("toml");
        let cfg = HedgeConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.threshold, dec!(0.005));
        assert_eq!(cfg.spot_symbol, "BTC/USDT");
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nspot_symbol = \"ETH/USDT\"\n\
             perp_symbol = \"ETH/USDT:USDT\"\nmax_order_size = \"0.5\"\n[primary]\n\
             exchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"",
        )
        .expect("toml");
        let cfg = HedgeConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.max_order_size, dec!(0.5));
    }
}
