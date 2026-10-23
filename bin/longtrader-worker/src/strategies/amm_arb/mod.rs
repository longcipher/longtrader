//! AMM Arbitrage — CEX-DEX arbitrage between a centralised exchange and an
//! automated market maker (AMM) venue.
//!
//! Monitors the price of an asset on both a CEX and a DEX AMM. When the
//! price discrepancy exceeds the gas cost + profit threshold, the strategy
//! buys on the cheaper venue and sells on the more expensive one.
//!
//! ```text
//!   spread = |cex_price − amm_price| / min(cex_price, amm_price)
//!   if spread > threshold + gas_cost → execute arb
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/amm-arbitrage/>

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

/// Configuration for the AMM arbitrage strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AmmArbConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Minimum spread fraction required to trigger an arbitrage.
    #[serde(default = "default_threshold")]
    pub threshold: Decimal,
    /// Estimated gas cost as a fraction of trade value.
    #[serde(default = "default_gas_cost")]
    pub gas_cost: Decimal,
    /// Maximum position size per arbitrage (base units).
    #[serde(default = "default_max_qty")]
    pub max_qty: Decimal,
    /// Minimum profit in quote units to cover fixed costs.
    #[serde(default = "default_min_profit")]
    pub min_profit: Decimal,
}

fn default_threshold() -> Decimal {
    dec!(0.005)
}
fn default_gas_cost() -> Decimal {
    dec!(0.001)
}
fn default_max_qty() -> Decimal {
    dec!(1)
}
fn default_min_profit() -> Decimal {
    dec!(1)
}

impl AmmArbConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// AMM arbitrage signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbSignal {
    /// Buy on CEX, sell on AMM.
    BuyCexSellAmm,
    /// Buy on AMM, sell on CEX.
    BuyAmmSellCex,
    /// No arbitrage opportunity.
    NoArb,
}

/// Detect an arbitrage opportunity between two venue prices.
///
/// `None` when inputs are invalid.
#[must_use]
pub fn detect_arb(
    cex_price: Decimal,
    amm_price: Decimal,
    threshold: Decimal,
    gas_cost: Decimal,
) -> Option<ArbSignal> {
    if cex_price <= Decimal::ZERO || amm_price <= Decimal::ZERO {
        return None;
    }
    let min_price = cex_price.min(amm_price);
    let spread = (cex_price - amm_price).abs() / min_price;
    let total_cost = threshold + gas_cost;
    if spread < total_cost {
        return Some(ArbSignal::NoArb);
    }
    if cex_price < amm_price {
        Some(ArbSignal::BuyCexSellAmm)
    } else {
        Some(ArbSignal::BuyAmmSellCex)
    }
}

/// Compute the profitable quantity given the price difference and depth.
#[must_use]
pub fn arb_qty(
    cex_price: Decimal,
    amm_price: Decimal,
    available_depth: Decimal,
    max_qty: Decimal,
    min_profit: Decimal,
) -> Option<Decimal> {
    if cex_price <= Decimal::ZERO || amm_price <= Decimal::ZERO {
        return None;
    }
    let price_diff = (cex_price - amm_price).abs();
    if price_diff.is_zero() {
        return None;
    }
    // Minimum qty to cover min_profit.
    let min_qty = min_profit / price_diff;
    if min_qty > max_qty {
        return None;
    }
    Some(min_qty.min(max_qty).min(available_depth))
}

/// AMM arbitrage strategy.
pub struct AmmArb {
    config: AmmArbConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl AmmArb {
    pub fn new(
        config: AmmArbConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
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
        let cex = self.config.venues.primary.proto();
        let amm = self.config.venues.hedge.proto();
        let cex_price = self.fetch_price(&cex, &self.config.venues.symbol).await?;
        let amm_price = self.fetch_price(&amm, &self.config.venues.symbol).await?;

        match detect_arb(cex_price, amm_price, self.config.threshold, self.config.gas_cost) {
            None | Some(ArbSignal::NoArb) => return Ok(()),
            Some(ArbSignal::BuyCexSellAmm) => {
                let qty = arb_qty(
                    cex_price,
                    amm_price,
                    self.config.max_qty,
                    self.config.max_qty,
                    self.config.min_profit,
                )
                .unwrap_or(self.config.max_qty);
                let coid = format!("ammab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &cex,
                        &self.config.venues.symbol,
                        coid,
                        true,
                        cex_price,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "AMM arb CEX buy failed");
                }
                let coid = format!("ammab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &amm,
                        &self.config.venues.symbol,
                        coid,
                        false,
                        amm_price,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "AMM arb AMM sell failed");
                }
            }
            Some(ArbSignal::BuyAmmSellCex) => {
                let qty = arb_qty(
                    cex_price,
                    amm_price,
                    self.config.max_qty,
                    self.config.max_qty,
                    self.config.min_profit,
                )
                .unwrap_or(self.config.max_qty);
                let coid = format!("ammab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &amm,
                        &self.config.venues.symbol,
                        coid,
                        true,
                        amm_price,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "AMM arb AMM buy failed");
                }
                let coid = format!("ammab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &cex,
                        &self.config.venues.symbol,
                        coid,
                        false,
                        cex_price,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "AMM arb CEX sell failed");
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for AmmArb {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.venues.symbol,
            cex = %self.config.venues.primary.exchange_id,
            amm = %self.config.venues.hedge.exchange_id,
            threshold = %self.config.threshold,
            "starting AMM arbitrage"
        );
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "AMM arb tick failed");
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
    fn detect_arb_buy_cex_when_cex_cheaper() {
        assert_eq!(
            detect_arb(dec!(99), dec!(102), dec!(0.005), dec!(0.001)),
            Some(ArbSignal::BuyCexSellAmm)
        );
    }

    #[test]
    fn detect_arb_buy_amm_when_amm_cheaper() {
        assert_eq!(
            detect_arb(dec!(102), dec!(99), dec!(0.005), dec!(0.001)),
            Some(ArbSignal::BuyAmmSellCex)
        );
    }

    #[test]
    fn detect_arb_no_opportunity() {
        assert_eq!(
            detect_arb(dec!(100), dec!(100.1), dec!(0.005), dec!(0.001)),
            Some(ArbSignal::NoArb)
        );
    }

    #[test]
    fn detect_arb_rejects_bad_input() {
        assert!(detect_arb(dec!(0), dec!(100), dec!(0.005), dec!(0.001)).is_none());
        assert!(detect_arb(dec!(100), dec!(0), dec!(0.005), dec!(0.001)).is_none());
    }

    #[test]
    fn arb_qty_respects_bounds() {
        let qty = arb_qty(dec!(100), dec!(95), dec!(10), dec!(5), dec!(1)).expect("qty");
        assert!(qty <= dec!(5));
        assert!(qty > Decimal::ZERO);
    }

    #[test]
    fn arb_qty_rejects_no_spread() {
        assert!(arb_qty(dec!(100), dec!(100), dec!(10), dec!(5), dec!(1)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nthreshold = \"0.01\"\n[primary]\n\
             exchange_id = \"binance\"\n[hedge]\nexchange_id = \"uniswap\"",
        )
        .expect("toml");
        let cfg = AmmArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.threshold, dec!(0.01));
        assert_eq!(cfg.venues.primary.exchange_id, "binance");
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\ngas_cost = \"0.002\"\nmax_qty = \"2\"\n\
             min_profit = \"5\"\n[primary]\nexchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"",
        )
        .expect("toml");
        let cfg = AmmArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.gas_cost, dec!(0.002));
        assert_eq!(cfg.max_qty, dec!(2));
        assert_eq!(cfg.min_profit, dec!(5));
    }
}
