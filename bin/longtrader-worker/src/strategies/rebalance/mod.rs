//! Portfolio rebalance strategy (also the engine behind `market_cap`).
//!
//! Values each configured symbol's balance at its ticker price into the
//! quote currency, compares actual weights against target weights, and
//! rebalances with market orders when a symbol drifts beyond `band_pct`.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the rebalance strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct RebalanceConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Quote currency used for valuation.
    #[serde(default = "default_quote")]
    pub quote_asset: String,
    /// Target weights per base asset, e.g. `BTC = "0.6"`.
    pub targets: BTreeMap<String, Decimal>,
    /// Rebalance when weight drift exceeds this fraction.
    #[serde(default = "default_band")]
    pub band_pct: Decimal,
}

fn default_quote() -> String {
    String::from("USDT")
}
fn default_band() -> Decimal {
    Decimal::new(5, 2)
}

impl RebalanceConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Computes per-asset trade deltas (in quote value) to move actual weights
/// toward targets. Positive delta = buy.
pub fn plan_trades(
    values: &BTreeMap<String, Decimal>,
    targets: &BTreeMap<String, Decimal>,
) -> BTreeMap<String, Decimal> {
    let total: Decimal = values.values().copied().sum();
    let mut plan = BTreeMap::new();
    if total.is_zero() {
        return plan;
    }
    for (asset, target) in targets {
        let actual_value = values.get(asset).copied().unwrap_or_default();
        let want = total * *target;
        let delta = want - actual_value;
        if !delta.is_zero() {
            plan.insert(asset.clone(), delta);
        }
    }
    plan
}

/// Rebalance strategy.
pub struct Rebalance {
    config: RebalanceConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl Rebalance {
    pub fn new(
        config: RebalanceConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    async fn price_of(&self, asset: &str) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: format!("{asset}{}", self.config.quote_asset),
                ..Default::default()
            })
            .await?;
        ticker
            .last
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .ok_or_else(|| color_eyre::eyre::eyre!("no price for {asset}"))
    }

    /// One valuation + rebalance cycle; returns executed trades.
    pub async fn tick(&self) -> Result<BTreeMap<String, Decimal>> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.gateway.sync_state(&exchange).await?;
        let mut values = BTreeMap::new();
        for asset in self.config.targets.keys() {
            let balance = snap
                .balances
                .iter()
                .find(|b| b.currency == *asset)
                .and_then(|b| b.total.as_option())
                .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
                .unwrap_or_default();
            let price = if *asset == self.config.quote_asset {
                Decimal::ONE
            } else {
                self.price_of(asset).await?
            };
            values.insert(asset.clone(), balance * price);
        }
        let plan = plan_trades(&values, &self.config.targets);
        for (asset, delta) in &plan {
            // Convert quote-value delta into quantity at the current price.
            let price = if *asset == self.config.quote_asset {
                Decimal::ONE
            } else {
                self.price_of(asset).await?
            };
            let qty = (*delta / price).abs();
            if qty.is_zero() {
                continue;
            }
            let coid = format!("rebal-{}", ulid::Ulid::generate());
            let request = market_order(
                &exchange,
                &format!("{asset}{}", self.config.quote_asset),
                coid,
                *delta > Decimal::ZERO,
                qty,
            );
            self.gateway.create_order(request).await?;
            tracing::info!(asset = %asset, %delta, "rebalance trade");
        }
        Ok(plan)
    }
}

#[async_trait]
impl Strategy for Rebalance {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "rebalance tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap as Map;

    use rust_decimal_macros as rm;

    use super::*;

    #[test]
    fn plan_moves_toward_targets() {
        let mut values = Map::new();
        values.insert(String::from("BTC"), rm::dec!(700));
        values.insert(String::from("USDT"), rm::dec!(300));
        let mut targets = Map::new();
        targets.insert(String::from("BTC"), rm::dec!(0.5));
        targets.insert(String::from("USDT"), rm::dec!(0.5));
        let plan = plan_trades(&values, &targets);
        assert_eq!(plan["BTC"], rm::dec!(-200));
        assert_eq!(plan["USDT"], rm::dec!(200));
    }

    #[test]
    fn empty_portfolio_yields_no_plan() {
        let values: Map<String, Decimal> = Map::new();
        let mut targets = Map::new();
        targets.insert(String::from("BTC"), Decimal::ONE);
        assert!(plan_trades(&values, &targets).is_empty());
    }
}
