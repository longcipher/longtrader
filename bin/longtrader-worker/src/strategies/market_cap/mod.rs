//! Market-cap weighted rebalance.
//!
//! A thin preset over the rebalance engine: target weights are supplied as
//! a static table (manually maintained from market-cap rankings) instead of
//! a live data feed.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the market-cap rebalance.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketCapConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_quote")]
    pub quote_asset: String,
    /// Static weights per asset derived from market-cap ranking.
    pub weights: std::collections::BTreeMap<String, Decimal>,
}

fn default_quote() -> String {
    String::from("USDT")
}

impl MarketCapConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Market-cap rebalance strategy.
pub struct MarketCap {
    inner: crate::strategies::rebalance::Rebalance,
}

impl MarketCap {
    pub fn new(
        config: MarketCapConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let rc = crate::strategies::rebalance::RebalanceConfig {
            common: config.common,
            quote_asset: config.quote_asset,
            targets: config.weights,
            band_pct: Decimal::new(5, 2),
        };
        Self { inner: crate::strategies::rebalance::Rebalance::new(rc, gateway, market) }
    }
}

#[async_trait]
impl Strategy for MarketCap {
    async fn run(&self) -> Result<()> {
        self.inner.run().await
    }
}
