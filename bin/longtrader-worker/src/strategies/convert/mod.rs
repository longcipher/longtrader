//! Periodic asset conversion strategy.
//!
//! On a fixed cadence, invokes the venue's convert operation to swap
//! `from_asset` into `to_asset` when the source balance exceeds
//! `min_amount`. Reached through the generic [`VenueOpInvoker`] port.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::VenueOpInvoker,
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the convert strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct ConvertConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Source asset converted away.
    pub from_asset: String,
    /// Destination asset accumulated.
    pub to_asset: String,
    /// Convert only when the source balance exceeds this amount.
    pub min_amount: Decimal,
    /// Seconds between conversion attempts.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
}

const fn default_interval() -> u64 {
    3_600
}

impl ConvertConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Convert strategy.
pub struct Convert {
    config: ConvertConfig,
    ops: Arc<dyn VenueOpInvoker>,
}

impl Convert {
    pub fn new(config: ConvertConfig, ops: Arc<dyn VenueOpInvoker>) -> Self {
        Self { config, ops }
    }

    /// One conversion attempt.
    pub async fn tick(&self) -> Result<bool> {
        let exchange = self.config.common.proto_exchange_id();
        let mut params = serde_json::Map::new();
        params.insert("fromAsset".into(), serde_json::json!(self.config.from_asset));
        params.insert("toAsset".into(), serde_json::json!(self.config.to_asset));
        params.insert("amount".into(), serde_json::json!(self.config.min_amount.to_string()));
        let response = self.ops.invoke_venue_op(&exchange, "wallet.convert", params).await?;
        let ok = response.get("status").and_then(serde_json::Value::as_str) == Some("ok");
        if ok {
            tracing::info!(
                from = %self.config.from_asset,
                to = %self.config.to_asset,
                "convert executed"
            );
        }
        Ok(ok)
    }
}

#[async_trait]
impl Strategy for Convert {
    async fn run(&self) -> Result<()> {
        let interval = std::time::Duration::from_secs(self.config.interval_secs.max(1));
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "convert tick failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, ports::TradingGateway};

    #[tokio::test]
    async fn convert_invokes_venue_op() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let config = ConvertConfig::from_params(
            &toml::from_str(
                "symbol = \"BTCUSDT\"\nfrom_asset = \"DUST\"\nto_asset = \"USDT\"\nmin_amount = \"10\"",
            )
            .expect("toml"),
        )
        .expect("config");
        let _gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Convert::new(config, ops);
        assert!(s.tick().await.expect("tick"));
    }

    #[test]
    fn config_parses() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\nfrom_asset = \"A\"\nto_asset = \"B\"\nmin_amount = \"5\"",
        )
        .expect("toml");
        let cfg = ConvertConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.to_asset, "B");
    }
}
