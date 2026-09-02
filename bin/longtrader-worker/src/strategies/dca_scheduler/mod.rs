//! Periodic accumulation / schedule refill strategy (wall-clock driven).
//!
//! Wall-clock driven: every `interval_secs` the strategy submits one market
//! order of `qty` (buy by default, sell for refill schedules). The first
//! order fires immediately on start so short intervals behave predictably.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::TradingGateway,
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the worker DCA scheduler.
#[derive(Debug, Clone, Deserialize)]
pub struct DcaSchedulerConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Seconds between orders.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_is_buy")]
    pub is_buy: bool,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_interval() -> u64 {
    86_400
}
const fn default_is_buy() -> bool {
    true
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl DcaSchedulerConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Worker DCA scheduler strategy.
pub struct DcaScheduler {
    config: DcaSchedulerConfig,
    gateway: Arc<dyn TradingGateway>,
}

impl DcaScheduler {
    pub fn new(config: DcaSchedulerConfig, gateway: Arc<dyn TradingGateway>) -> Self {
        Self { config, gateway }
    }

    /// Submits one scheduled order.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let coid = format!("dca-{}", ulid::Ulid::generate());
        let request = market_order(
            &exchange,
            &self.config.common.symbol,
            coid,
            self.config.is_buy,
            self.config.qty,
        );
        self.gateway.create_order(request).await?;
        tracing::info!(
            side = if self.config.is_buy { "buy" } else { "sell" },
            qty = %self.config.qty,
            "dca order submitted"
        );
        Ok(())
    }
}

#[async_trait]
impl Strategy for DcaScheduler {
    async fn run(&self) -> Result<()> {
        let interval = std::time::Duration::from_secs(self.config.interval_secs.max(1));
        // Fire immediately, then on the fixed cadence.
        if let Err(err) = self.tick().await {
            tracing::error!(error = %err, "dca initial tick failed");
        }
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "dca tick failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, proto::trading};

    #[tokio::test]
    async fn tick_submits_market_buy() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let config = DcaSchedulerConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nqty = \"0.5\"").expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let dca = DcaScheduler::new(config, gateway);
        dca.tick().await.expect("tick");
        let open = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("open orders");
        assert_eq!(open.len(), 1);
        assert!(open[0].client_order_id.starts_with("dca-"));
        assert_eq!(open[0].side, buffa::EnumValue::Known(trading::OrderSide::Buy));
    }

    #[test]
    fn sell_mode_parses() {
        let table: toml::Table =
            toml::from_str("symbol = \"BNBUSDT\"\nis_buy = false\ninterval_secs = 3600")
                .expect("toml");
        let cfg = DcaSchedulerConfig::from_params(&table).expect("parse");
        assert!(!cfg.is_buy);
        assert_eq!(cfg.interval_secs, 3600);
    }
}
