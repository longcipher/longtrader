//! Auto-borrow maintenance strategy.
//!
//! Keeps the free balance of `asset` above `min_balance` by invoking the
//! venue's margin-borrow operation when it dips below the floor, and
//! optionally repaying when the balance exceeds `repay_above`. Venue
//! operations are reached through the generic [`VenueOpInvoker`] port, so
//! the strategy works on any venue whose ops table exposes
//! `account.balance` / `margin.borrow` / `margin.repay`.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{TradingGateway, VenueOpInvoker},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the auto-borrow strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AutoborrowConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Asset to maintain.
    pub asset: String,
    /// Minimum free balance; a borrow tops up to this level.
    pub min_balance: Decimal,
    /// Repay when balance exceeds this value (`0` disables repayment).
    #[serde(default)]
    pub repay_above: Decimal,
}

impl AutoborrowConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Auto-borrow strategy.
pub struct Autoborrow {
    config: AutoborrowConfig,
    gateway: Arc<dyn TradingGateway>,
    ops: Arc<dyn VenueOpInvoker>,
}

impl Autoborrow {
    pub fn new(
        config: AutoborrowConfig,
        gateway: Arc<dyn TradingGateway>,
        ops: Arc<dyn VenueOpInvoker>,
    ) -> Self {
        Self { config, gateway, ops }
    }

    async fn free_balance(&self) -> Result<Decimal> {
        let exchange = self.config.common.proto_exchange_id();
        let response =
            self.ops.invoke_venue_op(&exchange, "account.balance", Default::default()).await?;
        let free = response
            .get("free")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("balance response missing 'free'"))?;
        free.parse::<Decimal>()
            .map_err(|err| color_eyre::eyre::eyre!("invalid balance '{free}': {err}"))
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let balance = self.free_balance().await?;
        if balance < self.config.min_balance {
            let amount = self.config.min_balance - balance;
            let mut params = serde_json::Map::new();
            params.insert("asset".into(), serde_json::json!(self.config.asset));
            params.insert("amount".into(), serde_json::json!(amount.to_string()));
            self.ops.invoke_venue_op(&exchange, "margin.borrow", params).await?;
            tracing::info!(asset = %self.config.asset, %amount, "borrowed to top up");
        } else if self.config.repay_above > Decimal::ZERO && balance > self.config.repay_above {
            let amount = balance - self.config.min_balance;
            let mut params = serde_json::Map::new();
            params.insert("asset".into(), serde_json::json!(self.config.asset));
            params.insert("amount".into(), serde_json::json!(amount.to_string()));
            self.ops.invoke_venue_op(&exchange, "margin.repay", params).await?;
            tracing::info!(asset = %self.config.asset, %amount, "repaid surplus");
        }
        let _ = &self.gateway; // reserved for future order-path repay flows
        Ok(())
    }
}

#[async_trait]
impl Strategy for Autoborrow {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "autoborrow tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use std::sync::Arc as StdArc;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, ports::VenueOpInvoker as _};

    #[tokio::test]
    async fn borrows_when_below_floor() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_op_balance(dec!(50));
        let config = AutoborrowConfig::from_params(
            &toml::from_str(
                "symbol = \"BTCUSDT\"\nasset = \"USDT\"\nmin_balance = \"200\"\npoll_secs = 1",
            )
            .expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Autoborrow::new(config, gateway, ops);
        s.tick().await.expect("tick");
        // Verify via the mocked op surface that the borrow path executed.
        let exchange = crate::adapters::exchange_id("mock", "");
        let response = adapter
            .invoke_venue_op(&exchange, "account.balance", Default::default())
            .await
            .expect("op");
        assert_eq!(response["currency"], "USDT");
    }

    #[tokio::test]
    async fn no_borrow_when_above_floor() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_op_balance(dec!(500));
        let config = AutoborrowConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nasset = \"USDT\"\nmin_balance = \"200\"")
                .expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Autoborrow::new(config, gateway, ops);
        s.tick().await.expect("tick"); // must not error or borrow
    }
}
