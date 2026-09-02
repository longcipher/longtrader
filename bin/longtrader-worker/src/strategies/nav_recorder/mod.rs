//! NAV (net asset value) recorder.
//!
//! Periodically snapshots the account, values every balance at the venue
//! ticker price into the quote currency, and logs the total. The log
//! stream doubles as the control-plane record.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the NAV recorder.
#[derive(Debug, Clone, Deserialize)]
pub struct NavRecorderConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Quote currency used for valuation.
    #[serde(default = "default_quote")]
    pub quote_asset: String,
}

fn default_quote() -> String {
    String::from("USDT")
}

impl NavRecorderConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Values one balance row into the quote currency.
pub fn value_balance(
    currency: &str,
    total: Decimal,
    price: Option<Decimal>,
    quote: &str,
) -> Decimal {
    if currency == quote {
        return total;
    }
    match price {
        Some(p) if p > Decimal::ZERO => total * p,
        _ => Decimal::ZERO,
    }
}

/// NAV recorder strategy.
pub struct NavRecorder {
    config: NavRecorderConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl NavRecorder {
    pub fn new(
        config: NavRecorderConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    async fn price_of(&self, currency: &str) -> Option<Decimal> {
        let symbol = format!("{currency}{}", self.config.quote_asset);
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol,
                ..Default::default()
            })
            .await
            .ok()?;
        ticker.last.as_option().and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
    }

    /// One snapshot cycle; returns the NAV in the quote currency.
    pub async fn tick(&self) -> Result<Decimal> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.gateway.sync_state(&exchange).await?;
        let mut nav = Decimal::ZERO;
        for balance in &snap.balances {
            let total = balance
                .total
                .as_option()
                .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
                .unwrap_or_default();
            let price = if balance.currency == self.config.quote_asset {
                None
            } else {
                self.price_of(&balance.currency).await
            };
            nav += value_balance(&balance.currency, total, price, &self.config.quote_asset);
        }
        tracing::info!(%nav, quote = %self.config.quote_asset, "nav snapshot");
        Ok(nav)
    }
}

#[async_trait]
impl Strategy for NavRecorder {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "nav-recorder tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}
