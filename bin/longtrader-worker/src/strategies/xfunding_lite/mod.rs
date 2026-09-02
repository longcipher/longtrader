//! Funding-rate carry strategy (single-venue lite version).
//!
//! Polls the funding rate: when it exceeds `enter_threshold` the strategy
//! holds a long (collecting funding paid by longs in positive-rate regimes
//! is venue-dependent — here we treat a *negative* rate as favourable for
//! longs and a *positive* rate as favourable for shorts, matching perp
//! conventions where longs pay when the rate is positive). Exits when the
//! rate crosses back through the exit threshold.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{FundingRateSource, TradingGateway},
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the funding-carry strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct XfundingLiteConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Enter (short) when rate >= this value.
    #[serde(default = "default_enter")]
    pub enter_threshold: Decimal,
    /// Exit when the rate magnitude falls below this value.
    #[serde(default = "default_exit")]
    pub exit_threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_enter() -> Decimal {
    Decimal::new(1, 4)
}
fn default_exit() -> Decimal {
    Decimal::new(1, 5)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl XfundingLiteConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Flat,
    Short,
}

/// Funding-carry strategy.
pub struct XfundingLite {
    config: XfundingLiteConfig,
    gateway: Arc<dyn TradingGateway>,
    funding: Arc<dyn FundingRateSource>,
    phase: Mutex<Phase>,
}

impl XfundingLite {
    pub fn new(
        config: XfundingLiteConfig,
        gateway: Arc<dyn TradingGateway>,
        funding: Arc<dyn FundingRateSource>,
    ) -> Self {
        Self { config, gateway, funding, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.funding.fetch_funding_rate(&exchange, &self.config.common.symbol).await?;
        let mut phase = self.phase.lock().await;
        match *phase {
            Phase::Flat => {
                // Positive rate: shorts collect → enter short.
                if snap.rate >= self.config.enter_threshold {
                    *phase = Phase::Short;
                    drop(phase);
                    let coid = format!("xfund-{}", ulid::Ulid::generate());
                    self.gateway
                        .create_order(market_order(
                            &exchange,
                            &self.config.common.symbol,
                            coid,
                            false,
                            self.config.qty,
                        ))
                        .await?;
                    tracing::info!(rate = %snap.rate, "funding carry short opened");
                }
            }
            Phase::Short => {
                if snap.rate.abs() < self.config.exit_threshold {
                    *phase = Phase::Flat;
                    drop(phase);
                    let coid = format!("xfund-{}", ulid::Ulid::generate());
                    self.gateway
                        .create_order(market_order(
                            &exchange,
                            &self.config.common.symbol,
                            coid,
                            true,
                            self.config.qty,
                        ))
                        .await?;
                    tracing::info!(rate = %snap.rate, "funding carry closed");
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for XfundingLite {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "xfunding-lite tick failed");
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
    use crate::{adapters::MockAdapter, ports::TradingGateway as _};

    #[tokio::test]
    async fn positive_rate_opens_short_and_zero_rate_closes() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.0005));
        let config = XfundingLiteConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nqty = \"0.1\"").expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let funding: StdArc<dyn FundingRateSource> = adapter.clone();
        let s = XfundingLite::new(config, gateway, funding);

        s.tick().await.expect("tick");
        assert_eq!(adapter.trigger_orders().len(), 0);
        let open = adapter
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(open.len(), 1, "short opened");

        adapter.set_funding_rate(Decimal::ZERO);
        s.tick().await.expect("tick");
        let open = adapter
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(open.len(), 2, "close buy placed");
    }
}
