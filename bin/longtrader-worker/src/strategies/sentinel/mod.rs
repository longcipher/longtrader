//! Connectivity / health sentinel.
//!
//! Periodically probes the backend (ticker + account reads). After
//! `max_consecutive_failures` consecutive failures it trips a kill switch
//! (cancel all orders) so an unhealthy link cannot leave resting orders
//! unmanaged. Recovery resets the counter.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the sentinel.
#[derive(Debug, Clone, Deserialize)]
pub struct SentinelConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_max_failures")]
    pub max_consecutive_failures: u32,
}

const fn default_max_failures() -> u32 {
    3
}

impl SentinelConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Health sentinel strategy.
pub struct Sentinel {
    config: SentinelConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    failures: Mutex<u32>,
}

impl Sentinel {
    pub fn new(
        config: SentinelConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, failures: Mutex::new(0) }
    }

    /// One probe cycle; returns `true` when healthy.
    #[allow(clippy::cognitive_complexity)]
    pub async fn tick(&self) -> Result<bool> {
        let exchange = self.config.common.proto_exchange_id();
        let probe = async {
            self.market
                .fetch_ticker(crate::proto::market::FetchTickerRequest {
                    exchange_id: buffa::MessageField::some(exchange.clone()),
                    symbol: self.config.common.symbol.clone(),
                    ..Default::default()
                })
                .await?;
            self.gateway.get_account(crate::proto::trading::GetAccountRequest::default()).await?;
            Ok::<(), crate::ports::PortError>(())
        }
        .await;

        let mut failures = self.failures.lock().await;
        match probe {
            Ok(()) => {
                if *failures >= self.config.max_consecutive_failures {
                    tracing::warn!("sentinel: link recovered after failures");
                }
                *failures = 0;
                Ok(true)
            }
            Err(err) => {
                *failures += 1;
                let n = *failures;
                drop(failures);
                tracing::error!(error = %err, failures = n, "sentinel: probe failed");
                if n >= self.config.max_consecutive_failures {
                    self.gateway
                        .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                            exchange_id: buffa::MessageField::some(exchange),
                            symbol: String::new(),
                            ..Default::default()
                        })
                        .await?;
                    tracing::error!("sentinel: kill switch tripped, all orders cancelled");
                }
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl Strategy for Sentinel {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            let _ = self.tick().await.inspect_err(|err| {
                tracing::error!(error = %err, "sentinel cycle error");
            });
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::adapters::MockAdapter;

    #[tokio::test]
    async fn healthy_probe_resets_counter() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let config = SentinelConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nmax_consecutive_failures = 2").expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let s = Sentinel::new(config, gateway, market);
        assert!(s.tick().await.expect("tick"));
        assert!(s.tick().await.expect("tick"));
    }

    #[test]
    fn config_parses() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nmax_consecutive_failures = 5").expect("toml");
        let cfg = SentinelConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.max_consecutive_failures, 5);
    }
}
