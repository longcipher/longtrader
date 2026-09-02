//! Predicted-return-rate entry strategy.
//!
//! Reads an externally produced signal file containing the predicted return
//! for the next horizon (one decimal number per line; the last line wins)
//! and trades its sign against `entry_threshold`. This keeps model
//! inference out of the trading process: any external system can write the
//! file and the strategy reacts on the next poll.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::TradingGateway,
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the IRR strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct IrrConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Path to the signal file (last non-empty line = predicted return).
    pub signal_file: String,
    /// Entry threshold on the predicted return.
    #[serde(default = "default_threshold")]
    pub entry_threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_threshold() -> Decimal {
    Decimal::new(1, 2)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl IrrConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Reads the latest predicted return from a signal file.
pub fn read_signal(path: &str) -> Option<Decimal> {
    std::fs::read_to_string(path).ok()?.lines().rev().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return None;
        }
        trimmed.parse::<Decimal>().ok()
    })
}

/// IRR strategy.
pub struct Irr {
    config: IrrConfig,
    gateway: Arc<dyn TradingGateway>,
    held: Mutex<bool>,
}

impl Irr {
    pub fn new(config: IrrConfig, gateway: Arc<dyn TradingGateway>) -> Self {
        Self { config, gateway, held: Mutex::new(false) }
    }

    /// One decision cycle.
    pub async fn tick(&self) -> Result<()> {
        let Some(signal) = read_signal(&self.config.signal_file) else {
            tracing::debug!("irr: no signal available");
            return Ok(());
        };
        let mut held = self.held.lock().await;
        let exchange = self.config.common.proto_exchange_id();
        if !*held && signal > self.config.entry_threshold {
            *held = true;
            drop(held);
            let coid = format!("irr-{}", ulid::Ulid::generate());
            self.gateway
                .create_order(market_order(
                    &exchange,
                    &self.config.common.symbol,
                    coid,
                    true,
                    self.config.qty,
                ))
                .await?;
        } else if *held && signal < -self.config.entry_threshold {
            *held = false;
            drop(held);
            let coid = format!("irr-{}", ulid::Ulid::generate());
            self.gateway
                .create_order(market_order(
                    &exchange,
                    &self.config.common.symbol,
                    coid,
                    false,
                    self.config.qty,
                ))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for Irr {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "irr tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_last_signal_line() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("signal.txt");
        std::fs::write(&path, "0.01\n0.05\n").expect("write");
        assert_eq!(read_signal(path.to_str().expect("utf8")), Some(Decimal::new(5, 2)));
        std::fs::write(&path, "not-a-number\n-0.02\n").expect("write");
        assert_eq!(read_signal(path.to_str().expect("utf8")), Some(Decimal::new(-2, 2)));
    }

    #[test]
    fn missing_file_yields_none() {
        assert!(read_signal("/nonexistent/signal.txt").is_none());
    }
}
