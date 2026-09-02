//! Deposit-sweep transfer strategy.
//!
//! Polls the deposit ledger; every newly completed deposit is transferred
//! to the destination account label (e.g. sweeping exchange sub-account
//! deposits into the master account). Seen deposit ids are tracked
//! in-memory so each deposit transfers exactly once per process lifetime.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::WalletGateway,
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the deposit-sweep strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct DepositTransferConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Destination account label for swept funds.
    pub dest_label: String,
    /// Ignore deposits below this amount.
    #[serde(default)]
    pub ignore_below: Decimal,
}

impl DepositTransferConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Deposit-sweep strategy.
pub struct DepositTransfer {
    config: DepositTransferConfig,
    wallet: Arc<dyn WalletGateway>,
    seen: Mutex<HashSet<String>>,
}

impl DepositTransfer {
    pub fn new(config: DepositTransferConfig, wallet: Arc<dyn WalletGateway>) -> Self {
        Self { config, wallet, seen: Mutex::new(HashSet::new()) }
    }

    /// One sweep cycle; returns the number of deposits transferred.
    pub async fn tick(&self) -> Result<usize> {
        let exchange = self.config.common.proto_exchange_id();
        let deposits = self.wallet.fetch_deposits(&exchange, 50).await?;
        let mut transferred = 0;
        let mut seen = self.seen.lock().await;
        for entry in deposits {
            if !entry.completed || !seen.insert(entry.id.clone()) {
                continue;
            }
            if entry.amount < self.config.ignore_below {
                continue;
            }
            self.wallet
                .transfer(&exchange, &entry.currency, entry.amount, &self.config.dest_label)
                .await?;
            tracing::info!(
                currency = %entry.currency,
                amount = %entry.amount,
                dest = %self.config.dest_label,
                "deposit swept"
            );
            transferred += 1;
        }
        Ok(transferred)
    }
}

#[async_trait]
impl Strategy for DepositTransfer {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "deposit-transfer tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, ports::TradingGateway};

    fn deposit(id: &str, amount: Decimal) -> crate::ports::LedgerEntry {
        crate::ports::LedgerEntry {
            id: id.to_string(),
            currency: String::from("USDT"),
            amount,
            entry_type: String::from("deposit"),
            completed: true,
            time_ms: 0,
        }
    }

    #[tokio::test]
    async fn sweeps_each_completed_deposit_once() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.push_deposit(deposit("d1", dec!(50)));
        adapter.push_deposit(deposit("d2", dec!(30)));
        let config = DepositTransferConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\ndest_label = \"master\"").expect("toml"),
        )
        .expect("config");
        let _gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let wallet: StdArc<dyn WalletGateway> = adapter.clone();
        let s = DepositTransfer::new(config, wallet);

        assert_eq!(s.tick().await.expect("tick"), 2);
        // Second cycle sees the same ledger but must not re-transfer.
        assert_eq!(s.tick().await.expect("tick"), 0);
        assert_eq!(adapter.transfers().len(), 2);
        assert_eq!(adapter.transfers()[0].2, "master");
    }

    #[tokio::test]
    async fn ignores_incomplete_and_dust() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let mut pending = deposit("p1", dec!(99));
        pending.completed = false;
        adapter.push_deposit(pending);
        adapter.push_deposit(deposit("dust", dec!(1)));
        let config = DepositTransferConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\ndest_label = \"master\"\nignore_below = \"10\"")
                .expect("toml"),
        )
        .expect("config");
        let _gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let wallet: StdArc<dyn WalletGateway> = adapter.clone();
        let s = DepositTransfer::new(config, wallet);
        assert_eq!(s.tick().await.expect("tick"), 0);
    }
}
