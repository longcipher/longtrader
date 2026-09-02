//! Cross-venue balance alignment.
//!
//! Reads both venues' state snapshots, compares the target asset balance
//! against an even (or configured) split, and transfers the drift across
//! when it exceeds the threshold.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{TradingGateway, WalletGateway},
    strategies::{CrossVenueParams, Strategy},
};

/// Config for balance alignment.
#[derive(Debug, Clone, Deserialize)]
pub struct BalanceAlignConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Asset to align.
    pub asset: String,
    /// Fraction of total held on primary (`0.5` = even split).
    #[serde(default = "default_target")]
    pub primary_share: Decimal,
    /// Transfer only when the drift exceeds this absolute amount.
    pub threshold: Decimal,
}

fn default_target() -> Decimal {
    Decimal::new(5, 1)
}

/// Extracts the free balance of `asset` from a reconcile snapshot.
pub fn free_balance_of(
    snapshot: &crate::proto::worker::ReconcileStateResponse,
    asset: &str,
) -> Decimal {
    snapshot
        .balances
        .iter()
        .find(|b| b.currency == asset)
        .and_then(|b| b.free.as_option())
        .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
        .unwrap_or_default()
}

/// Balance-align strategy.
pub struct BalanceAlign {
    config: BalanceAlignConfig,
    gateway: Arc<dyn TradingGateway>,
    wallet: Arc<dyn WalletGateway>,
}

impl BalanceAlign {
    pub fn new(
        config: BalanceAlignConfig,
        gateway: Arc<dyn TradingGateway>,
        wallet: Arc<dyn WalletGateway>,
    ) -> Self {
        Self { config, gateway, wallet }
    }

    /// One alignment cycle; returns the transferred amount (if any).
    pub async fn tick(&self) -> Result<Decimal> {
        let primary = self.config.venues.primary.proto();
        let hedge = self.config.venues.hedge.proto();
        let snap_a = self.gateway.sync_state(&primary).await?;
        let snap_b = self.gateway.sync_state(&hedge).await?;
        let bal_a = free_balance_of(&snap_a, &self.config.asset);
        let bal_b = free_balance_of(&snap_b, &self.config.asset);
        let total = bal_a + bal_b;
        let want_a = total * self.config.primary_share;
        let drift = bal_a - want_a;
        if drift.abs() < self.config.threshold || total.is_zero() {
            return Ok(Decimal::ZERO);
        }
        // Excess on primary → transfer primary→hedge; deficit → reverse.
        let amount = drift.abs();
        if drift > Decimal::ZERO {
            self.wallet.transfer(&primary, &self.config.asset, amount, "hedge").await?;
        } else {
            self.wallet.transfer(&hedge, &self.config.asset, amount, "primary").await?;
        }
        tracing::info!(asset = %self.config.asset, %amount, "balance aligned");
        Ok(amount)
    }
}

#[async_trait]
impl Strategy for BalanceAlign {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "balance-align tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn free_balance_extraction() {
        let mut snap = crate::proto::worker::ReconcileStateResponse::default();
        snap.balances.push(crate::proto::account::Balance {
            currency: String::from("USDT"),
            free: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(dec!(42))),
            ..Default::default()
        });
        assert_eq!(free_balance_of(&snap, "USDT"), dec!(42));
        assert_eq!(free_balance_of(&snap, "BTC"), Decimal::ZERO);
    }
}
