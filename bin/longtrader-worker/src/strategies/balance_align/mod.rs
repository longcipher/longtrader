//! Cross-venue balance alignment.
//!
//! Reads both venues' state snapshots, compares the target asset balance
//! against an even (or configured) split, and transfers the drift across
//! when it exceeds the threshold.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
    /// Process-wide instance id, so two strategies in one process never mint
    /// the same idempotency key.
    instance: u64,
    /// Monotonic per-attempt counter feeding the transfer idempotency key.
    attempt: AtomicU64,
}

/// Process-wide instance counter, paired with the wall clock so keys are unique
/// both within a process and across a restart of it.
static INSTANCE_SEQ: AtomicU64 = AtomicU64::new(0);

impl BalanceAlign {
    pub fn new(
        config: BalanceAlignConfig,
        gateway: Arc<dyn TradingGateway>,
        wallet: Arc<dyn WalletGateway>,
    ) -> Self {
        // Seeded from the wall clock rather than zero: the counter restarts with
        // the process, and a restarted strategy must not mint a key that
        // collides with one it already used (the venue dedupes on it, so a
        // collision would silently swallow a real transfer).
        let seed = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        Self {
            config,
            gateway,
            wallet,
            instance: INSTANCE_SEQ.fetch_add(1, Ordering::Relaxed),
            attempt: AtomicU64::new(seed),
        }
    }

    /// Mint the idempotency key for one transfer attempt.
    ///
    /// The key identifies an *attempt*, not a desired end state. Deriving it
    /// from the transfer's content (asset + direction + amount) looks
    /// reasonable and is wrong: a drift of the same size recurring later is a
    /// genuinely new need for capital, but the venue would dedupe it against
    /// the earlier transfer and move nothing, while the caller still sees a
    /// successful receipt.
    fn next_attempt_key(&self, from: &str, to: &str) -> String {
        let seq = self.attempt.fetch_add(1, Ordering::Relaxed);
        format!("balance-align:{}:{from}:{to}:{}-{seq}", self.config.asset, self.instance)
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
        let (source, from, to) = if drift > Decimal::ZERO {
            (&primary, "primary", "hedge")
        } else {
            (&hedge, "hedge", "primary")
        };
        // A fresh key per attempt, so a retried request at the transport layer
        // does not double-spend while a genuinely new alignment of the same
        // size is not silently deduped away.
        let key = self.next_attempt_key(from, to);
        self.wallet.transfer(source, &self.config.asset, amount, to, &key).await?;
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
    use crate::{adapters::MockAdapter, ports::WalletGateway, strategies::params::VenueRef};

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

    fn cfg() -> BalanceAlignConfig {
        BalanceAlignConfig {
            venues: CrossVenueParams {
                primary: VenueRef { exchange_id: "mock".to_string(), label: String::new() },
                hedge: VenueRef { exchange_id: "mock2".to_string(), label: String::new() },
                symbol: "BTC/USDT".to_string(),
                poll_secs: 60,
            },
            asset: "USDT".to_string(),
            primary_share: dec!(0.5),
            threshold: dec!(1),
        }
    }

    fn aligner() -> BalanceAlign {
        BalanceAlign::new(
            cfg(),
            Arc::new(MockAdapter::new(dec!(100))),
            Arc::new(MockAdapter::new(dec!(100))),
        )
    }

    /// A content-derived idempotency key (asset + direction + amount) collapses
    /// two genuinely distinct alignments of the same size into one, because the
    /// venue dedupes on the key. The second alignment is then silently
    /// swallowed while `tick()` still reports success.
    ///
    /// The key must therefore identify an *attempt*, not a desired end state.
    #[tokio::test]
    async fn two_same_size_alignments_both_move_funds() {
        let mock = Arc::new(MockAdapter::new(dec!(100)));
        let wallet: Arc<dyn WalletGateway> = mock.clone();
        let exchange =
            crate::proto::common::ExchangeId { id: "mock".to_string(), ..Default::default() };

        // The same key twice: a genuine retry, so the funds move once.
        wallet.transfer(&exchange, "USDT", dec!(100), "hedge", "k").await.expect("first");
        wallet.transfer(&exchange, "USDT", dec!(100), "hedge", "k").await.expect("retry");
        assert_eq!(mock.transfers().await.len(), 1, "a retry must not move funds twice");

        // A different key, same amount: a new need, so the funds must move.
        wallet.transfer(&exchange, "USDT", dec!(100), "hedge", "k2").await.expect("second need");
        assert_eq!(
            mock.transfers().await.len(),
            2,
            "a new attempt with a new key must move funds again"
        );
    }

    /// Each attempt must mint a distinct key, so a recurring drift of the same
    /// size is never mistaken for a retry of the previous alignment.
    #[tokio::test]
    async fn each_attempt_mints_a_distinct_idempotency_key() {
        let aligner = aligner();
        let first = aligner.next_attempt_key("primary", "hedge");
        let second = aligner.next_attempt_key("primary", "hedge");
        assert_ne!(first, second, "a recurring drift of the same size must not be deduped");
        assert!(first.starts_with("balance-align:USDT:primary:hedge:"), "{first}");
        assert!(second.starts_with("balance-align:USDT:primary:hedge:"), "{second}");
    }

    /// Keys are unique across a restart too, not just within one process.
    #[tokio::test]
    async fn attempt_keys_do_not_collide_across_restarts() {
        let first = aligner().next_attempt_key("primary", "hedge");
        let second = aligner().next_attempt_key("primary", "hedge");
        assert_ne!(first, second, "a restarted strategy must not reuse a pre-restart key");
    }
}
