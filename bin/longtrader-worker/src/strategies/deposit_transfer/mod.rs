//! Deposit-sweep transfer strategy.
//!
//! Polls the deposit ledger; every newly completed deposit is transferred
//! to the destination account label (e.g. sweeping exchange sub-account
//! deposits into the master account). A deposit id is remembered only once its
//! transfer has succeeded, so a failed sweep is retried on the next cycle — the
//! transfer's idempotency key is what keeps that retry from moving the money
//! twice. The seen-set lives in memory, so a restart sweeps the visible ledger
//! again and the idempotency key absorbs it.

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
    ///
    /// `0` — the default — means "sweep everything with a positive amount": the
    /// filter is `amount < ignore_below`, which a zero deposit passes, so the
    /// sweep would ask the venue to move zero and every venue refuses that. A
    /// non-positive deposit is therefore skipped outright, whatever this says.
    ///
    /// A deposit under the floor is left unswept, not marked as swept: if the
    /// venue later raises the same deposit above the floor it is still swept.
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
            if !entry.completed {
                continue;
            }
            // A transfer of zero (or less) is never meaningful and every venue
            // refuses it, so it is not a deposit to sweep at all — with the
            // default `ignore_below = 0` this is the only thing standing between
            // a zero-amount ledger row and a failing cycle.
            if entry.amount <= Decimal::ZERO {
                continue;
            }
            // The dust filter runs *before* the seen check, and a filtered
            // deposit is not marked seen: marking it would strand it in the
            // sub-account for the life of the process if the venue later raises
            // the same deposit above the floor.
            if entry.amount < self.config.ignore_below {
                continue;
            }
            if seen.contains(&entry.id) {
                continue;
            }
            // Key the transfer on the deposit id: a sweep that fails midway
            // and is retried must not send the same deposit twice.
            self.wallet
                .transfer(
                    &exchange,
                    &entry.currency,
                    entry.amount,
                    &self.config.dest_label,
                    &format!("deposit-sweep:{}", entry.id),
                )
                .await?;
            // Marked seen only now that the money has actually moved. A deposit
            // whose transfer failed is retried on the next cycle; the idempotency
            // key above is what keeps that retry from sending it twice.
            seen.insert(entry.id.clone());
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
    use std::sync::{
        Arc as StdArc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{LedgerEntry, PortError, TradingGateway, TransferReceipt},
        proto::common,
    };

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
        adapter.push_deposit(deposit("d1", dec!(50))).await;
        adapter.push_deposit(deposit("d2", dec!(30))).await;
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
        assert_eq!(adapter.transfers().await.len(), 2);
        assert_eq!(adapter.transfers().await[0].2, "master");
    }

    #[tokio::test]
    async fn ignores_incomplete_and_dust() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let mut pending = deposit("p1", dec!(99));
        pending.completed = false;
        adapter.push_deposit(pending).await;
        adapter.push_deposit(deposit("dust", dec!(1))).await;
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

    // -----------------------------------------------------------------------
    // The dust filter and the once-only bookkeeping
    //
    // `MockAdapter` refuses a zero-amount transfer and can never fail a ledger
    // read, and it hides the transfer's idempotency key, so the edge cases below
    // need a wallet whose ledger and failures are scripted.
    // -----------------------------------------------------------------------

    /// A wallet whose ledger rows, read failures and transfer failures are
    /// scripted, recording the idempotency key of every transfer.
    #[derive(Clone, Default)]
    struct StubWallet {
        deposits: StdArc<StdMutex<Vec<LedgerEntry>>>,
        reads: StdArc<StdMutex<Vec<(String, String, u32)>>>,
        keys: StdArc<StdMutex<Vec<String>>>,
        fail_reads: StdArc<AtomicBool>,
        fail_transfers: StdArc<AtomicBool>,
    }

    impl StubWallet {
        fn holding(entries: Vec<LedgerEntry>) -> Self {
            Self { deposits: StdArc::new(StdMutex::new(entries)), ..Self::default() }
        }

        fn replace_deposits(&self, entries: Vec<LedgerEntry>) {
            *self.deposits.lock().expect("wallet log") = entries;
        }

        fn refusing_reads(self) -> Self {
            self.fail_reads.store(true, Ordering::SeqCst);
            self
        }

        fn refusing_transfers(self) -> Self {
            self.fail_transfers.store(true, Ordering::SeqCst);
            self
        }

        fn allowing_transfers(&self) {
            self.fail_transfers.store(false, Ordering::SeqCst);
        }

        /// The `(currency, entry_type, limit)` of every ledger read.
        fn reads(&self) -> Vec<(String, String, u32)> {
            self.reads.lock().expect("wallet log").clone()
        }

        /// The idempotency key of every accepted transfer, in order.
        fn transfer_keys(&self) -> Vec<String> {
            self.keys.lock().expect("wallet log").clone()
        }
    }

    #[async_trait]
    impl WalletGateway for StubWallet {
        async fn list_ledger_entries(
            &self,
            _exchange_id: &common::ExchangeId,
            currency: &str,
            entry_type: &str,
            limit: u32,
        ) -> Result<Vec<LedgerEntry>, PortError> {
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(PortError::Transport("ledger unavailable".into()));
            }
            self.reads.lock().expect("wallet log").push((
                currency.to_string(),
                entry_type.to_string(),
                limit,
            ));
            let rows = self.deposits.lock().expect("wallet log").clone();
            Ok(rows
                .into_iter()
                .filter(|row| currency.is_empty() || row.currency == currency)
                .filter(|row| entry_type.is_empty() || row.entry_type == entry_type)
                .take(limit as usize)
                .collect())
        }

        async fn transfer(
            &self,
            _exchange_id: &common::ExchangeId,
            asset: &str,
            amount: Decimal,
            _dest_label: &str,
            client_transfer_id: &str,
        ) -> Result<TransferReceipt, PortError> {
            if self.fail_transfers.load(Ordering::SeqCst) {
                return Err(PortError::Transport("transfer refused".into()));
            }
            self.keys.lock().expect("wallet log").push(client_transfer_id.to_string());
            Ok(TransferReceipt {
                transfer_id: client_transfer_id.to_string(),
                entry: Some(LedgerEntry {
                    id: client_transfer_id.to_string(),
                    currency: asset.to_string(),
                    amount: -amount,
                    entry_type: "transfer".into(),
                    completed: true,
                    time_ms: 0,
                }),
            })
        }
    }

    fn sweeper(wallet: StubWallet, doc: &str) -> DepositTransfer {
        let full = format!("symbol = \"BTCUSDT\"\ndest_label = \"master\"\n{doc}");
        let config = DepositTransferConfig::from_params(&toml::from_str(&full).expect("toml"))
            .expect("config");
        DepositTransfer::new(config, StdArc::new(wallet))
    }

    /// A sweep that read the raw ledger rather than deposits would move
    /// withdrawals too, and a different page size would silently miss rows.
    #[tokio::test]
    async fn the_ledger_is_read_as_deposits_with_a_limit_of_fifty() {
        let wallet = StubWallet::holding(vec![deposit("d1", dec!(5))]);
        let s = sweeper(wallet.clone(), "");

        assert_eq!(s.tick().await.expect("tick"), 1);
        assert_eq!(wallet.reads(), vec![(String::new(), "deposit".to_string(), 50)]);
    }

    /// `ignore_below` defaults to zero and the filter is `amount < ignore_below`,
    /// which `0 < 0` passes — so without a positivity guard the sweep would ask
    /// the venue to move zero, which every venue refuses.
    #[tokio::test]
    async fn a_zero_amount_deposit_is_skipped_when_nothing_is_ignored() {
        let wallet = StubWallet::holding(vec![deposit("zero", Decimal::ZERO)]);
        let s = sweeper(wallet.clone(), "");

        assert_eq!(s.tick().await.expect("tick"), 0, "a transfer of zero is not a sweep");
        assert!(wallet.transfer_keys().is_empty(), "the venue must never be asked to move zero");
    }

    /// The same guard against a real venue: a zero-amount deposit on the default
    /// configuration is a no-op, not a failed cycle.
    #[tokio::test]
    async fn a_venue_that_refuses_zero_amount_transfers_is_never_asked() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.push_deposit(deposit("zero", Decimal::ZERO)).await;
        let config = DepositTransferConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\ndest_label = \"master\"").expect("toml"),
        )
        .expect("config");
        let wallet: StdArc<dyn WalletGateway> = adapter.clone();
        let s = DepositTransfer::new(config, wallet);

        assert_eq!(s.tick().await.expect("tick"), 0);
        assert!(adapter.transfers().await.is_empty(), "no funds must have moved");
    }

    /// A negative ledger row is a withdrawal wearing a deposit's type, and it is
    /// skipped for the same reason a zero is.
    #[tokio::test]
    async fn a_negative_amount_deposit_is_skipped() {
        let wallet = StubWallet::holding(vec![deposit("out", dec!(-50))]);
        let s = sweeper(wallet.clone(), "ignore_below = \"-100\"");

        assert_eq!(s.tick().await.expect("tick"), 0);
        assert!(wallet.transfer_keys().is_empty());
    }

    /// The id is recorded as seen only after the transfer succeeds, so a deposit
    /// whose sweep failed is picked up again by the next cycle. The idempotency
    /// key is what makes that retry safe.
    #[tokio::test]
    async fn a_deposit_whose_transfer_failed_is_retried_on_the_next_cycle() {
        let wallet = StubWallet::holding(vec![deposit("d1", dec!(50))]).refusing_transfers();
        let s = sweeper(wallet.clone(), "");

        assert!(s.tick().await.is_err(), "a refused transfer must surface");
        wallet.allowing_transfers();
        assert_eq!(s.tick().await.expect("second cycle"), 1, "the failed sweep was not lost");
        assert_eq!(wallet.transfer_keys(), vec!["deposit-sweep:d1".to_string()]);
    }

    /// A deposit that lands below the floor is left unswept rather than marked
    /// seen, so if the venue later raises that same deposit above the floor it is
    /// still swept instead of being stranded in the sub-account.
    #[tokio::test]
    async fn a_deposit_below_the_floor_is_swept_once_it_grows() {
        let wallet = StubWallet::holding(vec![deposit("d1", dec!(5))]);
        let s = sweeper(wallet.clone(), "ignore_below = \"10\"");

        assert_eq!(s.tick().await.expect("tick"), 0);
        wallet.replace_deposits(vec![deposit("d1", dec!(50))]);
        assert_eq!(s.tick().await.expect("tick"), 1, "the filter ran before the seen check");
        assert_eq!(wallet.transfer_keys(), vec!["deposit-sweep:d1".to_string()]);
    }

    /// An incomplete deposit is not swept and not marked seen either, so the same
    /// id is swept as soon as it completes.
    #[tokio::test]
    async fn a_deposit_that_completes_later_is_swept_on_the_next_cycle() {
        let mut pending = deposit("p1", dec!(50));
        pending.completed = false;
        let wallet = StubWallet::holding(vec![pending]);
        let s = sweeper(wallet.clone(), "");

        assert_eq!(s.tick().await.expect("tick"), 0);
        wallet.replace_deposits(vec![deposit("p1", dec!(50))]);
        assert_eq!(s.tick().await.expect("tick"), 1);
        assert_eq!(wallet.transfer_keys(), vec!["deposit-sweep:p1".to_string()]);
    }

    /// The idempotency key is what makes a sweep retryable, so it must name the
    /// deposit it moves.
    #[tokio::test]
    async fn the_transfer_key_names_the_deposit_it_moves() {
        let wallet = StubWallet::holding(vec![deposit("d1", dec!(50)), deposit("d2", dec!(30))]);
        let s = sweeper(wallet.clone(), "");

        assert_eq!(s.tick().await.expect("tick"), 2);
        assert_eq!(
            wallet.transfer_keys(),
            vec!["deposit-sweep:d1".to_string(), "deposit-sweep:d2".to_string()]
        );
    }

    /// An unreadable ledger must surface: reporting "nothing to sweep" would
    /// leave real deposits sitting in the sub-account.
    #[tokio::test]
    async fn a_failed_ledger_read_propagates() {
        let wallet = StubWallet::holding(Vec::new()).refusing_reads();
        let s = sweeper(wallet, "");

        let err = s.tick().await.expect_err("an unreadable ledger must surface");
        assert!(err.to_string().contains("ledger unavailable"), "{err}");
    }

    /// `ignore_below` defaults to zero, so no dust filter is armed unless the
    /// operator configures one.
    #[test]
    fn the_dust_floor_defaults_to_zero() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\ndest_label = \"master\"").expect("toml");
        let cfg = DepositTransferConfig::from_params(&table).expect("config");
        assert_eq!(cfg.ignore_below, Decimal::ZERO);
    }
}
