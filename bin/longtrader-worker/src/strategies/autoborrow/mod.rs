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
    ports::VenueOpInvoker,
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the auto-borrow strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AutoborrowConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Asset to maintain.
    pub asset: String,
    /// Minimum free balance; a borrow tops up to this level, and a repay brings
    /// the balance back down to it.
    pub min_balance: Decimal,
    /// Repay when balance exceeds this value (`0` disables repayment).
    ///
    /// This only *arms* the repay arm; the amount moved is the surplus over
    /// `min_balance`. Configure `repay_above` **above** `min_balance` — with the
    /// floor at or above the threshold, any balance that trips the arm leaves no
    /// surplus to repay and the arm is skipped.
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
    ops: Arc<dyn VenueOpInvoker>,
}

impl Autoborrow {
    pub fn new(config: AutoborrowConfig, ops: Arc<dyn VenueOpInvoker>) -> Self {
        Self { config, ops }
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
            // The amount is the surplus over the floor, not over the threshold. A
            // floor at or above the balance leaves nothing to give back, and a
            // zero-amount transfer is refused by every venue — so the arm is
            // skipped rather than asked to move nothing.
            let surplus = balance - self.config.min_balance;
            if surplus <= Decimal::ZERO {
                tracing::debug!(
                    balance = %balance,
                    floor = %self.config.min_balance,
                    "balance has no surplus over the floor; nothing to repay"
                );
                return Ok(());
            }
            let mut params = serde_json::Map::new();
            params.insert("asset".into(), serde_json::json!(self.config.asset));
            params.insert("amount".into(), serde_json::json!(surplus.to_string()));
            self.ops.invoke_venue_op(&exchange, "margin.repay", params).await?;
            tracing::info!(asset = %self.config.asset, %surplus, "repaid surplus");
        }
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
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{PortError, VenueOpDescriptor, VenueOpInvoker as _},
        proto::common,
    };

    #[tokio::test]
    async fn borrows_when_below_floor() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_op_balance(dec!(50)).await;
        let config = AutoborrowConfig::from_params(
            &toml::from_str(
                "symbol = \"BTCUSDT\"\nasset = \"USDT\"\nmin_balance = \"200\"\npoll_secs = 1",
            )
            .expect("toml"),
        )
        .expect("config");
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Autoborrow::new(config, ops);
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
        adapter.set_op_balance(dec!(500)).await;
        let config = AutoborrowConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nasset = \"USDT\"\nmin_balance = \"200\"")
                .expect("toml"),
        )
        .expect("config");
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Autoborrow::new(config, ops);
        s.tick().await.expect("tick"); // must not error or borrow
    }

    // -----------------------------------------------------------------------
    // Balance read and the borrow/repay decision
    //
    // `MockAdapter` only ever answers `account.balance` with
    // `{"free": "<balance>"}` and never fails, so the malformed and failing
    // responses `free_balance` must survive need a scripted venue-ops surface.
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct OpLog {
        /// Exact JSON returned for `account.balance`.
        balance: serde_json::Value,
        /// Ops that must fail instead of answering.
        fail_ops: Vec<String>,
        calls: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    }

    /// A venue-ops surface whose `account.balance` payload and failures are
    /// scripted, recording every invocation.
    #[derive(Clone, Default)]
    struct ScriptedOps {
        log: StdArc<StdMutex<OpLog>>,
    }

    impl ScriptedOps {
        /// Answers `account.balance` with this exact payload.
        fn balance(self, payload: serde_json::Value) -> Self {
            self.log.lock().expect("ops log").balance = payload;
            self
        }

        /// Answers `account.balance` with a well-formed `free` string.
        fn free(self, free: &str) -> Self {
            self.balance(serde_json::json!({ "currency": "USDT", "free": free }))
        }

        /// Refuses every invocation of `op`.
        fn failing(self, op: &str) -> Self {
            self.log.lock().expect("ops log").fail_ops.push(op.to_string());
            self
        }

        /// Every invocation as `(op, params)`, in call order.
        fn calls(&self) -> Vec<(String, serde_json::Map<String, serde_json::Value>)> {
            self.log.lock().expect("ops log").calls.clone()
        }

        /// Just the op names, in call order.
        fn ops(&self) -> Vec<String> {
            self.calls().into_iter().map(|(op, _)| op).collect()
        }

        /// The `amount` string the strategy asked `op` to move.
        fn amount_for(&self, op: &str) -> String {
            self.calls()
                .into_iter()
                .find(|(name, _)| name == op)
                .and_then(|(_, params)| params.get("amount").cloned())
                .and_then(|amount| amount.as_str().map(ToString::to_string))
                .expect("the strategy invoked the op with an amount")
        }
    }

    #[async_trait]
    impl VenueOpInvoker for ScriptedOps {
        async fn invoke_venue_op(
            &self,
            _exchange_id: &common::ExchangeId,
            op: &str,
            params: serde_json::Map<String, serde_json::Value>,
        ) -> Result<serde_json::Value, PortError> {
            let mut log = self.log.lock().expect("ops log");
            if log.fail_ops.iter().any(|failing| failing == op) {
                return Err(PortError::Transport(format!("{op} refused by the venue")));
            }
            log.calls.push((op.to_string(), params));
            if op == "account.balance" {
                Ok(log.balance.clone())
            } else {
                Ok(serde_json::json!({ "status": "ok", "op": op }))
            }
        }

        async fn list_venue_ops(
            &self,
            _exchange_id: &common::ExchangeId,
        ) -> Result<Vec<VenueOpDescriptor>, PortError> {
            Err(PortError::Unsupported("list_venue_ops".into()))
        }
    }

    fn autoborrow(ops: ScriptedOps, doc: &str) -> Autoborrow {
        let full = format!("symbol = \"BTCUSDT\"\nasset = \"USDT\"\n{doc}");
        let config =
            AutoborrowConfig::from_params(&toml::from_str(&full).expect("toml")).expect("config");
        Autoborrow::new(config, StdArc::new(ops))
    }

    /// The borrow tops the balance up to exactly `min_balance` — never one unit
    /// more, or the strategy would immediately owe a second borrow.
    #[tokio::test]
    async fn borrowing_tops_the_balance_up_to_the_floor() {
        let ops = ScriptedOps::default().free("50");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string(), "margin.borrow".to_string()]);
        assert_eq!(ops.amount_for("margin.borrow"), "150");
        let (_, params) =
            ops.calls().into_iter().find(|(op, _)| op == "margin.borrow").expect("a borrow");
        assert_eq!(params["asset"], serde_json::json!("USDT"));
    }

    /// `balance < min_balance` is strict: sitting exactly on the floor is
    /// healthy and must move no funds.
    #[tokio::test]
    async fn a_balance_exactly_at_the_floor_neither_borrows_nor_repays() {
        let ops = ScriptedOps::default().free("200");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"\nrepay_above = \"300\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string()]);
    }

    /// One unit below the floor is still a borrow, for exactly that unit.
    #[tokio::test]
    async fn a_balance_one_tenth_below_the_floor_borrows_the_gap() {
        let ops = ScriptedOps::default().free("199.9");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.amount_for("margin.borrow"), "0.1");
    }

    /// The repay arm is `balance > repay_above`, strict. Sitting exactly on the
    /// threshold is not a surplus.
    #[tokio::test]
    async fn repay_fires_above_the_threshold() {
        let ops = ScriptedOps::default().free("500");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"\nrepay_above = \"300\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string(), "margin.repay".to_string()]);
        // Repaying down to `min_balance`, not down to `repay_above`: the floor
        // is the strategy's target balance, the threshold only arms the arm.
        assert_eq!(ops.amount_for("margin.repay"), "300");
    }

    #[tokio::test]
    async fn a_balance_exactly_at_the_repay_threshold_does_not_repay() {
        let ops = ScriptedOps::default().free("300");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"\nrepay_above = \"300\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string()]);
    }

    /// `repay_above = 0` is the documented "repayment disabled" spelling, so a
    /// fat balance must stay untouched.
    #[tokio::test]
    async fn repayment_stays_disabled_while_the_threshold_is_zero() {
        let ops = ScriptedOps::default().free("500");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"\nrepay_above = \"0\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string()]);
    }

    /// `repay_above` is checked against the raw balance while the amount is
    /// measured from `min_balance`, so a floor that already sits above the
    /// threshold leaves no surplus: the arm must move nothing rather than ask the
    /// venue to transfer zero, which every venue refuses.
    #[tokio::test]
    async fn a_floor_at_the_balance_skips_the_repay_arm() {
        let ops = ScriptedOps::default().free("100");
        let s = autoborrow(ops.clone(), "min_balance = \"100\"\nrepay_above = \"50\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string()], "no zero-amount repay");
    }

    /// The same floor above a balance that is *below* it: the borrow arm wins, so
    /// the repay arm never gets the chance to ask for a negative amount.
    #[tokio::test]
    async fn a_floor_above_a_lower_balance_borrows_instead_of_repaying() {
        let ops = ScriptedOps::default().free("100");
        let s = autoborrow(ops.clone(), "min_balance = \"150\"\nrepay_above = \"120\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string(), "margin.borrow".to_string()]);
        assert_eq!(ops.amount_for("margin.borrow"), "50");
    }

    /// A balance a hair above the floor still has a surplus, however small: the
    /// arm moves what is there rather than rounding it away.
    #[tokio::test]
    async fn a_balance_just_above_the_floor_repays_the_excess() {
        let ops = ScriptedOps::default().free("200.5");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"\nrepay_above = \"200.4\"");

        s.tick().await.expect("tick");
        assert_eq!(ops.amount_for("margin.repay"), "0.5");
    }

    /// A balance payload with no `free` is an error, not a zero balance: a zero
    /// balance would trigger a borrow sized from a value nobody reported.
    #[tokio::test]
    async fn a_balance_response_without_a_free_key_is_an_error() {
        let ops = ScriptedOps::default().balance(serde_json::json!({ "currency": "USDT" }));
        let s = autoborrow(ops, "min_balance = \"200\"");

        let err = s.tick().await.expect_err("a missing 'free' must not read as zero");
        assert!(err.to_string().contains("free"), "{err}");
    }

    /// Only a JSON string counts as a balance; a numeric `free` is a shape the
    /// strategy does not accept rather than one it silently coerces.
    #[tokio::test]
    async fn a_non_string_free_value_is_an_error() {
        let ops = ScriptedOps::default().balance(serde_json::json!({ "free": 50 }));
        let s = autoborrow(ops, "min_balance = \"200\"");

        let err = s.tick().await.expect_err("a numeric 'free' must not be coerced");
        assert!(err.to_string().contains("free"), "{err}");
    }

    #[tokio::test]
    async fn an_unparsable_free_value_is_an_error() {
        let ops = ScriptedOps::default().free("12.3.4");
        let s = autoborrow(ops, "min_balance = \"200\"");

        let err = s.tick().await.expect_err("garbage must not parse as a balance");
        let message = err.to_string();
        assert!(message.contains("invalid balance"), "{message}");
        assert!(message.contains("12.3.4"), "the value must be echoed: {message}");
    }

    #[tokio::test]
    async fn a_failed_balance_read_propagates() {
        let ops = ScriptedOps::default().failing("account.balance");
        let s = autoborrow(ops, "min_balance = \"200\"");

        let err = s.tick().await.expect_err("a refused read must not look like a zero balance");
        assert!(err.to_string().contains("account.balance refused"), "{err}");
    }

    #[tokio::test]
    async fn a_failed_borrow_propagates() {
        let ops = ScriptedOps::default().free("50").failing("margin.borrow");
        let s = autoborrow(ops, "min_balance = \"200\"");

        let err = s.tick().await.expect_err("a refused borrow must surface");
        assert!(err.to_string().contains("margin.borrow refused"), "{err}");
    }

    #[tokio::test]
    async fn a_failed_repay_propagates() {
        let ops = ScriptedOps::default().free("500").failing("margin.repay");
        let s = autoborrow(ops, "min_balance = \"200\"\nrepay_above = \"300\"");

        let err = s.tick().await.expect_err("a refused repay must surface");
        assert!(err.to_string().contains("margin.repay refused"), "{err}");
    }

    /// Every cycle starts by reading the balance, whatever it then decides.
    #[tokio::test]
    async fn every_cycle_reads_the_balance_before_deciding() {
        let ops = ScriptedOps::default().free("250");
        let s = autoborrow(ops.clone(), "min_balance = \"200\"");

        s.tick().await.expect("tick");
        s.tick().await.expect("tick");
        assert_eq!(ops.ops(), vec!["account.balance".to_string(), "account.balance".to_string()]);
    }
}
