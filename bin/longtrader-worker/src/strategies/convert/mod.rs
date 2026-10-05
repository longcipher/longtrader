//! Periodic asset conversion strategy.
//!
//! On a fixed cadence, invokes the venue's `wallet.convert` operation to swap
//! `from_asset` into `to_asset`, converting `min_amount` units per attempt.
//! Reached through the generic [`VenueOpInvoker`] port.
//!
//! The swap is **not** gated on a balance: `tick` never reads the account, so
//! the op fires unconditionally on every cycle and the venue is left to reject an
//! amount it cannot cover. `min_amount` is therefore the size of each
//! conversion, *not* a threshold that has been compared against a balance. A
//! balance gate would need an account read this port does not expose; adding one
//! is a trading-behaviour change, not a doc fix, so the contract documented here
//! is the one the code implements.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::VenueOpInvoker,
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the convert strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct ConvertConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Source asset converted away.
    pub from_asset: String,
    /// Destination asset accumulated.
    pub to_asset: String,
    /// Amount converted per attempt. No balance is read before the swap, so
    /// this is the conversion size, not a balance threshold.
    pub min_amount: Decimal,
    /// Seconds between conversion attempts.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
}

const fn default_interval() -> u64 {
    3_600
}

impl ConvertConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Convert strategy.
pub struct Convert {
    config: ConvertConfig,
    ops: Arc<dyn VenueOpInvoker>,
}

impl Convert {
    pub fn new(config: ConvertConfig, ops: Arc<dyn VenueOpInvoker>) -> Self {
        Self { config, ops }
    }

    /// One conversion attempt. Reads no balance: the op is invoked every cycle with
    /// `amount = min_amount` (see the module doc).
    pub async fn tick(&self) -> Result<bool> {
        let exchange = self.config.common.proto_exchange_id();
        let mut params = serde_json::Map::new();
        params.insert("fromAsset".into(), serde_json::json!(self.config.from_asset));
        params.insert("toAsset".into(), serde_json::json!(self.config.to_asset));
        params.insert("amount".into(), serde_json::json!(self.config.min_amount.to_string()));
        let response = self.ops.invoke_venue_op(&exchange, "wallet.convert", params).await?;
        let ok = response.get("status").and_then(serde_json::Value::as_str) == Some("ok");
        if ok {
            tracing::info!(
                from = %self.config.from_asset,
                to = %self.config.to_asset,
                "convert executed"
            );
        }
        Ok(ok)
    }
}

#[async_trait]
impl Strategy for Convert {
    async fn run(&self) -> Result<()> {
        let interval = std::time::Duration::from_secs(self.config.interval_secs.max(1));
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "convert tick failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{PortError, TradingGateway, VenueOpDescriptor},
        proto::common,
    };

    #[tokio::test]
    async fn convert_invokes_venue_op() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let config = ConvertConfig::from_params(
            &toml::from_str(
                "symbol = \"BTCUSDT\"\nfrom_asset = \"DUST\"\nto_asset = \"USDT\"\nmin_amount = \"10\"",
            )
            .expect("toml"),
        )
        .expect("config");
        let _gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let ops: StdArc<dyn VenueOpInvoker> = adapter.clone();
        let s = Convert::new(config, ops);
        assert!(s.tick().await.expect("tick"));
    }

    #[test]
    fn config_parses() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\nfrom_asset = \"A\"\nto_asset = \"B\"\nmin_amount = \"5\"",
        )
        .expect("toml");
        let cfg = ConvertConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.to_asset, "B");
    }

    // -----------------------------------------------------------------------
    // The `status` contract
    //
    // `MockAdapter` only ever answers `{"status": "ok"}` and never fails, so
    // the non-`ok` and failing paths need a scripted venue-ops surface.
    // -----------------------------------------------------------------------

    /// A venue-ops surface whose `wallet.convert` payload and failures are
    /// scripted, recording every invocation.
    #[derive(Clone, Default)]
    struct StubOps {
        response: StdArc<StdMutex<serde_json::Value>>,
        error: StdArc<StdMutex<Option<String>>>,
        calls: StdArc<StdMutex<Vec<serde_json::Map<String, serde_json::Value>>>>,
    }

    impl StubOps {
        /// Answers `wallet.convert` with this exact payload.
        fn answering(self, payload: serde_json::Value) -> Self {
            *self.response.lock().expect("ops log") = payload;
            self
        }

        /// Refuses every invocation.
        fn refusing(self, op: &str) -> Self {
            *self.error.lock().expect("ops log") = Some(format!("{op} refused by the venue"));
            self
        }

        /// Every invocation's params, in call order.
        fn calls(&self) -> Vec<serde_json::Map<String, serde_json::Value>> {
            self.calls.lock().expect("ops log").clone()
        }
    }

    #[async_trait]
    impl VenueOpInvoker for StubOps {
        async fn invoke_venue_op(
            &self,
            _exchange_id: &common::ExchangeId,
            _op: &str,
            params: serde_json::Map<String, serde_json::Value>,
        ) -> Result<serde_json::Value, PortError> {
            self.calls.lock().expect("ops log").push(params);
            if let Some(message) = self.error.lock().expect("ops log").clone() {
                return Err(PortError::Transport(message));
            }
            Ok(self.response.lock().expect("ops log").clone())
        }

        async fn list_venue_ops(
            &self,
            _exchange_id: &common::ExchangeId,
        ) -> Result<Vec<VenueOpDescriptor>, PortError> {
            Err(PortError::Unsupported("list_venue_ops".into()))
        }
    }

    fn convert(ops: StubOps) -> Convert {
        let doc =
            "symbol = \"BTCUSDT\"\nfrom_asset = \"DUST\"\nto_asset = \"USDT\"\nmin_amount = \"10\"";
        let config =
            ConvertConfig::from_params(&toml::from_str(doc).expect("toml")).expect("config");
        Convert::new(config, StdArc::new(ops))
    }

    /// A venue that refuses the swap is not a successful conversion: the caller
    /// must see `false` rather than an `Ok(())` that reads as "converted".
    #[tokio::test]
    async fn a_status_other_than_ok_reports_failure() {
        let ops = StubOps::default().answering(serde_json::json!({ "status": "failed" }));
        let s = convert(ops);
        assert!(!s.tick().await.expect("a venue answer is not a transport error"));
    }

    /// A response with no `status` at all is also a failure. Reading it as
    /// success would report a conversion nobody performed.
    #[tokio::test]
    async fn a_response_without_a_status_reports_failure() {
        let ops = StubOps::default().answering(serde_json::json!({}));
        let s = convert(ops);
        assert!(!s.tick().await.expect("an empty answer is not a transport error"));
    }

    /// `status` is matched as a JSON string; a numeric status is not `"ok"`.
    #[tokio::test]
    async fn a_non_string_status_reports_failure() {
        let ops = StubOps::default().answering(serde_json::json!({ "status": 200 }));
        let s = convert(ops);
        assert!(!s.tick().await.expect("a numeric status is not a transport error"));
    }

    /// The comparison is exact and case-sensitive: a venue reporting `"OK"`
    /// must not be read as a success the strategy then stops retrying.
    #[tokio::test]
    async fn the_status_match_is_case_sensitive() {
        for status in ["OK", "Ok", "ok ", " ok", "success"] {
            let ops = StubOps::default().answering(serde_json::json!({ "status": status }));
            let s = convert(ops);
            assert!(!s.tick().await.expect("the venue answered"), "status {status:?}");
        }
    }

    /// The swap names both legs and the configured minimum amount; a wrong asset
    /// or amount would move the wrong funds.
    #[tokio::test]
    async fn the_invocation_names_both_assets_and_the_minimum_amount() {
        let ops = StubOps::default().answering(serde_json::json!({ "status": "ok" }));
        let s = convert(ops.clone());
        assert!(s.tick().await.expect("converted"));
        let calls = ops.calls();
        assert_eq!(calls.len(), 1, "one conversion attempt per cycle");
        assert_eq!(calls[0]["fromAsset"], serde_json::json!("DUST"));
        assert_eq!(calls[0]["toAsset"], serde_json::json!("USDT"));
        assert_eq!(calls[0]["amount"], serde_json::json!("10"));
    }

    /// The op fires unconditionally and always sends `min_amount`. This is the
    /// documented contract (see the module doc): there is no balance gate, so
    /// the test pins the behaviour the doc now describes rather than papering
    /// over the mismatch it used to flag.
    #[tokio::test]
    async fn the_convert_fires_without_reading_any_balance() {
        let ops = StubOps::default().answering(serde_json::json!({ "status": "ok" }));
        let s = convert(ops.clone());
        assert!(s.tick().await.expect("converted"));
        let calls = ops.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["amount"], serde_json::json!("10"), "the amount sent is min_amount");
    }

    #[tokio::test]
    async fn a_refused_convert_propagates() {
        let ops = StubOps::default().refusing("wallet.convert");
        let s = convert(ops);
        let err = s.tick().await.expect_err("a refused swap must surface");
        assert!(err.to_string().contains("wallet.convert refused"), "{err}");
    }

    /// The default interval is an hour; a misspelled key would silently poll
    /// every three seconds.
    #[test]
    fn the_default_conversion_interval_is_one_hour() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\nfrom_asset = \"A\"\nto_asset = \"B\"\nmin_amount = \"5\"",
        )
        .expect("toml");
        let cfg = ConvertConfig::from_params(&table).expect("config");
        assert_eq!(cfg.interval_secs, 3600);
    }
}
