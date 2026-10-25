//! Local strategy host: portable strategies over proto-only ports, pluggable
//! backends (exchange daemon / terminal API / mock), and a session control
//! plane for external-language strategies.
//!
//! Dependency rule: this crate depends on generated protobuf types and public
//! registry crates only — never on sibling-repo domain crates.

pub mod adapters;
pub mod config;
pub mod envelope;
pub mod indicators;
pub mod overflow;
pub mod ports;
pub mod risk;
pub mod session;
pub mod state_store;
pub mod strategies;

/// Shared aliases for the generated contract modules.
pub mod proto {
    pub use longtrader_contract::proto::longtrader::{
        account::v1 as account, backtest::v1 as backtest, common::v1 as common, data::v1 as data,
        market::v1 as market, ops::v1 as ops, stream::v1 as stream, terminal::v1 as terminal,
        trading::v1 as trading, worker::v1 as worker,
    };
}
