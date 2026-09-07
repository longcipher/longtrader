//! Connect-RPC protocol definitions and client for the trading terminal.
//!
//! Defines `longtrader.terminal.v1` services (MarketData / Trading / Runtime /
//! Strategy) from `proto/` via `connectrpc-build` + `buffa`, plus an optional
//! native (hpx) client.

#![allow(missing_docs)]
#![allow(missing_debug_implementations)]
#![allow(clippy::pedantic)]
#![allow(clippy::use_self)]
#![allow(elided_lifetimes_in_paths)]
#![allow(clippy::derive_partial_eq_without_eq)]

pub mod proto {
    connectrpc::include_generated!();
}

#[cfg(feature = "client-native")]
mod client_core;

/// Native (hpx) client for the terminal services.
#[cfg(feature = "client-native")]
pub mod client;

#[cfg(feature = "client-native")]
pub use client::{TerminalClient, TerminalClientError};

/// Canonical service name constants (Connect path `/{service}/{method}`).
pub mod service_name {
    pub const MARKET_DATA: &str = "longtrader.terminal.v1.MarketDataService";
    pub const TRADING: &str = "longtrader.terminal.v1.TradingService";
    pub const RUNTIME: &str = "longtrader.terminal.v1.RuntimeService";
    pub const STRATEGY: &str = "longtrader.terminal.v1.StrategyService";
}

pub use proto::longtrader::terminal::v1::*;
