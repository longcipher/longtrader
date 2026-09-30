//! Connect-RPC protocol definitions and client for the trading terminal.
//!
//! All generated protobuf types — including `longtrader.terminal.v1` — now come
//! from the single source of truth, [`longtrader_contract`]. This crate adds
//! only the native (hpx) client and a shared transport helper on top, so there
//! is exactly one generated code base for the whole workspace (no duplicate
//! `terminal.v1` codegen).

#![allow(missing_docs)]
#![allow(missing_debug_implementations)]
#![allow(clippy::pedantic)]
#![allow(clippy::use_self)]
#![allow(elided_lifetimes_in_paths)]
#![allow(clippy::derive_partial_eq_without_eq)]

/// Single source of truth for every generated protobuf type.
pub use longtrader_contract::proto;

#[cfg(any(feature = "client-native", feature = "client-wasm"))]
mod client_core;

/// Native (hpx) client for the terminal services.
#[cfg(feature = "client-native")]
pub mod client;

/// WASM-compatible client (gloo-net / web-sys) for the terminal services.
#[cfg(feature = "client-wasm")]
pub mod client_wasm;

// Re-export the active client types. When only one of `client-native` /
// `client-wasm` is enabled, this is a simple re-export. When both are enabled,
// the caller must disambiguate via `longtrader_proto::client::TerminalClient`
// or `longtrader_proto::client_wasm::TerminalClient`.
#[cfg(all(feature = "client-native", not(feature = "client-wasm")))]
pub use client::{TerminalClient, TerminalClientError};
#[cfg(all(feature = "client-wasm", not(feature = "client-native")))]
pub use client_wasm::{TerminalClient, TerminalClientError};

/// Shared Connect-RPC unary transport, reused by every client in the workspace
/// (the worker's `RemoteAdapter` and this crate's `TerminalClient`) so the
/// URL construction, bearer auth, status handling and decode logic lives once.
#[cfg(feature = "client-native")]
pub mod transport;

/// Canonical service name constants (Connect path `/{service}/{method}`).
pub mod service_name {
    /// Canonical market data service; the terminal surface never redefined it.
    pub const MARKET_DATA: &str = "longtrader.market.v1.MarketDataService";
    /// Canonical trading service.
    pub const TRADING: &str = "longtrader.trading.v1.TradingService";
    /// Canonical streaming service (same envelope as `RuntimeService::StreamUpdates`).
    pub const STREAM: &str = "longtrader.stream.v1.StreamService";
    /// Terminal-only runtime surface: health, venue listing, update stream.
    pub const RUNTIME: &str = "longtrader.terminal.v1.RuntimeService";
    /// Terminal-only strategy surface: lifecycle, actions, strategy events.
    pub const STRATEGY: &str = "longtrader.terminal.v1.StrategyService";
}

// Backwards-compatible convenience: surface the terminal.v1 types at the crate
// root exactly as before the contract convergence.
pub use longtrader_contract::proto::longtrader::terminal::v1::*;
