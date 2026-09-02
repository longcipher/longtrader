//! Backend adapters implementing the strategy-facing ports.
//!
//! Each adapter owns one backend transport and maps it onto the unified
//! contract. After contract convergence the daemon and terminal adapters
//! differ only in endpoint + credentials (design doc §6.1).
//!
//! Backpressure isolation (design doc §6.5): adapters do NOT share channels
//! across sessions. Every `subscribe_market_data` call returns an independent
//! bounded `mpsc` channel wrapped by `crate::overflow::policy_channel` with a
//! stream-appropriate `OverflowPolicy`:
//! - ticker / trades / ohlcv → `DropOldest`
//! - orderbook → `Coalesce`
//! - orders / balances / positions → `Block`
//!
//! This guarantees per-session isolation: one slow strategy cannot backpressure
//! another session or the upstream daemon feed.

#[cfg(test)] // in-process fake lives only in tests; dry-run uses api paper mode
pub mod mock;
pub mod remote;

#[cfg(test)]
pub use mock::MockAdapter;
pub use remote::RemoteAdapter;

use crate::proto::common;

/// Build an [`common::ExchangeId`] from config strings.
pub fn exchange_id(id: &str, label: &str) -> common::ExchangeId {
    common::ExchangeId { id: id.to_string(), label: label.to_string(), ..Default::default() }
}
