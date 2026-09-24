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

pub mod mock;
pub mod remote;
pub mod terminal;

pub use mock::MockAdapter;
pub use remote::RemoteAdapter;
pub use terminal::TerminalAdapter;

use crate::proto::{common, market};

/// Build an [`common::ExchangeId`] from config strings.
pub fn exchange_id(id: &str, label: &str) -> common::ExchangeId {
    common::ExchangeId { id: id.to_string(), label: label.to_string(), ..Default::default() }
}

/// Coalesce key shared by mock + remote subscriptions: `channel:symbol`.
/// Single owner so backpressure semantics cannot drift between backends.
#[must_use]
pub fn market_event_key(event: &market::MarketDataEvent) -> String {
    let channel = match event.event.as_ref() {
        Some(market::market_data_event::Event::Ticker(_)) => "ticker",
        Some(market::market_data_event::Event::Orderbook(_)) => "book",
        Some(market::market_data_event::Event::Trade(_)) => "trade",
        Some(market::market_data_event::Event::Ohlcv(_)) => "ohlcv",
        None => "none",
    };
    let symbol = match event.event.as_ref() {
        Some(market::market_data_event::Event::Ticker(t)) => t.symbol.clone(),
        Some(market::market_data_event::Event::Orderbook(b)) => b.symbol.clone(),
        Some(market::market_data_event::Event::Trade(t)) => t.symbol.clone(),
        _ => String::new(),
    };
    format!("{channel}:{symbol}")
}
