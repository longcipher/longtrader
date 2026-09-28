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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::overflow;
use crate::ports::{MarketEventStream, OverflowPolicy, PortError};
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

/// Shared poll-based market data subscription implementation.
///
/// This function implements the common pattern used by all adapters:
/// - Create a policy channel with the given overflow policy
/// - For each subscription, spawn a task that polls the backend
/// - Each poll fetches a snapshot and wraps it as a `MarketDataEvent`
///
/// The `fetcher` closure is called with `(channel, exchange_id, symbol, seq)`
/// and should return `Ok(Some(event))` on success, `Ok(None)` to skip, or
/// `Err` on failure.
///
/// # Arguments
/// * `req` - The stream market data request
/// * `policy` - The overflow policy to apply
/// * `fetcher` - A closure that fetches one snapshot for a given subscription.
///   The closure must be `Send + 'static` and the returned future must be `Send`.
///
/// # Returns
/// A receiver that yields `MarketDataEvent`s
///
/// # Panics
/// Panics if `req.exchange_id` is `None`. The `exchange_id` is a required field
/// in the proto definition, so this should never happen in practice. However,
/// if it does, the function will panic with a descriptive error message.
pub fn poll_market_data<F, Fut>(
    req: market::StreamMarketDataRequest,
    policy: OverflowPolicy,
    mut fetcher: F,
) -> MarketEventStream
where
    F: FnMut(market::StreamChannel, common::ExchangeId, String, Arc<AtomicU64>) -> Fut + Send + 'static + Clone,
    Fut: std::future::Future<Output = Result<Option<market::MarketDataEvent>, PortError>> + Send + 'static,
{
    const POLL_INTERVAL_MS: u64 = 1_000;
    const BUFFER_CAP: usize = 16;

    let seq = Arc::new(AtomicU64::new(0));
    let (tx, rx) =
        overflow::policy_channel(BUFFER_CAP, policy, market_event_key);

    // exchange_id is a required field in the proto definition.
    // If it is None, we cannot proceed.
    let default_exchange = req.exchange_id.as_option().cloned().unwrap_or_else(|| {
        panic!("poll_market_data: req.exchange_id is None, but it is a required field");
    });
    for sub in req.subscriptions {
        let channel = match sub.channel {
            buffa::EnumValue::Known(c) => c,
            buffa::EnumValue::Unknown(_) => market::StreamChannel::Unspecified,
        };
        if sub.symbol.is_empty() {
            continue;
        }
        let exchange_id = default_exchange.clone();
        let tx = tx.clone();
        let seq = Arc::clone(&seq);
        let symbol = sub.symbol;
        let mut fetcher = fetcher.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(POLL_INTERVAL_MS));
            loop {
                if tx.is_closed() {
                    break;
                }
                ticker.tick().await;
                if tx.is_closed() {
                    break;
                }
                match fetcher(channel, exchange_id.clone(), symbol.clone(), Arc::clone(&seq)).await {
                    Ok(Some(event)) => {
                        tx.send(event).await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, symbol = %symbol, "market poll failed");
                    }
                }
            }
        });
    }
    rx
}
