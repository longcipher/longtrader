//! Backend adapters implementing the strategy-facing ports.
//!
//! Each adapter owns one backend transport and maps it onto the unified
//! contract. The terminal and daemon backends converged onto the same
//! `longtrader.{market,trading}.v1` services, so one [`RemoteAdapter`] now
//! serves both — the earlier terminal-only translation layer is gone and the
//! `backend` value only picks endpoint + credentials (design doc §6.1).
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

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub use mock::MockAdapter;
pub use remote::RemoteAdapter;

use crate::{
    overflow,
    ports::{MarketEventStream, OverflowPolicy, PortError},
    proto::{common, market},
};

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
/// * `fetcher` - A closure that fetches one snapshot for a given subscription. The closure must be
///   `Send + 'static` and the returned future must be `Send`.
///
/// # Returns
/// A receiver that yields `MarketDataEvent`s
///
/// If `req.exchange_id` is `None`, returns an already-closed stream. The stream
/// also ends once every poll task has finished, either because the client dropped
/// the receiver or because a request produced no poll task at all (no
/// subscriptions, no symbol, no venue): in every one of those cases nothing can
/// ever write to the channel again, so a consumer waiting for end-of-stream gets
/// it instead of waiting forever.
pub fn poll_market_data<F, Fut>(
    req: market::StreamMarketDataRequest,
    policy: OverflowPolicy,
    fetcher: F,
) -> MarketEventStream
where
    F: FnMut(market::StreamChannel, common::ExchangeId, String, Arc<AtomicU64>) -> Fut
        + Send
        + 'static
        + Clone,
    Fut: std::future::Future<Output = Result<Option<market::MarketDataEvent>, PortError>>
        + Send
        + 'static,
{
    const POLL_INTERVAL_MS: u64 = 1_000;
    const BUFFER_CAP: usize = 16;

    let seq = Arc::new(AtomicU64::new(0));
    let (tx, rx) = overflow::policy_channel(BUFFER_CAP, policy, market_event_key);

    // exchange_id is a required field in the proto definition.
    // If it is None, return an empty stream.
    let Some(default_exchange) = req.exchange_id.as_option().cloned() else {
        tx.close();
        return rx;
    };
    // Poll tasks still running. The last one to finish closes the channel, which
    // is what lets `policy_channel`'s forward loop drain its buffer and return
    // instead of parking on the channel forever with no producer left.
    let live_tasks = Arc::new(AtomicUsize::new(0));
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
        let live_tasks = Arc::clone(&live_tasks);
        let symbol = sub.symbol;
        let mut fetcher = fetcher.clone();
        live_tasks.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(POLL_INTERVAL_MS));
            loop {
                if tx.is_closed() {
                    break;
                }
                ticker.tick().await;
                if tx.is_closed() {
                    break;
                }
                match fetcher(channel, exchange_id.clone(), symbol.clone(), Arc::clone(&seq)).await
                {
                    Ok(Some(event)) => {
                        tx.send(event).await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, symbol = %symbol, "market poll failed");
                    }
                }
            }
            // This subscription is finished; if it was the last one, no producer
            // remains and the stream ends rather than hanging.
            if live_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
                tx.close();
            }
        });
    }
    if live_tasks.load(Ordering::Acquire) == 0 {
        // Nothing was pollable (no subscriptions, or none with a symbol): there is
        // no producer to ever write to this channel.
        tx.close();
    }
    rx
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Boxed future alias for the fetchers below.
    type Fetch = futures_util::future::BoxFuture<
        'static,
        Result<Option<market::MarketDataEvent>, PortError>,
    >;

    #[test]
    fn exchange_id_carries_both_the_venue_and_the_label() {
        let id = exchange_id("binance", "sub-1");
        assert_eq!(id.id, "binance");
        assert_eq!(id.label, "sub-1");
    }

    /// The empty id is the documented "the backend's single active venue"
    /// selector, so it must survive construction rather than be defaulted away.
    #[test]
    fn an_empty_exchange_id_is_preserved() {
        let id = exchange_id("", "");
        assert!(id.id.is_empty());
        assert!(id.label.is_empty());
    }

    fn ticker_event(symbol: &str) -> market::MarketDataEvent {
        market::MarketDataEvent {
            event: Some(market::market_data_event::Event::Ticker(Box::new(market::Ticker {
                symbol: symbol.into(),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn book_event(symbol: &str) -> market::MarketDataEvent {
        market::MarketDataEvent {
            event: Some(market::market_data_event::Event::Orderbook(Box::new(market::OrderBook {
                symbol: symbol.into(),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn trade_event(symbol: &str) -> market::MarketDataEvent {
        market::MarketDataEvent {
            event: Some(market::market_data_event::Event::Trade(Box::new(market::PublicTrade {
                symbol: symbol.into(),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn ohlcv_event() -> market::MarketDataEvent {
        market::MarketDataEvent {
            event: Some(market::market_data_event::Event::Ohlcv(Box::default())),
            ..Default::default()
        }
    }

    /// The key is what `Coalesce` dedupes on, so a wrong symbol half would let
    /// two symbols overwrite each other's newest state.
    #[test]
    fn the_coalesce_key_is_channel_and_symbol() {
        assert_eq!(market_event_key(&ticker_event("BTC/USDT")), "ticker:BTC/USDT");
        assert_eq!(market_event_key(&book_event("BTC/USDT")), "book:BTC/USDT");
        assert_eq!(market_event_key(&trade_event("ETH/USDT")), "trade:ETH/USDT");
    }

    /// `OHLCV` carries no symbol of its own, and `None` carries no channel, so
    /// both fall back to empty segments. Pin the exact strings because a
    /// coalescing policy keys on them.
    #[test]
    fn the_coalesce_key_falls_back_for_symbolless_variants() {
        assert_eq!(market_event_key(&ohlcv_event()), "ohlcv:");
        assert_eq!(market_event_key(&market::MarketDataEvent::default()), "none:");
    }

    /// Two events on different channels but the same symbol must not collide,
    /// or an orderbook update would evict a ticker.
    #[test]
    fn distinct_channels_on_one_symbol_do_not_collide() {
        let ticker = market_event_key(&ticker_event("X"));
        let book = market_event_key(&book_event("X"));
        let trade = market_event_key(&trade_event("X"));
        assert_ne!(ticker, book);
        assert_ne!(ticker, trade);
        assert_ne!(book, trade);
    }

    /// The key must be stable: the same event always hashes to the same slot,
    /// which is what lets `Coalesce` replace in place instead of appending.
    #[test]
    fn the_coalesce_key_is_stable_for_the_same_event() {
        assert_eq!(market_event_key(&ticker_event("X")), market_event_key(&ticker_event("X")));
    }

    // -----------------------------------------------------------------------
    // poll_market_data
    // -----------------------------------------------------------------------

    fn request(symbols: &[&str]) -> market::StreamMarketDataRequest {
        market::StreamMarketDataRequest {
            exchange_id: buffa::MessageField::some(exchange_id("mock", "")),
            subscriptions: symbols
                .iter()
                .map(|symbol| market::StreamSubscription {
                    channel: buffa::EnumValue::Known(market::StreamChannel::Ticker),
                    symbol: (*symbol).to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Fetcher that always reports a ticker for whatever symbol it was asked
    /// for. Captures nothing, so it satisfies the `Clone` bound.
    fn ticker_fetcher()
    -> impl FnMut(market::StreamChannel, common::ExchangeId, String, Arc<AtomicU64>) -> Fetch
    + Send
    + Clone
    + 'static {
        move |_, _, symbol, _| {
            Box::pin(async move {
                Ok(Some(market::MarketDataEvent {
                    event: Some(market::market_data_event::Event::Ticker(Box::new(
                        market::Ticker { symbol, ..Default::default() },
                    ))),
                    ..Default::default()
                }))
            })
        }
    }

    /// Assert that no event arrives within a short window.
    ///
    /// Used only where a poll task is still running and simply has nothing to
    /// report: `Ok(None)` must not be surfaced as an event, and the poll must
    /// keep going. A request that produces no poll task at all ends its stream
    /// instead, which [`expect_end_of_stream`] asserts.
    async fn expect_silence(rx: &mut tokio::sync::mpsc::Receiver<market::MarketDataEvent>) -> bool {
        let idle = std::time::Duration::from_millis(250);
        tokio::time::timeout(idle, rx.recv()).await.is_err()
    }

    /// Assert that the stream ends with no event at all.
    ///
    /// The stronger of the two "nothing to report" properties: it proves no
    /// producer can appear later, which silence after 250ms cannot.
    async fn expect_end_of_stream(rx: &mut tokio::sync::mpsc::Receiver<market::MarketDataEvent>) {
        let idle = std::time::Duration::from_millis(250);
        let outcome = tokio::time::timeout(idle, rx.recv()).await;
        assert!(outcome.is_ok(), "the stream must end, not stay open with nothing to report");
        let received = outcome.expect("the timeout outcome was just checked");
        assert!(received.is_none(), "an unproducible request must emit no event: {received:?}");
    }

    /// A missing `exchange_id` is a malformed request, not a crash: the caller
    /// gets a stream that closes immediately rather than one that hangs with
    /// nothing that could ever write to it.
    #[tokio::test]
    async fn a_request_without_an_exchange_id_yields_an_empty_stream() {
        let req = market::StreamMarketDataRequest {
            exchange_id: buffa::MessageField::none(),
            subscriptions: vec![market::StreamSubscription {
                channel: buffa::EnumValue::Known(market::StreamChannel::Ticker),
                symbol: "BTC/USDT".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut rx = poll_market_data(req, OverflowPolicy::DropOldest, ticker_fetcher());
        expect_end_of_stream(&mut rx).await;
    }

    /// A subscription with no symbol is skipped rather than polled forever
    /// against a backend that cannot answer it — and with no poll task left, the
    /// stream ends instead of staying open.
    #[tokio::test]
    async fn subscriptions_without_a_symbol_are_skipped() {
        let mut rx = poll_market_data(request(&[""]), OverflowPolicy::DropOldest, ticker_fetcher());
        expect_end_of_stream(&mut rx).await;
    }

    /// A request with no subscriptions at all must still hand back a live
    /// channel rather than panic, and must end it because no producer exists.
    #[tokio::test]
    async fn a_request_with_no_subscriptions_yields_an_empty_stream() {
        let mut rx = poll_market_data(request(&[]), OverflowPolicy::DropOldest, ticker_fetcher());
        expect_end_of_stream(&mut rx).await;
    }

    /// A healthy poller delivers events under the requested overflow policy.
    #[tokio::test]
    async fn a_polled_subscription_delivers_events() {
        let mut rx =
            poll_market_data(request(&["BTC/USDT"]), OverflowPolicy::DropOldest, ticker_fetcher());
        let event = rx.recv().await.expect("the first tick delivers an event");
        assert!(
            matches!(event.event, Some(market::market_data_event::Event::Ticker(_))),
            "got a non-ticker event"
        );
    }

    /// A `None` from the fetcher means "nothing to report", which must not be
    /// surfaced as an event.
    #[tokio::test]
    async fn a_none_result_is_not_forwarded() {
        let fetcher = move |_, _, _, _| Box::pin(async { Ok(None) }) as Fetch;
        let mut rx = poll_market_data(request(&["BTC/USDT"]), OverflowPolicy::DropOldest, fetcher);
        assert!(expect_silence(&mut rx).await, "Ok(None) must not become an event");
    }

    /// A failing poll must be logged and retried, not tear down the stream: a
    /// transient venue error must not silently stop a session's market data.
    #[tokio::test]
    async fn a_poll_error_does_not_close_the_stream() {
        let fetcher = move |_, _, _, _| {
            Box::pin(async { Err(PortError::Transport("venue down".into())) }) as Fetch
        };
        let mut rx = poll_market_data(request(&["BTC/USDT"]), OverflowPolicy::DropOldest, fetcher);
        assert!(expect_silence(&mut rx).await, "an errored poll delivers nothing");
        assert!(!rx.is_closed(), "an errored poll must not close the channel");
    }

    /// Two subscriptions on different symbols keep separate coalescing slots, so
    /// neither evicts the other.
    #[tokio::test]
    async fn distinct_symbols_are_polled_independently() {
        let mut rx =
            poll_market_data(request(&["A", "B"]), OverflowPolicy::Coalesce, ticker_fetcher());
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let event = rx.recv().await.expect("both subscriptions deliver");
            let Some(market::market_data_event::Event::Ticker(ticker)) = event.event else {
                continue;
            };
            seen.push(ticker.symbol);
        }
        seen.sort();
        assert_eq!(seen, vec!["A".to_string(), "B".to_string()]);
    }

    /// A dropped receiver must end the poll loop, not leave it spinning for the
    /// life of the process with nobody reading — that leaked task also kept the
    /// forwarder parked on a channel that could never be closed. Observed
    /// through the fetcher's own call count, which stops growing exactly when the
    /// loop stops.
    #[tokio::test(start_paused = true)]
    async fn the_poll_loop_stops_once_the_client_drops_the_receiver() {
        let polls = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&polls);
        let fetcher = move |_, _, _, _| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }) as Fetch
        };
        let rx = poll_market_data(request(&["BTC/USDT"]), OverflowPolicy::DropOldest, fetcher);
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let running = polls.load(Ordering::Acquire);
        assert!(running > 0, "test setup: the poll task must have polled at least once");

        drop(rx);
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            polls.load(Ordering::Acquire),
            running,
            "a dropped receiver must end the poll loop instead of leaking a poller"
        );
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The key always has exactly the `channel:symbol` shape.
        #[test]
        fn the_coalesce_key_is_always_channel_colon_symbol(symbol in "[a-zA-Z0-9/_.:-]{0,24}") {
            let key = market_event_key(&ticker_event(&symbol));
            let (channel, rest) = key.split_once(':').expect("the key contains a colon");
            prop_assert_eq!(channel, "ticker");
            prop_assert_eq!(rest, symbol.as_str());
        }

        /// Distinct symbols on one channel never share a key, which is what
        /// makes `Coalesce` per-symbol.
        #[test]
        fn distinct_symbols_never_share_a_coalesce_key(
            a in "[a-z0-9]{1,8}",
            b in "[a-z0-9]{1,8}",
        ) {
            prop_assume!(a != b);
            prop_assert_ne!(
                market_event_key(&ticker_event(&a)),
                market_event_key(&ticker_event(&b))
            );
            prop_assert_ne!(market_event_key(&book_event(&a)), market_event_key(&book_event(&b)));
            prop_assert_ne!(
                market_event_key(&trade_event(&a)),
                market_event_key(&trade_event(&b))
            );
        }

        /// `exchange_id` is a pure projection of its two inputs.
        #[test]
        fn exchange_id_never_transforms_its_inputs(
            id in "[a-z0-9_-]{0,16}",
            label in "[a-z0-9_-]{0,16}",
        ) {
            let built = exchange_id(&id, &label);
            prop_assert_eq!(built.id.as_str(), id.as_str());
            prop_assert_eq!(built.label.as_str(), label.as_str());
        }
    }
}
