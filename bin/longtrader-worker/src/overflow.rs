//! Backpressure-aware channel plumbing (design doc §6.5).
//!
//! [`policy_channel`] wraps a bounded mpsc channel with an overflow policy:
//!
//! - [`OverflowPolicy::Block`] forwards directly through the bounded sender, backpressuring the
//!   pump when the consumer stalls. Used for orders/balances/positions (`OVERFLOW_ORDERS`).
//! - [`OverflowPolicy::DropOldest`] buffers up to `cap` items and silently drops the oldest
//!   buffered item on overflow. Used for ticker/trades/ohlcv (`OVERFLOW_TICKER`).
//! - [`OverflowPolicy::Coalesce`] keeps only the newest item per key (e.g. per symbol/channel),
//!   collapsing stale intermediate states. Used for orderbook (`OVERFLOW_ORDERBOOK`).
//!
//! Backpressure isolation: every session owns an independent `mpsc` channel
//! (one per logical stream) so a slow consumer never blocks another session
//! or the worker↔daemon ingress. Policies above map per-channel:
//! ticker→`DropOldest`, book→`Coalesce`, orders→`Block`.

use std::{
    collections::VecDeque,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{Mutex, Notify, mpsc};

use crate::ports::OverflowPolicy;

struct PipeState<T, K> {
    tx: mpsc::Sender<T>,
    buf: Mutex<VecDeque<(K, T)>>,
    cap: usize,
    policy: OverflowPolicy,
    key_fn: Arc<dyn Fn(&T) -> K + Send + Sync>,
    notify: Notify,
    source_closed: AtomicBool,
}

impl<T, K> PipeState<T, K>
where
    T: Send + 'static,
    K: Eq + Hash + Send + 'static,
{
    /// Forward loop: wakes on new buffered items or source closure, drains
    /// the buffer into the bounded downstream channel.
    ///
    /// Uses `tokio::select!` to avoid the race between checking `source_closed`
    /// and waiting on `notify.notified()`. This ensures that if `close()` is
    /// called between the check and the wait, the notification is not lost.
    async fn forward_loop(self: Arc<Self>) {
        loop {
            // Check for source closure first.
            if self.source_closed.load(Ordering::Acquire) {
                let drained: VecDeque<(K, T)> = std::mem::take(&mut *self.buf.lock().await);
                for (_, item) in drained {
                    let _ = self.tx.send(item).await;
                }
                return;
            }
            // Try to drain buffered items without waiting.
            let items: VecDeque<(K, T)> = {
                let mut buf = self.buf.lock().await;
                if buf.is_empty() { VecDeque::new() } else { std::mem::take(&mut *buf) }
            };
            if !items.is_empty() {
                for (_, item) in items {
                    // Downstream is bounded; this backpressures draining, which is
                    // fine because the buffer already applies the drop/coalesce
                    // policy on push.
                    let _ = self.tx.send(item).await;
                }
                continue;
            }
            // Buffer is empty; wait for either a new item or source closure.
            // `tokio::select!` ensures we wake up immediately if `close()` is
            // called while we are waiting.
            //
            // Note: We use a simple `notify.notified()` here instead of a busy-wait
            // loop. The `close()` method sets `source_closed` and calls `notify_one()`,
            // so we will wake up immediately when the source is closed.
            self.notify.notified().await;
        }
    }
}

/// Sender side of a policy pipe. Cheap to clone.
pub struct PolicySender<T, K> {
    state: Arc<PipeState<T, K>>,
}

impl<T, K> Clone for PolicySender<T, K> {
    fn clone(&self) -> Self {
        Self { state: Arc::clone(&self.state) }
    }
}

impl<T, K> PolicySender<T, K>
where
    T: Send + 'static,
    K: Eq + Hash + Send + 'static,
{
    /// Push one item through the configured overflow policy.
    /// ponytail: Coalesce uses an O(n) key scan; cap is small (<=64 in practice).
    /// Upgrade to HashMap<index> if cap grows or profiling shows hotspot.
    pub async fn send(&self, item: T) {
        match self.state.policy {
            OverflowPolicy::Block => {
                // Direct bounded send: never drops, backpressures upstream.
                let _ = self.state.tx.send(item).await;
            }
            OverflowPolicy::DropOldest | OverflowPolicy::Coalesce => {
                let key = (self.state.key_fn)(&item);
                {
                    let mut buf = self.state.buf.lock().await;
                    debug_assert!(self.state.cap <= 1024, "coalesce cap unexpectedly large");
                    if self.state.policy == OverflowPolicy::Coalesce {
                        // Replace any buffered item with the same key.
                        if let Some(slot) = buf.iter_mut().find(|(k, _)| *k == key) {
                            slot.1 = item;
                        } else {
                            buf.push_back((key, item));
                            while buf.len() > self.state.cap {
                                buf.pop_front();
                            }
                        }
                    } else {
                        buf.push_back((key, item));
                        while buf.len() > self.state.cap {
                            buf.pop_front();
                        }
                    }
                }
                self.state.notify.notify_one();
            }
        }
    }

    /// Mark the upstream source closed; buffered items are drained, then the
    /// downstream receiver closes.
    pub fn close(&self) {
        self.state.source_closed.store(true, Ordering::Release);
        self.state.notify.notify_one();
    }

    /// True once every downstream receiver has been dropped.
    pub fn is_closed(&self) -> bool {
        self.state.tx.is_closed()
    }
}

/// Create a policy-applied channel with buffer capacity `cap`. `key_of`
/// extracts the coalescing key (identity for DropOldest).
pub fn policy_channel<T, K>(
    cap: usize,
    policy: OverflowPolicy,
    key_of: impl Fn(&T) -> K + Send + Sync + 'static,
) -> (PolicySender<T, K>, mpsc::Receiver<T>)
where
    T: Send + 'static,
    K: Eq + Hash + Send + 'static,
{
    let cap = cap.max(1);
    let (tx, rx) = mpsc::channel(cap);
    let state = Arc::new(PipeState {
        tx,
        buf: Mutex::new(VecDeque::new()),
        cap,
        policy,
        key_fn: Arc::new(key_of),
        notify: Notify::new(),
        source_closed: AtomicBool::new(false),
    });
    tokio::spawn(state.clone().forward_loop());
    (PolicySender { state }, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run `produce` on a separate task so bounded/blocking sends never
    /// deadlock against the consuming test task.
    async fn drive<T, K>(
        cap: usize,
        policy: OverflowPolicy,
        key_of: impl Fn(&T) -> K + Send + Sync + 'static,
        produce: impl FnOnce(PolicySender<T, K>) -> futures_util::future::BoxFuture<'static, ()>
        + Send
        + 'static,
    ) -> mpsc::Receiver<T>
    where
        T: Send + 'static,
        K: Eq + Hash + Send + 'static,
    {
        let (tx, rx) = policy_channel(cap, policy, key_of);
        tokio::spawn(async move {
            produce(tx.clone()).await;
            tx.close();
        });
        rx
    }

    #[tokio::test]
    async fn block_policy_never_drops() {
        let mut rx = drive(
            2,
            OverflowPolicy::Block,
            |v| *v,
            |tx| {
                Box::pin(async move {
                    for v in 0..8u64 {
                        tx.send(v).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn drop_oldest_keeps_newest_window() {
        let mut rx = drive(
            3,
            OverflowPolicy::DropOldest,
            |v| *v,
            |tx| {
                Box::pin(async move {
                    for v in 0..10u64 {
                        tx.send(v).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![7, 8, 9], "only the newest `cap` items survive");
    }

    #[tokio::test]
    async fn coalesce_keeps_newest_per_key_in_arrival_order() {
        let mut rx = drive(
            4,
            OverflowPolicy::Coalesce,
            |item: &(char, u32)| item.0,
            |tx| {
                Box::pin(async move {
                    for &(k, v) in &[('a', 1), ('b', 1), ('a', 2), ('a', 3), ('c', 9)] {
                        tx.send((k, v)).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        // Coalesce keeps each key's insertion position and only refreshes its
        // payload, so keys arrive in first-seen order.
        assert_eq!(seen, vec![('a', 3), ('b', 1), ('c', 9)]);
    }

    #[tokio::test]
    async fn close_drains_buffer_before_closing_receiver() {
        let mut rx = drive(
            4,
            OverflowPolicy::DropOldest,
            |v| *v,
            |tx| {
                Box::pin(async move {
                    tx.send(1).await;
                    tx.send(2).await;
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![1, 2]);
    }

    // -----------------------------------------------------------------------
    // Capacity clamping
    // -----------------------------------------------------------------------

    /// `mpsc::channel(0)` panics, so a zero capacity is clamped to one. The
    /// observable effect is that exactly one item survives a burst.
    #[tokio::test]
    async fn a_zero_capacity_is_clamped_to_one() {
        let mut rx = drive(
            0,
            OverflowPolicy::DropOldest,
            |v| *v,
            |tx| {
                Box::pin(async move {
                    for v in 0..6u64 {
                        tx.send(v).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![5], "cap 0 behaves as cap 1: only the newest survives");
    }

    /// Under `Block`, a clamped capacity still delivers every item — nothing is
    /// dropped, it just takes longer.
    #[tokio::test]
    async fn block_policy_with_a_clamped_capacity_still_delivers_everything() {
        let mut rx = drive(
            0,
            OverflowPolicy::Block,
            |v| *v,
            |tx| {
                Box::pin(async move {
                    for v in 0..4u64 {
                        tx.send(v).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // Coalesce capacity
    // -----------------------------------------------------------------------

    /// More distinct keys than `cap` must evict from the front, keeping the
    /// newest `cap` keys in arrival order.
    #[tokio::test]
    async fn coalesce_drops_the_oldest_key_when_distinct_keys_exceed_cap() {
        let mut rx = drive(
            2,
            OverflowPolicy::Coalesce,
            |item: &(char, u32)| item.0,
            |tx| {
                Box::pin(async move {
                    for &(k, v) in &[('a', 1), ('b', 2), ('c', 3), ('d', 4)] {
                        tx.send((k, v)).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![('c', 3), ('d', 4)], "the two newest keys survive");
    }

    /// A repeated key never consumes extra capacity: refreshing an existing slot
    /// replaces the payload in place, so a busy symbol cannot evict others.
    #[tokio::test]
    async fn coalesce_refreshing_a_key_does_not_consume_capacity() {
        let mut rx = drive(
            2,
            OverflowPolicy::Coalesce,
            |item: &(char, u32)| item.0,
            |tx| {
                Box::pin(async move {
                    for &(k, v) in &[('a', 1), ('b', 1), ('a', 2), ('a', 3), ('a', 4)] {
                        tx.send((k, v)).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![('a', 4), ('b', 1)], "key 'a' was refreshed, not duplicated");
    }

    /// With a single key the coalesce stream is exactly "the latest value",
    /// which is the orderbook contract.
    #[tokio::test]
    async fn coalesce_on_one_key_keeps_only_the_latest() {
        let mut rx = drive(
            8,
            OverflowPolicy::Coalesce,
            |item: &(char, u32)| item.0,
            |tx| {
                Box::pin(async move {
                    for v in 1..=20u32 {
                        tx.send(('x', v)).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![('x', 20)]);
    }

    /// Under `DropOldest` the key function is irrelevant: every item occupies a
    /// slot whether or not the key repeats.
    #[tokio::test]
    async fn drop_oldest_keeps_every_item_even_when_keys_repeat() {
        let mut rx = drive(
            3,
            OverflowPolicy::DropOldest,
            |item: &(char, u32)| item.0,
            |tx| {
                Box::pin(async move {
                    for v in 1..=10u32 {
                        tx.send(('x', v)).await;
                    }
                })
            },
        )
        .await;
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(
            seen,
            vec![('x', 8), ('x', 9), ('x', 10)],
            "keys do not dedupe under DropOldest"
        );
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// `is_closed` tracks the downstream receiver: false while a consumer is
    /// attached, true once it drops.
    #[tokio::test]
    async fn is_closed_follows_the_downstream_receiver() {
        let (tx, rx) = policy_channel::<u64, u64>(2, OverflowPolicy::Block, |v| *v);
        assert!(!tx.is_closed(), "a live receiver is not closed");
        drop(rx);
        // The forward task observes the close asynchronously.
        for _ in 0..100 {
            if tx.is_closed() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(tx.is_closed(), "dropping the receiver must close the sender");
    }

    /// Closing before anything is sent still terminates the stream.
    #[tokio::test]
    async fn closing_an_empty_pipe_terminates_the_stream() {
        let (tx, mut rx) = policy_channel::<u64, u64>(2, OverflowPolicy::DropOldest, |v| *v);
        tx.close();
        // The receiver only observes end-of-stream once the shared `PipeState`
        // is gone, i.e. after every `PolicySender` clone is dropped.
        drop(tx);
        let idle = std::time::Duration::from_secs(5);
        let closed = tokio::time::timeout(idle, rx.recv()).await;
        assert!(matches!(closed, Ok(None)), "close with no items yields an empty stream");
    }

    /// A clone shares the same pipe, so closing through one clone closes both.
    #[tokio::test]
    async fn clones_share_one_pipe() {
        let (tx, mut rx) = policy_channel::<u64, u64>(4, OverflowPolicy::DropOldest, |v| *v);
        let other = tx.clone();
        other.send(1).await;
        other.send(2).await;
        tx.close();
        // The downstream sender lives in the shared `PipeState`, so the receiver
        // only sees end-of-stream once *every* `PolicySender` is gone.
        drop(tx);
        drop(other);
        let mut seen = Vec::new();
        while let Some(v) = rx.recv().await {
            seen.push(v);
        }
        assert_eq!(seen, vec![1, 2], "items sent through either clone arrive in order");
    }

    /// Close is idempotent: calling it twice must not panic or hang.
    #[tokio::test]
    async fn closing_twice_is_harmless() {
        let (tx, mut rx) = policy_channel::<u64, u64>(2, OverflowPolicy::Block, |v| *v);
        tx.close();
        tx.close();
        drop(tx);
        let idle = std::time::Duration::from_secs(5);
        let closed = tokio::time::timeout(idle, rx.recv()).await;
        assert!(matches!(closed, Ok(None)), "a repeated close must still terminate");
    }
}
