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
//! 每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders
//! Block）

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

#[rustfmt::skip]
const _DOC_BACKPRESSURE: &str = "每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders Block）";

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
    async fn forward_loop(self: Arc<Self>) {
        loop {
            if self.source_closed.load(Ordering::Acquire) {
                let drained: VecDeque<(K, T)> = std::mem::take(&mut *self.buf.lock().await);
                for (_, item) in drained {
                    let _ = self.tx.send(item).await;
                }
                return;
            }
            let items: VecDeque<(K, T)> = {
                let mut buf = self.buf.lock().await;
                if buf.is_empty() { VecDeque::new() } else { std::mem::take(&mut *buf) }
            };
            if items.is_empty() {
                self.notify.notified().await;
                continue;
            }
            for (_, item) in items {
                // Downstream is bounded; this backpressures draining, which is
                // fine because the buffer already applies the drop/coalesce
                // policy on push.
                let _ = self.tx.send(item).await;
            }
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
}
