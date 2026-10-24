//! Part of the session control plane; see [`super::SessionManager`].

use super::{
    Arc, CANCEL_ROUND_TRIP_TIMEOUT, ManagerError, Ordering, SessionHandle, SessionManager,
    SessionState,
};
use crate::proto::trading;

impl SessionManager {
    /// Gate a session-scoped order submission.
    ///
    /// This is the enforcement point behind `worker.proto`'s
    /// "Orders submitted before ACTIVE are rejected with reason
    /// SYNC_IN_PROGRESS". A session may only trade once it has reconciled, so
    /// it cannot act on stale state after a reconnect or a restart.
    ///
    /// `Ok(None)` means the caller passed an empty `session_id`: an unscoped
    /// operator action (the CLI) that is neither gated nor tracked. `Ok(Some)`
    /// carries the handle that callers must pass to
    /// [`Self::record_submitted_orders`].
    pub async fn authorize_order_submission(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<SessionHandle>>, ManagerError> {
        if session_id.is_empty() {
            return Ok(None);
        }
        let handle = self.get(session_id).await?;
        // A corrupt state byte is never treated as "active" — fail closed.
        if handle.state() != Some(SessionState::Active) {
            return Err(ManagerError::SyncInProgress {
                session_id: session_id.to_string(),
                state: handle.state(),
            });
        }
        Ok(Some(handle))
    }

    /// Attribute freshly submitted orders to a session so that
    /// `KillSwitchPolicy.SCOPE_SESSION_ORDERS` and
    /// `StopStrategy.cancel_open_orders` cancel exactly these orders.
    ///
    /// Orders with an empty venue-assigned id are skipped rather than tracked
    /// as an empty string, which would otherwise cancel an unrelated order.
    pub async fn record_submitted_orders(&self, handle: &SessionHandle, orders: &[trading::Order]) {
        let mut tracked = handle.tracked_coids.lock().await;
        // Count what was actually tracked, not what was handed over: an order
        // with no venue id is credited to the session but can never be cancelled
        // by the kill-switch, so counting it would make `StrategyStatus` report
        // more orders than the session is actually able to unwind.
        let mut attributed = 0u64;
        for order in orders {
            if order.id.is_empty() {
                continue;
            }
            tracked.insert(order.id.clone());
            attributed += 1;
        }
        drop(tracked);
        // No `attributed > 0` guard: `fetch_add(0)` is already a no-op, and a
        // conditional here only mutates into a tautology.
        handle.orders_submitted.fetch_add(attributed, Ordering::Relaxed);
    }

    /// Cancel every order this session tracked, returning one `"<id>: <error>"`
    /// entry per order the venue refused.
    ///
    /// Best-effort by design: one refused cancel must not stop the others from
    /// being attempted, or a venue that rejects a single id leaves the rest of the
    /// session's orders resting. Every failure is reported to the caller, so
    /// neither `StopStrategy` nor the kill-switch can report success over a
    /// partially unwound session.
    pub(crate) async fn cancel_session_orders(&self, handle: &SessionHandle) -> Vec<String> {
        let coids: Vec<String> = handle.tracked_orders().await.into_iter().collect();
        self.cancel_coids(&handle.id, &coids).await
    }

    /// Cancel the given venue order ids, concurrently and with a per-cancel
    /// budget.
    ///
    /// Concurrency and the budget are both load-bearing: this runs on the
    /// watchdog's kill-switch path, where a sequential loop of unbounded awaits
    /// would let one silent venue stall lease enforcement for every session.
    /// A cancel that exceeds [`CANCEL_ROUND_TRIP_TIMEOUT`] is reported as a
    /// failure — the order may still be resting — never dropped.
    pub(crate) async fn cancel_coids(&self, session_id: &str, coids: &[String]) -> Vec<String> {
        let exchange_id = self.default_exchange.clone();
        let jobs: Vec<_> = coids
            .iter()
            .map(|coid| {
                let gateway = Arc::clone(&self.gateway);
                let coid = coid.clone();
                let exchange_id = exchange_id.clone();
                async move {
                    let req = trading::CancelOrderRequest {
                        exchange_id: buffa::MessageField::some(exchange_id),
                        order_id: coid.clone(),
                        // No symbol: a session-scoped cancel addresses the venue
                        // order id alone, and `..Default::default()` already
                        // leaves the filter empty.
                        ..Default::default()
                    };
                    match tokio::time::timeout(CANCEL_ROUND_TRIP_TIMEOUT, gateway.cancel_order(req))
                        .await
                    {
                        Ok(Ok(_order)) => None,
                        Ok(Err(err)) => Some((coid, err.to_string())),
                        Err(_elapsed) => Some((
                            coid,
                            format!("cancel timed out after {CANCEL_ROUND_TRIP_TIMEOUT:?}"),
                        )),
                    }
                }
            })
            .collect();
        let outcomes = futures_util::future::join_all(jobs).await;
        let mut failures = Vec::new();
        for outcome in outcomes.into_iter().flatten() {
            tracing::error!(
                session = %session_id,
                coid = %outcome.0,
                error = %outcome.1,
                "session order cancel failed; it may still be resting on the venue"
            );
            failures.push(format!("{}: {}", outcome.0, outcome.1));
        }
        failures
    }

    /// Best-effort unwind of orders a session submitted moments before its
    /// lease lapsed or its kill-switch tripped.
    ///
    /// The gate is a point-in-time check: between `authorize_order_submission`
    /// and the venue's answer the session can go terminal, so an order can be
    /// filled on a session whose kill-switch already ran — and therefore never
    /// cancelled by it. Re-checking after the round trip catches exactly that
    /// window; the cancel is best-effort because the order may already be gone.
    pub(crate) async fn unwind_late_submissions(
        &self,
        handle: &SessionHandle,
        orders: &[trading::Order],
    ) {
        if orders.is_empty() || !handle.state().is_some_and(SessionState::is_terminal) {
            return;
        }
        let coids: Vec<String> =
            orders.iter().map(|o| o.id.clone()).filter(|id| !id.is_empty()).collect();
        tracing::error!(
            session = %handle.id,
            orders = coids.len(),
            "orders were submitted while the session went terminal; unwinding them best-effort"
        );
        self.cancel_coids(&handle.id, &coids).await;
    }
}
