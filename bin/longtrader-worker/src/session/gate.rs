//! Part of the session control plane; see [`super::SessionManager`].

use super::{Arc, ManagerError, Ordering, SessionHandle, SessionManager, SessionState};
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
        if attributed > 0 {
            handle.orders_submitted.fetch_add(attributed, Ordering::Relaxed);
        }
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
        let mut failures = Vec::new();
        for coid in handle.tracked_orders().await {
            let req = trading::CancelOrderRequest {
                exchange_id: buffa::MessageField::some(self.default_exchange.clone()),
                order_id: coid.clone(),
                symbol: String::new(),
                ..Default::default()
            };
            if let Err(err) = self.gateway.cancel_order(req).await {
                tracing::error!(
                    session = %handle.id,
                    coid = %coid,
                    error = %err,
                    "session order cancel failed; it may still be resting on the venue"
                );
                failures.push(format!("{coid}: {err}"));
            }
        }
        failures
    }
}
