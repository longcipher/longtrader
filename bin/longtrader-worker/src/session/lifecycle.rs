//! Part of the session control plane; see [`super::SessionManager`].

use super::{
    Arc, AtomicU8, AtomicU64, ClientIdentity, DEFAULT_HEARTBEAT_MS, HashSet, ManagerError, Mutex,
    Ordering, SessionHandle, SessionManager, SessionPolicy, SessionState, VecDeque, apply_policy,
    broadcast,
};
use crate::proto::worker;

impl SessionManager {
    /// Validate the terminal token and create a new session.
    ///
    /// Token validation reuses the same `api_token` logic as `longtrader-api`
    /// and `longtrader-terminal` (constant-time comparison via `subtle`);
    /// `expected_token` is populated from `Config::api_token()` (file
    /// `api_token_file`). When `expected_token` is `None`/empty the server
    /// runs unauthenticated (local dev). On success the returned `session_id`
    /// binds all subsequent requests (`KeepAlive`, `ReconcileState`, ...).
    /// If `reconnect_session_id` is non-empty and a session with that id
    /// already exists, the call reuses it (after token validation) and
    /// refreshes its lease — enabling reconnect with the previous `session_id`.
    /// A reconnect must also present the same `client` identity the session was
    /// attached with (see [`ClientIdentity`]).
    pub async fn attach(
        &self,
        token: &str,
        policy: Option<worker::KillSwitchPolicy>,
        client: &ClientIdentity,
    ) -> Result<(String, u32), ManagerError> {
        self.attach_with_reconnect(token, policy, "", client).await
    }

    /// Same as [`Self::attach`] but supports reconnect via `reconnect_session_id`.
    ///
    /// The reuse branch additionally checks the client identity, so holding a
    /// live `session_id` is not enough to take a session over: it has to be the
    /// client that attached it.
    pub async fn attach_with_reconnect(
        &self,
        token: &str,
        policy: Option<worker::KillSwitchPolicy>,
        reconnect_session_id: &str,
        client: &ClientIdentity,
    ) -> Result<(String, u32), ManagerError> {
        if let Some(expected) = &self.expected_token {
            let ok =
                !expected.is_empty() && constant_time_eq(expected.as_bytes(), token.as_bytes());
            if !ok {
                return Err(ManagerError::Unauthenticated);
            }
        }
        // Reconnect path: reuse existing session if the caller supplied a
        // previously issued session_id and it still exists and is not terminal.
        #[allow(clippy::collapsible_if)]
        if !reconnect_session_id.is_empty() {
            if let Some(handle) = self.sessions.lock().await.get(reconnect_session_id).cloned() {
                match handle.state() {
                    Some(SessionState::KillSwitchTripped | SessionState::GracefulShutdown) => {
                        // Terminal sessions are not resurrectable; the caller must attach
                        // fresh. Silently falling through here would leak the old session.
                        return Err(ManagerError::InvalidArgument(format!(
                            "session {reconnect_session_id} is terminal; attach a new session"
                        )));
                    }
                    Some(SessionState::Attached | SessionState::Syncing | SessionState::Active) => {
                        // Identity before anything else the reconnect mutates: a refused
                        // reconnect must not have refreshed the lease.
                        handle.assert_reconnect_client(client)?;
                        handle.touch().await;
                        if let Some(policy) = policy.clone() {
                            let mut new_policy = handle.policy().await;
                            apply_policy(&mut new_policy, &policy)?;
                            handle.set_policy(new_policy).await;
                        }
                        return Ok((reconnect_session_id.to_string(), handle.heartbeat_interval_ms));
                    }
                    None => {
                        // Corrupt session state byte; refuse rather than guess.
                        return Err(ManagerError::InvalidArgument(format!(
                            "session {reconnect_session_id} has an invalid state"
                        )));
                    }
                }
            }
        }
        let id = ulid::Ulid::generate().to_string();
        let mut session_policy = SessionPolicy::default();
        if let Some(policy) = policy.as_ref() {
            apply_policy(&mut session_policy, policy)?;
        }
        let (events_tx, _) = broadcast::channel(256);
        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            state: AtomicU8::new(SessionState::Attached as u8),
            policy: Mutex::new(session_policy),
            last_seen: Mutex::new(tokio::time::Instant::now()),
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_MS,
            client: client.clone(),
            events_tx,
            ring: Mutex::new(VecDeque::new()),
            tracked_coids: Mutex::new(HashSet::new()),
            strategy: Mutex::new(None),
            orders_submitted: AtomicU64::new(0),
            log_events: AtomicU64::new(0),
            seq: AtomicU64::new(0),
            snapshot_seq: AtomicU64::new(0),
        });
        self.sessions.lock().await.insert(id.clone(), Arc::clone(&handle));
        self.ensure_watchdog().await;
        Ok((id, DEFAULT_HEARTBEAT_MS))
    }

    pub async fn get(&self, session_id: &str) -> Result<Arc<SessionHandle>, ManagerError> {
        self.sessions
            .lock()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| ManagerError::NotFound(session_id.to_string()))
    }

    /// Transition ATTACHED → SYNCING before the snapshot fetch.
    /// Idempotent: an already-SYNCING or already-ACTIVE session is left alone
    /// so concurrent reconciles never regress ACTIVE back to SYNCING.
    /// Terminal sessions are never resurrected.
    pub async fn begin_reconcile(&self, session_id: &str) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        match handle.state() {
            Some(SessionState::Attached) => {
                if let Some(prev) = handle.transition(SessionState::Attached, SessionState::Syncing)
                {
                    self.publish_state_change(
                        &handle,
                        prev,
                        SessionState::Syncing,
                        "reconcile started",
                    )
                    .await;
                }
                Ok(())
            }
            Some(SessionState::Syncing | SessionState::Active) => Ok(()),
            Some(terminal) => Err(ManagerError::InvalidArgument(format!(
                "session {session_id} is {terminal:?}; cannot begin reconcile from a terminal state"
            ))),
            None => Err(ManagerError::InvalidArgument(format!(
                "session {session_id} has an invalid state"
            ))),
        }
    }

    /// Complete reconciliation: transition SYNCING → ACTIVE. Orders submitted before
    /// this point would be rejected by session-scoped trading paths.
    /// `snapshot_sequence` is the watermark at snapshot time (§7) — buffered
    /// deltas with `header.sequence <= snapshot_sequence` must be discarded,
    /// remainder applied in order before trading. Idempotent: repeating with the
    /// same (or newer) watermark keeps ACTIVE without a second publish.
    pub async fn complete_reconcile(
        &self,
        session_id: &str,
        snapshot_sequence: u64,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        // Watermarks are monotonic: a stale retry must not move the watermark
        // backwards under a concurrent newer reconcile.
        handle.snapshot_seq.fetch_max(snapshot_sequence, Ordering::Release);
        match handle.state() {
            Some(SessionState::Syncing) => {
                if let Some(prev) = handle.transition(SessionState::Syncing, SessionState::Active) {
                    self.publish_state_change(
                        &handle,
                        prev,
                        SessionState::Active,
                        "reconcile complete",
                    )
                    .await;
                }
                Ok(())
            }
            Some(SessionState::Active) => Ok(()),
            Some(other) => Err(ManagerError::InvalidArgument(format!(
                "session {session_id} is {other:?}; complete_reconcile requires SYNCING"
            ))),
            None => Err(ManagerError::InvalidArgument(format!(
                "session {session_id} has an invalid state"
            ))),
        }
    }

    /// Current snapshot watermark for this session (0 if never reconciled).
    pub async fn snapshot_sequence(&self, session_id: &str) -> Result<u64, ManagerError> {
        let handle = self.get(session_id).await?;
        Ok(handle.snapshot_seq.load(Ordering::Acquire))
    }

    /// Stop a session: optionally cancel its tracked orders, then transition
    /// to `GRACEFUL_SHUTDOWN`. Returns the state actually reached.
    ///
    /// Idempotent: a session that already reached a terminal state is returned as
    /// it is. Re-running the cancels would address orders the kill switch (or a
    /// previous stop) already unwound, and publishing
    /// `GRACEFUL_SHUTDOWN -> GRACEFUL_SHUTDOWN` would make a consumer that
    /// treats every state change as an action act twice.
    ///
    /// The state transition is unconditional — a refused cancel never leaves a
    /// session trading — but a refused cancel is *not* hidden: the caller gets
    /// [`ManagerError::CancelsFailed`] carrying the state that was reached, so
    /// `StopStrategy` cannot answer `final_state: GRACEFUL_SHUTDOWN` while
    /// orders are still resting.
    ///
    /// Shared by the `StopStrategy` RPC and by embedders; the previous
    /// inline copy in the service layer is replaced by this.
    pub async fn stop_session(
        &self,
        session_id: &str,
        cancel_open_orders: bool,
    ) -> Result<Option<SessionState>, ManagerError> {
        let handle = self.get(session_id).await?;
        let state = handle.state();
        if state.is_some_and(SessionState::is_terminal) {
            return Ok(state);
        }
        let failures =
            if cancel_open_orders { self.cancel_session_orders(&handle).await } else { Vec::new() };
        // Terminal states are never overwritten: a kill-switch that won the race
        // keeps its state and its single publish. Otherwise CAS from the observed
        // state so a concurrent begin/complete/trip cannot be silently clobbered.
        let prev = handle.state().ok_or_else(|| {
            ManagerError::InvalidArgument(format!("session {session_id} has an invalid state"))
        })?;
        if prev.is_terminal() {
            return Ok(Some(prev));
        }
        let Some(prev) = handle.transition(prev, SessionState::GracefulShutdown) else {
            // Lost a concurrent transition (e.g. kill-switch tripped first):
            // the winner's terminal state stands, no second publish.
            return Ok(handle.state());
        };
        self.publish_state_change(&handle, prev, SessionState::GracefulShutdown, "stop requested")
            .await;
        if !failures.is_empty() {
            return Err(ManagerError::CancelsFailed {
                session_id: session_id.to_string(),
                state: handle.state(),
                failures,
            });
        }
        Ok(handle.state())
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}
