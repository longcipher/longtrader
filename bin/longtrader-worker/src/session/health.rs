//! Part of the session control plane; see [`super::SessionManager`].

use super::{
    Arc, Duration, HEALTH_PROBE_FAILURE_THRESHOLD, HEALTH_PROBE_TIMEOUT, ManagerError, Ordering,
    SessionHandle, SessionManager, SessionState, TERMINAL_SESSION_RETENTION, Weak,
};
use crate::proto::{trading, worker};

impl SessionManager {
    /// Whether the watchdog should renew venue COD for this session on this
    /// tick: the session must be live and the venue must actually support COD.
    /// Anything else falls back to the L1 (lease) and L2 (gateway probe)
    /// tiers. Pure predicate (no I/O) so the gating rule is unit-testable;
    /// see `cod_renewal_is_gated_on_active_sessions_with_native_support`.
    pub(crate) fn should_renew_cod(
        state: Option<SessionState>,
        cod: Option<&Arc<dyn CodProvider>>,
    ) -> bool {
        state == Some(SessionState::Active) && cod.is_some_and(|c| c.supports_cod())
    }

    async fn gateway_health_check(&self) -> Result<(), crate::ports::PortError> {
        // L2 analogue: lightweight probe via `sync_state` with short timeout.
        // Propagate timeout / transport errors so watchdog can trigger L2 logic.
        let res = tokio::time::timeout(
            HEALTH_PROBE_TIMEOUT,
            self.gateway.sync_state(&self.default_exchange),
        )
        .await
        .map_err(|_| {
            crate::ports::PortError::Transport("gateway health check timeout".to_string())
        })?;
        res?;
        Ok(())
    }

    /// Fold one L2 probe outcome into the consecutive-failure counter and return
    /// the number of consecutive misses it makes (0 when the probe succeeded).
    ///
    /// Consecutive, not per-tick. A single miss is not evidence that the venue is
    /// down: the probe is a wall-clock budget over `sync_state`, whose own
    /// contract says its reads are not atomic, so on a saturated host a
    /// slow-but-healthy backend misses it — and the consequence of believing that
    /// is a host-wide mass cancel of every ACTIVE session. Two back-to-back
    /// misses is the first real evidence of a partition, and the gap is still
    /// covered by L1 (per-session lease) and L3 (venue cancel-on-disconnect).
    pub(crate) fn note_health_probe(&self, probe: &Result<(), crate::ports::PortError>) -> u32 {
        if probe.is_ok() {
            self.health_failures.store(0, Ordering::Release);
            return 0;
        }
        // `try_update` performs a single CAS: on contention it reports `Err`
        // instead of retrying, so loop rather than map the miss onto a
        // fabricated full-miss count. Contention needs a second concurrent
        // writer (the watchdog tick is the only production caller), but a
        // spurious `u32::MAX` would trip L2 immediately rather than after two
        // consecutive misses.
        loop {
            if self.health_failures.load(Ordering::Acquire) == u32::MAX {
                // Saturated: a very long outage keeps reporting "at threshold"
                // instead of wrapping back to 1.
                return u32::MAX;
            }
            if let Ok(prev) =
                self.health_failures
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(1))
            {
                return prev.saturating_add(1);
            }
        }
    }

    pub async fn check_lease_expiry(&self, session_id: &str) -> Result<bool, ManagerError> {
        let handle = self.get(session_id).await?;
        let policy = handle.policy().await;
        let expired = handle.state() == Some(SessionState::Active) &&
            u128::from(handle.last_seen_elapsed_ms().await) > policy.lease_timeout.as_millis();
        if expired {
            self.trip_kill_switch(&handle, policy.lease_timeout).await;
        }
        Ok(expired)
    }

    /// Spawn the lease watchdog once; subsequent calls are no-ops.
    ///
    /// Only the `Weak` is handed to the task. Upgrading it into a strong `Arc`
    /// that the loop holds would make the manager immortal — its session map, its
    /// adapter and its sockets could never be dropped, so a worker that stopped
    /// accepting connections would still keep every one of them alive.
    ///
    /// The started-flag is claimed *after* the `Weak` is known to exist: claiming
    /// it first turned a missing `install_self()` into one warning followed by
    /// permanent silence — every later attach a no-op, leases never enforced,
    /// nothing to indicate why.
    pub(crate) async fn ensure_watchdog(&self) {
        if self.watchdog_started.load(Ordering::Acquire) {
            return;
        }
        let Some(weak) = self.self_weak.lock().await.clone() else {
            tracing::warn!(
                "SessionManager::ensure_watchdog without install_self: leases will never expire; \
                 call install_self() after SessionManager::new()"
            );
            return;
        };
        if self.watchdog_started.swap(true, Ordering::AcqRel) {
            // Lost the race to a concurrent caller; its loop is the one running.
            return;
        }
        tokio::spawn(watchdog_loop(weak));
    }

    /// Lease expiry tripwire for one session: flip state and execute the
    /// configured kill-switch scope.
    ///
    /// Idempotent and terminal-safe: an already-terminal session is left alone
    /// (no second publish), and only ACTIVE sessions trip — Attached/Syncing
    /// sessions have no live orders to unwind.
    ///
    /// Returns nothing on purpose: this runs on the watchdog's own path, which
    /// must not abort on a venue that refuses a cancel, and the scope's outcome
    /// is logged (`error` for a refused cancel, because the order may still be
    /// resting) rather than propagated.
    async fn trip_kill_switch(&self, handle: &SessionHandle, timeout: Duration) {
        // Only an ACTIVE session trips: it is the only state with live orders
        // to unwind, and an already-terminal session must not publish a second
        // transition. Attaching, syncing and corrupt states all fall through
        // to the same silent return — except a corrupt state byte, which is
        // logged and refused rather than guessed at. A single `if` (not one
        // arm per state) keeps every non-ACTIVE state on the identical path.
        match handle.state() {
            Some(SessionState::Active) => {}
            state => {
                if state.is_none() {
                    tracing::error!(session = %handle.id, "corrupt session state; refusing kill-switch");
                }
                return;
            }
        }
        let Some(prev) = handle.transition(SessionState::Active, SessionState::KillSwitchTripped)
        else {
            // Lost a concurrent transition (stop won the race): the winner's
            // terminal state stands, no second publish.
            return;
        };
        handle.mark_terminal().await;
        tracing::error!(
            session = %handle.id,
            lease_timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            "session lease expired; tripping kill-switch"
        );
        self.publish_state_change(handle, prev, SessionState::KillSwitchTripped, "lease expired")
            .await;
        let policy = handle.policy().await;
        match policy.scope {
            worker::kill_switch_policy::Scope::SessionOrders => {
                let failures = self.cancel_session_orders(handle).await;
                if !failures.is_empty() {
                    tracing::error!(
                        session = %handle.id,
                        failures = ?failures,
                        "kill-switch scope SESSION_ORDERS left orders resting on the venue"
                    );
                }
            }
            worker::kill_switch_policy::Scope::AllOrders => {
                let req = trading::CancelAllOrdersRequest {
                    exchange_id: buffa::MessageField::some(self.default_exchange.clone()),
                    // No symbol filter: a kill-switch cancel-all is account-wide
                    // by definition. The field defaults to empty; spelling it
                    // out would only invite a filter that must never be set.
                    ..Default::default()
                };
                if let Err(err) = self.gateway.cancel_all_orders(req).await {
                    tracing::error!(
                        session = %handle.id,
                        error = %err,
                        "kill-switch cancel-all failed"
                    );
                }
            }
            _ => {
                tracing::warn!(session = %handle.id, "kill-switch scope NONE; no cancellations");
            }
        }
    }
}

/// Venue-native Cancel-On-Disconnect provider (L3). Implemented by venues
/// that support `countdownCancelAll` / `cancel-on-disconnect`. When present,
/// the watchdog renews the COD countdown each tick while healthy; on silence
/// the venue auto-cancels. Capability-gated — venues without COD fall back to
/// L1+L2 only.
#[async_trait::async_trait]
pub trait CodProvider: Send + Sync {
    async fn keepalive(&self, session_id: &str) -> Result<(), crate::ports::PortError>;
    fn supports_cod(&self) -> bool {
        true
    }
}

/// Honest no-op COD provider used when no venue-native cancel-on-disconnect is
/// configured. `supports_cod` returns `false` so the watchdog never relies on
/// it; venues that expose COD inject a real provider via
/// [`SessionManager::with_cod_provider`].
pub struct NullCodProvider;

#[async_trait::async_trait]
impl CodProvider for NullCodProvider {
    async fn keepalive(&self, _session_id: &str) -> Result<(), crate::ports::PortError> {
        Ok(())
    }
    fn supports_cod(&self) -> bool {
        false
    }
}

#[allow(clippy::cognitive_complexity, clippy::collapsible_if)]
async fn watchdog_loop(weak: Weak<SessionManager>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    // A mass-cancel storm (L1 tripping many sessions, each awaiting bounded
    // unary timeouts) may overrun the 1s budget: skip missed ticks rather than
    // bursting to catch up, so one slow pass cannot cascade into a hot loop.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        // Upgraded per pass and dropped at the end of it, so the task itself never
        // keeps the manager alive; a dropped manager ends the loop on the next
        // pass instead of spinning against a registry nobody can reach.
        let Some(manager) = weak.upgrade() else {
            return;
        };
        // Retire terminal sessions past their retention window first: it keeps
        // the registry — and everything that scans it below — bounded, and it
        // is the exit condition for strategy-side lease guards.
        manager.retire_terminal_sessions(TERMINAL_SESSION_RETENTION).await;
        // Snapshot session handles to avoid holding the lock during processing.
        let handles: Vec<Arc<SessionHandle>> = {
            let sessions = manager.sessions.lock().await;
            sessions.values().map(Arc::clone).collect()
        };
        // Process each session's L1 lease check in parallel with bounded concurrency.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        let mut futures = Vec::with_capacity(handles.len());
        for handle in handles {
            let manager = Arc::clone(&manager);
            let sem = Arc::clone(&semaphore);
            futures.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                // L3: renew venue COD while session is Active (best-effort, capability-gated).
                if SessionManager::should_renew_cod(handle.state(), manager.cod_provider.as_ref())
                    && let Some(cod) = manager.cod_provider.as_ref()
                    && let Err(e) = cod.keepalive(&handle.id).await
                {
                    tracing::debug!(session = %handle.id, error = %e, "COD keepalive failed — will rely on L1/L2");
                }
                // `check_lease_expiry` owns the expiry predicate so the loop and
                // the ops-facing check can never disagree about what "expired"
                // means; the loop only observes the outcome.
                match manager.check_lease_expiry(&handle.id).await {
                    Ok(true) => {
                        tracing::warn!(
                            session = %handle.id,
                            "L1 lease expired — kill-switch executed (L2/L3 as fallback)"
                        );
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(
                            session = %handle.id,
                            error = %error,
                            "L1 lease check failed; leaving the session alone"
                        );
                    }
                }
            }));
        }
        // Wait for all L1 checks to complete before L2.
        for f in futures {
            let _ = f.await;
        }
        // L2: detect gateway gRPC loss via health probe (daemon watchdog analogue).
        // On failure, proactively trip every Active session so stale orders cannot
        // survive a partition — the final backstop beyond the L1 lease. Only after
        // `HEALTH_PROBE_FAILURE_THRESHOLD` *consecutive* misses, because a single
        // probe timeout is not evidence the venue is down.
        let probe = manager.gateway_health_check().await;
        let misses = manager.note_health_probe(&probe);
        // `misses` only reaches the threshold through failures, so a healthy probe
        // can never trip.
        let tripped = misses >= HEALTH_PROBE_FAILURE_THRESHOLD;
        if let Err(error) = &probe {
            if tripped {
                tracing::error!(
                    error = %error,
                    consecutive_misses = misses,
                    "L2 daemon watchdog tripping all active sessions"
                );
            } else {
                tracing::warn!(
                    error = %error,
                    consecutive_misses = misses,
                    "gateway health probe failed; below the L2 trip threshold"
                );
            }
        }
        if tripped {
            let active: Vec<Arc<SessionHandle>> = {
                let sessions = manager.sessions.lock().await;
                sessions
                    .values()
                    .filter(|h| h.state() == Some(SessionState::Active))
                    .cloned()
                    .collect()
            };
            // Mass-cancel is concurrent and per-cancel bounded (see
            // `CANCEL_ROUND_TRIP_TIMEOUT`): a sequential loop awaiting one
            // unresponsive venue cancel per session could stall this tick —
            // and with it lease enforcement for every session — indefinitely.
            let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
            let mut jobs = Vec::with_capacity(active.len());
            for handle in active {
                let manager = Arc::clone(&manager);
                let sem = Arc::clone(&semaphore);
                jobs.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let lease_timeout = handle.policy().await.lease_timeout;
                    manager.trip_kill_switch(&handle, lease_timeout).await;
                }));
            }
            for job in jobs {
                let _ = job.await;
            }
        }
    }
}

/// Strategy-side lease timeout simulation (design doc §6.6 multi-level watchdog).
///
/// External strategies SHOULD spawn this guard after `AttachSession`. It wakes
/// every `heartbeat_interval` (the negotiated `heartbeat_interval_ms`) and
/// checks whether the local lease has timed out since the last successful
/// `KeepAlive`. On timeout it logs and asks the manager to cancel via the
/// configured `KillSwitchPolicy` scope — a stub that mirrors what a real
/// sidecar SDK does over Connect RPC.
///
/// The decision uses the session's own policy lease, the same number
/// `check_lease_expiry` uses. There is deliberately no caller-supplied budget to
/// override it with: when the guard was handed one, it decided on *its* value
/// and tripped on the session's, so a guard configured with a longer budget than
/// the session held never fired at all.
///
/// Ponytail stub: uses the manager's `check_lease_expiry` internally so the
/// same scope-routing logic (SESSION_ORDERS / ALL_ORDERS / NONE) is exercised
/// without duplicating cancel code. Spawn via `tokio::spawn`.
pub fn spawn_strategy_lease_guard(
    manager: Arc<SessionManager>,
    session_id: String,
    heartbeat_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(heartbeat_interval);
        // Strategy-side watchdog: a timer that checks the lease every
        // `heartbeat_interval` and trips the cancel path on timeout.
        //
        // The predicate is not re-implemented here: this loop asks
        // `check_lease_expiry` — the single owner of "expired" — every tick and
        // only reports/acts on its verdict. A local `elapsed > lease_timeout`
        // recomputation (what this used to do) was a second definition of the
        // same rule that could drift from the one the watchdog enforces.
        loop {
            tick.tick().await;
            match manager.check_lease_expiry(&session_id).await {
                Ok(true) => {
                    tracing::warn!(
                        session = %session_id,
                        "strategy-side lease timeout detected; kill-switch executed"
                    );
                    break;
                }
                Ok(false) => {}
                // Session gone (retired or never existed): nothing left to watch.
                Err(_) => break,
            }
        }
    })
}
