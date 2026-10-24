//! Session control plane: deterministic recovery state machine, lease
//! watchdog with kill-switch execution, and the strategy event bus.
//!
//! Lifecycle (server-enforced, design doc §6.3):
//! ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED
//! `ATTACHED → SYNCING → ACTIVE`, with `KILL_SWITCH_TRIPPED` on lease expiry
//! and `GRACEFUL_SHUTDOWN` on explicit stop. Orders before ACTIVE are
//! rejected `SYNC_IN_PROGRESS`.
//!
//! ReconcileState is a best-effort snapshot stamped with `snapshot_sequence` /
//! `snapshot_time` (balances/positions/open_orders near one watermark). The
//! open backend has no server-side atomic snapshot yet, so the three reads can
//! tear across millis (see `RemoteAdapter::sync_state`); buffered deltas after
//! `snapshot_sequence` must be replayed to converge.
//!
//! Lease: `lease_timeout` defaults to `3x heartbeat` interval
//! (`LEASE_HEARTBEAT_BUDGET = 3`). Missed-heartbeat budget exhausted while
//! in ACTIVE trips the configured `KillSwitchPolicy`.
//!
//! KillSwitchPolicy Scope routing:
//! - `SESSION_ORDERS` (SCOPE_SESSION_ORDERS) cancels only orders submitted by this session (tracked
//!   `client_order_id`s);
//! - `ALL_ORDERS` (SCOPE_ALL_ORDERS) cancels every open order of the bound account(s) via
//!   `CancelAllOrders`;
//! - `NONE` (SCOPE_NONE) logs only, no cancellations.
//!
//! Multi-level watchdog: session / daemon / exchange (design doc §6.6):
//! - L_session (worker lease): this module’s `watchdog_loop` + `check_lease_expiry` — 3x heartbeat
//!   budget.
//! - L_daemon (exchange daemon): worker gRPC loss → daemon cancels that worker’s sessions. The
//!   probe must miss `HEALTH_PROBE_FAILURE_THRESHOLD` times *in a row* before L2 trips anything:
//!   one slow-but-healthy backend is not a partition (see `note_health_probe`).
//! - L_exchange (venue native COD): venue Cancel-All-After / cancel-on-disconnect countdown
//!   extended while daemon healthy; on silence the venue cancels automatically. Capability-gated;
//!   venues without native COD fall back to daemon level only.
//!
//! Backpressure isolation: each session owns an independent `mpsc` channel
//! per logical stream with a stream-appropriate `OverflowPolicy`:
//! ticker → `DropOldest`, orderbook → `Coalesce`, orders/balances/positions
//! → `Block` (see `crate::ports::OVERFLOW_*` and `crate::overflow::policy_channel`).
//!
//! Strategy-side lease simulation: external strategies SHOULD spawn a timer
//! task at `heartbeat_interval` that checks elapsed time since last
//! successful `KeepAlive`; on lease timeout they locally cancel and stop
//! trading (see `spawn_strategy_lease_guard` helper below).

pub mod events;
pub mod gate;
pub mod health;
pub mod lease;
pub mod lifecycle;
pub mod proxy;
pub mod server;
pub mod service;
pub mod state;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

pub use health::{CodProvider, NullCodProvider, spawn_strategy_lease_guard};
use lease::{default_lease, lease_timeout_from_proto};
pub use state::SessionState;
use tokio::sync::{Mutex, broadcast};

use crate::{
    ports::{
        FundingRateSource, MarketDataSource, TradingGateway, TriggerOrderGateway, VenueOpInvoker,
        WalletGateway,
    },
    proto::{common, worker},
};

/// Default heartbeat interval negotiated at attach.
pub const DEFAULT_HEARTBEAT_MS: u32 = 10_000;
/// Replay ring capacity per session.
const EVENT_RING_CAPACITY: usize = 1024;
/// Wall-clock budget for one L2 gateway-health probe.
///
/// `sync_state` is the lightest round trip available, but its own contract says
/// its reads are not atomic, so a busy venue can legitimately exceed this. The
/// budget is there to notice a *dead* daemon, not a slow one.
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Consecutive L2 probe failures required before every ACTIVE session is tripped.
///
/// Two, not one: see [`SessionManager::note_health_probe`].
const HEALTH_PROBE_FAILURE_THRESHOLD: u32 = 2;

/// Kill-switch configuration for one session.
#[derive(Debug, Clone)]
pub struct SessionPolicy {
    pub lease_timeout: Duration,
    pub scope: worker::kill_switch_policy::Scope,
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            lease_timeout: default_lease(u64::from(DEFAULT_HEARTBEAT_MS)),
            scope: worker::kill_switch_policy::Scope::SessionOrders,
        }
    }
}

/// Apply a proto policy onto a session policy; single owner for lease/scope parsing.
///
/// The one place a `worker.v1.KillSwitchPolicy` is interpreted: `AttachSession`
/// (fresh + reconnect) and `SetKillSwitchPolicy` all route through here, so they
/// cannot drift on what `SCOPE_UNSPECIFIED` means or on which leases are
/// honourable. A rejected policy is rejected whole — nothing is applied
/// partially, or the caller would believe in a lease the worker is not enforcing.
fn apply_policy(
    current: &mut SessionPolicy,
    policy: &worker::KillSwitchPolicy,
) -> Result<(), ManagerError> {
    if let Some(timeout) = policy.lease_timeout.as_option() {
        current.lease_timeout = lease_timeout_from_proto(timeout)
            .map_err(|e| ManagerError::InvalidArgument(e.to_string()))?;
    }
    match policy.scope {
        buffa::EnumValue::Known(s) => {
            if s == worker::kill_switch_policy::Scope::Unspecified {
                // Explicit Unspecified means "leave unchanged".
            } else {
                current.scope = s;
            }
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(ManagerError::InvalidArgument(format!(
                "unknown KillSwitchPolicy scope discriminant {v}"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct StrategyInfo {
    pub id: String,
    pub name: String,
    pub started_at_ms: i64,
    pub orders_submitted: u64,
    pub log_events: u64,
}

/// The client identity presented at `AttachSession`.
///
/// Recorded with the session so a later reconnect has to present the same one:
/// without that comparison the issued `session_id` is a bearer token, and anyone
/// holding it (plus the worker token) can adopt another principal's session, its
/// lease, and the order ids its kill-switch cancels.
///
/// A half that is *absent* is not a mismatch. Clients that predate the field
/// send nothing, and refusing those would break reconnect for every one of them;
/// only two definite halves that differ are refused (see
/// [`SessionHandle::assert_reconnect_client`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientIdentity {
    /// Free-form client name (e.g. `python-sdk`). Empty when not supplied.
    pub name: String,
    /// Client version string. Empty when not supplied.
    pub version: String,
}

/// One live session: state, lease bookkeeping, kill-switch policy, tracked
/// orders, and the replayable event log.
#[derive(Debug)]
pub struct SessionHandle {
    pub id: String,
    state: AtomicU8,
    policy: Mutex<SessionPolicy>,
    last_seen: Mutex<tokio::time::Instant>,
    pub heartbeat_interval_ms: u32,
    /// Client that attached; see [`ClientIdentity`].
    client: ClientIdentity,
    events_tx: broadcast::Sender<Arc<worker::StrategyEvent>>,
    ring: Mutex<VecDeque<Arc<worker::StrategyEvent>>>,
    tracked_coids: Mutex<HashSet<String>>,
    strategy: Mutex<Option<StrategyInfo>>,
    /// Session-scoped order submissions, counted independently of whether a
    /// strategy was registered so attribution survives `RegisterStrategy`
    /// ordering. Surfaced through `StrategyStatus.orders_submitted`.
    orders_submitted: AtomicU64,
    /// Session-scoped log events, counted the same way for the same reason: a
    /// log reported before `RegisterStrategy` still reaches the ring, so the
    /// count must not start at the registration boundary.
    log_events: AtomicU64,
    seq: AtomicU64,
    /// Last reconciled snapshot watermark (`snapshot_sequence`) for recovery.
    snapshot_seq: AtomicU64,
}

impl SessionHandle {
    pub fn state(&self) -> Option<SessionState> {
        SessionState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// CAS transition `expected -> next`. Returns the previous state on
    /// success, `None` when the current state is corrupt or is not `expected`.
    /// Concurrent `begin/complete/trip` calls can no longer silently overwrite
    /// each other.
    fn transition(&self, expected: SessionState, next: SessionState) -> Option<SessionState> {
        self.state
            .compare_exchange(expected as u8, next as u8, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .and_then(SessionState::from_u8)
    }

    pub async fn touch(&self) {
        *self.last_seen.lock().await = tokio::time::Instant::now();
    }

    /// Milliseconds elapsed since the last heartbeat (pause-aware).
    async fn last_seen_elapsed_ms(&self) -> u64 {
        self.last_seen.lock().await.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
    }

    pub async fn policy(&self) -> SessionPolicy {
        self.policy.lock().await.clone()
    }

    pub async fn set_policy(&self, policy: SessionPolicy) {
        *self.policy.lock().await = policy;
    }

    /// The identity recorded when this session was attached, as
    /// `(name, version)`.
    #[must_use]
    pub fn client_identity(&self) -> (&str, &str) {
        (&self.client.name, &self.client.version)
    }

    /// Refuse a reconnect whose client identity contradicts the one recorded at
    /// attach.
    ///
    /// Only a definite mismatch is refused. An absent half on either side is
    /// accepted: not every client sends one, and a reconnect that sent nothing
    /// is not evidence of a different principal.
    fn assert_reconnect_client(&self, client: &ClientIdentity) -> Result<(), ManagerError> {
        for (recorded, offered, field) in [
            (&self.client.name, &client.name, "client_name"),
            (&self.client.version, &client.version, "client_version"),
        ] {
            if !recorded.is_empty() && !offered.is_empty() && recorded != offered {
                return Err(ManagerError::InvalidArgument(format!(
                    "session {} was attached by {field} {recorded:?}; {offered:?} may not \
                     reconnect to it",
                    self.id
                )));
            }
        }
        Ok(())
    }

    pub async fn strategy_info(&self) -> Option<StrategyInfo> {
        let mut info = self.strategy.lock().await.clone()?;
        // The handle-level counters are authoritative: both are incremented on
        // the reporting path itself, so neither can be lost to a late
        // `RegisterStrategy` or a re-registration.
        info.orders_submitted = self.orders_submitted.load(Ordering::Acquire);
        info.log_events = self.log_events.load(Ordering::Acquire);
        Some(info)
    }

    async fn set_strategy(&self, info: StrategyInfo) {
        *self.strategy.lock().await = Some(info);
    }

    /// Append an event to the replay ring and fan it out to subscribers.
    /// Sequence is monotonically increasing per subscription (§7 gap-free);
    /// gap detection via `crate::ports::is_sequence_gap(prev, next)` — gaps
    /// trigger resync (re-fetch snapshot or full `ReconcileState`).
    async fn publish(&self, mut event: worker::StrategyEvent) -> Arc<worker::StrategyEvent> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut header = event.header.as_option().cloned().unwrap_or_default();
        header.sequence = seq;
        event.header = buffa::MessageField::some(header);
        event.resume_token = seq.to_string();
        let shared = Arc::new(event);
        {
            let mut ring = self.ring.lock().await;
            if ring.len() >= EVENT_RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(Arc::clone(&shared));
        }
        let _ = self.events_tx.send(Arc::clone(&shared));
        shared
    }

    /// Replay events strictly after `after_seq` from the ring buffer.
    /// Used for `resume_token` breakpoint resume (`StreamStrategyEventsRequest.resume_token`).
    /// Caller should check `is_sequence_gap` on the first replayed event vs
    /// its last seen sequence — any gap means the ring overflowed and a full
    /// `ReconcileState` is required instead of guessing.
    pub async fn replay_after(&self, after_seq: u64) -> Vec<Arc<worker::StrategyEvent>> {
        self.ring.lock().await.iter().filter(|e| e.header.sequence > after_seq).cloned().collect()
    }

    /// Check whether `next_seq` follows `prev_seq` without a gap; gap triggers resync.
    #[inline]
    pub fn is_gap(prev_seq: u64, next_seq: u64) -> bool {
        crate::ports::is_sequence_gap(prev_seq, next_seq)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<worker::StrategyEvent>> {
        self.events_tx.subscribe()
    }

    pub async fn track_order(&self, coid: String) {
        self.tracked_coids.lock().await.insert(coid);
    }

    pub async fn tracked_orders(&self) -> HashSet<String> {
        self.tracked_coids.lock().await.clone()
    }

    /// Session-scoped orders submitted through the gate, counted regardless of
    /// whether `RegisterStrategy` ran first.
    pub fn orders_submitted_count(&self) -> u64 {
        self.orders_submitted.load(Ordering::Acquire)
    }

    /// Log events accepted for this session, counted regardless of whether
    /// `RegisterStrategy` ran first.
    pub fn log_events_count(&self) -> u64 {
        self.log_events.load(Ordering::Acquire)
    }

    pub async fn record_order_submitted(&self) {
        self.orders_submitted.fetch_add(1, Ordering::Relaxed);
    }
}

/// Errors surfaced by the session manager; mapped onto Connect codes by the
/// service layer.
#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("authentication failed")]
    Unauthenticated,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A session-scoped call was made from a state that forbids it. Orders
    /// submitted before the session reaches ACTIVE carry the stable machine
    /// reason `SYNC_IN_PROGRESS` mandated by `worker.v1`.
    #[error("SYNC_IN_PROGRESS: session {session_id} is {state:?}, not ACTIVE")]
    SyncInProgress { session_id: String, state: Option<SessionState> },
    /// The session reached `state`, but one or more cancels the caller asked for
    /// were refused by the venue.
    ///
    /// Answering `StopStrategy` with a plain `GRACEFUL_SHUTDOWN` here would tell
    /// the client its orders are unwound when some of them may still be resting,
    /// so the state is carried alongside the per-order detail instead. The
    /// transition itself always happened: this reports an incomplete unwind, not
    /// a session that is still live.
    #[error("session {session_id}: {} cancel(s) failed on {state:?}: {failures:?}", failures.len())]
    CancelsFailed {
        session_id: String,
        state: Option<SessionState>,
        /// One `"<order id>: <error>"` entry per order the venue refused.
        failures: Vec<String>,
    },
    #[error("port error: {0}")]
    Port(#[from] crate::ports::PortError),
}

/// Central registry and lease-watchdog owner for all sessions.
pub struct SessionManager {
    expected_token: Option<String>,
    default_exchange: common::ExchangeId,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    sessions: Mutex<HashMap<String, Arc<SessionHandle>>>,
    watchdog_started: AtomicBool,
    /// Consecutive failed L2 health probes; reset by the first success. See
    /// [`Self::note_health_probe`].
    health_failures: AtomicU32,
    self_weak: Mutex<Option<Weak<Self>>>,
    /// Optional L3 COD provider (venue native cancel-on-disconnect).
    pub cod_provider: Option<Arc<dyn CodProvider>>,
    /// Optional venue capabilities (funding / triggers / wallet / ops).
    capabilities: Capabilities,
}

/// Optional venue capabilities a backend may expose.
///
/// Each field is an `Option` on purpose: a backend that genuinely lacks a
/// capability must answer `unimplemented` — a clear, non-retryable signal —
/// rather than an empty success that a strategy would read as "none exist".
#[derive(Clone, Default)]
pub struct Capabilities {
    /// Perpetual funding rates.
    pub funding: Option<Arc<dyn FundingRateSource>>,
    /// Venue-side conditional orders (crash-safe stop backstops).
    pub triggers: Option<Arc<dyn TriggerOrderGateway>>,
    /// Wallet ledger and internal transfers.
    pub wallet: Option<Arc<dyn WalletGateway>>,
    /// Self-describing venue-specific operations.
    pub ops: Option<Arc<dyn VenueOpInvoker>>,
}

impl Capabilities {
    /// Build a complete set from a single backend that implements all four.
    #[must_use]
    pub fn all<T>(backend: Arc<T>) -> Self
    where
        T: FundingRateSource
            + TriggerOrderGateway
            + WalletGateway
            + VenueOpInvoker
            + Send
            + Sync
            + 'static,
    {
        Self {
            funding: Some(backend.clone()),
            triggers: Some(backend.clone()),
            wallet: Some(backend.clone()),
            ops: Some(backend),
        }
    }
}

impl std::fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Capabilities")
            .field("funding", &self.funding.is_some())
            .field("triggers", &self.triggers.is_some())
            .field("wallet", &self.wallet.is_some())
            .field("ops", &self.ops.is_some())
            .finish()
    }
}

impl SessionManager {
    pub fn new(
        expected_token: Option<String>,
        default_exchange: common::ExchangeId,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            expected_token,
            default_exchange,
            gateway,
            market,
            sessions: Mutex::new(HashMap::new()),
            watchdog_started: AtomicBool::new(false),
            health_failures: AtomicU32::new(0),
            self_weak: Mutex::new(None),
            cod_provider: Some(Arc::new(NullCodProvider)),
            capabilities: Capabilities::default(),
        }
    }

    /// Attach the backend's venue capabilities. Without this the proxies
    /// answer `unimplemented` for those RPCs, which is the honest answer for a
    /// backend that has not declared them.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_cod_provider(mut self, provider: Arc<dyn CodProvider>) -> Self {
        self.cod_provider = Some(provider);
        self
    }

    /// Bind the manager's own `Arc` so the watchdog task can reach it.
    /// Call once after wrapping the manager in `Arc`.
    pub async fn install_self(self: &Arc<Self>) {
        *self.self_weak.lock().await = Some(Arc::downgrade(self));
    }

    /// The backend's optional venue capabilities, for the proxies to consult.
    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    pub fn gateway(&self) -> Arc<dyn TradingGateway> {
        Arc::clone(&self.gateway)
    }

    pub fn market(&self) -> Arc<dyn MarketDataSource> {
        Arc::clone(&self.market)
    }

    pub fn default_exchange(&self) -> &common::ExchangeId {
        &self.default_exchange
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use buffa::{EnumValue, MessageField};

    use super::*;
    use crate::{adapters::MockAdapter, ports::PortError, proto::trading};

    fn test_manager(price: rust_decimal::Decimal) -> Arc<SessionManager> {
        let adapter = StdArc::new(MockAdapter::new(price));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter;
        let manager = SessionManager::new(None, common::ExchangeId::default(), gateway, market);
        let manager = Arc::new(manager);
        // Note: install_self is now async, but we don't need the watchdog in tests
        // manager.install_self().await;
        manager
    }

    fn kill_switch_policy(lease_ms: u64) -> worker::KillSwitchPolicy {
        worker::KillSwitchPolicy {
            lease_timeout: MessageField::some(buffa_types::google::protobuf::Duration {
                seconds: (lease_ms / 1000) as i64,
                nanos: ((lease_ms % 1000) * 1_000_000) as i32,
                ..Default::default()
            }),
            scope: EnumValue::Known(worker::kill_switch_policy::Scope::SessionOrders),
            ..Default::default()
        }
    }

    /// Attach with an anonymous client — no name, no version — which is what a
    /// client that sends neither field looks like, and therefore what every test
    /// that is not about identity starts from.
    async fn attach_anonymous(
        manager: &Arc<SessionManager>,
        policy: Option<worker::KillSwitchPolicy>,
    ) -> Result<(String, u32), ManagerError> {
        manager.attach("t", policy, &ClientIdentity::default()).await
    }

    // ---- Fixtures for the coverage below -----------------------------------
    //
    // `kill_switch_policy` above always negotiates `SCOPE_SESSION_ORDERS`, so the
    // scope-routing tests mutate `policy.scope` instead of duplicating the whole
    // message; this helper only supplies the wire `Duration` they need.

    /// A `google.protobuf.Duration` of `millis`.
    fn lease_ms(millis: u64) -> buffa_types::google::protobuf::Duration {
        buffa_types::google::protobuf::Duration {
            seconds: i64::try_from(millis / 1000).expect("whole seconds fit i64"),
            nanos: i32::try_from((millis % 1000) * 1_000_000).expect("millis fit i32 nanos"),
            ..Default::default()
        }
    }

    /// `FetchOpenOrdersRequest` for every symbol on the manager's default venue.
    fn open_orders_req(manager: &SessionManager) -> trading::FetchOpenOrdersRequest {
        trading::FetchOpenOrdersRequest {
            exchange_id: MessageField::some(manager.default_exchange().clone()),
            symbol: String::new(),
            pagination: MessageField::none(),
            ..Default::default()
        }
    }

    /// Backdate a session's lease clock by exactly `elapsed`.
    ///
    /// `tokio::time::sleep` rounds its deadline up to the next millisecond, so it
    /// cannot be used to land *precisely* on a lease boundary — and the boundary
    /// is the whole point of these assertions.
    async fn backdate_lease(handle: &SessionHandle, elapsed: Duration) {
        *handle.last_seen.lock().await = tokio::time::Instant::now() - elapsed;
    }

    /// A gateway that answers exactly like the mock but whose `sync_state` — the
    /// call the watchdog's L2 health probe makes — can be broken on demand, and
    /// counted so a test can prove the probe actually ran.
    struct HealthProbeGateway {
        inner: StdArc<MockAdapter>,
        fail_sync: StdArc<AtomicBool>,
        sync_calls: StdArc<AtomicU64>,
    }

    impl HealthProbeGateway {
        fn new() -> Self {
            Self {
                inner: StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(100))),
                fail_sync: StdArc::new(AtomicBool::new(false)),
                sync_calls: StdArc::new(AtomicU64::new(0)),
            }
        }

        /// Simulate a lost daemon connection: `sync_state` starts failing.
        fn break_sync(&self) {
            self.fail_sync.store(true, Ordering::Release);
        }

        fn sync_calls(&self) -> u64 {
            self.sync_calls.load(Ordering::Acquire)
        }
    }

    /// A `SessionManager` over [`HealthProbeGateway`], sharing one mock adapter as
    /// both the trading gateway and the market-data source.
    fn manager_with_probe() -> (StdArc<HealthProbeGateway>, StdArc<SessionManager>) {
        let probe = StdArc::new(HealthProbeGateway::new());
        let market: StdArc<dyn MarketDataSource> = probe.inner.clone();
        let manager = StdArc::new(SessionManager::new(
            None,
            common::ExchangeId::default(),
            StdArc::clone(&probe) as StdArc<dyn TradingGateway>,
            market,
        ));
        (probe, manager)
    }

    #[async_trait::async_trait]
    impl TradingGateway for HealthProbeGateway {
        async fn create_order(
            &self,
            req: trading::CreateOrderRequest,
        ) -> Result<trading::Order, PortError> {
            self.inner.create_order(req).await
        }

        async fn batch_create_orders(
            &self,
            req: trading::CreateOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.inner.batch_create_orders(req).await
        }

        async fn cancel_order(
            &self,
            req: trading::CancelOrderRequest,
        ) -> Result<trading::Order, PortError> {
            self.inner.cancel_order(req).await
        }

        async fn cancel_all_orders(
            &self,
            req: trading::CancelAllOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.inner.cancel_all_orders(req).await
        }

        async fn fetch_open_orders(
            &self,
            req: trading::FetchOpenOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.inner.fetch_open_orders(req).await
        }

        async fn get_account(
            &self,
            req: trading::GetAccountRequest,
        ) -> Result<trading::GetAccountResponse, PortError> {
            self.inner.get_account(req).await
        }

        async fn get_positions(
            &self,
            req: trading::GetPositionsRequest,
        ) -> Result<trading::GetPositionsResponse, PortError> {
            self.inner.get_positions(req).await
        }

        async fn get_order_history(
            &self,
            req: trading::GetOrderHistoryRequest,
        ) -> Result<trading::GetOrderHistoryResponse, PortError> {
            self.inner.get_order_history(req).await
        }

        async fn get_closed_positions(
            &self,
            req: trading::GetClosedPositionsRequest,
        ) -> Result<trading::GetClosedPositionsResponse, PortError> {
            self.inner.get_closed_positions(req).await
        }

        async fn close_position(
            &self,
            req: trading::ClosePositionRequest,
        ) -> Result<trading::ClosePositionResponse, PortError> {
            self.inner.close_position(req).await
        }

        async fn close_all_positions(
            &self,
            req: trading::CloseAllPositionsRequest,
        ) -> Result<trading::CloseAllPositionsResponse, PortError> {
            self.inner.close_all_positions(req).await
        }

        async fn modify_position(
            &self,
            req: trading::ModifyPositionRequest,
        ) -> Result<trading::ModifyPositionResponse, PortError> {
            self.inner.modify_position(req).await
        }

        /// The only method this stub changes: L2's probe goes through here, so
        /// breaking it models a daemon the worker can no longer reach.
        async fn sync_state(
            &self,
            exchange_id: &common::ExchangeId,
        ) -> Result<worker::ReconcileStateResponse, PortError> {
            self.sync_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_sync.load(Ordering::Acquire) {
                return Err(PortError::Transport("daemon unreachable".to_string()));
            }
            self.inner.sync_state(exchange_id).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_attaches_syncs_and_activates() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _hb) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        assert_eq!(handle.state(), Some(SessionState::Attached));

        manager.begin_reconcile(&session_id).await.expect("test setup");
        assert_eq!(handle.state(), Some(SessionState::Syncing));

        // Snapshot fetch happens through the port; completion activates.
        let snapshot =
            manager.gateway().sync_state(manager.default_exchange()).await.expect("test setup");
        manager
            .complete_reconcile(&session_id, snapshot.snapshot_sequence)
            .await
            .expect("test setup");
        assert_eq!(handle.state(), Some(SessionState::Active));
    }

    #[tokio::test]
    async fn lease_expiry_trips_kill_switch_and_cancels_session_orders() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");

        // Submit one order through the session's gateway and track the
        // venue-assigned order id (what the kill-switch cancels by).
        let coid = "grid-test-1".to_string();
        let order = manager
            .gateway()
            .create_order(trading::CreateOrderRequest {
                exchange_id: MessageField::some(manager.default_exchange().clone()),
                order: MessageField::some(trading::OrderRequest {
                    client_order_id: coid,
                    symbol: "BTC/USDT".to_string(),
                    r#type: EnumValue::Known(trading::OrderType::Limit),
                    side: EnumValue::Known(trading::OrderSide::Buy),
                    amount: MessageField::some(longtrader_contract::ext::decimal_to_common(
                        rust_decimal_macros::dec!(1),
                    )),
                    price: MessageField::some(longtrader_contract::ext::decimal_to_common(
                        rust_decimal_macros::dec!(99),
                    )),
                    trigger_price: MessageField::none(),
                    time_in_force: EnumValue::Known(trading::TimeInForce::Gtc),
                    post_only: false,
                    reduce_only: false,
                    params: Default::default(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("order placed");
        handle.track_order(order.id.clone()).await;

        // Heartbeat once so the lease clock starts from now.
        handle.touch().await;
        assert_eq!(handle.state(), Some(SessionState::Attached));

        // Reconciliation activates trading before the lease can expire.
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        assert_eq!(handle.state(), Some(SessionState::Active));

        // Let the real 600ms lease lapse, then run the expiry check the
        // watchdog would run.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let tripped = manager.check_lease_expiry(&session_id).await.expect("test setup");
        assert!(tripped, "lease must be expired after 700ms with a 600ms budget");
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));
        let open = manager
            .gateway()
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: MessageField::some(manager.default_exchange().clone()),
                symbol: String::new(),
                pagination: MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert!(
            !open.iter().any(|o| o.id == order.id),
            "kill-switch must cancel the session's open orders"
        );
    }

    #[tokio::test]
    async fn unauthenticated_token_is_rejected() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(1)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter;
        let manager = SessionManager::new(
            Some("secret".to_string()),
            common::ExchangeId::default(),
            gateway,
            market,
        );
        let err = manager
            .attach("wrong", None, &ClientIdentity::default())
            .await
            .expect_err("must reject");
        assert!(matches!(err, ManagerError::Unauthenticated));
    }

    #[tokio::test(start_paused = true)]
    async fn event_stream_replays_ring_after_resume_token() {
        let manager = test_manager(rust_decimal_macros::dec!(5));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager
            .report_log(&session_id, worker::LogLevel::Info, "one".into(), HashMap::new())
            .await
            .expect("test setup");
        manager
            .report_log(&session_id, worker::LogLevel::Info, "two".into(), HashMap::new())
            .await
            .expect("test setup");
        let handle = manager.get(&session_id).await.expect("test setup");
        let ring = handle.replay_after(0).await;
        assert_eq!(ring.len(), 2);
        let resume = ring[0].resume_token.clone();
        let after_first: u64 = resume.parse().expect("test setup");
        let rest = handle.replay_after(after_first).await;
        assert_eq!(rest.len(), 1);
    }

    // ---- Order-submission gate + order attribution ------------------------
    //
    // `worker.proto` states orders submitted before ACTIVE are rejected with
    // reason SYNC_IN_PROGRESS, and that `KillSwitchPolicy.SCOPE_SESSION_ORDERS`
    // cancels the session's own orders. Both only hold if the host (a) gates
    // session-scoped submissions and (b) records the resulting order ids.
    // `trading.v1.CreateOrderRequest.session_id` is what makes both possible.

    fn limit_order_req(manager: &SessionManager, coid: &str) -> trading::CreateOrderRequest {
        trading::CreateOrderRequest {
            exchange_id: buffa::MessageField::some(manager.default_exchange().clone()),
            order: buffa::MessageField::some(trading::OrderRequest {
                client_order_id: coid.to_string(),
                symbol: "BTC/USDT".to_string(),
                r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
                side: buffa::EnumValue::Known(trading::OrderSide::Buy),
                amount: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(
                    rust_decimal_macros::dec!(1),
                )),
                price: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(
                    rust_decimal_macros::dec!(99),
                )),
                time_in_force: buffa::EnumValue::Known(trading::TimeInForce::Gtc),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unattached_session_cannot_submit_orders() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let err = manager
            .authorize_order_submission("no-such-session")
            .await
            .expect_err("unknown session must be rejected");
        assert!(matches!(err, ManagerError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn pre_active_session_cannot_submit_orders() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");

        // ATTACHED: not yet reconciled.
        let err = manager
            .authorize_order_submission(&session_id)
            .await
            .expect_err("ATTACHED session must be gated");
        assert!(matches!(err, ManagerError::SyncInProgress { .. }), "got {err:?}");

        // SYNCING: snapshot in flight, still not ACTIVE.
        manager.begin_reconcile(&session_id).await.expect("test setup");
        let err = manager
            .authorize_order_submission(&session_id)
            .await
            .expect_err("SYNCING session must be gated");
        assert!(matches!(err, ManagerError::SyncInProgress { .. }), "got {err:?}");

        // ACTIVE: admitted.
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        assert!(
            manager
                .authorize_order_submission(&session_id)
                .await
                .expect("ACTIVE session")
                .is_some(),
            "an ACTIVE session must be authorized"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_session_cannot_submit_orders() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        manager.stop_session(&session_id, false).await.expect("stop");

        let err = manager
            .authorize_order_submission(&session_id)
            .await
            .expect_err("a stopped session must be gated");
        assert!(matches!(err, ManagerError::SyncInProgress { .. }), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn unscoped_submission_is_not_gated_or_tracked() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        // Empty session_id = operator/unscoped call (the CLI). It must pass the
        // gate without a session and be recorded nowhere.
        assert!(
            manager.authorize_order_submission("").await.expect("unscoped").is_none(),
            "unscoped submissions carry no session handle"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn session_submitted_orders_are_tracked_for_kill_switch() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let handle = manager
            .authorize_order_submission(&session_id)
            .await
            .expect("ACTIVE session")
            .expect("session handle");
        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "grid-1"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;

        // This is the production path that used to be test-only, which is why
        // SCOPE_SESSION_ORDERS was a no-op.
        assert_eq!(
            handle.tracked_orders().await,
            HashSet::from([order.id.clone()]),
            "the submitted order id must be recorded against the session"
        );
        assert_eq!(
            handle.orders_submitted_count(),
            1,
            "the session must count the order even before RegisterStrategy runs"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn kill_switch_cancels_orders_submitted_through_the_gate() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        // Submit through the real gate + attribution path — no test-side
        // `track_order` call, unlike the pre-existing lease test.
        let handle = manager
            .authorize_order_submission(&session_id)
            .await
            .expect("ACTIVE session")
            .expect("session handle");
        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "grid-gated"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;

        // Lease lapses; the watchdog trips the kill-switch.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));

        let open = manager
            .gateway()
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(manager.default_exchange().clone()),
                symbol: String::new(),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert!(
            !open.iter().any(|o| o.id == order.id),
            "kill-switch must cancel orders submitted through the session gate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stop_strategy_cancels_tracked_orders() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let handle = manager
            .authorize_order_submission(&session_id)
            .await
            .expect("ACTIVE session")
            .expect("session handle");
        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "grid-stop"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;

        let final_state = manager.stop_session(&session_id, true).await.expect("stop");
        assert_eq!(final_state, Some(SessionState::GracefulShutdown));

        let open = manager
            .gateway()
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(manager.default_exchange().clone()),
                symbol: String::new(),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert!(
            !open.iter().any(|o| o.id == order.id),
            "StopStrategy(cancel_open_orders) must cancel the session's orders"
        );
    }

    /// A cancel the venue refuses must not be reported as a clean stop. The
    /// session still reaches its terminal state — it must not be left trading
    /// because a venue was slow — but the caller is told, because
    /// `final_state: GRACEFUL_SHUTDOWN` is otherwise indistinguishable from "your
    /// orders are all unwound".
    #[tokio::test(start_paused = true)]
    async fn a_refused_cancel_is_reported_but_still_reaches_a_terminal_state() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        let handle = manager.get(&session_id).await.expect("test setup");
        // The mock refuses to cancel an id it does not know.
        handle.track_order("an-order-the-venue-lost".to_string()).await;

        let err = manager
            .stop_session(&session_id, true)
            .await
            .expect_err("a refused cancel must not be reported as a clean stop");
        assert!(
            matches!(&err, ManagerError::CancelsFailed { .. }),
            "a refused cancel must surface its detail: {err:?}"
        );
        // What the RPC client actually reads is the message, so pin that.
        let message = err.to_string();
        assert!(
            message.contains("an-order-the-venue-lost"),
            "the failure must name the order that is still resting: {message}"
        );
        assert!(
            message.contains("GracefulShutdown"),
            "the error must say which terminal state was reached: {message}"
        );
        assert_eq!(
            handle.state(),
            Some(SessionState::GracefulShutdown),
            "a refused cancel must not leave the session live"
        );
    }

    /// `stop_session` is idempotent, and idempotent has to mean *invisible* too: a
    /// `GRACEFUL_SHUTDOWN -> GRACEFUL_SHUTDOWN` event makes a consumer that
    /// treats every state change as an action act twice on a session that is
    /// already stopped.
    #[tokio::test(start_paused = true)]
    async fn a_repeated_stop_publishes_nothing_the_second_time() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        let handle = manager.get(&session_id).await.expect("test setup");

        assert_eq!(
            manager.stop_session(&session_id, false).await.expect("first stop"),
            Some(SessionState::GracefulShutdown)
        );
        let after_first = handle.replay_after(0).await.len();
        assert_eq!(
            manager.stop_session(&session_id, false).await.expect("a repeated stop"),
            Some(SessionState::GracefulShutdown),
            "the state must stay terminal across a repeated stop"
        );
        assert_eq!(
            handle.replay_after(0).await.len(),
            after_first,
            "a repeated stop must not publish a self-transition"
        );
    }

    // ---- apply_policy -------------------------------------------------------
    //
    // `apply_policy` is the single owner of lease/scope parsing, shared by attach, by
    // reconnect and by `SetKillSwitchPolicy` (`session::service`). Each arm is pinned
    // here so a change to one cannot quietly alter the others.

    #[test]
    fn apply_policy_applies_a_lease_and_leaves_an_unspecified_scope_alone() {
        let mut policy = SessionPolicy::default();
        apply_policy(
            &mut policy,
            &worker::KillSwitchPolicy {
                lease_timeout: MessageField::some(lease_ms(2_000)),
                scope: EnumValue::Known(worker::kill_switch_policy::Scope::Unspecified),
                ..Default::default()
            },
        )
        .expect("a lease inside the documented bounds");
        assert_eq!(policy.lease_timeout, Duration::from_secs(2));
        assert_eq!(
            policy.scope,
            worker::kill_switch_policy::Scope::SessionOrders,
            "an explicitly unspecified scope means leave-unchanged, not reset"
        );
    }

    #[test]
    fn apply_policy_applies_a_concrete_scope() {
        let mut policy = SessionPolicy::default();
        apply_policy(
            &mut policy,
            &worker::KillSwitchPolicy {
                lease_timeout: MessageField::none(),
                scope: EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders),
                ..Default::default()
            },
        )
        .expect("a routable scope");
        assert_eq!(policy.scope, worker::kill_switch_policy::Scope::AllOrders);
    }

    /// A lease the watchdog could not honour is the caller's mistake, and a
    /// partially-applied policy is worse than a rejected one: the caller would
    /// believe it had a 30s budget while the worker runs on the default.
    #[test]
    fn apply_policy_rejects_a_lease_outside_the_bounds_without_partial_application() {
        for bad in [lease_ms(400), lease_ms(7_200_000), lease_ms(u64::MAX)] {
            let mut policy = SessionPolicy::default();
            let err = apply_policy(
                &mut policy,
                &worker::KillSwitchPolicy {
                    lease_timeout: MessageField::some(bad.clone()),
                    scope: EnumValue::Known(worker::kill_switch_policy::Scope::None),
                    ..Default::default()
                },
            )
            .expect_err("an un-honourable lease must be refused");
            assert!(matches!(err, ManagerError::InvalidArgument(_)), "{bad:?} produced {err:?}");
            assert_eq!(policy.lease_timeout, SessionPolicy::default().lease_timeout);
            assert_eq!(policy.scope, SessionPolicy::default().scope, "the scope must not move");
        }
    }

    /// An unroutable discriminant is not a scope the worker can execute, so it has
    /// to be an error rather than a silently ignored field — otherwise a client
    /// asking for a cancel-everything scope it cannot spell would get the default.
    #[test]
    fn apply_policy_rejects_an_unknown_scope_discriminant() {
        let mut policy = SessionPolicy::default();
        let err = apply_policy(
            &mut policy,
            &worker::KillSwitchPolicy {
                lease_timeout: MessageField::none(),
                scope: EnumValue::Unknown(7),
                ..Default::default()
            },
        )
        .expect_err("an unroutable scope discriminant");
        assert!(matches!(err, ManagerError::InvalidArgument(_)), "got {err:?}");
        assert!(err.to_string().contains('7'), "the message must name the discriminant: {err}");
        assert_eq!(policy.scope, SessionPolicy::default().scope);
    }

    // ---- attach_with_reconnect ----------------------------------------------

    /// Reconnect is only useful if it actually reuses the issued session: a fresh
    /// one would silently orphan the old handle, lose its lease clock, and drop
    /// every tracked order id the kill-switch depends on.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_reuses_the_session_and_refreshes_its_lease() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, heartbeat) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        handle.touch().await;

        // Let the negotiated 600ms budget lapse before reconnecting.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(
            u128::from(handle.last_seen_elapsed_ms().await) > 600,
            "the lease has lapsed before the reconnect"
        );

        let mut wider = kill_switch_policy(1_500);
        wider.scope = EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders);
        let (reused, heartbeat_again) = manager
            .attach_with_reconnect("t", Some(wider), &session_id, &ClientIdentity::default())
            .await
            .expect("reconnect");

        assert_eq!(reused, session_id, "a live session must be reused, not duplicated");
        assert_eq!(heartbeat_again, heartbeat, "the negotiated interval must be reported back");
        assert!(
            u128::from(handle.last_seen_elapsed_ms().await) <= 600,
            "a reconnect must restart the lease clock"
        );
        let policy = handle.policy().await;
        assert_eq!(policy.lease_timeout, Duration::from_millis(1_500), "the new policy must win");
        assert_eq!(policy.scope, worker::kill_switch_policy::Scope::AllOrders);
        assert_eq!(
            handle.state(),
            Some(SessionState::Active),
            "a reconnect must not rewind the lifecycle"
        );
        assert!(
            !manager.check_lease_expiry(&session_id).await.expect("test setup"),
            "the refreshed lease must not be expired"
        );
    }

    /// A terminal session must never be resurrected: doing so hands a strategy a
    /// live id whose kill-switch has already fired, so its orders would never be
    /// cancelled again.
    #[tokio::test(start_paused = true)]
    async fn a_terminal_session_refuses_a_reconnect() {
        for terminal in [SessionState::KillSwitchTripped, SessionState::GracefulShutdown] {
            let manager = test_manager(rust_decimal_macros::dec!(100));
            let mut policy = kill_switch_policy(600);
            policy.scope = EnumValue::Known(worker::kill_switch_policy::Scope::None);
            let (session_id, _) = attach_anonymous(&manager, Some(policy)).await.expect("attach");
            let handle = manager.get(&session_id).await.expect("test setup");
            manager.begin_reconcile(&session_id).await.expect("test setup");
            manager.complete_reconcile(&session_id, 0).await.expect("test setup");
            handle.touch().await;

            match terminal {
                SessionState::KillSwitchTripped => {
                    tokio::time::sleep(Duration::from_millis(700)).await;
                    assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
                }
                _ => {
                    manager.stop_session(&session_id, false).await.expect("stop");
                }
            }
            assert_eq!(handle.state(), Some(terminal), "test setup");

            let err = manager
                .attach_with_reconnect("t", None, &session_id, &ClientIdentity::default())
                .await
                .expect_err("a terminal session must not be resurrectable");
            assert!(
                matches!(err, ManagerError::InvalidArgument(_)),
                "{terminal:?} produced {err:?}"
            );
            assert_eq!(
                manager.get(&session_id).await.expect("test setup").state(),
                Some(terminal),
                "the refused reconnect must leave the registry untouched"
            );
        }
    }

    /// A state byte outside the assigned discriminants is corruption, not a state
    /// the worker can reason about: the reuse branch must fail closed rather than
    /// treat it as "not terminal" and hand back a session it cannot manage.
    #[tokio::test(start_paused = true)]
    async fn a_corrupt_state_byte_refuses_a_reconnect() {
        for byte in [0u8, 6, 99, 255] {
            let manager = test_manager(rust_decimal_macros::dec!(100));
            let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
            let handle = manager.get(&session_id).await.expect("test setup");
            handle.state.store(byte, Ordering::Release);
            assert_eq!(handle.state(), None, "byte {byte} must not decode to a state");

            let err = manager
                .attach_with_reconnect("t", None, &session_id, &ClientIdentity::default())
                .await
                .expect_err("a corrupt state byte must be refused, not guessed at");
            assert!(
                matches!(err, ManagerError::InvalidArgument(_)),
                "byte {byte} produced {err:?}"
            );
        }
    }

    /// A reconnect id the worker never issued is not an error — the client
    /// restarted from a stale config — so a fresh session is minted instead of
    /// wedging the client out of the worker entirely.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_reconnect_id_attaches_a_fresh_session() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (fresh, _) = manager
            .attach_with_reconnect(
                "t",
                None,
                "a-session-from-a-previous-life",
                &ClientIdentity::default(),
            )
            .await
            .expect("a stale reconnect id must not wedge the client out");
        assert_ne!(fresh, "a-session-from-a-previous-life");
        assert_eq!(
            manager.get(&fresh).await.expect("test setup").state(),
            Some(SessionState::Attached),
        );
        assert!(
            manager.get("a-session-from-a-previous-life").await.is_err(),
            "the stale id must not be invented in the registry"
        );
    }

    /// Token validation runs *before* the reuse branch. Otherwise reconnecting
    /// with a live session id would be an unauthenticated way to take over a
    /// session another principal owns.
    #[tokio::test]
    async fn a_reconnect_is_authenticated_like_an_attach() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(1)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter;
        let manager = Arc::new(SessionManager::new(
            Some("secret".to_string()),
            common::ExchangeId::default(),
            gateway,
            market,
        ));
        let (session_id, _) =
            manager.attach("secret", None, &ClientIdentity::default()).await.expect("attach");

        let err = manager
            .attach_with_reconnect("wrong", None, &session_id, &ClientIdentity::default())
            .await
            .expect_err("a reconnect must be authenticated before it may reuse a session");
        assert!(matches!(err, ManagerError::Unauthenticated), "got {err:?}");
    }

    // ---- Reconnect identity --------------------------------------------------
    //
    // `AttachSessionRequest` carries a `client_name` / `client_version` pair. They
    // used to be discarded, which left the issued `session_id` as a bearer token:
    // the worker token plus a live id was enough to adopt another principal's
    // session, its lease, and the order ids its kill-switch cancels.

    /// A definite mismatch is refused, and refused *before* anything the reconnect
    /// would have mutated.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_from_a_different_client_is_refused() {
        let cases = [
            ("python-sdk", "0.2.0", "someone-else", "0.2.0"),
            ("python-sdk", "0.2.0", "python-sdk", "9.9.9"),
        ];
        for (name, version, other_name, other_version) in cases {
            let manager = test_manager(rust_decimal_macros::dec!(100));
            let (session_id, _) = manager
                .attach("t", None, &ClientIdentity { name: name.into(), version: version.into() })
                .await
                .expect("attach");
            let handle = manager.get(&session_id).await.expect("test setup");
            manager.begin_reconcile(&session_id).await.expect("test setup");
            manager.complete_reconcile(&session_id, 0).await.expect("test setup");
            handle.touch().await;
            // Let the lease lapse: a refused reconnect must not refresh it.
            tokio::time::sleep(Duration::from_millis(700)).await;

            let err = manager
                .attach_with_reconnect(
                    "t",
                    None,
                    &session_id,
                    &ClientIdentity {
                        name: other_name.to_string(),
                        version: other_version.to_string(),
                    },
                )
                .await
                .expect_err("a different client must not adopt the session");
            assert!(matches!(err, ManagerError::InvalidArgument(_)), "{err:?}");
            assert!(
                u128::from(handle.last_seen_elapsed_ms().await) > 600,
                "a refused reconnect must not have refreshed the lease"
            );
        }
    }

    /// An *absent* half is not a mismatch. Every client that predates these fields
    /// sends nothing, and a reconnect that presents no identity must keep working —
    /// otherwise adding the check would break every existing client at once.
    #[tokio::test(start_paused = true)]
    async fn an_absent_identity_on_either_side_is_still_accepted() {
        /// A client that does identify itself.
        fn sdk() -> ClientIdentity {
            ClientIdentity { name: "python-sdk".to_string(), version: "0.2.0".to_string() }
        }
        for attached in [ClientIdentity::default(), sdk()] {
            let manager = test_manager(rust_decimal_macros::dec!(100));
            let (session_id, _) = manager.attach("t", None, &attached).await.expect("attach");
            manager.begin_reconcile(&session_id).await.expect("test setup");
            manager.complete_reconcile(&session_id, 0).await.expect("test setup");

            // Both directions: sending nothing after a named attach, and attaching
            // anonymously then reconnecting with a name.
            for reconnecting in [ClientIdentity::default(), sdk()] {
                let (reused, _) = manager
                    .attach_with_reconnect("t", None, &session_id, &reconnecting)
                    .await
                    .expect("an absent identity is not a mismatch");
                assert_eq!(reused, session_id);
            }
            assert_eq!(
                manager.get(&session_id).await.expect("test setup").client_identity(),
                (attached.name.as_str(), attached.version.as_str()),
                "a reconnect must not overwrite the identity recorded at attach"
            );
        }
    }

    // ---- check_lease_expiry -------------------------------------------------

    /// Only an ACTIVE session's lease can trip the kill-switch. An ATTACHED or
    /// SYNCING session has placed nothing, and tripping it would make a client
    /// that attaches and then reconciles lose the session it just created.
    #[tokio::test(start_paused = true)]
    async fn a_lapsed_lease_on_a_non_active_session_does_not_trip() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");

        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(
            !manager.check_lease_expiry(&session_id).await.expect("test setup"),
            "an ATTACHED session has nothing to cancel"
        );
        assert_eq!(handle.state(), Some(SessionState::Attached));

        manager.begin_reconcile(&session_id).await.expect("test setup");
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(
            !manager.check_lease_expiry(&session_id).await.expect("test setup"),
            "a session mid-reconcile is not a dead session"
        );
        assert_eq!(handle.state(), Some(SessionState::Syncing));
    }

    /// The comparison is strict: a session quiet for exactly its negotiated
    /// timeout is still inside the budget, and one millisecond later is not. An
    /// off-by-one here either kills healthy sessions early or lets a dead one
    /// trade for an extra heartbeat — and a heartbeat that missed its deadline is
    /// exactly the situation the lease exists for.
    #[tokio::test(start_paused = true)]
    async fn the_lease_boundary_is_exclusive() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        assert_eq!(handle.policy().await.lease_timeout, Duration::from_millis(600));

        backdate_lease(&handle, Duration::from_millis(600)).await;
        assert_eq!(
            u128::from(handle.last_seen_elapsed_ms().await),
            600,
            "test setup: the clock must land exactly on the timeout"
        );
        assert!(
            !manager.check_lease_expiry(&session_id).await.expect("test setup"),
            "a heartbeat that lands exactly on the deadline has still made it"
        );
        assert_eq!(handle.state(), Some(SessionState::Active), "a boundary check must not trip");

        backdate_lease(&handle, Duration::from_millis(601)).await;
        assert_eq!(u128::from(handle.last_seen_elapsed_ms().await), 601, "test setup");
        assert!(
            manager.check_lease_expiry(&session_id).await.expect("test setup"),
            "one millisecond past the timeout must trip"
        );
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));
    }

    // ---- Kill-switch scope routing -----------------------------------------

    /// `SCOPE_ALL_ORDERS` is the "flatten everything" scope: it must reach orders
    /// this session never submitted, which is precisely what `SESSION_ORDERS`
    /// cannot do.
    #[tokio::test(start_paused = true)]
    async fn the_all_orders_scope_cancels_orders_the_session_never_placed() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let mut policy = kill_switch_policy(600);
        policy.scope = EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders);
        let (session_id, _) = attach_anonymous(&manager, Some(policy)).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let mine = manager
            .gateway()
            .create_order(limit_order_req(&manager, "mine"))
            .await
            .expect("session order");
        let theirs = manager
            .gateway()
            .create_order(limit_order_req(&manager, "theirs"))
            .await
            .expect("foreign order");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&mine)).await;
        assert_eq!(
            handle.tracked_orders().await,
            HashSet::from([mine.id.clone()]),
            "test setup: only the session's own order is tracked"
        );
        handle.touch().await;

        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));

        let open = manager
            .gateway()
            .fetch_open_orders(open_orders_req(&manager))
            .await
            .expect("test setup");
        assert!(
            !open.iter().any(|o| o.id == theirs.id),
            "SCOPE_ALL_ORDERS must cancel an order this session never submitted"
        );
        assert!(open.is_empty(), "no open order may survive the account-wide scope: {open:?}");
    }

    /// A kill-switch trip with `SCOPE_ALL_ORDERS` cancels on the bound venue
    /// with no symbol filter: the recorded request must carry the session's
    /// exchange id and an empty symbol, not a defaulted or narrowed one. The
    /// venue is deliberately non-empty here — an empty exchange id would make
    /// a dropped field indistinguishable from the real request.
    #[tokio::test(start_paused = true)]
    async fn an_all_orders_trip_targets_the_bound_venue_unfiltered() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let manager = Arc::new(SessionManager::new(
            None,
            crate::adapters::exchange_id("mock", ""),
            gateway,
            market,
        ));
        let mut policy = kill_switch_policy(600);
        policy.scope = EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders);
        let (session_id, _) = attach_anonymous(&manager, Some(policy)).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        backdate_lease(&handle, Duration::from_secs(3600)).await;

        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));
        assert_eq!(
            adapter.cancel_all_seen().await,
            vec![("mock".to_string(), String::new())],
            "the trip must cancel on the bound venue with no symbol filter"
        );
    }

    /// A kill-switch trip targets the bound venue with no symbol filter: the
    /// request the venue sees must carry the session's exchange id and an
    /// empty symbol, not a defaulted or narrowed one.
    #[tokio::test(start_paused = true)]
    async fn a_kill_switch_cancel_all_targets_the_bound_venue_unfiltered() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let manager = Arc::new(SessionManager::new(
            None,
            crate::adapters::exchange_id("mock", ""),
            gateway,
            market,
        ));
        let mut policy = kill_switch_policy(600);
        policy.scope = EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders);
        let (session_id, _) = attach_anonymous(&manager, Some(policy)).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        backdate_lease(&handle, Duration::from_secs(3600)).await;

        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
        assert_eq!(
            adapter.cancel_all_seen().await,
            vec![("mock".to_string(), String::new())],
            "the trip must cancel on the bound venue with no symbol filter"
        );
    }

    /// Layer that records `(level, message)` pairs so tests can assert the
    /// worker stays loud on failure paths instead of silently swallowing them.
    /// Installed with thread-local `set_default`, which is restored on drop:
    /// no global subscriber, so parallel tests cannot interfere with each
    /// other. The future under test is polled inline on the current-thread
    /// runtime, so the thread-local default always applies.
    struct CaptureLayer {
        events: StdArc<std::sync::Mutex<Vec<(tracing::Level, String)>>>,
    }

    struct CapturedMessage(Option<String>);

    impl tracing::field::Visit for CapturedMessage {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.0 = Some(value.to_string());
            }
        }
    }

    impl<S> tracing_subscriber::Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = CapturedMessage(None);
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("log capture")
                .push((event.metadata().level().clone(), visitor.0.unwrap_or_default()));
        }
    }

    /// A venue that refuses a kill-switch cancel must surface as an error log,
    /// not a silent unwind: operators page on this line.
    #[tokio::test(start_paused = true)]
    async fn a_refused_kill_switch_cancel_is_logged_as_an_error() {
        use tracing_subscriber::prelude::*;

        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let manager =
            Arc::new(SessionManager::new(None, common::ExchangeId::default(), gateway, market));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "doomed"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;
        handle.touch().await;

        // The only failure injection: the venue refuses the cancel the
        // kill-switch is about to send.
        adapter.fail_next_cancels(1).await;
        backdate_lease(&handle, Duration::from_secs(3600)).await;

        let events: StdArc<std::sync::Mutex<Vec<(tracing::Level, String)>>> = StdArc::default();
        let subscriber = tracing_subscriber::registry::Registry::default()
            .with(CaptureLayer { events: StdArc::clone(&events) });
        let _guard = tracing_subscriber::util::SubscriberInitExt::set_default(subscriber);
        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));

        let events = events.lock().expect("log capture");
        assert!(
            events.iter().any(|(level, message)| *level == tracing::Level::ERROR &&
                message.contains("left orders resting")),
            "a refused kill-switch cancel must be loud, got: {events:?}"
        );
    }

    /// `SCOPE_NONE` is a deliberate "log only": the session still trips, so the
    /// state machine stays honest, but the venue's orders are left resting. A
    /// scope that cancelled anything here would make the setting a lie.
    #[tokio::test(start_paused = true)]
    async fn the_none_scope_trips_the_state_without_cancelling_anything() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let mut policy = kill_switch_policy(600);
        policy.scope = EnumValue::Known(worker::kill_switch_policy::Scope::None);
        let (session_id, _) = attach_anonymous(&manager, Some(policy)).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "kept"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;
        handle.touch().await;

        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(manager.check_lease_expiry(&session_id).await.expect("test setup"));
        assert_eq!(handle.state(), Some(SessionState::KillSwitchTripped));

        let open = manager
            .gateway()
            .fetch_open_orders(open_orders_req(&manager))
            .await
            .expect("test setup");
        assert!(
            open.iter().any(|o| o.id == order.id),
            "SCOPE_NONE must not cancel: the operator asked for log-only"
        );
    }

    // ---- Order attribution --------------------------------------------------

    /// An order the venue issued no id for cannot be cancelled later, so it must
    /// not enter the tracked set: tracking the empty string would make a later
    /// cancel address an unrelated order on the venue.
    #[tokio::test(start_paused = true)]
    async fn an_order_without_a_venue_id_is_not_tracked() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");

        let anonymous = trading::Order { id: String::new(), ..Default::default() };
        manager.record_submitted_orders(&handle, std::slice::from_ref(&anonymous)).await;
        assert!(
            handle.tracked_orders().await.is_empty(),
            "an id-less order must not enter the tracked set"
        );
        assert_eq!(
            handle.orders_submitted_count(),
            0,
            "the counter must track what the session can actually cancel, or \
             StrategyStatus over-reports orders the kill-switch cannot reach"
        );
    }

    /// An empty batch is a no-op, so it must not move the counter — otherwise a
    /// client retrying with a drained list would look busier than it is.
    #[tokio::test(start_paused = true)]
    async fn an_empty_batch_does_not_move_the_submission_counter() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.record_submitted_orders(&handle, &[]).await;
        assert_eq!(handle.orders_submitted_count(), 0);
    }

    // ---- The watchdog -------------------------------------------------------
    //
    // The watchdog only exists behind `install_self`, which the other fixtures
    // deliberately skip, so this is the first coverage of the loop at all. Its
    // tick is 1s of virtual time, so `start_paused` drives it deterministically.

    /// L1 is the in-process backstop: nothing on the request path polls the lease,
    /// so if the loop did not run a dead strategy's orders would rest forever.
    #[tokio::test(start_paused = true)]
    async fn the_watchdog_trips_an_expired_active_lease() {
        let (probe, manager) = manager_with_probe();
        // `attach` calls `ensure_watchdog`, which needs the weak self-reference to
        // already be bound — hence install before attach, not after.
        manager.install_self().await;

        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");

        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "grid-watchdog"))
            .await
            .expect("order placed");
        manager.record_submitted_orders(&handle, std::slice::from_ref(&order)).await;

        // The health probe is healthy here, so the only thing that can trip this
        // session is the L1 lease check inside the loop.
        let calls_before = probe.sync_calls();
        for _ in 0..8 {
            if handle.state() == Some(SessionState::KillSwitchTripped) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            handle.state(),
            Some(SessionState::KillSwitchTripped),
            "the watchdog must trip an expired ACTIVE lease without any RPC asking it to"
        );
        assert!(
            probe.sync_calls() > calls_before,
            "the loop must have run a full pass (L1 checks followed by the L2 probe)"
        );

        let open = manager
            .gateway()
            .fetch_open_orders(open_orders_req(&manager))
            .await
            .expect("test setup");
        assert!(
            !open.iter().any(|o| o.id == order.id),
            "the kill-switch the watchdog ran must reach the venue"
        );
    }

    /// L2 is the backstop beyond the lease: a lost daemon must not leave stale
    /// orders resting just because a strategy keeps heartbeating on a healthy
    /// timer. It trips every ACTIVE session, and only those — but only after
    /// `HEALTH_PROBE_FAILURE_THRESHOLD` consecutive misses, so the loop below
    /// waits for more than one probe interval.
    #[tokio::test(start_paused = true)]
    async fn a_failing_health_probe_trips_every_active_session() {
        let (probe, manager) = manager_with_probe();
        manager.install_self().await;

        // A long, freshly-touched lease, so L1 can never be the thing that trips.
        let (active_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(30_000))).await.expect("attach");
        manager.begin_reconcile(&active_id).await.expect("test setup");
        manager.complete_reconcile(&active_id, 0).await.expect("test setup");
        let active = manager.get(&active_id).await.expect("test setup");

        // A second session left ATTACHED must be left alone: L2 protects live
        // orders, and this one has none.
        let (attached_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let attached = manager.get(&attached_id).await.expect("test setup");

        let calls_before = probe.sync_calls();
        probe.break_sync();

        // Generous bound: the probe runs on the 1s watchdog tick and two
        // consecutive misses are required, so the trip lands at the second tick
        // after the break at the earliest.
        for _ in 0..16 {
            if active.state() == Some(SessionState::KillSwitchTripped) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            probe.sync_calls() > calls_before,
            "the health probe must have been consulted after the break"
        );
        assert_eq!(
            active.state(),
            Some(SessionState::KillSwitchTripped),
            "an unreachable gateway must trip an ACTIVE session even on a fresh lease"
        );
        assert_eq!(
            attached.state(),
            Some(SessionState::Attached),
            "L2 must not touch a session that never reached ACTIVE"
        );
    }

    /// One missed probe is not a partition. `sync_state` is not atomic (its own
    /// contract) and the probe has a wall-clock budget, so a merely slow backend
    /// on a saturated host can miss one — and believing it cancelled every ACTIVE
    /// session on the worker, host-wide, for a hiccup. The counter is what makes
    /// the difference, so it is pinned directly here.
    #[test]
    fn a_health_probe_miss_only_trips_after_the_threshold() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let miss = || Err(crate::ports::PortError::Transport("daemon unreachable".to_string()));
        let healthy = || Ok(());

        assert_eq!(manager.note_health_probe(&healthy()), 0, "a healthy probe reports no misses");
        // One miss stays below the threshold, and a healthy probe in between resets
        // the run: two misses separated by a success are not a partition either.
        for _ in 0..2 {
            assert!(manager.note_health_probe(&miss()) < HEALTH_PROBE_FAILURE_THRESHOLD);
            assert_eq!(manager.note_health_probe(&healthy()), 0, "a success resets the run");
        }
        // Back-to-back, they cross the threshold — and stay over it for as long as
        // the outage lasts, rather than wrapping back to zero.
        assert!(manager.note_health_probe(&miss()) < HEALTH_PROBE_FAILURE_THRESHOLD);
        assert!(manager.note_health_probe(&miss()) >= HEALTH_PROBE_FAILURE_THRESHOLD);
        assert!(manager.note_health_probe(&miss()) >= HEALTH_PROBE_FAILURE_THRESHOLD);
    }

    /// A venue with native cancel-on-disconnect keeps the trait default
    /// (`supports_cod() == true`); the null provider used when no venue
    /// supports it reports `false` so the watchdog never relies on it.
    struct NativeCod;

    #[async_trait::async_trait]
    impl crate::session::CodProvider for NativeCod {
        async fn keepalive(&self, _session_id: &str) -> Result<(), crate::ports::PortError> {
            Ok(())
        }
    }

    #[test]
    fn cod_support_is_opt_in_not_assumed() {
        assert!(
            !crate::session::NullCodProvider.supports_cod(),
            "the null provider must not claim native cancel-on-disconnect"
        );
        assert!(
            NativeCod.supports_cod(),
            "the trait default opts providers in; only an explicit false opts out"
        );
    }

    #[test]
    fn cod_renewal_is_gated_on_active_sessions_with_native_support() {
        let cod: StdArc<dyn crate::session::CodProvider> = StdArc::new(NativeCod);
        let null: StdArc<dyn crate::session::CodProvider> =
            StdArc::new(crate::session::NullCodProvider);
        assert!(SessionManager::should_renew_cod(Some(SessionState::Active), Some(&cod)));
        assert!(!SessionManager::should_renew_cod(Some(SessionState::Attached), Some(&cod)));
        assert!(!SessionManager::should_renew_cod(Some(SessionState::Syncing), Some(&cod)));
        assert!(!SessionManager::should_renew_cod(
            Some(SessionState::KillSwitchTripped),
            Some(&cod)
        ));
        assert!(!SessionManager::should_renew_cod(None, Some(&cod)));
        assert!(!SessionManager::should_renew_cod(Some(SessionState::Active), None));
        assert!(
            !SessionManager::should_renew_cod(Some(SessionState::Active), Some(&null)),
            "a venue without native COD must fall back to lease + probe tiers"
        );
    }

    /// The miss counter saturates instead of wrapping: an outage long enough to
    /// reach `u32::MAX` must keep reporting "at threshold", never a fresh run
    /// that looks like recovery followed by a brand-new single miss.
    #[test]
    fn a_health_probe_miss_counter_saturates_at_the_maximum() {
        use std::sync::atomic::Ordering;

        let manager = test_manager(rust_decimal_macros::dec!(100));
        let miss = || Err(crate::ports::PortError::Transport("daemon unreachable".to_string()));
        manager.health_failures.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(manager.note_health_probe(&miss()), u32::MAX);
        assert_eq!(
            manager.note_health_probe(&miss()),
            u32::MAX,
            "a saturated counter must not wrap back to the trip threshold"
        );
    }

    // ---- The strategy-side lease guard --------------------------------------

    /// The guard runs for the life of the worker, so a session that disappears
    /// (the client restarted, or the session was never real) must end its loop
    /// instead of spinning on a lookup that can never succeed.
    #[tokio::test(start_paused = true)]
    async fn the_strategy_lease_guard_exits_when_its_session_is_gone() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let guard = spawn_strategy_lease_guard(
            StdArc::clone(&manager),
            "a-session-that-never-existed".to_string(),
            Duration::from_millis(500),
        );
        guard.await.expect("the guard must return, not panic");
    }

    /// The other exit: the strategy's own lease has lapsed, so the guard asks the
    /// manager to run the configured cancel scope and then stops. This is the path
    /// an external-language strategy relies on to stop trading on its own.
    #[tokio::test(start_paused = true)]
    async fn the_strategy_lease_guard_trips_a_lapsed_lease() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        handle.touch().await;

        let guard = spawn_strategy_lease_guard(
            StdArc::clone(&manager),
            session_id.clone(),
            Duration::from_millis(500),
        );
        tokio::time::timeout(Duration::from_secs(30), guard)
            .await
            .expect("the guard must return once it has tripped")
            .expect("the guard must not panic");
        assert_eq!(
            handle.state(),
            Some(SessionState::KillSwitchTripped),
            "the guard must run the cancel path, not just log"
        );
    }

    /// The guard decides on the session's policy lease and nothing else. It used
    /// to take a caller-supplied budget for its own decision and then trip on the
    /// session's, so a guard configured with the longer budget never fired at all
    /// — the session's lease could lapse indefinitely with only the in-process
    /// watchdog left to notice. A guard handed no budget of its own now fires on
    /// the session's 600ms lease.
    #[tokio::test(start_paused = true)]
    async fn the_guard_decides_on_the_session_policy_lease() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        handle.touch().await;

        let guard = spawn_strategy_lease_guard(
            StdArc::clone(&manager),
            session_id.clone(),
            Duration::from_millis(500),
        );
        tokio::time::timeout(Duration::from_secs(30), guard)
            .await
            .expect("the guard must fire on the session's own lease")
            .expect("the guard must not panic");
        assert_eq!(
            handle.state(),
            Some(SessionState::KillSwitchTripped),
            "the session's policy lease is the only budget the guard knows"
        );
    }

    /// The lease boundary is exclusive: an elapsed time exactly equal to the
    /// lease has not lapsed it. `tokio::time::sleep` rounds up to the next
    /// millisecond and can never land on the boundary, so the lease clock is
    /// backdated to it instead.
    #[tokio::test(start_paused = true)]
    async fn the_strategy_lease_guard_respects_the_exact_lease_boundary() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _) =
            attach_anonymous(&manager, Some(kill_switch_policy(600))).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        manager.begin_reconcile(&session_id).await.expect("test setup");
        manager.complete_reconcile(&session_id, 0).await.expect("test setup");
        handle.touch().await;
        backdate_lease(&handle, Duration::from_millis(100)).await;

        let guard = spawn_strategy_lease_guard(
            StdArc::clone(&manager),
            session_id.clone(),
            Duration::from_millis(500),
        );
        // Ticks land at 500ms (elapsed 600ms == lease: no trip) and 1000ms
        // (elapsed 1100ms: trip). Let the first tick run, then assert the
        // session is still trading before allowing the second.
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            handle.state(),
            Some(SessionState::Active),
            "an elapsed time exactly equal to the lease is not a lapse"
        );
        tokio::time::timeout(Duration::from_secs(30), guard)
            .await
            .expect("the guard must fire once the lease truly lapses")
            .expect("the guard must not panic");
        assert_eq!(
            handle.state(),
            Some(SessionState::KillSwitchTripped),
            "the guard must still trip past the boundary"
        );
    }

    /// The mock's `sync_state` must hand back a usable recovery snapshot: a
    /// strictly increasing watermark starting at 1 (not the old constant 0,
    /// which made resume-token paths untestable), a real snapshot timestamp,
    /// the seeded USD balance, and exactly the open orders.
    #[tokio::test(start_paused = true)]
    async fn mock_sync_state_hands_out_monotonic_watermarks() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let gateway = manager.gateway();
        let order =
            gateway.create_order(limit_order_req(&manager, "mine")).await.expect("order placed");

        let before = std::time::SystemTime::now();
        let first =
            gateway.sync_state(&manager.default_exchange().clone()).await.expect("sync_state");
        let after = std::time::SystemTime::now();
        let second =
            gateway.sync_state(&manager.default_exchange().clone()).await.expect("sync_state");

        // Watermarks start at 1 and increase by exactly one per snapshot.
        assert_eq!(first.snapshot_sequence, 1, "the first snapshot stamps 1, not 0");
        assert_eq!(second.snapshot_sequence, 2, "watermarks must be monotonic");
        // A real snapshot time, not a defaulted zero timestamp: it must fall
        // inside the wall-clock window bracketing the call.
        let stamped = first.snapshot_time.as_option().expect("snapshots carry their snapshot time");
        let stamped_ms = i128::from(stamped.seconds) * 1000 + i128::from(stamped.nanos) / 1_000_000;
        let ms_since_epoch = |t: std::time::SystemTime| {
            t.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i128).unwrap_or(0)
        };
        assert!(
            (ms_since_epoch(before)..=ms_since_epoch(after).max(ms_since_epoch(before)))
                .contains(&stamped_ms),
            "snapshot_time must bracket the call, got {stamped_ms}"
        );
        // The seeded USD balance and exactly the open orders.
        assert_eq!(first.balances.len(), 1);
        assert_eq!(first.balances[0].currency, "USD");
        assert_eq!(
            first.open_orders.iter().map(|o| o.id.clone()).collect::<Vec<_>>(),
            vec![order.id.clone()],
            "only open orders are reported"
        );
    }

    // ---- The replay ring ----------------------------------------------------

    /// The ring is bounded, so an overflow has to be *visible*: a client resuming
    /// from an evicted watermark sees a sequence gap and therefore runs a full
    /// `ReconcileState` instead of assuming it is caught up. Evicting the newest
    /// instead would leave a gap-free prefix that looks complete.
    #[tokio::test(start_paused = true)]
    async fn an_overflowed_ring_reports_a_gap_to_a_client_resuming_from_zero() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");

        for i in 0..=EVENT_RING_CAPACITY {
            manager
                .report_log(&session_id, worker::LogLevel::Info, format!("e{i}"), HashMap::new())
                .await
                .expect("report log");
        }

        let ring = handle.replay_after(0).await;
        assert_eq!(ring.len(), EVENT_RING_CAPACITY, "the ring keeps exactly its capacity");
        assert_eq!(ring[0].header.sequence, 2, "the oldest event must be evicted, not the newest");
        assert_eq!(
            ring[ring.len() - 1].header.sequence,
            u64::try_from(EVENT_RING_CAPACITY + 1).expect("capacity fits u64"),
            "the newest event must be the one retained"
        );
        assert!(
            SessionHandle::is_gap(0, ring[0].header.sequence),
            "a client resuming from zero must be told it missed events"
        );

        let sequences: Vec<u64> = ring.iter().map(|e| e.header.sequence).collect();
        assert!(
            sequences.windows(2).all(|w| !SessionHandle::is_gap(w[0], w[1])),
            "the retained window must be contiguous, or a client would resync forever"
        );

        // A client whose watermark is exactly the evicted event really is caught
        // up: the gap is a property of what the client knows, not of the ring.
        let from_one = handle.replay_after(1).await;
        assert_eq!(from_one.len(), EVENT_RING_CAPACITY);
        assert!(!SessionHandle::is_gap(1, from_one[0].header.sequence));
        assert_eq!(
            handle.replay_after(EVENT_RING_CAPACITY as u64).await.len(),
            1,
            "resuming from the newest watermark yields only what came after it"
        );
    }

    // ---- Log and event recording --------------------------------------------

    /// Every level must reach the ring verbatim, including the ones with no
    /// dedicated `tracing` target: a strategy filtering its own logs by level
    /// depends on the recorded value, not on which target it was printed to.
    #[tokio::test(start_paused = true)]
    async fn report_log_records_every_level_verbatim() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let levels_in = [
            worker::LogLevel::Error,
            worker::LogLevel::Warn,
            worker::LogLevel::Debug,
            worker::LogLevel::Info,
            worker::LogLevel::Unspecified,
        ];

        for level in levels_in {
            manager
                .report_log(&session_id, level, "m".to_string(), HashMap::new())
                .await
                .expect("report log");
        }

        let handle = manager.get(&session_id).await.expect("test setup");
        let ring = handle.replay_after(0).await;
        let mut logs = Vec::new();
        for event in &ring {
            if let Some(worker::strategy_event::Event::Log(log)) = event.event.as_ref() {
                logs.push(&**log);
            }
        }
        let levels: Vec<EnumValue<worker::LogLevel>> = logs.iter().map(|log| log.level).collect();
        assert_eq!(
            levels,
            vec![
                EnumValue::Known(worker::LogLevel::Error),
                EnumValue::Known(worker::LogLevel::Warn),
                EnumValue::Known(worker::LogLevel::Debug),
                EnumValue::Known(worker::LogLevel::Info),
                EnumValue::Known(worker::LogLevel::Unspecified),
            ]
        );
        let first = logs[0];
        assert!(first.timestamp.as_option().is_some(), "a log event must be timestamped");
        assert_eq!(first.session_id, session_id, "the event must name the session that sent it");
    }

    /// A log reported before `RegisterStrategy` still happened: it is on the ring and
    /// it is counted, because the count lives on the session rather than on the
    /// record. Counting from the registration boundary made every log a strategy
    /// emitted before it registered — the normal startup order — vanish from
    /// `StrategyStatus.log_events`.
    #[tokio::test(start_paused = true)]
    async fn the_log_counter_counts_events_reported_before_registration() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");

        manager
            .report_log(&session_id, worker::LogLevel::Info, "before".into(), HashMap::new())
            .await
            .expect("report log");
        assert!(
            handle.strategy_info().await.is_none(),
            "there is no record to read the count from yet"
        );
        assert_eq!(handle.log_events_count(), 1, "the session-scoped count moves regardless");

        handle
            .set_strategy(StrategyInfo {
                id: "s-1".to_string(),
                name: "grid".to_string(),
                started_at_ms: 0,
                orders_submitted: 0,
                log_events: 0,
            })
            .await;
        manager
            .report_log(&session_id, worker::LogLevel::Info, "after".into(), HashMap::new())
            .await
            .expect("report log");

        let info = handle.strategy_info().await.expect("registered strategy");
        assert_eq!(info.log_events, 2, "both logs count, including the pre-registration one");
        assert_eq!(
            handle.replay_after(0).await.len(),
            2,
            "both events are on the ring regardless of registration"
        );
    }

    /// A log for a session that does not exist must be an error, not a silent
    /// success: a strategy that has lost its session needs to know its log channel
    /// is gone rather than assume its diagnostics are still being recorded.
    #[tokio::test(start_paused = true)]
    async fn report_log_for_an_unknown_session_is_not_found() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let err = manager
            .report_log(
                "a-session-that-never-existed",
                worker::LogLevel::Info,
                "m".into(),
                HashMap::new(),
            )
            .await
            .expect_err("an unknown session must be refused");
        assert!(matches!(err, ManagerError::NotFound(_)), "got {err:?}");
    }

    /// An order update must carry the venue's record and the update kind: a
    /// strategy acts on `update_type` ("fill" vs "canceled") and on the order's own
    /// remaining quantity, so a synthesised stub would be indistinguishable from a
    /// real fill.
    #[tokio::test(start_paused = true)]
    async fn publish_order_update_records_the_order_and_the_kind() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        let order = manager
            .gateway()
            .create_order(limit_order_req(&manager, "grid-fill"))
            .await
            .expect("order placed");

        manager
            .publish_order_update(&session_id, order.clone(), "fill".to_string())
            .await
            .expect("publish order update");

        let ring = handle.replay_after(0).await;
        let updates: Vec<&worker::OrderUpdate> = ring
            .iter()
            .filter_map(|event| match event.event.as_ref() {
                Some(worker::strategy_event::Event::OrderUpdate(update)) => Some(&**update),
                _ => None,
            })
            .collect();
        assert_eq!(updates.len(), 1, "exactly one order update must be recorded");
        assert_eq!(updates[0].update_type, "fill", "the update kind must not be normalised away");
        assert_eq!(updates[0].order.as_option().expect("order").id, order.id);
        assert_eq!(ring[0].resume_token, "1", "an order update must be resumable");
    }

    /// `StrategyStatus.orders_submitted` is served from the handle's counter, not
    /// from the strategy record: a session that traded before `RegisterStrategy`
    /// would otherwise report zero, and a supervisor would read that as idle. The
    /// same holds for the log count — the record supplies identity, the handle
    /// supplies the numbers.
    #[tokio::test(start_paused = true)]
    async fn strategy_info_overrides_the_recorded_counts() {
        let manager = test_manager(rust_decimal_macros::dec!(1));
        let (session_id, _) = attach_anonymous(&manager, None).await.expect("attach");
        let handle = manager.get(&session_id).await.expect("test setup");
        assert!(handle.strategy_info().await.is_none(), "a fresh session has no strategy record");

        handle
            .set_strategy(StrategyInfo {
                id: "s-1".to_string(),
                name: "grid".to_string(),
                started_at_ms: 42,
                orders_submitted: 99,
                log_events: 7,
            })
            .await;
        // The record claims 99 submissions and 7 logs; the handle has neither.
        let info = handle.strategy_info().await.expect("strategy record");
        assert_eq!(info.id, "s-1");
        assert_eq!(info.name, "grid");
        assert_eq!(info.started_at_ms, 42);
        assert_eq!(
            info.log_events, 0,
            "the log count comes from the handle too, not from the record"
        );
        assert_eq!(info.orders_submitted, 0, "the handle's counter is authoritative");

        for _ in 0..3 {
            handle.record_order_submitted().await;
        }
        let after = handle.strategy_info().await.expect("strategy record");
        assert_eq!(after.orders_submitted, 3, "the counter must win over the stale record");
        assert_eq!(handle.orders_submitted_count(), 3);
    }

    // ---- Capabilities -------------------------------------------------------

    /// `Capabilities::all` must wire all four ports from one backend, and the
    /// default must wire none: a proxy that saw a half-populated set answers
    /// `unimplemented` for a capability the backend really does have.
    #[test]
    fn capabilities_all_wires_every_port_and_default_wires_none() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(1)));
        let all = Capabilities::all(StdArc::clone(&adapter));
        assert!(all.funding.is_some(), "funding must be declared");
        assert!(all.triggers.is_some(), "conditional orders must be declared");
        assert!(all.wallet.is_some(), "the wallet must be declared");
        assert!(all.ops.is_some(), "venue ops must be declared");

        let none = Capabilities::default();
        assert!(none.funding.is_none() && none.triggers.is_none());
        assert!(none.wallet.is_none() && none.ops.is_none());
    }

    /// The hand-rolled `Debug` prints presence booleans only. A derived `Debug`
    /// would dump the backends themselves, and these hold venue credentials, so
    /// the rendered form is pinned: adding a field that formats a backend would
    /// break this rather than leak.
    #[test]
    fn the_capabilities_debug_renders_presence_booleans_only() {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(1)));
        assert_eq!(
            format!("{:?}", Capabilities::all(StdArc::clone(&adapter))),
            "Capabilities { funding: true, triggers: true, wallet: true, ops: true }"
        );
        assert_eq!(
            format!("{:?}", Capabilities::default()),
            "Capabilities { funding: false, triggers: false, wallet: false, ops: false }"
        );
    }
}
