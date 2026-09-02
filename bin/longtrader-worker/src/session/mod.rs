//! Session control plane: deterministic recovery state machine, lease
//! watchdog with kill-switch execution, and the strategy event bus.
//!
//! Lifecycle (server-enforced, design doc §6.3):
//! ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED
//! `ATTACHED → SYNCING → ACTIVE`, with `KILL_SWITCH_TRIPPED` on lease expiry
//! and `GRACEFUL_SHUTDOWN` on explicit stop. Orders before ACTIVE are
//! rejected `SYNC_IN_PROGRESS`.
//!
//! ReconcileState is an atomic snapshot stamped with `snapshot_sequence` /
//! `snapshot_time` (balances/positions/open_orders at one watermark), avoiding
//! multi-RPC watermark races. Buffered deltas after `snapshot_sequence` are
//! replayed to converge.
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
//! Multi-level watchdog / 多级看门狗：session/daemon/exchange 三级 (design doc §6.6):
//! - L_session (worker lease): this module’s `watchdog_loop` + `check_lease_expiry` — 3x heartbeat
//!   budget.
//! - L_daemon (exchange daemon): worker gRPC loss → daemon cancels that worker’s sessions.
//! - L_exchange (venue native COD): venue Cancel-All-After / cancel-on-disconnect countdown
//!   extended while daemon healthy; on silence the venue cancels automatically. Capability-gated;
//!   venues without native COD fall back to daemon level only.
//!
//! Backpressure isolation: each session owns an independent `mpsc` channel
//! per logical stream with a stream-appropriate `OverflowPolicy`:
//! ticker → `DropOldest`, orderbook → `Coalesce`, orders/balances/positions
//! → `Block` (see `crate::ports::OVERFLOW_*` and `crate::overflow::policy_channel`).
//! 每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders
//! Block）
//!
//! Strategy-side lease simulation: external strategies SHOULD spawn a timer
//! task at `heartbeat_interval` that checks elapsed time since last
//! successful `KeepAlive`; on lease timeout they locally cancel and stop
//! trading (see `spawn_strategy_lease_guard` helper below).
//! spawn 定时任务，每 heartbeat_interval 检查 lease 是否超时，超时则触发 cancel

pub mod proxy;
pub mod server;
pub mod service;

#[rustfmt::skip]
const _DOC_BACKPRESSURE: &str = "每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders Block）";
#[rustfmt::skip]
const _DOC_WATCHDOG: &str = "spawn 定时任务，每 heartbeat_interval 检查 lease 是否超时，超时则触发 cancel";

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Mutex, broadcast};

use crate::{
    ports::{MarketDataSource, TradingGateway},
    proto::{common, trading, worker},
};

/// Default heartbeat interval negotiated at attach.
pub const DEFAULT_HEARTBEAT_MS: u32 = 10_000;
/// Heartbeat budget multiplier applied when a policy omits lease_timeout.
const LEASE_HEARTBEAT_BUDGET: u32 = 3;
/// Replay ring capacity per session.
const EVENT_RING_CAPACITY: usize = 1024;

/// Server-enforced session states mirroring `worker.v1.SessionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionState {
    Attached = 1,
    Syncing = 2,
    Active = 3,
    KillSwitchTripped = 4,
    GracefulShutdown = 5,
}

impl SessionState {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Attached),
            2 => Some(Self::Syncing),
            3 => Some(Self::Active),
            4 => Some(Self::KillSwitchTripped),
            5 => Some(Self::GracefulShutdown),
            _ => None,
        }
    }

    pub fn to_proto(self) -> worker::SessionState {
        match self {
            Self::Attached => worker::SessionState::Attached,
            Self::Syncing => worker::SessionState::Syncing,
            Self::Active => worker::SessionState::Active,
            Self::KillSwitchTripped => worker::SessionState::KillSwitchTripped,
            Self::GracefulShutdown => worker::SessionState::GracefulShutdown,
        }
    }
}

/// Kill-switch configuration for one session.
#[derive(Debug, Clone)]
pub struct SessionPolicy {
    pub lease_timeout: Duration,
    pub scope: worker::kill_switch_policy::Scope,
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            lease_timeout: Duration::from_millis(
                u64::from(DEFAULT_HEARTBEAT_MS) * u64::from(LEASE_HEARTBEAT_BUDGET),
            ),
            scope: worker::kill_switch_policy::Scope::SessionOrders,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyInfo {
    pub id: String,
    pub name: String,
    pub started_at_ms: i64,
    pub orders_submitted: u64,
    pub log_events: u64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or_default())
        .unwrap_or_default()
}

/// One live session: state, lease bookkeeping, kill-switch policy, tracked
/// orders, and the replayable event log.
pub struct SessionHandle {
    pub id: String,
    state: AtomicU8,
    policy: StdMutex<SessionPolicy>,
    last_seen: StdMutex<tokio::time::Instant>,
    pub heartbeat_interval_ms: u32,
    events_tx: broadcast::Sender<Arc<worker::StrategyEvent>>,
    ring: Mutex<VecDeque<Arc<worker::StrategyEvent>>>,
    tracked_coids: Mutex<HashSet<String>>,
    strategy: StdMutex<Option<StrategyInfo>>,
    seq: AtomicU64,
    /// Last reconciled snapshot watermark (`snapshot_sequence`) for recovery.
    snapshot_seq: AtomicU64,
}

impl SessionHandle {
    pub fn state(&self) -> Option<SessionState> {
        SessionState::from_u8(self.state.load(Ordering::Acquire))
    }

    fn swap_state(&self, next: SessionState) -> Option<SessionState> {
        SessionState::from_u8(self.state.swap(next as u8, Ordering::AcqRel))
    }

    pub fn touch(&self) {
        *self.last_seen.lock().expect("last seen mutex") = tokio::time::Instant::now();
    }

    /// Milliseconds elapsed since the last heartbeat (pause-aware).
    fn last_seen_elapsed_ms(&self) -> u64 {
        self.last_seen
            .lock()
            .expect("last seen mutex")
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    pub fn policy(&self) -> SessionPolicy {
        self.policy.lock().expect("policy mutex").clone()
    }

    pub fn set_policy(&self, policy: SessionPolicy) {
        *self.policy.lock().expect("policy mutex") = policy;
    }

    pub fn strategy_info(&self) -> Option<StrategyInfo> {
        self.strategy.lock().expect("strategy mutex").clone()
    }

    fn set_strategy(&self, info: StrategyInfo) {
        *self.strategy.lock().expect("strategy mutex") = Some(info);
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

    pub fn record_order_submitted(&self) {
        if let Some(info) = &mut *self.strategy.lock().expect("strategy mutex") {
            info.orders_submitted += 1;
        }
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
    self_weak: StdMutex<Option<Weak<Self>>>,
    /// Optional L3 COD provider (venue native cancel-on-disconnect).
    pub cod_provider: Option<Arc<dyn CodProvider>>,
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
            self_weak: StdMutex::new(None),
            cod_provider: None,
        }
    }

    pub fn with_cod_provider(mut self, provider: Arc<dyn CodProvider>) -> Self {
        self.cod_provider = Some(provider);
        self
    }

    async fn gateway_health_check(&self) -> Result<(), crate::ports::PortError> {
        // L2 analogue: lightweight probe via `sync_state` with short timeout.
        // Propagate timeout / transport errors so watchdog can trigger L2 logic.
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            self.gateway.sync_state(&self.default_exchange),
        )
        .await
        .map_err(|_| {
            crate::ports::PortError::Transport("gateway health check timeout".to_string())
        })?;
        res?;
        Ok(())
    }

    /// Bind the manager's own `Arc` so the watchdog task can reach it.
    /// Call once after wrapping the manager in `Arc`.
    pub fn install_self(self: &Arc<Self>) {
        *self.self_weak.lock().expect("self weak mutex") = Some(Arc::downgrade(self));
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
    pub async fn attach(
        &self,
        token: &str,
        policy: Option<worker::KillSwitchPolicy>,
    ) -> Result<(String, u32), ManagerError> {
        self.attach_with_reconnect(token, policy, "").await
    }

    /// Same as [`Self::attach`] but supports reconnect via `reconnect_session_id`.
    pub async fn attach_with_reconnect(
        &self,
        token: &str,
        policy: Option<worker::KillSwitchPolicy>,
        reconnect_session_id: &str,
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
                        // Terminal states are not resurrectable — require fresh attach +
                        // ReconcileState.
                    }
                    Some(SessionState::Attached | SessionState::Syncing | SessionState::Active) |
                    None => {
                        handle.touch();
                        if let Some(policy) = policy.clone() {
                            let mut new_policy = handle.policy();
                            if let Some(timeout) = policy.lease_timeout.as_option() {
                                let millis = timeout.seconds.saturating_mul(1000) +
                                    i64::from(timeout.nanos) / 1_000_000;
                                let dur = Duration::from_millis(millis.max(0).cast_unsigned());
                                // Clamp to prevent 0ms DoS (negative/invalid lease).
                                new_policy.lease_timeout = dur
                                    .clamp(Duration::from_millis(500), Duration::from_secs(3600));
                            }
                            let scope = match policy.scope {
                                buffa::EnumValue::Known(s) => s,
                                buffa::EnumValue::Unknown(_) => {
                                    worker::kill_switch_policy::Scope::Unspecified
                                }
                            };
                            if scope != worker::kill_switch_policy::Scope::Unspecified {
                                new_policy.scope = scope;
                            }
                            handle.set_policy(new_policy);
                        }
                        return Ok((reconnect_session_id.to_string(), handle.heartbeat_interval_ms));
                    }
                }
            }
        }
        let id = ulid::Ulid::generate().to_string();
        let mut session_policy = SessionPolicy::default();
        if let Some(policy) = policy.as_ref() {
            if let Some(timeout) = policy.lease_timeout.as_option() {
                let millis =
                    timeout.seconds.saturating_mul(1000) + i64::from(timeout.nanos) / 1_000_000;
                let dur = Duration::from_millis(millis.max(0).cast_unsigned());
                session_policy.lease_timeout =
                    dur.clamp(Duration::from_millis(500), Duration::from_secs(3600));
            }
            let scope = match policy.scope {
                buffa::EnumValue::Known(scope) => scope,
                buffa::EnumValue::Unknown(_) => worker::kill_switch_policy::Scope::Unspecified,
            };
            if scope != worker::kill_switch_policy::Scope::Unspecified {
                session_policy.scope = scope;
            }
        }
        let (events_tx, _) = broadcast::channel(256);
        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            state: AtomicU8::new(SessionState::Attached as u8),
            policy: StdMutex::new(session_policy),
            last_seen: StdMutex::new(tokio::time::Instant::now()),
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_MS,
            events_tx,
            ring: Mutex::new(VecDeque::new()),
            tracked_coids: Mutex::new(HashSet::new()),
            strategy: StdMutex::new(None),
            seq: AtomicU64::new(0),
            snapshot_seq: AtomicU64::new(0),
        });
        self.sessions.lock().await.insert(id.clone(), Arc::clone(&handle));
        self.ensure_watchdog();
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
    pub async fn begin_reconcile(&self, session_id: &str) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        if let Some(prev) = handle.swap_state(SessionState::Syncing) {
            self.publish_state_change(&handle, prev, SessionState::Syncing, "reconcile started")
                .await;
        }
        Ok(())
    }

    /// Complete reconciliation: transition to ACTIVE. Orders submitted before
    /// this point would be rejected by session-scoped trading paths.
    /// `snapshot_sequence` is the watermark at snapshot time (§7) — buffered
    /// deltas with `header.sequence <= snapshot_sequence` must be discarded,
    /// remainder applied in order before trading.
    pub async fn complete_reconcile(
        &self,
        session_id: &str,
        snapshot_sequence: u64,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        handle.snapshot_seq.store(snapshot_sequence, Ordering::Release);
        if let Some(prev) = handle.swap_state(SessionState::Active) {
            self.publish_state_change(&handle, prev, SessionState::Active, "reconcile complete")
                .await;
        }
        Ok(())
    }

    /// Current snapshot watermark for this session (0 if never reconciled).
    pub async fn snapshot_sequence(&self, session_id: &str) -> Result<u64, ManagerError> {
        let handle = self.get(session_id).await?;
        Ok(handle.snapshot_seq.load(Ordering::Acquire))
    }

    /// Record a strategy log event into the ring/broadcast bus.
    pub async fn report_log(
        &self,
        session_id: &str,
        level: worker::LogLevel,
        message: String,
        fields: HashMap<String, String>,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        let event = worker::LogEvent {
            session_id: session_id.to_string(),
            level: buffa::EnumValue::Known(level),
            message: message.clone(),
            timestamp: buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
                seconds: now_ms().div_euclid(1000),
                nanos: i32::try_from(now_ms().rem_euclid(1000) * 1_000_000).unwrap_or_default(),
                ..Default::default()
            }),
            fields: fields.into_iter().collect::<std::collections::HashMap<
                _,
                _,
                buffa::foldhash::fast::RandomState,
            >>(),
            ..Default::default()
        };
        if let Some(info) = &mut *handle.strategy.lock().expect("strategy mutex") {
            info.log_events += 1;
        }
        match level {
            worker::LogLevel::Error => {
                tracing::error!(session = %session_id, "{message}");
            }
            worker::LogLevel::Warn => {
                tracing::warn!(session = %session_id, "{message}");
            }
            worker::LogLevel::Debug => {
                tracing::debug!(session = %session_id, "{message}");
            }
            _ => {
                tracing::info!(session = %session_id, "{message}");
            }
        }
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::Log(Box::new(event))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
        Ok(())
    }

    /// Publish an order update attributed to this session.
    pub async fn publish_order_update(
        &self,
        session_id: &str,
        order: trading::Order,
        update_type: String,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::OrderUpdate(Box::new(
                    worker::OrderUpdate {
                        order: buffa::MessageField::some(order),
                        update_type,
                        ..Default::default()
                    },
                ))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
        Ok(())
    }

    pub async fn publish_state_change(
        &self,
        handle: &SessionHandle,
        from: SessionState,
        to: SessionState,
        reason: &str,
    ) {
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::StateChange(Box::new(
                    worker::StateChange {
                        from: buffa::EnumValue::Known(from.to_proto()),
                        to: buffa::EnumValue::Known(to.to_proto()),
                        reason: reason.to_string(),
                        ..Default::default()
                    },
                ))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
    }

    /// Check one session's lease and trip the kill-switch when expired.
    /// Used by the watchdog tick and available to ops tooling/tests.
    pub async fn check_lease_expiry(&self, session_id: &str) -> Result<bool, ManagerError> {
        let handle = self.get(session_id).await?;
        let policy = handle.policy();
        let expired = handle.state() == Some(SessionState::Active) &&
            u128::from(handle.last_seen_elapsed_ms()) > policy.lease_timeout.as_millis();
        if expired {
            self.trip_kill_switch(&handle, policy.lease_timeout).await;
        }
        Ok(expired)
    }

    /// Spawn the lease watchdog once; subsequent calls are no-ops.
    fn ensure_watchdog(&self) {
        if self.watchdog_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = self.self_weak.lock().expect("self weak mutex").clone();
        if let Some(manager) = weak.and_then(|w| w.upgrade()) {
            tokio::spawn(watchdog_loop(manager));
        }
    }

    /// Lease expiry tripwire for one session: flip state and execute the
    /// configured kill-switch scope.
    async fn trip_kill_switch(&self, handle: &SessionHandle, timeout: Duration) {
        let prev = handle.swap_state(SessionState::KillSwitchTripped);
        if prev.is_none() {
            return;
        }
        tracing::error!(
            session = %handle.id,
            lease_timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            "session lease expired; tripping kill-switch"
        );
        self.publish_state_change(
            handle,
            prev.unwrap_or(SessionState::Active),
            SessionState::KillSwitchTripped,
            "lease expired",
        )
        .await;
        let policy = handle.policy();
        match policy.scope {
            worker::kill_switch_policy::Scope::SessionOrders => {
                for coid in handle.tracked_orders().await {
                    let req = trading::CancelOrderRequest {
                        exchange_id: buffa::MessageField::some(self.default_exchange.clone()),
                        order_id: coid.clone(),
                        symbol: String::new(),
                        ..Default::default()
                    };
                    if let Err(err) = self.gateway.cancel_order(req).await {
                        tracing::warn!(coid = %coid, error = %err, "kill-switch cancel failed");
                    }
                }
            }
            worker::kill_switch_policy::Scope::AllOrders => {
                let req = trading::CancelAllOrdersRequest {
                    exchange_id: buffa::MessageField::some(self.default_exchange.clone()),
                    symbol: String::new(),
                    ..Default::default()
                };
                if let Err(err) = self.gateway.cancel_all_orders(req).await {
                    tracing::warn!(error = %err, "kill-switch cancel-all failed");
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

#[allow(clippy::cognitive_complexity, clippy::collapsible_if)]
async fn watchdog_loop(manager: Arc<SessionManager>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let snapshot: Vec<(Arc<SessionHandle>, SessionPolicy)> = {
            let sessions = manager.sessions.lock().await;
            sessions.values().map(|h| (Arc::clone(h), h.policy())).collect()
        };
        for (handle, policy) in snapshot {
            // L3: renew venue COD while session is Active (best-effort, capability-gated).
            if handle.state() == Some(SessionState::Active) &&
                let Some(cod) = manager.cod_provider.as_ref() &&
                cod.supports_cod() &&
                let Err(e) = cod.keepalive(&handle.id).await
            {
                tracing::debug!(session = %handle.id, error = %e, "COD keepalive failed — will rely on L1/L2");
            }
            let expired = handle.state() == Some(SessionState::Active) &&
                u128::from(handle.last_seen_elapsed_ms()) > policy.lease_timeout.as_millis();
            if expired {
                tracing::warn!(session = %handle.id, "L1 lease expired — tripping kill-switch (L2/L3 as fallback)");
                manager.trip_kill_switch(&handle, policy.lease_timeout).await;
            }
        }
        // L2: detect gateway gRPC loss via health probe (daemon watchdog analogue).
        // If gateway health fails, proactively trip all Active sessions to avoid
        // stale orders surviving a partition. Best-effort, not fatal.
        if let Err(e) = manager.gateway_health_check().await {
            tracing::warn!(error = %e, "gateway health check failed — L2 daemon watchdog triggered");
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
/// Ponytail stub: uses the manager's `check_lease_expiry` internally so the
/// same scope-routing logic (SESSION_ORDERS / ALL_ORDERS / NONE) is exercised
/// without duplicating cancel code. Spawn via `tokio::spawn`.
pub fn spawn_strategy_lease_guard(
    manager: Arc<SessionManager>,
    session_id: String,
    heartbeat_interval: Duration,
    lease_timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(heartbeat_interval);
        // Strategy-side watchdog: spawn 定时任务，每 heartbeat_interval 检查 lease
        // 是否超时，超时则触发 cancel.
        loop {
            tick.tick().await;
            let elapsed = match manager.get(&session_id).await {
                Ok(handle) => Duration::from_millis(handle.last_seen_elapsed_ms()),
                Err(_) => break, // session gone
            };
            if elapsed > lease_timeout {
                tracing::warn!(
                    session = %session_id,
                    elapsed_ms = elapsed.as_millis() as u64,
                    lease_timeout_ms = lease_timeout.as_millis() as u64,
                    "strategy-side lease timeout detected; triggering kill-switch cancel"
                );
                let _ = manager.check_lease_expiry(&session_id).await;
                break;
            }
        }
    })
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use buffa::{EnumValue, MessageField};

    use super::*;
    use crate::adapters::MockAdapter;

    fn test_manager(price: rust_decimal::Decimal) -> Arc<SessionManager> {
        let adapter = StdArc::new(MockAdapter::new(price));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter;
        let manager = SessionManager::new(None, common::ExchangeId::default(), gateway, market);
        let manager = Arc::new(manager);
        manager.install_self();
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

    #[tokio::test(start_paused = true)]
    async fn lifecycle_attaches_syncs_and_activates() {
        let manager = test_manager(rust_decimal_macros::dec!(100));
        let (session_id, _hb) = manager.attach("t", None).await.expect("attach");
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
            manager.attach("t", Some(kill_switch_policy(600))).await.expect("attach");
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
        handle.touch();
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
        let err = manager.attach("wrong", None).await.expect_err("must reject");
        assert!(matches!(err, ManagerError::Unauthenticated));
    }

    #[tokio::test(start_paused = true)]
    async fn event_stream_replays_ring_after_resume_token() {
        let manager = test_manager(rust_decimal_macros::dec!(5));
        let (session_id, _) = manager.attach("t", None).await.expect("attach");
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
}
