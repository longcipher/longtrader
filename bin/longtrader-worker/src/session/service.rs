//! Connect-RPC implementation of `longtrader.worker.v1.WorkerSessionService`.
//!
//! State machine (server-enforced, §6.3):
//! ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED
//! - `AttachSession` creates ATTACHED and negotiates `heartbeat_interval_ms`.
//! - `ReconcileState` drives `ATTACHED→SYNCING`: buffer deltas, fetch atomic snapshot stamped
//!   `snapshot_sequence`/`snapshot_time` (single RPC avoids multi-RPC watermark races), then replay
//!   deltas after snapshot and transition to ACTIVE. Orders before ACTIVE are rejected
//!   `SYNC_IN_PROGRESS`.
//! - Lease: `KeepAlive` feeds the watchdog each `heartbeat_interval_ms`; expiry after
//!   `lease_timeout` (default 3x heartbeat interval) while in ACTIVE trips `KILL_SWITCH_TRIPPED`
//!   and executes the configured `KillSwitchPolicy` scope.
//! - `KillSwitchPolicy` Scope routing: SESSION_ORDERS cancels only this session’s tracked orders;
//!   ALL_ORDERS cancels all open orders of the bound account(s); NONE logs only (see
//!   `SessionManager::trip_kill_switch`).
//! - `StopStrategy` → `GRACEFUL_SHUTDOWN`.
//!
//! Also exposes `RegisterStrategy` / `StrategyStatus` / `StopStrategy` /
//! `ReportLog` / `StreamStrategyEvents` per the FMZ observability model.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use connectrpc::{
    InboundStream, PreEncoded, RequestContext, Response, ServiceRequest, error::ConnectError,
};
use futures_util::StreamExt;

use super::{ManagerError, SessionManager, SessionState};
use crate::proto::worker;

fn now_ts() -> buffa_types::google::protobuf::Timestamp {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    buffa_types::google::protobuf::Timestamp {
        seconds: i64::try_from(dur.as_secs()).unwrap_or_default(),
        nanos: i32::try_from(dur.subsec_nanos()).unwrap_or_default(),
        ..Default::default()
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or_default())
        .unwrap_or_default()
}

impl From<ManagerError> for ConnectError {
    fn from(err: ManagerError) -> Self {
        match err {
            ManagerError::NotFound(msg) => Self::not_found(msg),
            ManagerError::Unauthenticated => Self::unauthenticated("authentication failed"),
            ManagerError::InvalidArgument(msg) => Self::invalid_argument(msg),
            ManagerError::Port(e) => Self::internal(e.to_string()),
        }
    }
}

/// Service implementation delegating to [`SessionManager`].
pub struct WorkerSessionServiceImpl {
    pub manager: Arc<SessionManager>,
}

#[allow(refining_impl_trait)]
impl worker::WorkerSessionService for WorkerSessionServiceImpl {
    async fn attach_session(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::AttachSessionRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::AttachSessionResponse>> {
        // Token validation reuses the terminal/longtrader-api `api_token` (constant-time
        // comparison). The issued `session_id` binds all subsequent requests;
        // clients may reconnect by sending the previous `session_id` in
        // `AttachSessionRequest.session_id`.
        let req = request.to_owned_message();
        let policy = req.policy.as_option().cloned();
        let (session_id, heartbeat_interval_ms) =
            self.manager.attach_with_reconnect(&req.token, policy, &req.session_id).await?;
        let resp = worker::AttachSessionResponse {
            session_id,
            heartbeat_interval_ms,
            server_time: buffa::MessageField::some(now_ts()),
            capabilities: vec![
                "reconcile".to_string(),
                "kill_switch".to_string(),
                "event_replay".to_string(),
            ],
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn keep_alive(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::KeepAliveRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::KeepAliveResponse>> {
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        handle.touch();
        let resp = worker::KeepAliveResponse {
            server_time_ns: now_ns(),
            heartbeat_interval_ms: handle.heartbeat_interval_ms,
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn reconcile_state(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::ReconcileStateRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::ReconcileStateResponse>> {
        let req = request.to_owned_message();
        // Deterministic recovery: ATTACHED → SYNCING → atomic snapshot → ACTIVE.
        // The snapshot is stamped with `snapshot_sequence` / `snapshot_time`
        // so buffered deltas can be replayed exactly after the watermark without
        // multi-RPC races (§6.3). lease_timeout = 3x heartbeat interval by default.
        self.manager.begin_reconcile(&req.session_id).await?;
        let snapshot = self
            .manager
            .gateway()
            .sync_state(self.manager.default_exchange())
            .await
            .map_err(ManagerError::from)?;
        // Stamp: snapshot_sequence is the stream watermark at snapshot time (§6.3).
        self.manager.complete_reconcile(&req.session_id, snapshot.snapshot_sequence).await?;
        let resp = worker::ReconcileStateResponse {
            snapshot_sequence: snapshot.snapshot_sequence,
            snapshot_time: snapshot.snapshot_time,
            balances: snapshot.balances,
            positions: snapshot.positions,
            open_orders: snapshot.open_orders,
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn set_kill_switch_policy(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::SetKillSwitchPolicyRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::SetKillSwitchPolicyResponse>> {
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        // KillSwitchPolicy scope routing: SESSION_ORDERS vs ALL_ORDERS vs NONE.
        // SESSION_ORDERS cancels only this session’s tracked client_order_id set;
        // ALL_ORDERS cancels every open order of the bound account(s);
        // NONE logs only — see SessionManager::trip_kill_switch.
        if let Some(policy) = req.policy.as_option() {
            let mut session_policy = handle.policy();
            if let Some(timeout) = policy.lease_timeout.as_option() {
                let millis =
                    timeout.seconds.saturating_mul(1000) + i64::from(timeout.nanos) / 1_000_000;
                let dur = std::time::Duration::from_millis(millis.max(0).cast_unsigned());
                session_policy.lease_timeout = dur.clamp(
                    std::time::Duration::from_millis(500),
                    std::time::Duration::from_secs(3600),
                );
            }
            let scope = match policy.scope {
                buffa::EnumValue::Known(scope) => scope,
                buffa::EnumValue::Unknown(_) => worker::kill_switch_policy::Scope::Unspecified,
            };
            if scope != worker::kill_switch_policy::Scope::Unspecified {
                // SCOPE_SESSION_ORDERS / SCOPE_ALL_ORDERS / SCOPE_NONE
                session_policy.scope = scope;
            }
            handle.set_policy(session_policy);
        }
        let resp = worker::SetKillSwitchPolicyResponse::default();
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn register_strategy(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::RegisterStrategyRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::RegisterStrategyResponse>> {
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        let strategy_id = ulid::Ulid::generate().to_string();
        handle.set_strategy(super::StrategyInfo {
            id: strategy_id.clone(),
            name: req.name.clone(),
            started_at_ms: now_ns() / 1_000_000,
            orders_submitted: 0,
            log_events: 0,
        });
        let resp = worker::RegisterStrategyResponse { strategy_id, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn strategy_status(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::StrategyStatusRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::StrategyStatusResponse>> {
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        let info = handle.strategy_info();
        let resp = worker::StrategyStatusResponse {
            state: buffa::EnumValue::Known(
                handle.state().map_or(worker::SessionState::Attached, SessionState::to_proto),
            ),
            strategy_id: info.as_ref().map(|i| i.id.clone()).unwrap_or_default(),
            name: info.as_ref().map(|i| i.name.clone()).unwrap_or_default(),
            started_at: info.as_ref().map_or(buffa::MessageField::none(), |i| {
                buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
                    seconds: i.started_at_ms.div_euclid(1000),
                    nanos: i32::try_from(i.started_at_ms.rem_euclid(1000) * 1_000_000)
                        .unwrap_or_default(),
                    ..Default::default()
                })
            }),
            orders_submitted: info.as_ref().map_or(0, |i| i.orders_submitted),
            log_events: info.as_ref().map_or(0, |i| i.log_events),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn stop_strategy(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::StopStrategyRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::StopStrategyResponse>> {
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        if req.cancel_open_orders {
            for coid in handle.tracked_orders().await {
                let cancel = crate::proto::trading::CancelOrderRequest {
                    exchange_id: buffa::MessageField::some(self.manager.default_exchange().clone()),
                    order_id: coid.clone(),
                    symbol: String::new(),
                    ..Default::default()
                };
                if let Err(err) = self.manager.gateway().cancel_order(cancel).await {
                    tracing::warn!(coid = %coid, error = %err, "stop_strategy cancel failed");
                }
            }
        }
        if let Some(prev) = handle.swap_state(SessionState::GracefulShutdown) {
            self.manager
                .publish_state_change(
                    &handle,
                    prev,
                    SessionState::GracefulShutdown,
                    "stop requested",
                )
                .await;
        }
        let resp = worker::StopStrategyResponse {
            final_state: buffa::EnumValue::Known(worker::SessionState::GracefulShutdown),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn report_log(
        &self,
        _ctx: RequestContext,
        mut requests: InboundStream<worker::LogEvent>,
    ) -> connectrpc::ServiceResult<PreEncoded<worker::ReportLogResponse>> {
        let mut accepted: u64 = 0;
        while let Some(item) = requests.next().await {
            let event = item?.to_owned_message();
            let level = match event.level {
                buffa::EnumValue::Known(level) => level,
                buffa::EnumValue::Unknown(_) => worker::LogLevel::Unspecified,
            };
            let fields: std::collections::HashMap<String, String> =
                event.fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            self.manager
                .report_log(&event.session_id, level, event.message.clone(), fields)
                .await?;
            accepted += 1;
        }
        let resp = worker::ReportLogResponse { accepted, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn stream_strategy_events(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, worker::StreamStrategyEventsRequest>,
    ) -> connectrpc::ServiceResult<connectrpc::ServiceStream<worker::StrategyEvent>> {
        // Streaming uses `Content-Type: application/connect+proto` with
        // 5-byte-prefixed envelopes (flags 0x00=message, 0x02=EOS JSON).
        // See `crate::envelope::{encode_envelope,decode_envelope}` and
        // `docs/bare-protocol-guide.md` §5. `connectrpc` handles framing
        // internally; this path only deals with typed `StrategyEvent`s.
        let req = request.to_owned_message();
        let handle = self.manager.get(&req.session_id).await?;
        // resume_token is the opaque cursor (sequence as string) for
        // 断点续传: replay from ring buffer after `after_seq`.
        // Sequence is gap-free per subscription; gaps trigger resync
        // (see `crate::ports::is_sequence_gap`).
        let after_seq: u64 = req.resume_token.parse().unwrap_or(0);
        let replay = handle.replay_after(after_seq).await;
        let mut rx = handle.subscribe();
        let stream = async_stream::stream! {
            for event in replay {
                yield Ok((*event).clone());
            }
            loop {
                match rx.recv().await {
                    Ok(event) => yield Ok((*event).clone()),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(Response::stream(stream))
    }
}
