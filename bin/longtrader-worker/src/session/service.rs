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

use std::sync::Arc;

use connectrpc::{
    InboundStream, PreEncoded, RequestContext, Response, ServiceRequest, error::ConnectError,
};
use futures_util::StreamExt;

use super::{ClientIdentity, ManagerError, SessionManager, SessionState, time};
use crate::proto::{common, worker};

/// Build the structured `longtrader.common.v1.ErrorDetail` carrying a stable
/// reason. Split out from [`error_detail`] so a test can assert the reason
/// without base64-decoding the wire payload.
fn error_detail_message(reason: common::ErrorReason) -> common::ErrorDetail {
    common::ErrorDetail {
        reason: buffa::EnumValue::Known(reason),
        domain: "longtrader.worker".to_string(),
        ..Default::default()
    }
}

/// Attach a stable [`common::ErrorReason`] so clients branch on the typed
/// reason instead of matching the human-readable message text.
fn error_detail(reason: common::ErrorReason) -> connectrpc::ErrorDetail {
    connectrpc::ErrorDetail::from_message(
        "longtrader.common.v1.ErrorDetail",
        &error_detail_message(reason),
    )
}

impl From<ManagerError> for ConnectError {
    fn from(err: ManagerError) -> Self {
        match err {
            ManagerError::NotFound(msg) => Self::not_found(msg),
            ManagerError::Unauthenticated => Self::unauthenticated("authentication failed"),
            ManagerError::InvalidArgument(msg) => Self::invalid_argument(msg),
            // `failed_precondition` (not `unavailable`): retrying without first
            // reconciling can never succeed. The stable reason rides in a
            // structured `ErrorDetail`; the message text is for logs only.
            ManagerError::SyncInProgress { .. } => Self::failed_precondition(err.to_string())
                .with_detail(error_detail(common::ErrorReason::SyncInProgress)),
            // The stop itself succeeded (the message names the terminal state it
            // reached); what failed is the venue's willingness to cancel. `internal`
            // because there is no code for "your order may still be resting": the
            // client must not read the session as cleanly unwound, and it does not
            // learn that from a success.
            ManagerError::CancelsFailed { .. } => Self::internal(err.to_string()),
            ManagerError::Port(e) => Self::internal(e.to_string()),
        }
    }
}

#[cfg(test)]
mod error_detail_tests {
    use super::*;

    /// The reason must be the typed enum value, not a string token that a
    /// future message rewording could silently break.
    #[test]
    fn sync_in_progress_carries_the_typed_reason() {
        let detail = error_detail_message(common::ErrorReason::SyncInProgress);
        assert!(matches!(
            detail.reason,
            buffa::EnumValue::Known(common::ErrorReason::SyncInProgress)
        ));
        assert_eq!(detail.domain, "longtrader.worker");
    }

    /// The Connect detail must name the contract message the client decodes.
    #[test]
    fn detail_names_the_contract_message() {
        let detail = error_detail(common::ErrorReason::SyncInProgress);
        assert_eq!(detail.type_url, "longtrader.common.v1.ErrorDetail");
        assert!(detail.value.is_some());
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
        let client =
            ClientIdentity { name: req.client_name.clone(), version: req.client_version.clone() };
        let (session_id, heartbeat_interval_ms) = self
            .manager
            .attach_with_reconnect(&req.token, policy, &req.session_id, &client)
            .await?;
        let resp = worker::AttachSessionResponse {
            session_id,
            heartbeat_interval_ms,
            server_time: buffa::MessageField::some(time::now_ts()),
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
        handle.touch().await;
        let resp = worker::KeepAliveResponse {
            server_time_ns: time::now_ns(),
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
        //
        // The lease/scope parsing is NOT re-implemented here: `apply_policy` is the
        // single owner, so attach, reconnect and this RPC cannot drift on what
        // `SCOPE_UNSPECIFIED` or an out-of-bounds lease mean. It also rejects a
        // policy without applying any part of it, so a caller can never believe it
        // has a lease the worker is not enforcing.
        if let Some(policy) = req.policy.as_option() {
            // Atomic read-modify-write: the clone-mutate-write this replaces let
            // a concurrent reconnect (or a second SetKillSwitchPolicy) land
            // between the read and the write and silently overwrite it.
            handle.update_policy(|current| super::apply_policy(current, policy)).await?;
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
        handle
            .set_strategy(super::StrategyInfo {
                id: strategy_id.clone(),
                name: req.name.clone(),
                started_at_ms: time::now_ns() / 1_000_000,
                orders_submitted: 0,
                log_events: 0,
            })
            .await;
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
        let info = handle.strategy_info().await;
        let resp = worker::StrategyStatusResponse {
            state: buffa::EnumValue::Known(
                handle.state().map_or(worker::SessionState::Attached, SessionState::to_proto),
            ),
            strategy_id: info.as_ref().map_or_default(|i| i.id.clone()),
            name: info.as_ref().map_or_default(|i| i.name.clone()),
            started_at: info.as_ref().map_or(buffa::MessageField::none(), |i| {
                buffa::MessageField::some(time::timestamp_from_ms(i.started_at_ms))
            }),
            // Reported from the handle, not the strategy record, so a session
            // that traded before `RegisterStrategy` still reports its count.
            orders_submitted: handle.orders_submitted_count(),
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
        // Cancel + transition live in the manager so the RPC and embedders
        // share one implementation (and one tested path).
        let final_state =
            self.manager.stop_session(&req.session_id, req.cancel_open_orders).await?;
        let resp = worker::StopStrategyResponse {
            final_state: buffa::EnumValue::Known(
                final_state.map_or(worker::SessionState::GracefulShutdown, SessionState::to_proto),
            ),
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
        // resume_token is the opaque cursor (sequence as string) for resuming
        // from the ring buffer after `after_seq`.
        // Sequence is gap-free per subscription; gaps trigger resync
        // (see `crate::ports::is_sequence_gap`).
        let after_seq: u64 = if req.resume_token.is_empty() {
            0
        } else {
            req.resume_token.parse().map_err(|_| {
                ConnectError::invalid_argument(format!(
                    "bad resume_token {:?}; expected decimal sequence",
                    req.resume_token
                ))
            })?
        };
        // Atomic replay/subscribe handoff. The order matters: subscribing
        // *first* and snapshotting the sequence watermark *second* partitions
        // the event space exactly once —
        //   - events with `sequence <= watermark` are already in the ring, so the replay (filtered
        //     to the watermark) delivers them and the receiver never will;
        //   - events published after the subscription are delivered by the receiver and excluded
        //     from the replay.
        // Replaying before subscribing — the previous order — left everything
        // published in between absent from both: a silent gap the client could
        // detect but never repair.
        let mut rx = handle.subscribe();
        let watermark = handle.current_seq();
        let replay = handle.replay_after(after_seq).await;
        let stream = async_stream::stream! {
            for event in replay {
                if event.header.as_option().is_some_and(|h| h.sequence > watermark) {
                    // Published after the subscription: the receiver owns it.
                    continue;
                }
                yield Ok((*event).clone());
            }
            loop {
                match rx.recv().await {
                    Ok(event) => yield Ok((*event).clone()),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // The ring/broadcast fell behind: continuity is broken
                        // and only the client can repair it (re-fetch the
                        // snapshot / full ReconcileState). Ending the stream is
                        // that signal — swallowing the lag would leave a slow
                        // consumer silently behind forever.
                        tracing::warn!(
                            session = %req.session_id,
                            lagged = n,
                            "event stream lagged; terminating so the client resyncs"
                        );
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(Response::stream(stream))
    }
}

/// Response-contract tests for the service handlers.
///
/// The handlers build the reply messages by hand, so what the *client* receives
/// is a property of this layer alone: a field dropped from a struct literal
/// here is invisible to every manager-level test. Each test drives the handler
/// the way the dispatcher does (encode → decode a view → call → decode the
/// pre-encoded reply) and asserts on the decoded reply.
#[cfg(test)]
mod service_contract_tests {
    use std::sync::Arc as StdArc;

    use buffa::{HasMessageView, Message};
    use bytes::Bytes;
    use connectrpc::{CodecFormat, Encodable, RequestContext, ServiceRequest};

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::TradingGateway as _,
        proto::{trading, worker, worker::WorkerSessionService as _},
    };

    /// A service over the mock venue, with the mock kept in hand.
    fn test_service() -> (StdArc<MockAdapter>, StdArc<WorkerSessionServiceImpl>) {
        let adapter = StdArc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let gateway: StdArc<dyn crate::ports::TradingGateway> = adapter.clone();
        let market: StdArc<dyn crate::ports::MarketDataSource> = adapter.clone();
        let manager = StdArc::new(SessionManager::new(
            None,
            crate::ports::exchange_id("mock", ""),
            gateway,
            market,
        ));
        (adapter, StdArc::new(WorkerSessionServiceImpl { manager }))
    }

    /// Call a unary handler the way the dispatcher does: encode the request,
    /// decode a view over the same bytes, hand both to the handler, then decode
    /// the pre-encoded reply. The round trip is what makes the assertions below
    /// about *what the client receives*, not about internal state.
    async fn attach_reply(
        svc: &WorkerSessionServiceImpl,
        request: worker::AttachSessionRequest,
    ) -> worker::AttachSessionResponse {
        let body = Bytes::from(request.encode_to_vec());
        let view = worker::AttachSessionRequest::decode_view(&body).expect("the request decodes");
        let req = ServiceRequest::from_parts(&view, &body);
        let resp = svc
            .attach_session(RequestContext::new(Default::default()), req)
            .await
            .expect("a well-formed attach must be accepted");
        worker::AttachSessionResponse::decode_from_slice(
            &resp.body.encode(CodecFormat::Proto).expect("the reply encodes"),
        )
        .expect("the reply decodes as AttachSessionResponse")
    }

    async fn reconcile_reply(
        svc: &WorkerSessionServiceImpl,
        session_id: &str,
    ) -> worker::ReconcileStateResponse {
        let request = worker::ReconcileStateRequest {
            session_id: session_id.to_string(),
            ..Default::default()
        };
        let body = Bytes::from(request.encode_to_vec());
        let view = worker::ReconcileStateRequest::decode_view(&body).expect("the request decodes");
        let req = ServiceRequest::from_parts(&view, &body);
        let resp = svc
            .reconcile_state(RequestContext::new(Default::default()), req)
            .await
            .expect("a reconcile on an attached session must be accepted");
        worker::ReconcileStateResponse::decode_from_slice(
            &resp.body.encode(CodecFormat::Proto).expect("the reply encodes"),
        )
        .expect("the reply decodes as ReconcileStateResponse")
    }

    /// `attach_session` answers with everything the client needs to drive the
    /// lease: the id, the negotiated heartbeat, a server clock and the
    /// capabilities it may feature-detect. A field dropped here is invisible
    /// until a client stalls on a heartbeat it never negotiated.
    #[tokio::test]
    async fn attach_session_advertises_the_full_lease() {
        let (_mock, svc) = test_service();
        let resp = attach_reply(
            &svc,
            worker::AttachSessionRequest {
                token: "t".to_string(),
                client_name: "test".to_string(),
                client_version: "0.0.0".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert!(!resp.session_id.is_empty(), "the session id binds every later call");
        assert_eq!(
            resp.heartbeat_interval_ms,
            crate::session::DEFAULT_HEARTBEAT_MS,
            "the client paces its KeepAlive on the negotiated interval"
        );
        assert!(resp.server_time.as_option().is_some(), "the server clock anchors lease math");
        assert_eq!(
            resp.capabilities,
            vec!["reconcile".to_string(), "kill_switch".to_string(), "event_replay".to_string()],
            "capabilities are what the client feature-detects against"
        );
    }

    /// `reconcile_state` returns the atomic snapshot verbatim, watermark
    /// included: the watermark is what lets a client replay buffered deltas
    /// after recovery without a multi-RPC race, and the balances are the
    /// snapshot itself. A dropped field would leave the client reconciling
    /// against an empty world it believes is complete.
    #[tokio::test]
    async fn reconcile_state_returns_the_snapshot_verbatim() {
        let (mock, svc) = test_service();
        // A snapshot with content: every field pinned below has to be
        // non-empty, or "returned verbatim" and "field dropped" read the same.
        mock.set_positions(vec![trading::Position {
            symbol: "BTC/USDT".to_string(),
            ..Default::default()
        }])
        .await;
        let resting = mock
            .create_order(trading::CreateOrderRequest {
                exchange_id: buffa::MessageField::some(svc.manager.default_exchange().clone()),
                order: buffa::MessageField::some(trading::OrderRequest {
                    client_order_id: "resting-1".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
                    side: buffa::EnumValue::Known(trading::OrderSide::Buy),
                    amount: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(
                        rust_decimal_macros::dec!(1),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("test setup");

        let attached = attach_reply(
            &svc,
            worker::AttachSessionRequest { token: "t".to_string(), ..Default::default() },
        )
        .await;
        let resp = reconcile_reply(&svc, &attached.session_id).await;

        assert_eq!(resp.snapshot_sequence, 1, "the mock's first snapshot watermark");
        assert!(resp.snapshot_time.as_option().is_some(), "the snapshot is stamped in time");
        assert_eq!(resp.balances.len(), 1, "the mock venue reports one balance");
        assert_eq!(resp.positions.len(), 1, "the seeded position must be returned");
        assert_eq!(
            resp.open_orders.iter().map(|o| o.id.clone()).collect::<Vec<_>>(),
            vec![resting.id.clone()],
            "the resting order must be returned"
        );
    }

    /// `register_strategy` stamps the strategy record with the wall-clock start
    /// in **milliseconds**. The ns-to-ms conversion is the whole content of
    /// `started_at_ms`: a `%` where the `/` belongs reports the sub-millisecond
    /// remainder, and every reader of the age of a strategy is then wrong by
    /// six orders of magnitude.
    #[tokio::test]
    async fn register_strategy_stamps_the_start_in_epoch_milliseconds() {
        let (_mock, svc) = test_service();
        let attached = attach_reply(
            &svc,
            worker::AttachSessionRequest { token: "t".to_string(), ..Default::default() },
        )
        .await;
        let request = worker::RegisterStrategyRequest {
            session_id: attached.session_id.clone(),
            name: "grid".to_string(),
            ..Default::default()
        };
        let body = Bytes::from(request.encode_to_vec());
        let view =
            worker::RegisterStrategyRequest::decode_view(&body).expect("the request decodes");
        let req = ServiceRequest::from_parts(&view, &body);
        let resp = svc
            .register_strategy(RequestContext::new(Default::default()), req)
            .await
            .expect("a register on an attached session must be accepted");
        let resp = worker::RegisterStrategyResponse::decode_from_slice(
            &resp.body.encode(CodecFormat::Proto).expect("the reply encodes"),
        )
        .expect("the reply decodes as RegisterStrategyResponse");
        assert!(!resp.strategy_id.is_empty(), "the strategy id is what later RPCs address");

        let handle = svc.manager.get(&attached.session_id).await.expect("test setup");
        let info = handle.strategy_info().await.expect("the record was stored");
        assert_eq!(info.name, "grid");
        assert_eq!(info.id, resp.strategy_id);
        assert!(
            info.started_at_ms > 1_600_000_000_000,
            "started_at_ms must be epoch milliseconds, got {}",
            info.started_at_ms
        );
    }
}
