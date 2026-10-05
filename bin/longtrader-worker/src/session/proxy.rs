//! Thin proxies exposing the unified `TradingService` / `MarketDataService`
//! to external-language strategies. Every call forwards to the worker's
//! backend adapter through the ports — no business logic lives here.
//!
//! Backpressure isolation (design doc §6.5): each session gets its own
//! independent `mpsc` channel (via `crate::overflow::policy_channel`) so a
//! slow strategy never blocks another session or the worker↔daemon ingress.
//! Overflow policies are chosen per stream kind:
//! - ticker / trades / ohlcv → `OverflowPolicy::DropOldest` (`OVERFLOW_TICKER`)
//! - orderbook → `OverflowPolicy::Coalesce` (`OVERFLOW_ORDERBOOK`)
//! - orders / balances / positions → `OverflowPolicy::Block` (`OVERFLOW_ORDERS`)
//!
//! Each `subscribe_market_data` below returns one independent bounded mpsc per
//! session wrapped by `policy_channel`; HTTP/2 flow control is per-stream so
//! one slow strategy cannot block another.
//!
//! Isolation rules: HTTP/2 flow control is per-stream/per-connection and
//! worker↔daemon ingress uses its own bounded `Coalesce` buffers for market
//! data; private events fan out per-session without sharing queue capacity.
#![allow(clippy::unused_async_trait_impl)] // passthrough stubs are intentionally await-free

use std::{collections::HashSet, sync::Arc};

use connectrpc::{PreEncoded, RequestContext, Response, ServiceRequest};

use crate::{
    ports::{
        FundingRateSource, MarketDataSource, TradingGateway, TriggerOrderGateway, VenueOpInvoker,
        WalletGateway,
    },
    proto::{account, common, market, ops, trading},
    session::{SessionHandle, SessionManager},
};

fn internal<E: std::fmt::Display>(err: E) -> connectrpc::ConnectError {
    connectrpc::ConnectError::internal(err.to_string())
}

/// Rebuild a Connect error from a gRPC status code.
///
/// `connectrpc` exposes only per-code constructors, so the mapping is written
/// out rather than derived — a new upstream code has to be added here
/// deliberately, which is the point.
fn rpc_code_to_connect(code: u32, message: &str) -> connectrpc::ConnectError {
    use connectrpc::ErrorCode as E;
    match E::from_grpc_code(code) {
        Some(E::Unknown) | None => connectrpc::ConnectError::internal(message.to_string()),
        // A cancelled upstream call is the caller's cancellation surfacing, not a
        // server fault: it has its own Connect code (and its own HTTP status), and
        // answering `internal` invites the client to retry a call that was already
        // abandoned.
        Some(E::Canceled) => connectrpc::ConnectError::canceled(message.to_string()),
        Some(E::InvalidArgument) => connectrpc::ConnectError::invalid_argument(message.to_string()),
        Some(E::DeadlineExceeded) => {
            connectrpc::ConnectError::deadline_exceeded(message.to_string())
        }
        Some(E::NotFound) => connectrpc::ConnectError::not_found(message.to_string()),
        Some(E::AlreadyExists) => connectrpc::ConnectError::already_exists(message.to_string()),
        Some(E::PermissionDenied) => {
            connectrpc::ConnectError::permission_denied(message.to_string())
        }
        Some(E::ResourceExhausted) => {
            connectrpc::ConnectError::resource_exhausted(message.to_string())
        }
        Some(E::FailedPrecondition) => {
            connectrpc::ConnectError::failed_precondition(message.to_string())
        }
        Some(E::Aborted) => connectrpc::ConnectError::aborted(message.to_string()),
        Some(E::OutOfRange) => connectrpc::ConnectError::invalid_argument(message.to_string()),
        Some(E::Unimplemented) => connectrpc::ConnectError::unimplemented(message.to_string()),
        Some(E::Internal) => connectrpc::ConnectError::internal(message.to_string()),
        Some(E::Unavailable) => connectrpc::ConnectError::unavailable(message.to_string()),
        Some(E::DataLoss) => connectrpc::ConnectError::data_loss(message.to_string()),
        Some(E::Unauthenticated) => connectrpc::ConnectError::unauthenticated(message.to_string()),
        // A code this build does not know: surface it verbatim rather than
        // guessing, since the message still names the cause.
        Some(other) => connectrpc::ConnectError::internal(format!("{:?}: {message}", other)),
    }
}

/// Map a port error onto the Connect code a client can act on.
///
/// Flattening every failure to `internal` is actively harmful: a client sees a
/// server fault for what is really a bad argument, retries forever, and a
/// genuinely missing object is indistinguishable from a broken worker. The
/// distinction matters most for the capability ports, where `unimplemented` is
/// the whole point of saying "this backend cannot do that".
fn map_port(err: crate::ports::PortError) -> connectrpc::ConnectError {
    use crate::ports::PortError;
    match err {
        PortError::InvalidArgument(m) => connectrpc::ConnectError::invalid_argument(m),
        PortError::MissingField(m) => {
            connectrpc::ConnectError::invalid_argument(format!("missing required field: {m}"))
        }
        PortError::NotFound(m) => connectrpc::ConnectError::not_found(m),
        PortError::Unsupported(m) => connectrpc::ConnectError::unimplemented(m),
        // Preserve the upstream code: the client is talking to a gateway, and
        // the venue's own verdict (rate limited, unauthenticated, insufficient
        // margin, ...) is the actionable one. An unrecognised code degrades to
        // `internal` rather than being guessed at.
        PortError::Rpc { code, message } => rpc_code_to_connect(code, &message),
        other => internal(other),
    }
}

/// The default page size when a request does not set one.
const DEFAULT_PAGE_LIMIT: u32 = 100;

/// Page size from an optional `common.v1.Pagination`, bounded and defaulted.
fn pagination_limit(pagination: Option<&common::Pagination>) -> u32 {
    let requested = pagination.map_or(0, |p| p.limit);
    let requested = u32::try_from(requested).unwrap_or(u32::MAX);
    if requested == 0 { DEFAULT_PAGE_LIMIT } else { requested.min(1000) }
}

/// Map a port ledger record onto the unified `account.v1.LedgerEntry`.
///
/// The port's amount is already signed, so it round-trips without the sign
/// being inferred from `direction` a second time (and possibly getting it
/// wrong on the way out).
fn ledger_entry_to_unified(entry: &crate::ports::LedgerEntry) -> account::LedgerEntry {
    account::LedgerEntry {
        id: entry.id.clone(),
        currency: entry.currency.clone(),
        direction: if entry.amount.is_sign_negative() { "out" } else { "in" }.to_string(),
        r#type: entry.entry_type.clone(),
        amount: longtrader_contract::ext::decimal_to_common(entry.amount).into(),
        timestamp: timestamp_from_ms(entry.time_ms).into(),
        status: if entry.completed { "completed" } else { "pending" }.to_string(),
        ..Default::default()
    }
}

/// Build a `google.protobuf.Timestamp` from Unix milliseconds.
fn timestamp_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        // The ms remainder (< 1000) scaled to nanos always fits in i32.
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
        ..Default::default()
    }
}

/// Build a unified `TriggerOrderRequest` from the wire message.
///
/// # Errors
///
/// Rejects a request with no trigger price or quantity, and an order side that
/// is neither Buy nor Sell — defaulting the side would silently invert a stop.
fn trigger_request_from_unified(
    req: Option<&trading::TriggerOrderRequest>,
) -> std::result::Result<crate::ports::TriggerOrderRequest, connectrpc::ConnectError> {
    use crate::ports::TriggerOrderRequest as PortRequest;
    let req = req.ok_or_else(|| connectrpc::ConnectError::invalid_argument("order is required"))?;
    let trigger_price = req
        .trigger_price
        .as_option()
        .ok_or_else(|| connectrpc::ConnectError::invalid_argument("trigger_price is required"))
        .and_then(|d| {
            longtrader_contract::ext::common_to_decimal(d)
                .map_err(|e| connectrpc::ConnectError::invalid_argument(e.to_string()))
        })?;
    let qty = req
        .qty
        .as_option()
        .ok_or_else(|| connectrpc::ConnectError::invalid_argument("qty is required"))
        .and_then(|d| {
            longtrader_contract::ext::common_to_decimal(d)
                .map_err(|e| connectrpc::ConnectError::invalid_argument(e.to_string()))
        })?;
    let is_buy = match req.side {
        buffa::EnumValue::Known(trading::OrderSide::Buy) => true,
        buffa::EnumValue::Known(trading::OrderSide::Sell) => false,
        buffa::EnumValue::Known(other) => {
            return Err(connectrpc::ConnectError::invalid_argument(format!(
                "order.side {other:?} is not valid for a conditional order; use BUY or SELL"
            )));
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(connectrpc::ConnectError::invalid_argument(format!(
                "unknown order side discriminant {v}"
            )));
        }
    };
    Ok(PortRequest {
        client_order_id: req.client_order_id.clone(),
        symbol: req.symbol.clone(),
        is_buy,
        trigger_price,
        qty,
        reduce_only: req.reduce_only,
    })
}

/// Map a port trigger order onto the unified wire message.
fn trigger_order_to_unified(order: &crate::ports::TriggerOrder) -> trading::TriggerOrder {
    use crate::ports::TriggerOrderStatus as PortStatus;
    let status = match order.status {
        PortStatus::Open => trading::TriggerOrderStatus::Open,
        PortStatus::Triggered => trading::TriggerOrderStatus::Triggered,
        PortStatus::Canceled => trading::TriggerOrderStatus::Canceled,
        PortStatus::Rejected => trading::TriggerOrderStatus::Rejected,
    };
    trading::TriggerOrder {
        id: order.id.clone(),
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        side: buffa::EnumValue::Known(if order.is_buy {
            trading::OrderSide::Buy
        } else {
            trading::OrderSide::Sell
        }),
        trigger_price: longtrader_contract::ext::decimal_to_common(order.trigger_price).into(),
        qty: longtrader_contract::ext::decimal_to_common(order.qty).into(),
        // The port does not model a trigger source or a post-trigger price
        // leg, so those are reported as LAST / MARKET rather than invented.
        trigger_type: buffa::EnumValue::Known(trading::TriggerPriceType::Last),
        order_price: buffa::MessageField::none(),
        order_type: buffa::EnumValue::Known(trading::OrderType::Market),
        reduce_only: order.reduce_only,
        status: buffa::EnumValue::Known(status),
        created_at: timestamp_from_ms(order.created_at_ms).into(),
        order_id: order.order_id.clone(),
        triggered_at: order
            .triggered_at_ms
            .map_or_else(buffa::MessageField::none, |ms| timestamp_from_ms(ms).into()),
        ..Default::default()
    }
}

/// A capability this backend never declared.
///
/// `unimplemented` rather than `internal`: retrying cannot help, and a caller
/// that sees `internal` will retry forever against a permanent answer.
fn not_implemented(what: &str) -> connectrpc::ConnectError {
    connectrpc::ConnectError::unimplemented(format!(
        "this backend does not expose '{what}'; use the mock backend or a venue-native daemon"
    ))
}

fn resolved_exchange<S: buffa::ProtoBox<common::ExchangeId>>(
    field: &buffa::MessageField<common::ExchangeId, S>,
    default_exchange: &common::ExchangeId,
) -> common::ExchangeId {
    match field.as_option() {
        Some(id) => id.clone(),
        None => default_exchange.clone(),
    }
}

/// Forwards unified trading RPCs onto the [`TradingGateway`] port.
pub struct TradingProxy {
    pub gateway: Arc<dyn TradingGateway>,
    /// Used when a request omits `exchange_id`.
    pub default_exchange: common::ExchangeId,
    /// Session control plane, consulted only by the order-submitting RPCs.
    /// `None` disables session gating and attribution (standalone proxy use).
    pub manager: Option<Arc<SessionManager>>,
    /// Venue-side conditional orders. `None` answers `unimplemented`.
    pub triggers: Option<Arc<dyn TriggerOrderGateway>>,
    /// Wallet ledger and transfers. `None` answers `unimplemented`.
    pub wallet: Option<Arc<dyn WalletGateway>>,
}

/// Apply the session gate to a request carrying `trading.v1.*.session_id`.
///
/// Returns the session handle a submitted order must be attributed to, or
/// `None` for an unscoped (operator / CLI) call. Propagates
/// [`ManagerError::SyncInProgress`] when a session-scoped submission arrives
/// before that session is ACTIVE, which is what `worker.proto` mandates.
async fn gate_submission(
    manager: Option<&Arc<SessionManager>>,
    session_id: &str,
) -> Result<Option<Arc<SessionHandle>>, connectrpc::error::ConnectError> {
    match manager {
        Some(manager) => manager.authorize_order_submission(session_id).await.map_err(Into::into),
        None => Ok(None),
    }
}

/// Attribute submitted orders to the session that passed the gate.
async fn attribute(
    manager: Option<&Arc<SessionManager>>,
    handle: Option<&Arc<SessionHandle>>,
    orders: &[trading::Order],
) {
    if let (Some(manager), Some(handle)) = (manager, handle) {
        manager.record_submitted_orders(handle, orders).await;
    }
}

/// Ask the venue which of a failed batch's legs it actually accepted.
///
/// `batch_create_orders` has no partial-success channel — an `Err` discards the
/// per-order results — but the venue may already hold the legs it placed before
/// the failure (the mock places them one by one, exactly like a real venue). Those
/// legs would otherwise be live on the venue while being invisible to
/// `KillSwitchPolicy.SCOPE_SESSION_ORDERS` and to `StrategyStatus.orders_submitted`,
/// i.e. unreachable by the kill switch.
///
/// What is recoverable, precisely: the venue's open-order book, filtered by the
/// `client_order_id`s this batch asked for. What is *not* recoverable is any leg
/// the venue no longer reports as open — one that was filled or cancelled in the
/// meantime is not attributed, because an order the venue has already closed
/// cannot be cancelled by a later kill-switch and `record_submitted_orders` only
/// counts what the session could still unwind. If the query itself fails, the
/// legs stay untracked: that residual risk is logged rather than papered over
/// with a guess.
async fn accepted_batch_orders(
    gateway: &dyn crate::ports::TradingGateway,
    exchange_id: &common::ExchangeId,
    requested: &[String],
) -> Vec<trading::Order> {
    // An empty client_order_id matches every anonymous order on the venue, so it
    // is never used as a match key.
    let wanted: HashSet<&str> =
        requested.iter().map(String::as_str).filter(|c| !c.is_empty()).collect();
    let open = gateway
        .fetch_open_orders(trading::FetchOpenOrdersRequest {
            exchange_id: buffa::MessageField::some(exchange_id.clone()),
            // Unscoped: a batch can span symbols, and a per-symbol query would
            // miss every leg outside the first symbol's answer.
            symbol: String::new(),
            ..Default::default()
        })
        .await;
    match open {
        Ok(open) => {
            open.into_iter().filter(|o| wanted.contains(o.client_order_id.as_str())).collect()
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                "batch failed and its accepted legs could not be recovered; \
                 those orders are not tracked by the session kill-switch"
            );
            Vec::new()
        }
    }
}

#[allow(refining_impl_trait)]
impl trading::TradingService for TradingProxy {
    async fn create_order(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CreateOrderRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CreateOrderResponse>> {
        let mut req = request.to_owned_message();
        let session = gate_submission(self.manager.as_ref(), &req.session_id).await?;
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let order = self.gateway.create_order(req).await.map_err(map_port)?;
        attribute(self.manager.as_ref(), session.as_ref(), std::slice::from_ref(&order)).await;
        let resp = trading::CreateOrderResponse {
            order: buffa::MessageField::some(order),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn create_orders(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CreateOrdersRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CreateOrdersResponse>> {
        let mut req = request.to_owned_message();
        let session = gate_submission(self.manager.as_ref(), &req.session_id).await?;
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        req.exchange_id = buffa::MessageField::some(exchange.clone());
        // Captured before the round trip: if the batch fails, these are the ids
        // the recovery query matches the venue's open orders against.
        let requested: Vec<String> = req.orders.iter().map(|o| o.client_order_id.clone()).collect();
        let orders = match self.gateway.batch_create_orders(req).await {
            Ok(orders) => orders,
            Err(error) => {
                // The batch failed *part-way* on the venue, so the legs it did
                // accept are still live. Attribute them before returning, or the
                // session's kill-switch would never reach them.
                if session.is_some() {
                    let accepted =
                        accepted_batch_orders(self.gateway.as_ref(), &exchange, &requested).await;
                    attribute(self.manager.as_ref(), session.as_ref(), &accepted).await;
                }
                return Err(map_port(error));
            }
        };
        attribute(self.manager.as_ref(), session.as_ref(), &orders).await;
        let resp = trading::CreateOrdersResponse { orders, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn cancel_order(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CancelOrderRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CancelOrderResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let order = self.gateway.cancel_order(req).await.map_err(map_port)?;
        let resp = trading::CancelOrderResponse {
            order: buffa::MessageField::some(order),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn cancel_all_orders(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CancelAllOrdersRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CancelAllOrdersResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let orders = self.gateway.cancel_all_orders(req).await.map_err(map_port)?;
        let resp = trading::CancelAllOrdersResponse { orders, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_open_orders(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::FetchOpenOrdersRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::FetchOpenOrdersResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let orders = self.gateway.fetch_open_orders(req).await.map_err(map_port)?;
        let resp = trading::FetchOpenOrdersResponse { orders, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    // ---- Account / history queries ----
    //
    // These mirror the `TradingGateway` port one-to-one; the worker proxies
    // them to the backend so external-language strategies get the same surface
    // as native strategies.

    async fn get_account(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::GetAccountRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetAccountResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.get_account(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn get_positions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::GetPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetPositionsResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.get_positions(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn get_order_history(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::GetOrderHistoryRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetOrderHistoryResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.get_order_history(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn get_closed_positions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::GetClosedPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetClosedPositionsResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.get_closed_positions(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn close_position(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::ClosePositionRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::ClosePositionResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.close_position(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn close_all_positions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CloseAllPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CloseAllPositionsResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.close_all_positions(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn modify_position(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::ModifyPositionRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::ModifyPositionResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let resp = self.gateway.modify_position(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    // ---- Venue capabilities ---------------------------------------------
    //
    // Both session-scoped: a conditional order and a transfer are both
    // financially meaningful, so a session that has not reconciled must not be
    // able to place one. They are also resolved against the default exchange,
    // so an unqualified call behaves like every other trading RPC here.
    //
    // What session binding deliberately does *not* do is make a conditional
    // order cancellable by the kill-switch: a backstop that vanished when the
    // lease expired would defeat its purpose. Only the session id is honoured
    // here, for gating.

    async fn create_trigger_order(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CreateTriggerOrderRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CreateTriggerOrderResponse>> {
        let triggers =
            self.triggers.as_ref().ok_or_else(|| not_implemented("conditional orders"))?;
        let req = request.to_owned_message();
        // Resolved once: the port call takes the venue directly, so writing the
        // resolved id back into the request and resolving it a second time out of
        // that same field would be the same answer reached by a longer route.
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        // Gate before validating, so a pre-ACTIVE session learns it is out of
        // sync rather than that its arguments were wrong.
        gate_submission(self.manager.as_ref(), &req.session_id).await?;
        // Validate before the round trip: a request the venue would reject (or
        // worse, misread) is caught here rather than after a live order.
        let order_req = trigger_request_from_unified(req.order.as_option())?;
        let order = triggers.create_trigger_order(&exchange, order_req).await.map_err(map_port)?;
        let resp = trading::CreateTriggerOrderResponse {
            order: buffa::MessageField::some(trigger_order_to_unified(&order)),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn cancel_trigger_order(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CancelTriggerOrderRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CancelTriggerOrderResponse>> {
        let triggers =
            self.triggers.as_ref().ok_or_else(|| not_implemented("conditional orders"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        let canceled = triggers
            .cancel_trigger_order(&exchange, &req.order_id, &req.symbol)
            .await
            .map_err(map_port)?;
        // The venue's record, so the caller can confirm *which* order was
        // cancelled. A response rebuilt from the request's own id/symbol would
        // look identical whether the right order was cancelled or not.
        let resp = trading::CancelTriggerOrderResponse {
            order: buffa::MessageField::some(trigger_order_to_unified(&canceled)),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn list_trigger_orders(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::ListTriggerOrdersRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::ListTriggerOrdersResponse>> {
        let triggers =
            self.triggers.as_ref().ok_or_else(|| not_implemented("conditional orders"))?;
        let req = request.to_owned_message();
        let orders = triggers
            .list_trigger_orders(
                &resolved_exchange(&req.exchange_id, &self.default_exchange),
                &req.symbols,
            )
            .await
            .map_err(map_port)?;
        let orders: Vec<trading::TriggerOrder> =
            orders.iter().map(trigger_order_to_unified).collect();
        let resp = trading::ListTriggerOrdersResponse { orders, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_ledger_entries(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::FetchLedgerEntriesRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::FetchLedgerEntriesResponse>> {
        let wallet = self.wallet.as_ref().ok_or_else(|| not_implemented("wallet ledger"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        let entries = wallet
            .list_ledger_entries(
                &exchange,
                &req.currency,
                &req.r#type,
                pagination_limit(req.pagination.as_option()),
            )
            .await
            .map_err(map_port)?;
        let entries: Vec<account::LedgerEntry> =
            entries.iter().map(ledger_entry_to_unified).collect();
        let total = u32::try_from(entries.len()).unwrap_or(u32::MAX);
        let resp = trading::FetchLedgerEntriesResponse {
            entries,
            page: common::Page { next_cursor: String::new(), total, ..Default::default() }.into(),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn transfer(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::TransferRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::TransferResponse>> {
        let wallet = self.wallet.as_ref().ok_or_else(|| not_implemented("transfers"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        gate_submission(self.manager.as_ref(), &req.session_id).await?;
        let amount = req
            .amount
            .as_option()
            .ok_or_else(|| connectrpc::ConnectError::invalid_argument("amount is required"))?;
        let amount = longtrader_contract::ext::common_to_decimal(amount)
            .map_err(|e| connectrpc::ConnectError::invalid_argument(e.to_string()))?;
        let receipt = wallet
            .transfer(&exchange, &req.asset, amount, &req.dest_label, &req.client_transfer_id)
            .await
            .map_err(map_port)?;
        let resp = trading::TransferResponse {
            transfer_id: receipt.transfer_id,
            entry: receipt
                .entry
                .as_ref()
                .map_or_else(buffa::MessageField::none, |e| ledger_entry_to_unified(e).into()),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }
}

/// Forwards unified market data RPCs onto the [`MarketDataSource`] port.
pub struct MarketDataProxy {
    pub market: Arc<dyn MarketDataSource>,
    /// Used when a request omits `exchange_id`, so the funding RPCs behave like
    /// every other method on this service rather than being the only ones that
    /// reject an unqualified request.
    pub default_exchange: common::ExchangeId,
    /// Perpetual funding rates. `None` answers `unimplemented`.
    pub funding: Option<Arc<dyn FundingRateSource>>,
}

/// Forwards `ops.v1.VenueOpService` onto the [`VenueOpInvoker`] port.
///
/// Self-describing discovery plus dynamic invocation, for the venue-specific
/// operations (`account.transfer`, `margin.borrow`, ...) that do not fit the
/// universal trading contract. A strategy reads the parameter schema first and
/// only then calls, so a venue that lacks an op reports it absent rather than
/// failing at invoke time.
pub struct VenueOpProxy {
    pub ops: Option<Arc<dyn VenueOpInvoker>>,
    /// Used when a request omits `exchange_id`.
    pub default_exchange: common::ExchangeId,
}

#[allow(refining_impl_trait)]
impl market::MarketDataService for MarketDataProxy {
    async fn list_symbols(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::ListSymbolsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::ListSymbolsResponse>> {
        let req = request.to_owned_message();
        let resp = self.market.list_symbols(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn search_symbols(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::SearchSymbolsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::SearchSymbolsResponse>> {
        let req = request.to_owned_message();
        let resp = self.market.search_symbols(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn list_tickers(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::ListTickersRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::ListTickersResponse>> {
        let req = request.to_owned_message();
        let resp = self.market.list_tickers(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_ticker(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::FetchTickerRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::FetchTickerResponse>> {
        let req = request.to_owned_message();
        let ticker = self.market.fetch_ticker(req).await.map_err(map_port)?;
        let resp = market::FetchTickerResponse {
            ticker: buffa::MessageField::some(ticker),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_order_book(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::FetchOrderBookRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::FetchOrderBookResponse>> {
        let req = request.to_owned_message();
        let book = self.market.fetch_order_book(req).await.map_err(map_port)?;
        let resp = market::FetchOrderBookResponse {
            orderbook: buffa::MessageField::some(book),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn stream_market_data(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::StreamMarketDataRequest>,
    ) -> connectrpc::ServiceResult<connectrpc::ServiceStream<market::MarketDataEvent>> {
        let req = request.to_owned_message();
        // Backpressure isolation: per-session independent mpsc channel.
        // Policy per stream kind: ticker→DropOldest, book→Coalesce, orders→Block.
        // Market data here covers ticker/book/trades/ohlcv; pick the most
        // conservative needed among the requested subscriptions.
        let policy = if req
            .subscriptions
            .iter()
            .any(|s| s.channel == buffa::EnumValue::Known(market::StreamChannel::Orderbook))
        {
            crate::ports::OVERFLOW_ORDERBOOK // Coalesce for book
        } else {
            crate::ports::OVERFLOW_TICKER // DropOldest for ticker/trades/ohlcv
        };
        // Note: private orders/balances streams (not via this proxy) use Block.
        let mut rx = self.market.subscribe_market_data(req, policy).await.map_err(map_port)?;
        let stream = async_stream::stream! {
            while let Some(event) = rx.recv().await {
                yield Ok(event);
            }
        };
        Ok(Response::stream(stream))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn get_candles(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::GetCandlesRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::GetCandlesResponse>> {
        let req = request.to_owned_message();
        let resp = self.market.get_candles(req).await.map_err(map_port)?;
        Response::ok(PreEncoded::from_message(&resp))
    }
    // ---- Funding --------------------------------------------------------

    async fn fetch_funding_rate(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::FetchFundingRateRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::FetchFundingRateResponse>> {
        let funding = self.funding.as_ref().ok_or_else(|| not_implemented("funding rates"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        let snapshot =
            funding.fetch_funding_rate(&exchange, &req.symbol).await.map_err(map_port)?;
        let resp = market::FetchFundingRateResponse {
            funding_rate: buffa::MessageField::some(funding_snapshot_to_unified(&snapshot)),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn list_funding_rates(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::ListFundingRatesRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::ListFundingRatesResponse>> {
        let funding = self.funding.as_ref().ok_or_else(|| not_implemented("funding rates"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        let snapshots =
            funding.list_funding_rates(&exchange, &req.symbols).await.map_err(map_port)?;
        let resp = market::ListFundingRatesResponse {
            funding_rates: snapshots.iter().map(funding_snapshot_to_unified).collect(),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_funding_rate_history(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::FetchFundingRateHistoryRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::FetchFundingRateHistoryResponse>> {
        let funding = self.funding.as_ref().ok_or_else(|| not_implemented("funding rates"))?;
        let req = request.to_owned_message();
        let exchange = resolved_exchange(&req.exchange_id, &self.default_exchange);
        let points = funding
            .fetch_funding_rate_history(&exchange, &req.symbol, req.limit)
            .await
            .map_err(map_port)?;
        let points: Vec<market::FundingRatePoint> =
            points.iter().map(funding_point_to_unified).collect();
        let resp = market::FetchFundingRateHistoryResponse { points, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }
}

/// Map a port funding snapshot onto the unified wire message.
///
/// `mark_price` is `optional` and stays unset when the port has none: emitting
/// a zero would make "the venue does not report a mark" look like a real
/// measurement to a carry strategy.
fn funding_snapshot_to_unified(
    snapshot: &crate::ports::FundingRateSnapshot,
) -> market::FundingRate {
    market::FundingRate {
        symbol: snapshot.symbol.clone(),
        rate: longtrader_contract::ext::decimal_to_common(snapshot.rate).into(),
        next_funding_time_ms: snapshot.next_funding_time_ms,
        mark_price: snapshot
            .mark_price
            .map(longtrader_contract::ext::decimal_to_common)
            .map_or_else(buffa::MessageField::none, Into::into),
        // The port does not model a settlement interval, so the field is absent
        // rather than zero. Presence carries "unknown", which is why a venue
        // that really does settle hourly stays distinguishable.
        funding_interval_hours: None,
        open_interest: buffa::MessageField::none(),
        volume_24h: buffa::MessageField::none(),
        ..Default::default()
    }
}

/// Map a port funding point onto the unified wire message.
fn funding_point_to_unified(point: &crate::ports::FundingRatePoint) -> market::FundingRatePoint {
    market::FundingRatePoint {
        rate: longtrader_contract::ext::decimal_to_common(point.rate).into(),
        funding_time_ms: point.time_ms,
        ..Default::default()
    }
}

#[allow(refining_impl_trait)]
impl ops::VenueOpService for VenueOpProxy {
    async fn list_venue_ops(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ops::ListVenueOpsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<ops::ListVenueOpsResponse>> {
        let req = request.to_owned_message();
        let exchange = self.exchange_for(&req.exchange_id);
        let invoker = self.ops.as_ref().ok_or_else(|| not_implemented("venue ops"))?;
        let descriptors = invoker.list_venue_ops(&exchange).await.map_err(map_port)?;
        // The backend's own descriptors, not name-only stubs: `mutating` and
        // the parameter schema are what make the list usable, and synthesising
        // a bare name would report a fund-moving op as read-only.
        let ops: Vec<ops::OpDescriptor> = descriptors.iter().map(op_descriptor_to_wire).collect();
        let resp = ops::ListVenueOpsResponse { ops, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn describe_venue_op(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ops::DescribeVenueOpRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<ops::DescribeVenueOpResponse>> {
        let req = request.to_owned_message();
        if req.op.trim().is_empty() {
            return Err(connectrpc::ConnectError::invalid_argument("op is required"));
        }
        let exchange = self.exchange_for(&req.exchange_id);
        let invoker = self.ops.as_ref().ok_or_else(|| not_implemented("venue ops"))?;
        let descriptor = invoker.describe_venue_op(&exchange, &req.op).await.map_err(map_port)?;
        let resp = ops::DescribeVenueOpResponse {
            op: buffa::MessageField::some(op_descriptor_to_wire(&descriptor)),
            ..Default::default()
        };
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn invoke_venue_op(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ops::InvokeVenueOpRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<ops::InvokeVenueOpResponse>> {
        let req = request.to_owned_message();
        if req.op.trim().is_empty() {
            return Err(connectrpc::ConnectError::invalid_argument("op is required"));
        }
        let exchange = self.exchange_for(&req.exchange_id);
        let invoker = self.ops.as_ref().ok_or_else(|| not_implemented("venue ops"))?;
        // A missing `params` is an empty parameter set, not an error: many ops
        // take no arguments and a caller should not have to send `{}`.
        let params: serde_json::Map<String, serde_json::Value> = req
            .params
            .as_option()
            .and_then(|p| serde_json::to_value(p).ok())
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        let result = invoker.invoke_venue_op(&exchange, &req.op, params).await.map_err(map_port)?;
        let as_json = serde_json::to_value(&result)
            .map_err(|e| connectrpc::ConnectError::internal(format!("result: {e}")))?;
        let result: buffa_types::google::protobuf::Struct = serde_json::from_value(as_json)
            .map_err(|e| {
                connectrpc::ConnectError::internal(format!("result is not a Struct: {e}"))
            })?;
        let resp = ops::InvokeVenueOpResponse { result: result.into(), ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }
}

/// Map a port descriptor onto the `ops.v1` wire message.
///
/// `mutating` and the parameter schema are carried through unchanged: the
/// whole point of `DescribeVenueOp` is to hand a caller enough to build a valid
/// `InvokeVenueOp` request and to know whether it is dangerous.
fn op_descriptor_to_wire(d: &crate::ports::VenueOpDescriptor) -> ops::OpDescriptor {
    ops::OpDescriptor {
        name: d.name.clone(),
        category: d.category.clone(),
        summary: d.summary.clone(),
        mutating: d.mutating,
        params: d
            .params
            .iter()
            .map(|p| ops::ParamDescriptor {
                name: p.name.clone(),
                r#type: buffa::EnumValue::Known(param_type_to_wire(p.r#type)),
                required: p.required,
                default: String::new(),
                doc: p.doc.clone(),
                enum_values: p.enum_values.clone(),
                ..Default::default()
            })
            .collect(),
        examples: Vec::new(),
        ..Default::default()
    }
}

/// Map a port param type onto the `ops.v1.ParamType` enum.
fn param_type_to_wire(t: crate::ports::VenueOpParamType) -> ops::ParamType {
    use crate::ports::VenueOpParamType as P;
    match t {
        P::Unspecified => ops::ParamType::Unspecified,
        P::String => ops::ParamType::String,
        P::Int64 => ops::ParamType::Int64,
        P::Decimal => ops::ParamType::Decimal,
        P::Bool => ops::ParamType::Bool,
        P::Enum => ops::ParamType::Enum,
        P::List => ops::ParamType::List,
        P::Map => ops::ParamType::Map,
    }
}

impl VenueOpProxy {
    /// Resolve the request's exchange against the configured default.
    fn exchange_for(&self, requested: &str) -> common::ExchangeId {
        if requested.is_empty() {
            self.default_exchange.clone()
        } else {
            common::ExchangeId { id: requested.to_string(), ..Default::default() }
        }
    }
}

#[cfg(test)]
mod tests {
    use longtrader_contract::ext::{DecimalConvertError, common_to_decimal, decimal_to_common};
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        ports::{LedgerEntry, PortError, TriggerOrder, TriggerOrderStatus},
        session::ClientIdentity,
    };

    /// The decode failure for a payload no `Decimal` can hold.
    ///
    /// `Decimal` carries one base-10 string, so "cannot represent" is a property
    /// of that payload rather than of a numeric pair: 29 nines is grammar-clean
    /// and still wider than the 96-bit mantissa. Built by running the real
    /// decoder so the fixture cannot drift from what the parser does.
    fn out_of_range(payload: &str) -> DecimalConvertError {
        let wire = common::Decimal { value: payload.to_string(), ..Default::default() };
        match common_to_decimal(&wire) {
            Err(err) => err,
            Ok(_) => unreachable!("{payload} must not decode"),
        }
    }

    // -----------------------------------------------------------------------
    // rpc_code_to_connect
    // -----------------------------------------------------------------------

    /// Every gRPC code the table maps, with the Connect code it must become.
    ///
    /// Two codes are deliberately *not* one-to-one, and the doc comment says
    /// flattening is harmful — so those collapses are pinned explicitly:
    /// - `UNKNOWN` (2) collapses to `internal`
    /// - `OUT_OF_RANGE` (11) collapses to `invalid_argument`
    ///
    /// `CANCELED` (1) is *not* one of them: it keeps its own code, because a
    /// cancelled upstream call is the caller's cancellation rather than a server
    /// fault, and `internal` would invite a retry of a call already abandoned.
    #[test]
    fn the_grpc_code_table_maps_to_the_expected_connect_codes() {
        use connectrpc::ErrorCode;
        let cases: &[(u32, ErrorCode)] = &[
            (1, ErrorCode::Canceled),
            (2, ErrorCode::Internal),
            (3, ErrorCode::InvalidArgument),
            (4, ErrorCode::DeadlineExceeded),
            (5, ErrorCode::NotFound),
            (6, ErrorCode::AlreadyExists),
            (7, ErrorCode::PermissionDenied),
            (8, ErrorCode::ResourceExhausted),
            (9, ErrorCode::FailedPrecondition),
            (10, ErrorCode::Aborted),
            (11, ErrorCode::InvalidArgument),
            (12, ErrorCode::Unimplemented),
            (13, ErrorCode::Internal),
            (14, ErrorCode::Unavailable),
            (15, ErrorCode::DataLoss),
            (16, ErrorCode::Unauthenticated),
        ];
        for (code, expected) in cases {
            let err = rpc_code_to_connect(*code, "boom");
            assert_eq!(err.code, *expected, "gRPC code {code}");
        }
    }

    /// The message must survive verbatim: the code says what class of failure
    /// this is, the message says which one.
    #[test]
    fn the_upstream_message_survives_the_mapping() {
        for code in 1..=16u32 {
            let err = rpc_code_to_connect(code, "the venue said no");
            assert_eq!(err.message.as_deref(), Some("the venue said no"), "gRPC code {code}");
        }
    }

    /// Code 0 is `OK`, not an error, and any code this build does not know is
    /// passed through verbatim rather than guessed at. Neither reaches the
    /// catch-all arm — `from_grpc_code` returns `None` for both.
    #[test]
    fn an_unrecognised_code_becomes_internal_with_the_message_verbatim() {
        use connectrpc::ErrorCode;
        for code in [0u32, 17, 99, u32::MAX] {
            let err = rpc_code_to_connect(code, "vendor-specific failure");
            assert_eq!(err.code, ErrorCode::Internal, "code {code}");
            assert_eq!(
                err.message.as_deref(),
                Some("vendor-specific failure"),
                "an unmapped code must not be rewritten, got {:?}",
                err.message
            );
        }
    }

    /// The catch-all arm exists so a *newly added* `ErrorCode` variant is
    /// surfaced rather than silently mishandled. It is unreachable today (all 16
    /// variants are matched above), so it is exercised by construction rather
    /// than by a synthetic `ErrorCode` — pinned here as documentation.
    #[test]
    fn the_catch_all_arm_is_dead_code_with_every_known_code_mapped() {
        use connectrpc::ErrorCode;
        // All 16 documented `ErrorCode` variants, each with its gRPC number.
        let every = [
            ErrorCode::Canceled,
            ErrorCode::Unknown,
            ErrorCode::InvalidArgument,
            ErrorCode::DeadlineExceeded,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::PermissionDenied,
            ErrorCode::ResourceExhausted,
            ErrorCode::FailedPrecondition,
            ErrorCode::Aborted,
            ErrorCode::OutOfRange,
            ErrorCode::Unimplemented,
            ErrorCode::Internal,
            ErrorCode::Unavailable,
            ErrorCode::DataLoss,
            ErrorCode::Unauthenticated,
        ];
        let mapped: Vec<u32> = every.iter().map(ErrorCode::grpc_code).collect();
        assert_eq!(mapped.len(), 16);
        for code in mapped {
            // A mapped code never falls through to the `{:?}: message` catch-all.
            let err = rpc_code_to_connect(code, "plain");
            assert_eq!(
                err.message.as_deref(),
                Some("plain"),
                "code {code} reached the catch-all arm"
            );
        }
    }

    // -----------------------------------------------------------------------
    // map_port
    // -----------------------------------------------------------------------

    /// Each `PortError` must reach the client as a code it can act on.
    #[test]
    fn every_port_error_variant_maps_to_its_documented_code() {
        use connectrpc::ErrorCode;
        let cases: Vec<(PortError, ErrorCode)> = vec![
            (PortError::InvalidArgument("qty".into()), ErrorCode::InvalidArgument),
            (PortError::MissingField("last".into()), ErrorCode::InvalidArgument),
            (PortError::NotFound("venue operation nope".into()), ErrorCode::NotFound),
            (PortError::Unsupported("trigger orders".into()), ErrorCode::Unimplemented),
            (PortError::Transport("socket closed".into()), ErrorCode::Internal),
            (PortError::Decimal(out_of_range(&"9".repeat(29))), ErrorCode::Internal),
        ];
        for (err, expected) in cases {
            assert_eq!(map_port(err).code, expected);
        }
    }

    /// `MissingField` is `invalid_argument` *and* names the field, or the client
    /// cannot tell which part of its request to fix.
    #[test]
    fn a_missing_field_names_the_field() {
        use connectrpc::ErrorCode;
        let err = map_port(PortError::MissingField("trigger_price".into()));
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        let message = err.message.unwrap_or_default();
        assert!(message.contains("missing required field"), "{message}");
        assert!(message.contains("trigger_price"), "{message}");
    }

    /// `Unsupported` is `unimplemented`, never `internal`: the doc comment on
    /// `map_port` says retrying against a permanent answer is the harm.
    #[test]
    fn an_unsupported_capability_is_unimplemented_not_internal() {
        use connectrpc::ErrorCode;
        let err = map_port(PortError::Unsupported("conditional orders".into()));
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert_ne!(err.code, ErrorCode::Internal);
        assert_eq!(err.message.as_deref(), Some("conditional orders"));
    }

    /// `NotFound` and `Unsupported` are different answers and must stay
    /// distinguishable: the object is missing now, versus the backend can never
    /// have it.
    #[test]
    fn not_found_and_unsupported_stay_distinguishable() {
        use connectrpc::ErrorCode;
        let not_found = map_port(PortError::NotFound("venue operation nope".into()));
        let unsupported = map_port(PortError::Unsupported("conditional orders".into()));
        assert_eq!(not_found.code, ErrorCode::NotFound);
        assert_eq!(unsupported.code, ErrorCode::Unimplemented);
        assert_ne!(not_found.code, unsupported.code);
    }

    /// A `Transport` failure and a `Decimal` decode failure are both ours, and
    /// both are `internal` — they carry no action for the client.
    #[test]
    fn transport_and_decimal_failures_both_collapse_to_internal() {
        use connectrpc::ErrorCode;
        let transport = map_port(PortError::Transport("connection reset".into()));
        assert_eq!(transport.code, ErrorCode::Internal);
        assert_eq!(transport.message.as_deref(), Some("transport error: connection reset"));
        // There is no second representation to recover from, so a payload outside
        // the grammar is a plain failure — and the message echoes what arrived.
        let decimal = map_port(PortError::Decimal(DecimalConvertError::NotBase10 {
            value: "not-a-number".into(),
            reason: "exponents are not accepted",
        }));
        assert_eq!(decimal.code, ErrorCode::Internal);
        let message = decimal.message.unwrap_or_default();
        assert!(message.contains("not-a-number"), "{message}");
    }

    /// The upstream code is preserved so the client sees the venue's own verdict
    /// (rate limited, unauthenticated, insufficient margin, ...).
    #[test]
    fn an_rpc_error_preserves_the_upstream_code() {
        use connectrpc::ErrorCode;
        let rate_limited =
            map_port(PortError::Rpc { code: 8, message: "too many requests".into() });
        assert_eq!(rate_limited.code, ErrorCode::ResourceExhausted);
        assert_eq!(rate_limited.message.as_deref(), Some("too many requests"));

        let unauthenticated = map_port(PortError::Rpc { code: 16, message: "bad api key".into() });
        assert_eq!(unauthenticated.code, ErrorCode::Unauthenticated);
    }

    /// An unrecognised upstream code degrades to `internal` rather than being
    /// guessed at — the documented behaviour, and it must not panic.
    #[test]
    fn an_unrecognised_upstream_rpc_code_degrades_to_internal() {
        use connectrpc::ErrorCode;
        for code in [0u32, 17, 4_242] {
            let err = map_port(PortError::Rpc { code, message: "vendor".into() });
            assert_eq!(err.code, ErrorCode::Internal, "code {code}");
        }
    }

    // -----------------------------------------------------------------------
    // pagination_limit
    // -----------------------------------------------------------------------

    fn pagination(limit: u64) -> common::Pagination {
        common::Pagination { limit, ..Default::default() }
    }

    /// An absent pagination and an explicit `limit = 0` are the same request: the
    /// client did not ask for a page size, so the default applies. Answering `0`
    /// would return an empty page and read as "no rows exist".
    #[test]
    fn an_absent_or_zero_limit_gives_the_default_page_size() {
        assert_eq!(pagination_limit(None), DEFAULT_PAGE_LIMIT);
        assert_eq!(pagination_limit(Some(&pagination(0))), DEFAULT_PAGE_LIMIT);
    }

    /// A requested size is honoured as-is below the cap, so a client asking for
    /// one row gets one row.
    #[test]
    fn a_small_requested_page_size_is_honoured() {
        for limit in [1u64, 2, 10, 999, 1000] {
            assert_eq!(pagination_limit(Some(&pagination(limit))), limit as u32, "limit {limit}");
        }
    }

    /// The cap bounds what one call can pull back — including a `u64` too large
    /// for `u32`, which must saturate to the cap rather than wrap.
    #[test]
    fn a_large_requested_page_size_clamps_to_the_cap() {
        for limit in [1001u64, 1_000_000, u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX] {
            assert_eq!(pagination_limit(Some(&pagination(limit))), 1000, "limit {limit}");
        }
    }

    // -----------------------------------------------------------------------
    // timestamp_from_ms
    // -----------------------------------------------------------------------

    /// `seconds` plus the whole-millisecond part of `nanos`, widened so the very
    /// extremes of `i64` milliseconds recompose without overflowing.
    fn recompose_ms(ts: &buffa_types::google::protobuf::Timestamp) -> i128 {
        i128::from(ts.seconds) * 1_000 + i128::from(ts.nanos / 1_000_000)
    }

    /// Milliseconds split into seconds plus a nanosecond remainder, with both
    /// halves non-negative (the protobuf convention for pre-epoch values).
    #[test]
    fn milliseconds_split_into_seconds_and_nanos() {
        let zero = timestamp_from_ms(0);
        assert_eq!(zero.seconds, 0);
        assert_eq!(zero.nanos, 0);

        let whole = timestamp_from_ms(1_700_000_000_000);
        assert_eq!(whole.seconds, 1_700_000_000);
        assert_eq!(whole.nanos, 0);

        let fractional = timestamp_from_ms(1_700_000_000_123);
        assert_eq!(fractional.seconds, 1_700_000_000);
        assert_eq!(fractional.nanos, 123_000_000);

        let just_under = timestamp_from_ms(999);
        assert_eq!(just_under.seconds, 0);
        assert_eq!(just_under.nanos, 999_000_000);
    }

    /// A negative millisecond value must not produce a negative `nanos`: the
    /// protobuf wire format requires `0 <= nanos < 1e9` and encodes the fraction
    /// as counting *forward* from a negative second.
    #[test]
    fn a_negative_millisecond_keeps_nanos_non_negative() {
        for ms in [-1i64, -2, -999, -1_000, -1_001, -1_700_000_000_000] {
            let ts = timestamp_from_ms(ms);
            assert!(ts.nanos >= 0, "ms={ms} produced nanos={}", ts.nanos);
            assert!(ts.nanos < 1_000_000_000, "ms={ms} produced nanos={}", ts.nanos);
            assert_eq!(recompose_ms(&ts), i128::from(ms), "ms={ms}");
        }
    }

    /// The full `i64` range must not overflow the split itself: `div_euclid` /
    /// `rem_euclid` keep it well-defined where `/` and `%` would round toward
    /// zero. Recomposition is done in `i128` because `seconds * 1000` genuinely
    /// overflows `i64` for the very extremes — a property of the range, not of
    /// the mapping.
    #[test]
    fn the_extremes_of_the_millisecond_range_stay_in_range() {
        for ms in [i64::MIN, i64::MAX, i64::MIN + 1, i64::MAX - 1, -1, 0] {
            let ts = timestamp_from_ms(ms);
            assert!(ts.nanos >= 0 && ts.nanos < 1_000_000_000, "ms={ms} nanos={}", ts.nanos);
            assert_eq!(recompose_ms(&ts), i128::from(ms), "ms={ms}");
        }
    }

    // -----------------------------------------------------------------------
    // ledger_entry_to_unified
    // -----------------------------------------------------------------------

    fn ledger(amount: Decimal, completed: bool) -> LedgerEntry {
        LedgerEntry {
            id: "l1".into(),
            currency: "USDT".into(),
            amount,
            entry_type: "deposit".into(),
            completed,
            time_ms: 1_700_000_000_123,
        }
    }

    /// `direction` comes from the sign of the signed port amount, so a withdrawal
    /// can never be published as a deposit.
    #[test]
    fn the_direction_is_derived_from_the_sign_of_the_amount() {
        let deposit = ledger_entry_to_unified(&ledger(dec!(100), true));
        assert_eq!(deposit.direction, "in");
        let withdrawal = ledger_entry_to_unified(&ledger(dec!(-100), true));
        assert_eq!(withdrawal.direction, "out");
        assert_ne!(deposit.direction, withdrawal.direction);
    }

    /// A zero amount is neither positive nor negative, so it publishes as `in`.
    /// Pinned so the boundary is a decision, not an accident.
    #[test]
    fn a_zero_amount_publishes_as_incoming() {
        assert_eq!(ledger_entry_to_unified(&ledger(Decimal::ZERO, true)).direction, "in");
    }

    /// `status` is the completed flag verbatim; every other field passes through
    /// unchanged, and the amount round-trips exactly.
    #[test]
    fn the_status_and_scalar_fields_pass_through() {
        let entry = ledger(dec!(42.5), false);
        let wire = ledger_entry_to_unified(&entry);
        assert_eq!(wire.status, "pending");
        assert_eq!(ledger_entry_to_unified(&ledger(dec!(1), true)).status, "completed");
        assert_eq!(wire.id, entry.id);
        assert_eq!(wire.currency, entry.currency);
        assert_eq!(wire.r#type, entry.entry_type);
        let amount = wire.amount.as_option().expect("the amount is always set");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(amount).expect("the amount decodes"),
            entry.amount
        );
        let ts = wire.timestamp.as_option().expect("the timestamp is always set");
        assert_eq!(ts.seconds, 1_700_000_000);
        assert_eq!(ts.nanos, 123_000_000);
    }

    // -----------------------------------------------------------------------
    // resolved_exchange
    // -----------------------------------------------------------------------

    fn exchange(id: &str, label: &str) -> common::ExchangeId {
        common::ExchangeId { id: id.into(), label: label.into(), ..Default::default() }
    }

    /// An absent `exchange_id` resolves to the configured default, so an
    /// unqualified call behaves like every other trading RPC.
    #[test]
    fn an_absent_exchange_id_resolves_to_the_default() {
        let default = exchange("binance", "main");
        let absent: buffa::MessageField<common::ExchangeId> = buffa::MessageField::none();
        assert_eq!(resolved_exchange(&absent, &default), default);
    }

    /// A present `exchange_id` wins, and its label survives with it.
    #[test]
    fn a_present_exchange_id_wins_over_the_default() {
        let default = exchange("binance", "main");
        let requested = exchange("okx", "hedge");
        let present = buffa::MessageField::<common::ExchangeId>::some(requested.clone());
        assert_eq!(resolved_exchange(&present, &default), requested);
    }

    /// `VenueOpProxy::exchange_for` is the string-typed twin of
    /// `resolved_exchange`; the two must agree on the empty-string fallback, or
    /// an unqualified venue-op call would resolve differently from every trading
    /// RPC.
    #[test]
    fn the_venue_op_exchange_resolution_matches_the_message_field_one() {
        let default = exchange("binance", "main");
        let proxy = VenueOpProxy { ops: None, default_exchange: default.clone() };
        let absent: buffa::MessageField<common::ExchangeId> = buffa::MessageField::none();
        assert_eq!(proxy.exchange_for(""), resolved_exchange(&absent, &default));
        let requested = exchange("okx", "hedge");
        assert_eq!(proxy.exchange_for("okx").id, requested.id);
    }

    // -----------------------------------------------------------------------
    // gate_submission
    // -----------------------------------------------------------------------

    /// With no session manager the gate is disabled: the standalone-proxy path
    /// must not consult a session for an id it cannot resolve.
    #[tokio::test]
    async fn a_missing_manager_never_gates_a_submission() {
        for session_id in ["", "anything", "a-session-that-does-not-exist"] {
            let handle = gate_submission(None, session_id).await.expect("no manager cannot fail");
            assert!(handle.is_none(), "session_id {session_id:?} must not resolve to a session");
        }
    }

    /// A session manager over the mock backend; `install_self` is deliberately not
    /// called, so `attach` does not spawn a watchdog.
    fn manager() -> Arc<SessionManager> {
        let adapter = Arc::new(crate::adapters::MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = adapter;
        Arc::new(SessionManager::new(None, common::ExchangeId::default(), gateway, market))
    }

    /// A `TradingProxy` over the mock backend, with no session manager: the batch
    /// recovery query is a gateway round trip, not a session one.
    fn proxy_with(gateway: Arc<dyn TradingGateway>) -> TradingProxy {
        TradingProxy {
            gateway,
            default_exchange: exchange("mock", ""),
            manager: None,
            triggers: None,
            wallet: None,
        }
    }

    fn mock_gateway() -> Arc<crate::adapters::MockAdapter> {
        Arc::new(crate::adapters::MockAdapter::new(dec!(100)))
    }

    /// A `trading.v1.OrderRequest` for `coid`.
    fn order_request(coid: &str) -> trading::OrderRequest {
        trading::OrderRequest {
            client_order_id: coid.to_string(),
            symbol: "BTC/USDT".to_string(),
            r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
            side: buffa::EnumValue::Known(trading::OrderSide::Buy),
            amount: buffa::MessageField::some(decimal_to_common(dec!(1))),
            price: buffa::MessageField::some(decimal_to_common(dec!(99))),
            time_in_force: buffa::EnumValue::Known(trading::TimeInForce::Gtc),
            ..Default::default()
        }
    }

    fn order_req(coid: &str) -> trading::CreateOrderRequest {
        trading::CreateOrderRequest {
            exchange_id: buffa::MessageField::some(exchange("mock", "")),
            order: buffa::MessageField::some(order_request(coid)),
            ..Default::default()
        }
    }

    /// Every open order on the venue, unscoped — the same query the recovery
    /// makes, so a test can compare it against what recovery found.
    async fn open_order_ids(gateway: &Arc<dyn TradingGateway>) -> Vec<String> {
        gateway
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange("mock", "")),
                symbol: String::new(),
                ..Default::default()
            })
            .await
            .expect("test setup: the open-order book")
            .into_iter()
            .map(|o| o.id)
            .collect()
    }

    /// The whole point of the recovery query: a batch that fails part-way leaves
    /// the legs the venue already accepted live on it, and the only way to learn
    /// which those are is to ask the venue — the error carries no per-leg result.
    #[tokio::test]
    async fn a_failed_batch_leaves_earlier_legs_recoverable_from_the_venue() {
        let adapter = mock_gateway();
        let proxy = proxy_with(adapter.clone());
        let exchange_id = proxy.default_exchange.clone();

        // The mock places legs one at a time, like a real venue. Failing from the
        // *second* leg leaves the first one live, which is the partial-success
        // shape this recovery exists for: failing from the first would leave
        // nothing resting and prove nothing.
        adapter.fail_creates_from(1).await;
        let _failed = proxy
            .gateway
            .batch_create_orders(trading::CreateOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange_id.clone()),
                orders: vec![order_request("leg-1"), order_request("leg-2")],
                ..Default::default()
            })
            .await
            .expect_err("a batch with a scripted failure must fail");

        let open = open_order_ids(&proxy.gateway).await;
        assert_eq!(open.len(), 1, "test setup: exactly one leg is resting on the venue");

        let recovered = accepted_batch_orders(
            proxy.gateway.as_ref(),
            &exchange_id,
            &["leg-1".to_string(), "leg-2".to_string()],
        )
        .await;
        let ids: Vec<String> = recovered.into_iter().map(|o| o.id).collect();
        assert_eq!(
            ids, open,
            "recovery must attribute exactly what the venue is holding, not what we hoped"
        );
    }

    /// A `client_order_id` the batch did not ask for must never be attributed:
    /// that would hand another principal's order to this session's kill-switch.
    #[tokio::test]
    async fn batch_recovery_only_returns_the_requested_client_order_ids() {
        let proxy = proxy_with(mock_gateway());
        let exchange_id = proxy.default_exchange.clone();
        let ours =
            proxy.gateway.create_order(order_req("mine")).await.expect("test setup: our leg");
        proxy.gateway.create_order(order_req("theirs")).await.expect("test setup: a foreign order");

        let recovered =
            accepted_batch_orders(proxy.gateway.as_ref(), &exchange_id, &["mine".to_string()])
                .await;
        let ids: Vec<&str> = recovered.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, vec![ours.id.as_str()], "only the requested id may be attributed");
    }

    /// An anonymous order cannot be matched: every anonymous order on the venue
    /// shares that id, so an empty `client_order_id` must not be used as a key.
    #[tokio::test]
    async fn batch_recovery_never_matches_an_empty_client_order_id() {
        let proxy = proxy_with(mock_gateway());
        let exchange_id = proxy.default_exchange.clone();
        proxy.gateway.create_order(order_req("")).await.expect("test setup: an anonymous order");

        let recovered =
            accepted_batch_orders(proxy.gateway.as_ref(), &exchange_id, &[String::new()]).await;
        assert!(recovered.is_empty(), "an empty client_order_id must not sweep in other orders");
    }

    /// With a manager attached, an empty session id is still an unscoped operator
    /// call — `authorize_order_submission` returns `Ok(None)` for it, and the
    /// submission proceeds ungated.
    #[tokio::test]
    async fn an_empty_session_id_is_an_unscoped_operator_call() {
        let manager = manager();
        let handle =
            gate_submission(Some(&manager), "").await.expect("an operator call is not gated");
        assert!(handle.is_none());
    }

    /// A *named* session that the manager does not know is refused, so a strategy
    /// cannot bypass the gate by inventing a session id.
    #[tokio::test]
    async fn an_unknown_session_id_is_refused_by_the_gate() {
        let manager = manager();
        let err = gate_submission(Some(&manager), "no-such-session")
            .await
            .expect_err("an unknown session must not pass the gate");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    /// A session that exists but has not reached `ACTIVE` is refused with
    /// `failed_precondition` (not `internal`), so a strategy learns it must
    /// reconcile first. This is the `SYNC_IN_PROGRESS` contract.
    #[tokio::test]
    async fn a_session_that_has_not_reconciled_is_refused_by_the_gate() {
        let manager = manager();
        let (id, _heartbeat) = manager
            .attach("", None, &ClientIdentity::default())
            .await
            .expect("attach must succeed");
        let err = gate_submission(Some(&manager), &id)
            .await
            .expect_err("a pre-ACTIVE session must not pass the gate");
        assert_eq!(err.code, connectrpc::ErrorCode::FailedPrecondition);
        assert!(
            err.message.as_deref().is_some_and(|m| m.contains("SYNC_IN_PROGRESS")),
            "{:?}",
            err.message
        );
    }

    // -----------------------------------------------------------------------
    // not_implemented
    // -----------------------------------------------------------------------

    /// Every missing capability answers `unimplemented` naming the capability
    /// and pointing at an alternative, never `internal`.
    #[test]
    fn a_missing_capability_is_unimplemented_and_actionable() {
        use connectrpc::ErrorCode;
        for what in ["conditional orders", "wallet ledger", "funding rates", "venue ops"] {
            let err = not_implemented(what);
            assert_eq!(err.code, ErrorCode::Unimplemented, "{what}");
            let message = err.message.unwrap_or_default();
            assert!(message.contains(what), "{message}");
            assert!(message.contains("mock backend"), "{message}");
        }
    }

    // -----------------------------------------------------------------------
    // trigger wire mapping
    // -----------------------------------------------------------------------

    /// A request with no order at all is `invalid_argument`, not a panic.
    #[test]
    fn a_missing_trigger_order_is_rejected() {
        use connectrpc::ErrorCode;
        let err = trigger_request_from_unified(None).expect_err("an absent order must be rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.unwrap_or_default().contains("order is required"));
    }

    /// `trigger_price` and `qty` are mandatory: defaulting either would arm an
    /// order that fires immediately (price 0) or never trades (qty 0).
    #[test]
    fn a_missing_trigger_price_or_quantity_is_rejected() {
        use connectrpc::ErrorCode;
        let base = trading::TriggerOrderRequest {
            client_order_id: "c1".into(),
            symbol: "BTC/USDT".into(),
            side: buffa::EnumValue::Known(trading::OrderSide::Buy),
            trigger_price: buffa::MessageField::some(decimal_to_common(dec!(90))),
            qty: buffa::MessageField::some(decimal_to_common(dec!(1))),
            reduce_only: true,
            ..Default::default()
        };
        let mut no_price = base.clone();
        no_price.trigger_price = buffa::MessageField::none();
        let err = trigger_request_from_unified(Some(&no_price)).expect_err("price is required");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.unwrap_or_default().contains("trigger_price is required"));

        let mut no_qty = base;
        no_qty.qty = buffa::MessageField::none();
        let err = trigger_request_from_unified(Some(&no_qty)).expect_err("qty is required");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.unwrap_or_default().contains("qty is required"));
    }

    /// Defaulting the side would silently invert a stop, so `UNSPECIFIED` and an
    /// unknown discriminant are both rejected.
    #[test]
    fn an_unusable_trigger_side_is_rejected_rather_than_defaulted() {
        use connectrpc::ErrorCode;
        let base = trading::TriggerOrderRequest {
            client_order_id: "c1".into(),
            symbol: "BTC/USDT".into(),
            trigger_price: buffa::MessageField::some(decimal_to_common(dec!(90))),
            qty: buffa::MessageField::some(decimal_to_common(dec!(1))),
            ..Default::default()
        };
        let mut unspecified = base.clone();
        unspecified.side = buffa::EnumValue::Known(trading::OrderSide::Unspecified);
        let err =
            trigger_request_from_unified(Some(&unspecified)).expect_err("UNSPECIFIED is not Buy");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        let message = err.message.unwrap_or_default();
        assert!(message.contains("BUY or SELL"), "{message}");

        let mut unknown = base;
        unknown.side = buffa::EnumValue::Unknown(77);
        let err = trigger_request_from_unified(Some(&unknown)).expect_err("77 is not a side");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.unwrap_or_default().contains("77"));
    }

    /// A well-formed request reaches the port unchanged on both sides.
    #[test]
    fn a_well_formed_trigger_request_reaches_the_port() {
        for (side, expected_buy) in
            [(trading::OrderSide::Buy, true), (trading::OrderSide::Sell, false)]
        {
            let req = trading::TriggerOrderRequest {
                client_order_id: "c1".into(),
                symbol: "BTC/USDT".into(),
                side: buffa::EnumValue::Known(side),
                trigger_price: buffa::MessageField::some(decimal_to_common(dec!(90.5))),
                qty: buffa::MessageField::some(decimal_to_common(dec!(0.25))),
                reduce_only: true,
                ..Default::default()
            };
            let port = trigger_request_from_unified(Some(&req)).expect("well formed");
            assert_eq!(port.client_order_id, "c1");
            assert_eq!(port.symbol, "BTC/USDT");
            assert_eq!(port.is_buy, expected_buy);
            assert_eq!(port.trigger_price, dec!(90.5));
            assert_eq!(port.qty, dec!(0.25));
            assert!(port.reduce_only);
        }
    }

    /// The port models neither a trigger source nor a post-trigger price leg, so
    /// both are reported as LAST / MARKET rather than invented, and a firing time
    /// appears only for a fired order.
    #[test]
    fn the_unfilled_fields_of_a_trigger_order_are_not_invented() {
        let resting = trigger_order_to_unified(&TriggerOrder {
            id: "t1".into(),
            client_order_id: "c1".into(),
            symbol: "BTC/USDT".into(),
            is_buy: true,
            trigger_price: dec!(90),
            qty: dec!(0.5),
            reduce_only: true,
            status: TriggerOrderStatus::Open,
            created_at_ms: 1_700_000_000_000,
            order_id: None,
            triggered_at_ms: None,
        });
        assert_eq!(resting.trigger_type, buffa::EnumValue::Known(trading::TriggerPriceType::Last));
        assert_eq!(resting.order_type, buffa::EnumValue::Known(trading::OrderType::Market));
        assert!(resting.order_price.as_option().is_none(), "no post-trigger price is invented");
        assert!(resting.triggered_at.as_option().is_none(), "an unfired order has no fire time");
        assert_eq!(resting.status, buffa::EnumValue::Known(trading::TriggerOrderStatus::Open));
        let created = resting.created_at.as_option().expect("the creation time is always set");
        assert_eq!(created.seconds, 1_700_000_000);
        assert_eq!(created.nanos, 0);
    }

    /// Every `TriggerOrderStatus` maps one-to-one onto the wire enum; no variant
    /// is collapsed.
    #[test]
    fn every_trigger_status_maps_to_its_wire_enum() {
        let cases = [
            (TriggerOrderStatus::Open, trading::TriggerOrderStatus::Open),
            (TriggerOrderStatus::Triggered, trading::TriggerOrderStatus::Triggered),
            (TriggerOrderStatus::Canceled, trading::TriggerOrderStatus::Canceled),
            (TriggerOrderStatus::Rejected, trading::TriggerOrderStatus::Rejected),
        ];
        for (status, expected) in cases {
            let triggered = status == TriggerOrderStatus::Triggered;
            let fired_at = triggered.then_some(1_500);
            let order_id = triggered.then_some("venue-1");
            let wire = trigger_order_to_unified(&TriggerOrder {
                id: "t1".into(),
                client_order_id: "c1".into(),
                symbol: "BTC/USDT".into(),
                is_buy: false,
                trigger_price: dec!(90),
                qty: dec!(1),
                reduce_only: false,
                status,
                created_at_ms: 1_000,
                order_id: order_id.map(str::to_string),
                triggered_at_ms: fired_at,
            });
            assert_eq!(wire.status, buffa::EnumValue::Known(expected));
            assert_eq!(wire.side, buffa::EnumValue::Known(trading::OrderSide::Sell));
            assert_eq!(wire.order_id.as_deref(), order_id, "{status:?}");
            if fired_at.is_some() {
                let at = wire.triggered_at.as_option().expect("a fired order has a fire time");
                assert_eq!((at.seconds, at.nanos), (1, 500_000_000));
            } else {
                assert!(wire.triggered_at.as_option().is_none(), "{status:?} must not carry one");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Funding wire mapping
    // -----------------------------------------------------------------------

    /// `mark_price` is `optional` and stays unset when the port has none: a zero
    /// would make "the venue does not report a mark" look like a measurement to
    /// a carry strategy.
    #[test]
    fn an_absent_mark_price_stays_unset_on_the_wire() {
        let without = funding_snapshot_to_unified(&crate::ports::FundingRateSnapshot {
            symbol: "BTC/USDT".into(),
            rate: dec!(0.0001),
            next_funding_time_ms: 1_700_000_000_000,
            mark_price: None,
        });
        assert!(without.mark_price.as_option().is_none(), "an absent mark must not become a zero");
        let with = funding_snapshot_to_unified(&crate::ports::FundingRateSnapshot {
            symbol: "BTC/USDT".into(),
            rate: dec!(0.0001),
            next_funding_time_ms: 1_700_000_000_000,
            mark_price: Some(dec!(42_000)),
        });
        let mark = with.mark_price.as_option().expect("a present mark is carried through");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(mark).expect("the mark decodes"),
            dec!(42_000)
        );
    }

    /// The remaining funding fields pass through untouched, and the interval the
    /// port does not model is reported as zero rather than guessed.
    #[test]
    fn the_funding_snapshot_fields_pass_through() {
        let wire = funding_snapshot_to_unified(&crate::ports::FundingRateSnapshot {
            symbol: "BTC/USDT".into(),
            rate: dec!(-0.0025),
            next_funding_time_ms: 1_700_003_600_000,
            mark_price: None,
        });
        assert_eq!(wire.symbol, "BTC/USDT");
        assert_eq!(wire.next_funding_time_ms, 1_700_003_600_000);
        assert_eq!(
            wire.funding_interval_hours, None,
            "an unmodelled interval must be absent, not a zero that reads as a real one"
        );
        assert!(wire.open_interest.as_option().is_none());
        assert!(wire.volume_24h.as_option().is_none());
        let rate = wire.rate.as_option().expect("the rate is always set");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(rate).expect("the rate decodes"),
            dec!(-0.0025)
        );
    }

    /// A funding point carries its rate and settlement time; there is nothing
    /// else to lose.
    #[test]
    fn a_funding_point_publishes_its_rate_and_time() {
        let wire = funding_point_to_unified(&crate::ports::FundingRatePoint {
            rate: dec!(0.0003),
            time_ms: 1_700_000_000_000,
        });
        assert_eq!(wire.funding_time_ms, 1_700_000_000_000);
        let rate = wire.rate.as_option().expect("the rate is always set");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(rate).expect("the rate decodes"),
            dec!(0.0003)
        );
    }

    // -----------------------------------------------------------------------
    // Venue-op wire mapping
    // -----------------------------------------------------------------------

    /// `mutating` is what a client reads to decide whether an op needs a
    /// confirmation prompt; reporting a fund-moving op as read-only is the harm.
    #[test]
    fn an_op_descriptor_carries_its_schema_and_mutating_flag() {
        let descriptor = crate::ports::VenueOpDescriptor {
            name: "account.transfer".into(),
            category: "account".into(),
            summary: "Move funds between accounts.".into(),
            mutating: true,
            params: vec![
                crate::ports::VenueOpParam {
                    name: "amount".into(),
                    r#type: crate::ports::VenueOpParamType::Decimal,
                    required: true,
                    doc: "Amount to move; must be positive.".into(),
                    enum_values: Vec::new(),
                },
                crate::ports::VenueOpParam {
                    name: "dry_run".into(),
                    r#type: crate::ports::VenueOpParamType::Bool,
                    required: false,
                    doc: String::new(),
                    enum_values: Vec::new(),
                },
            ],
        };
        let wire = op_descriptor_to_wire(&descriptor);
        assert_eq!(wire.name, "account.transfer");
        assert_eq!(wire.category, "account");
        assert_eq!(wire.summary, "Move funds between accounts.");
        assert!(wire.mutating, "a fund-moving op must not read as read-only");
        assert!(wire.examples.is_empty(), "no example is invented");
        assert_eq!(wire.params.len(), 2);
        let amount = wire.params.first().expect("the amount parameter");
        assert_eq!(amount.name, "amount");
        assert_eq!(amount.r#type, buffa::EnumValue::Known(ops::ParamType::Decimal));
        assert!(amount.required);
        assert_eq!(amount.doc, "Amount to move; must be positive.");
        assert!(amount.default.is_empty(), "no default is invented");
        let dry_run = wire.params.get(1).expect("the dry_run parameter");
        assert_eq!(dry_run.r#type, buffa::EnumValue::Known(ops::ParamType::Bool));
        assert!(!dry_run.required);
    }

    /// Every `VenueOpParamType` maps onto its `ops.v1.ParamType`, so a client
    /// reading the schema sees the type the descriptor promised.
    #[test]
    fn every_param_type_maps_to_its_wire_enum() {
        let cases = [
            (crate::ports::VenueOpParamType::Unspecified, ops::ParamType::Unspecified),
            (crate::ports::VenueOpParamType::String, ops::ParamType::String),
            (crate::ports::VenueOpParamType::Int64, ops::ParamType::Int64),
            (crate::ports::VenueOpParamType::Decimal, ops::ParamType::Decimal),
            (crate::ports::VenueOpParamType::Bool, ops::ParamType::Bool),
            (crate::ports::VenueOpParamType::Enum, ops::ParamType::Enum),
            (crate::ports::VenueOpParamType::List, ops::ParamType::List),
            (crate::ports::VenueOpParamType::Map, ops::ParamType::Map),
        ];
        for (port, expected) in cases {
            assert_eq!(param_type_to_wire(port), expected, "{port:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Whichever gRPC code the venue answers with, the result is one of the
        /// documented Connect codes and never a `Debug`-prefixed rewrite for a
        /// code the build knows.
        #[test]
        fn a_known_grpc_code_never_reaches_the_catch_all_arm(code in 1u32..=16) {
            let err = rpc_code_to_connect(code, "propagated");
            prop_assert_eq!(
                err.message.as_deref(),
                Some("propagated"),
                "code {} fell through to the catch-all arm",
                code
            );
        }

        /// A `Pagination` limit is answered from a bounded, always-nonzero range
        /// whatever the client asked for — including a value that does not fit
        /// `u32`.
        #[test]
        fn the_page_size_is_always_bounded_and_nonzero(limit in any::<u64>()) {
            let page = pagination_limit(Some(&pagination(limit)));
            prop_assert!(page >= 1, "a zero-length page would read as no results");
            prop_assert!(page <= 1000, "an unbounded page could pull the whole ledger");
            prop_assert_eq!(
                page,
                if limit == 0 {
                    DEFAULT_PAGE_LIMIT
                } else {
                    u32::try_from(limit).unwrap_or(u32::MAX).min(1000)
                },
                "limit={}",
                limit
            );
        }

        /// The millisecond split is lossless: recomposing `seconds` and the
        /// whole-millisecond part of `nanos` returns the input, and `nanos` is
        /// always a legal protobuf fraction. Recomposition is widened to `i128`
        /// because `seconds * 1000` overflows `i64` at the range extremes.
        #[test]
        fn the_millisecond_split_is_lossless(ms in any::<i64>()) {
            let ts = timestamp_from_ms(ms);
            prop_assert!(ts.nanos >= 0, "ms={} nanos={}", ms, ts.nanos);
            prop_assert!(ts.nanos < 1_000_000_000, "ms={} nanos={}", ms, ts.nanos);
            prop_assert_eq!(recompose_ms(&ts), i128::from(ms), "ms={}", ms);
        }

        /// `direction` tracks the sign of the amount for every value, and the
        /// status tracks `completed` — the pair a client branches on.
        #[test]
        fn the_ledger_direction_always_tracks_the_sign(
            mantissa in any::<i64>(),
            scale in 0u32..=10,
            completed in any::<bool>(),
        ) {
            let Ok(amount) = Decimal::try_from_i128_with_scale(i128::from(mantissa), scale) else {
                return Ok(());
            };
            let wire = ledger_entry_to_unified(&ledger(amount, completed));
            let expected = if amount.is_sign_negative() { "out" } else { "in" };
            prop_assert_eq!(wire.direction.as_str(), expected, "amount={}", amount);
            prop_assert_eq!(
                wire.status.as_str(),
                if completed { "completed" } else { "pending" }
            );
        }

        /// `resolved_exchange` is a projection: whichever side wins, both halves
        /// of the winning `ExchangeId` reach the wire untouched.
        #[test]
        fn the_resolved_exchange_never_transforms_the_venue(id in "[a-z0-9_-]{1,10}") {
            let default = exchange("fallback", "");
            let requested = common::ExchangeId {
                id: id.clone(),
                label: format!("{id}-sub"),
                ..Default::default()
            };
            let label = format!("{id}-sub");
            let present =
                buffa::MessageField::<common::ExchangeId>::some(requested);
            let resolved = resolved_exchange(&present, &default);
            prop_assert_eq!(resolved.id.as_str(), id.as_str());
            prop_assert_eq!(resolved.label.as_str(), label.as_str());
            let absent: buffa::MessageField<common::ExchangeId> = buffa::MessageField::none();
            let fallback = resolved_exchange(&absent, &default);
            prop_assert_eq!(fallback.id.as_str(), "fallback");
        }
    }
}
