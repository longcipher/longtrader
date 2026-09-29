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

use std::sync::Arc;

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
        Some(E::Canceled) => connectrpc::ConnectError::internal(message.to_string()),
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
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let orders = self.gateway.batch_create_orders(req).await.map_err(map_port)?;
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
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
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
        funding_interval_hours: 0,
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
