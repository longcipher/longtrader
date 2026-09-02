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
//! 每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders
//! Block）
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

#[rustfmt::skip]
const _DOC_BACKPRESSURE: &str = "每 session 独立 mpsc channel 与不同 OverflowPolicy（ticker DropOldest, book Coalesce, orders Block）";

use crate::{
    ports::{MarketDataSource, TradingGateway},
    proto::{common, market, trading},
};

fn internal<E: std::fmt::Display>(err: E) -> connectrpc::ConnectError {
    connectrpc::ConnectError::internal(err.to_string())
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
}

#[allow(refining_impl_trait)]
impl trading::TradingService for TradingProxy {
    async fn create_order(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, trading::CreateOrderRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CreateOrderResponse>> {
        let mut req = request.to_owned_message();
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let order = self.gateway.create_order(req).await.map_err(internal)?;
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
        req.exchange_id =
            buffa::MessageField::some(resolved_exchange(&req.exchange_id, &self.default_exchange));
        let orders = self.gateway.batch_create_orders(req).await.map_err(internal)?;
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
        let order = self.gateway.cancel_order(req).await.map_err(internal)?;
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
        let orders = self.gateway.cancel_all_orders(req).await.map_err(internal)?;
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
        let orders = self.gateway.fetch_open_orders(req).await.map_err(internal)?;
        let resp = trading::FetchOpenOrdersResponse { orders, ..Default::default() };
        Response::ok(PreEncoded::from_message(&resp))
    }

    // ---- Account / history queries ----
    //
    // The worker's gateway port does not surface these yet; they are served
    // natively by the terminal API. Reply `unimplemented` instead of failing
    // the whole service registration.

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn get_account(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::GetAccountRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetAccountResponse>> {
        Err(connectrpc::ConnectError::unimplemented("GetAccount is not proxied by the worker yet"))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn get_positions(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::GetPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetPositionsResponse>> {
        Err(connectrpc::ConnectError::unimplemented(
            "GetPositions is not proxied by the worker yet",
        ))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn get_order_history(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::GetOrderHistoryRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetOrderHistoryResponse>> {
        Err(connectrpc::ConnectError::unimplemented(
            "GetOrderHistory is not proxied by the worker yet",
        ))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn get_closed_positions(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::GetClosedPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::GetClosedPositionsResponse>> {
        Err(connectrpc::ConnectError::unimplemented(
            "GetClosedPositions is not proxied by the worker yet",
        ))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn close_position(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::ClosePositionRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::ClosePositionResponse>> {
        Err(connectrpc::ConnectError::unimplemented(
            "ClosePosition is not proxied by the worker yet",
        ))
    }

    #[allow(clippy::unused_async_trait_impl)] // passthrough stub
    async fn close_all_positions(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, trading::CloseAllPositionsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<trading::CloseAllPositionsResponse>> {
        Err(connectrpc::ConnectError::unimplemented(
            "CloseAllPositions is not proxied by the worker yet",
        ))
    }
}

/// Forwards unified market data RPCs onto the [`MarketDataSource`] port.
pub struct MarketDataProxy {
    pub market: Arc<dyn MarketDataSource>,
}

#[allow(refining_impl_trait)]
impl market::MarketDataService for MarketDataProxy {
    async fn list_symbols(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::ListSymbolsRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::ListSymbolsResponse>> {
        let req = request.to_owned_message();
        let resp = self.market.list_symbols(req).await.map_err(internal)?;
        Response::ok(PreEncoded::from_message(&resp))
    }

    async fn fetch_ticker(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, market::FetchTickerRequest>,
    ) -> connectrpc::ServiceResult<PreEncoded<market::FetchTickerResponse>> {
        let req = request.to_owned_message();
        let ticker = self.market.fetch_ticker(req).await.map_err(internal)?;
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
        let book = self.market.fetch_order_book(req).await.map_err(internal)?;
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
        let mut rx = self.market.subscribe_market_data(req, policy).await.map_err(internal)?;
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
        let resp = self.market.get_candles(req).await.map_err(internal)?;
        Response::ok(PreEncoded::from_message(&resp))
    }
}
