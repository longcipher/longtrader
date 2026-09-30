//! Native (non-WASM) Connect-RPC client backed by `hpx`.
//!
//! Speaks the canonical `longtrader.{market,trading,ops}.v1` services plus the
//! terminal-only `longtrader.terminal.v1` runtime/strategy surfaces, over the
//! Connect protocol's binary (`application/connect+proto`) encoding.
//!
//! There is no separate terminal market/trading surface any more: the terminal
//! backend serves the same canonical services, so one client covers both. The
//! update stream (`RuntimeService::StreamUpdates`) carries the canonical
//! `stream.v1.UpdateEnvelope` and is exposed as an NDJSON-free length-prefixed
//! message stream.

#![allow(clippy::pedantic)]

use std::pin::Pin;

use buffa::Message;
use futures_util::Stream;
use thiserror::Error;

use crate::{
    client_core::{
        SERVICE_MARKET, SERVICE_RUNTIME, SERVICE_STRATEGY, SERVICE_TRADING, service_url,
        trim_base_url,
    },
    proto::longtrader::{
        account::v1 as account, common::v1 as common, market::v1 as umarket, ops::v1 as ops,
        stream::v1 as stream, terminal::v1 as proto, trading::v1 as utrading,
    },
    transport::TransportError,
};

/// Client errors.
#[derive(Debug, Error)]
pub enum TerminalClientError {
    #[error("HTTP transport error: {0}")]
    Http(String),
    #[error("ConnectRPC error ({code}): {message}")]
    Rpc { code: u32, message: String },
    #[error("protobuf decode error: {0}")]
    Decode(String),
    #[error("missing required field: {0}")]
    MissingField(String),
}

/// Connect-RPC client for the trading terminal and canonical contract services.
#[derive(Clone)]
pub struct TerminalClient {
    base_url: String,
    auth_token: Option<String>,
    http: hpx::Client,
}

impl std::fmt::Debug for TerminalClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TerminalClient").field("base_url", &self.base_url).finish_non_exhaustive()
    }
}

impl TerminalClient {
    /// Create a new client targeting `base_url` (e.g. `http://127.0.0.1:8810`).
    #[must_use]
    pub fn new(base_url: &str) -> Self {
        Self::new_inner(base_url, None)
    }

    /// Create a client with a static bearer token.
    #[must_use]
    pub fn new_with_token(base_url: &str, token: &str) -> Self {
        Self::new_inner(base_url, Some(token.to_string()))
    }

    fn new_inner(base_url: &str, auth_token: Option<String>) -> Self {
        let base_url = trim_base_url(base_url);
        let http = match hpx::Client::builder().http1_only().build() {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!("failed to build hpx client, using default: {e}");
                hpx::Client::new()
            }
        };
        Self { base_url, auth_token, http }
    }

    fn url(&self, service: &str, method: &str) -> String {
        service_url(&self.base_url, service, method)
    }

    fn request(&self, url: &str, body: Vec<u8>, content_type: &str) -> hpx::RequestBuilder {
        let mut builder = self.http.post(url).header("content-type", content_type).body(body);
        if let Some(token) = self.auth_token.as_deref().filter(|t| !t.is_empty()) {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
    }

    async fn unary<Q: Message, R: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: Q,
    ) -> Result<R, TerminalClientError> {
        crate::transport::unary(
            &self.http,
            &self.base_url,
            service,
            method,
            self.auth_token.as_deref(),
            req,
        )
        .await
        .map_err(|e| match e {
            TransportError::Http(m) => TerminalClientError::Http(m),
            TransportError::Rpc { code, message } => TerminalClientError::Rpc { code, message },
            TransportError::Decode(m) => TerminalClientError::Decode(m),
        })
    }

    // ---- MarketDataService (canonical) ----

    /// Every symbol the exchange lists.
    pub async fn list_symbols(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<umarket::SymbolInfo>, TerminalClientError> {
        let req = umarket::ListSymbolsRequest {
            exchange_id: exchange_id.clone().into(),
            ..Default::default()
        };
        let resp: umarket::ListSymbolsResponse =
            self.unary(SERVICE_MARKET, "ListSymbols", req).await?;
        Ok(resp.symbols)
    }

    /// Case-insensitive substring search over symbol names.
    pub async fn search_symbols(
        &self,
        exchange_id: &common::ExchangeId,
        query: &str,
        limit: u32,
    ) -> Result<Vec<umarket::SymbolInfo>, TerminalClientError> {
        let req = umarket::SearchSymbolsRequest {
            exchange_id: exchange_id.clone().into(),
            query: query.to_string(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: umarket::SearchSymbolsResponse =
            self.unary(SERVICE_MARKET, "SearchSymbols", req).await?;
        Ok(resp.symbols)
    }

    /// Fetch OHLCV candles.
    pub async fn get_candles(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        timeframe: umarket::Timeframe,
        limit: u32,
    ) -> Result<Vec<umarket::Candle>, TerminalClientError> {
        self.get_candles_window(exchange_id, symbol, timeframe, None, None, limit).await
    }

    /// Fetch OHLCV candles within an inclusive time window (Unix milliseconds).
    ///
    /// Either bound may be absent, which leaves that end open.
    pub async fn get_candles_window(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        timeframe: umarket::Timeframe,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: u32,
    ) -> Result<Vec<umarket::Candle>, TerminalClientError> {
        let req = umarket::GetCandlesRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            timeframe: buffa::EnumValue::Known(timeframe),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            start_ms,
            end_ms,
            ..Default::default()
        };
        let resp: umarket::GetCandlesResponse =
            self.unary(SERVICE_MARKET, "GetCandles", req).await?;
        Ok(resp.candles)
    }

    /// Fetch the current order book snapshot.
    pub async fn fetch_order_book(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<umarket::OrderBook, TerminalClientError> {
        let req = umarket::FetchOrderBookRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: umarket::FetchOrderBookResponse =
            self.unary(SERVICE_MARKET, "FetchOrderBook", req).await?;
        resp.orderbook
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("orderbook".to_string()))
    }

    /// Single-symbol ticker snapshot.
    pub async fn fetch_ticker(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<umarket::Ticker, TerminalClientError> {
        let req = umarket::FetchTickerRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: umarket::FetchTickerResponse =
            self.unary(SERVICE_MARKET, "FetchTicker", req).await?;
        resp.ticker
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("ticker".to_string()))
    }

    /// Batch ticker snapshots (empty `symbols` = every symbol).
    pub async fn list_tickers(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<umarket::Ticker>, TerminalClientError> {
        let req = umarket::ListTickersRequest {
            exchange_id: exchange_id.clone().into(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: umarket::ListTickersResponse =
            self.unary(SERVICE_MARKET, "ListTickers", req).await?;
        Ok(resp.tickers)
    }

    /// Current funding rate for one perpetual contract.
    ///
    /// `Ok(None)` means the venue tracks no perps for the symbol, which is
    /// distinct from the venue having no funding surface at all (an error).
    pub async fn fetch_funding_rate(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<Option<umarket::FundingRate>, TerminalClientError> {
        let req = umarket::FetchFundingRateRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: umarket::FetchFundingRateResponse =
            self.unary(SERVICE_MARKET, "FetchFundingRate", req).await?;
        Ok(resp.funding_rate.as_option().cloned())
    }

    /// Batch funding rates (empty `symbols` = every symbol).
    pub async fn list_funding_rates(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<umarket::FundingRate>, TerminalClientError> {
        let req = umarket::ListFundingRatesRequest {
            exchange_id: exchange_id.clone().into(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: umarket::ListFundingRatesResponse =
            self.unary(SERVICE_MARKET, "ListFundingRates", req).await?;
        Ok(resp.funding_rates)
    }

    /// Recent funding settlements, newest first.
    pub async fn fetch_funding_rate_history(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<umarket::FundingRatePoint>, TerminalClientError> {
        let req = umarket::FetchFundingRateHistoryRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            limit,
            ..Default::default()
        };
        let resp: umarket::FetchFundingRateHistoryResponse =
            self.unary(SERVICE_MARKET, "FetchFundingRateHistory", req).await?;
        Ok(resp.points)
    }

    // ---- TradingService (canonical) ----

    /// Submit one order.
    pub async fn create_order(
        &self,
        req: utrading::CreateOrderRequest,
    ) -> Result<utrading::Order, TerminalClientError> {
        let resp: utrading::CreateOrderResponse =
            self.unary(SERVICE_TRADING, "CreateOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Submit an all-or-nothing batch of orders.
    pub async fn create_orders(
        &self,
        req: utrading::CreateOrdersRequest,
    ) -> Result<Vec<utrading::Order>, TerminalClientError> {
        let resp: utrading::CreateOrdersResponse =
            self.unary(SERVICE_TRADING, "CreateOrders", req).await?;
        Ok(resp.orders)
    }

    /// Cancel an open order. Returns the updated order.
    pub async fn cancel_order(
        &self,
        req: utrading::CancelOrderRequest,
    ) -> Result<utrading::Order, TerminalClientError> {
        let resp: utrading::CancelOrderResponse =
            self.unary(SERVICE_TRADING, "CancelOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Cancel every open order on a venue (optionally one symbol).
    pub async fn cancel_all_orders(
        &self,
        req: utrading::CancelAllOrdersRequest,
    ) -> Result<Vec<utrading::Order>, TerminalClientError> {
        let resp: utrading::CancelAllOrdersResponse =
            self.unary(SERVICE_TRADING, "CancelAllOrders", req).await?;
        Ok(resp.orders)
    }

    /// Fetch open orders, optionally filtered to one symbol.
    pub async fn fetch_open_orders(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<utrading::Order>, TerminalClientError> {
        let req = utrading::FetchOpenOrdersRequest {
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: utrading::FetchOpenOrdersResponse =
            self.unary(SERVICE_TRADING, "FetchOpenOrders", req).await?;
        Ok(resp.orders)
    }

    /// Fetch the account snapshot.
    pub async fn get_account(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<utrading::Account, TerminalClientError> {
        let req = utrading::GetAccountRequest {
            exchange_id: exchange_id.clone().into(),
            ..Default::default()
        };
        let resp: utrading::GetAccountResponse =
            self.unary(SERVICE_TRADING, "GetAccount", req).await?;
        resp.account
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("account".to_string()))
    }

    /// Fetch open positions (empty `symbols` = every symbol).
    pub async fn get_positions(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<utrading::Position>, TerminalClientError> {
        let req = utrading::GetPositionsRequest {
            exchange_id: exchange_id.clone().into(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: utrading::GetPositionsResponse =
            self.unary(SERVICE_TRADING, "GetPositions", req).await?;
        Ok(resp.positions)
    }

    /// Fetch order history.
    pub async fn get_order_history(
        &self,
        exchange_id: &common::ExchangeId,
        limit: u32,
    ) -> Result<Vec<utrading::Order>, TerminalClientError> {
        let req = utrading::GetOrderHistoryRequest {
            exchange_id: exchange_id.clone().into(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: utrading::GetOrderHistoryResponse =
            self.unary(SERVICE_TRADING, "GetOrderHistory", req).await?;
        Ok(resp.orders)
    }

    /// Fetch recently closed positions.
    pub async fn get_closed_positions(
        &self,
        exchange_id: &common::ExchangeId,
        limit: u32,
    ) -> Result<Vec<utrading::ClosedPosition>, TerminalClientError> {
        let req = utrading::GetClosedPositionsRequest {
            exchange_id: exchange_id.clone().into(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: utrading::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp.positions)
    }

    /// Close a position (full close, market).
    pub async fn close_position(
        &self,
        exchange_id: &common::ExchangeId,
        position_id: &str,
    ) -> Result<utrading::Position, TerminalClientError> {
        let req = utrading::ClosePositionRequest {
            exchange_id: exchange_id.clone().into(),
            position_id: position_id.to_string(),
            ..Default::default()
        };
        let resp: utrading::ClosePositionResponse =
            self.unary(SERVICE_TRADING, "ClosePosition", req).await?;
        resp.position
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("position".to_string()))
    }

    /// Close every open position on a venue.
    pub async fn close_all_positions(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<(), TerminalClientError> {
        let req = utrading::CloseAllPositionsRequest {
            exchange_id: exchange_id.clone().into(),
            ..Default::default()
        };
        let _resp: utrading::CloseAllPositionsResponse =
            self.unary(SERVICE_TRADING, "CloseAllPositions", req).await?;
        Ok(())
    }

    /// Modify the take-profit / stop-loss of an open position.
    ///
    /// `None` leaves the corresponding bracket unchanged.
    pub async fn modify_position(
        &self,
        exchange_id: &common::ExchangeId,
        position_id: &str,
        take_profit: Option<rust_decimal::Decimal>,
        stop_loss: Option<rust_decimal::Decimal>,
    ) -> Result<utrading::Position, TerminalClientError> {
        let req = utrading::ModifyPositionRequest {
            exchange_id: exchange_id.clone().into(),
            position_id: position_id.to_string(),
            take_profit: take_profit
                .map(|v| longtrader_contract::ext::decimal_to_common(v).into())
                .unwrap_or_default(),
            stop_loss: stop_loss
                .map(|v| longtrader_contract::ext::decimal_to_common(v).into())
                .unwrap_or_default(),
            ..Default::default()
        };
        let resp: utrading::ModifyPositionResponse =
            self.unary(SERVICE_TRADING, "ModifyPosition", req).await?;
        resp.position
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("position".to_string()))
    }

    // ---- Trading: venue-side conditional orders ----

    /// Place a venue-side conditional order that fires even if this process dies.
    pub async fn create_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order: utrading::TriggerOrderRequest,
    ) -> Result<utrading::TriggerOrder, TerminalClientError> {
        let req = utrading::CreateTriggerOrderRequest {
            exchange_id: exchange_id.clone().into(),
            order: order.into(),
            ..Default::default()
        };
        let resp: utrading::CreateTriggerOrderResponse =
            self.unary(SERVICE_TRADING, "CreateTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Cancel a resting conditional order. Returns the updated order.
    pub async fn cancel_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<utrading::TriggerOrder, TerminalClientError> {
        let req = utrading::CancelTriggerOrderRequest {
            exchange_id: exchange_id.clone().into(),
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: utrading::CancelTriggerOrderResponse =
            self.unary(SERVICE_TRADING, "CancelTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Resting conditional orders, optionally filtered by symbol.
    pub async fn list_trigger_orders(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<utrading::TriggerOrder>, TerminalClientError> {
        let req = utrading::ListTriggerOrdersRequest {
            exchange_id: exchange_id.clone().into(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: utrading::ListTriggerOrdersResponse =
            self.unary(SERVICE_TRADING, "ListTriggerOrders", req).await?;
        Ok(resp.orders)
    }

    // ---- Trading: wallet ----

    /// Wallet ledger rows, newest first, optionally filtered.
    pub async fn fetch_ledger_entries(
        &self,
        exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<account::LedgerEntry>, TerminalClientError> {
        let req = utrading::FetchLedgerEntriesRequest {
            exchange_id: exchange_id.clone().into(),
            currency: currency.to_string(),
            r#type: entry_type.to_string(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: utrading::FetchLedgerEntriesResponse =
            self.unary(SERVICE_TRADING, "FetchLedgerEntries", req).await?;
        Ok(resp.entries)
    }

    /// Move `amount` of `asset` to `dest_label` on the same venue.
    ///
    /// `client_transfer_id` makes the call idempotent: a retry with the same
    /// id returns the first receipt instead of moving funds twice.
    pub async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: rust_decimal::Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<utrading::TransferResponse, TerminalClientError> {
        let req = utrading::TransferRequest {
            exchange_id: exchange_id.clone().into(),
            asset: asset.to_string(),
            amount: longtrader_contract::ext::decimal_to_common(amount).into(),
            dest_label: dest_label.to_string(),
            client_transfer_id: client_transfer_id.to_string(),
            ..Default::default()
        };
        self.unary(SERVICE_TRADING, "Transfer", req).await
    }

    // ---- StrategyService (terminal) ----

    /// List strategies.
    pub async fn list_strategies(&self) -> Result<Vec<proto::StrategyStatus>, TerminalClientError> {
        let req = proto::ListStrategiesRequest::default();
        let resp: proto::ListStrategiesResponse =
            self.unary(SERVICE_STRATEGY, "ListStrategies", req).await?;
        Ok(resp.strategies)
    }

    /// Start a strategy.
    pub async fn start_strategy(
        &self,
        strategy_id: &str,
        name: &str,
        params_json: &str,
    ) -> Result<proto::StrategyStatus, TerminalClientError> {
        let req = proto::StartStrategyRequest {
            strategy_id: strategy_id.to_string(),
            name: name.to_string(),
            params_json: params_json.to_string(),
            ..Default::default()
        };
        let resp: proto::StrategyStatus =
            self.unary(SERVICE_STRATEGY, "StartStrategy", req).await?;
        Ok(resp)
    }

    /// Pause a strategy.
    pub async fn pause_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<proto::StrategyStatus, TerminalClientError> {
        let req = proto::PauseStrategyRequest {
            strategy_id: strategy_id.to_string(),
            ..Default::default()
        };
        let resp: proto::StrategyStatus =
            self.unary(SERVICE_STRATEGY, "PauseStrategy", req).await?;
        Ok(resp)
    }

    /// Resume a strategy.
    pub async fn resume_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<proto::StrategyStatus, TerminalClientError> {
        let req = proto::ResumeStrategyRequest {
            strategy_id: strategy_id.to_string(),
            ..Default::default()
        };
        let resp: proto::StrategyStatus =
            self.unary(SERVICE_STRATEGY, "ResumeStrategy", req).await?;
        Ok(resp)
    }

    /// Stop a strategy.
    pub async fn stop_strategy(
        &self,
        strategy_id: &str,
        cancel_all_orders: bool,
    ) -> Result<proto::StrategyStatus, TerminalClientError> {
        let req = proto::StopStrategyRequest {
            strategy_id: strategy_id.to_string(),
            cancel_all_orders,
            ..Default::default()
        };
        let resp: proto::StrategyStatus = self.unary(SERVICE_STRATEGY, "StopStrategy", req).await?;
        Ok(resp)
    }

    /// Get strategy status.
    pub async fn get_strategy_status(
        &self,
        strategy_id: &str,
    ) -> Result<proto::StrategyStatus, TerminalClientError> {
        let req = proto::GetStrategyStatusRequest {
            strategy_id: strategy_id.to_string(),
            ..Default::default()
        };
        let resp: proto::StrategyStatus =
            self.unary(SERVICE_STRATEGY, "GetStrategyStatus", req).await?;
        Ok(resp)
    }

    /// Submit a strategy-scoped action.
    pub async fn submit_action(
        &self,
        strategy_id: &str,
        action: proto::submit_action_request::Action,
    ) -> Result<proto::ActionReceipt, TerminalClientError> {
        let req = proto::SubmitActionRequest {
            strategy_id: strategy_id.to_string(),
            action: Some(action),
            ..Default::default()
        };
        self.unary(SERVICE_STRATEGY, "SubmitAction", req).await
    }

    // ---- RuntimeService (terminal) ----

    /// Health check.
    pub async fn health(&self) -> Result<proto::HealthResponse, TerminalClientError> {
        let req = proto::HealthRequest::default();
        let resp: proto::HealthResponse = self.unary(SERVICE_RUNTIME, "Health", req).await?;
        Ok(resp)
    }

    /// List venues.
    pub async fn list_venues(&self) -> Result<Vec<proto::VenueStatus>, TerminalClientError> {
        let req = proto::ListVenuesRequest::default();
        let resp: proto::ListVenuesResponse =
            self.unary(SERVICE_RUNTIME, "ListVenues", req).await?;
        Ok(resp.venues)
    }

    /// Open the streaming updates channel.
    ///
    /// Returns a stream of canonical `stream.v1.UpdateEnvelope` messages; the
    /// same envelope the worker's `StreamService` emits. The response body is
    /// read incrementally with the Connect streaming encoding (no NDJSON).
    pub async fn stream_updates(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
        topics: &[stream::TopicClass],
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<stream::UpdateEnvelope, TerminalClientError>> + Send>>,
        TerminalClientError,
    > {
        let url = self.url(SERVICE_RUNTIME, "StreamUpdates");
        let req = stream::StreamUpdatesRequest {
            exchange_id: exchange_id.clone().into(),
            symbols: symbols.to_vec(),
            topics: topics.iter().map(|t| buffa::EnumValue::Known(*t)).collect(),
            ..Default::default()
        };
        let payload = req.encode_to_vec();
        // Connect streaming framing for the request: 1 flag byte (0 =
        // uncompressed) + 4-byte big-endian length + protobuf payload.
        let mut body = Vec::with_capacity(5 + payload.len());
        body.push(0);
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(&payload);

        let resp = self
            .request(&url, body, crate::transport::CONNECT_PROTO_STREAM)
            .send()
            .await
            .map_err(|e| TerminalClientError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp
                .text()
                .await
                .map_err(|e| TerminalClientError::Http(format!("read error: {e}")))?;
            return Err(TerminalClientError::Rpc { code: status.as_u16().into(), message: text });
        }

        // Connect streaming framing: per-message prefix of 1 byte flags +
        // 4-byte big-endian length, followed by the raw protobuf payload.
        let stream = futures_util::stream::unfold(
            (resp, Vec::<u8>::new()),
            |(mut resp, mut buf)| async move {
                loop {
                    if buf.len() >= 5 {
                        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                        let total = 5 + len;
                        if buf.len() >= total {
                            let payload: Vec<u8> = buf[5..total].to_vec();
                            buf.drain(..total);
                            match stream::UpdateEnvelope::decode_from_slice(&payload) {
                                Ok(env) => return Some((Ok(env), (resp, buf))),
                                Err(e) => {
                                    return Some((
                                        Err(TerminalClientError::Decode(e.to_string())),
                                        (resp, buf),
                                    ));
                                }
                            }
                        }
                    }
                    match resp.chunk().await {
                        Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
                        Ok(None) => return None,
                        Err(e) => {
                            return Some((
                                Err(TerminalClientError::Http(format!("stream read: {e}"))),
                                (resp, buf),
                            ));
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    // ---- ops.v1 ----------------------------------------------------------

    /// Names of the venue-specific operations `exchange_id` supports.
    pub async fn list_venue_ops(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<ops::OpDescriptor>, TerminalClientError> {
        let req =
            ops::ListVenueOpsRequest { exchange_id: exchange_id.id.clone(), ..Default::default() };
        let resp: ops::ListVenueOpsResponse =
            self.unary(SERVICE_VENUE_OPS, "ListVenueOps", req).await?;
        Ok(resp.ops)
    }

    /// Full schema of one venue operation.
    pub async fn describe_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
    ) -> Result<ops::OpDescriptor, TerminalClientError> {
        let req = ops::DescribeVenueOpRequest {
            exchange_id: exchange_id.id.clone(),
            op: op.to_string(),
            ..Default::default()
        };
        let resp: ops::DescribeVenueOpResponse =
            self.unary(SERVICE_VENUE_OPS, "DescribeVenueOp", req).await?;
        resp.op
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("op".to_string()))
    }

    /// Invoke a venue operation with dynamically-typed parameters.
    pub async fn invoke_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, TerminalClientError> {
        let req = ops::InvokeVenueOpRequest {
            exchange_id: exchange_id.id.clone(),
            op: op.to_string(),
            params: json_to_struct(params)?.into(),
            ..Default::default()
        };
        let resp: ops::InvokeVenueOpResponse =
            self.unary(SERVICE_VENUE_OPS, "InvokeVenueOp", req).await?;
        struct_to_json(
            resp.result
                .as_option()
                .ok_or_else(|| TerminalClientError::MissingField("result".to_string()))?,
        )
    }
}

/// `longtrader.ops.v1.VenueOpService` — the self-describing registry of
/// exchange-specific operations.
pub const SERVICE_VENUE_OPS: &str = "longtrader.ops.v1.VenueOpService";
/// `longtrader.market.v1.MarketDataService`.
pub const SERVICE_UNIFIED_MARKET: &str = "longtrader.market.v1.MarketDataService";
/// `longtrader.trading.v1.TradingService`.
pub const SERVICE_UNIFIED_TRADING: &str = "longtrader.trading.v1.TradingService";
/// `longtrader.stream.v1.StreamService`.
pub const SERVICE_UNIFIED_STREAM: &str = "longtrader.stream.v1.StreamService";

/// Convert a JSON value into a `google.protobuf.Struct` for the ops wire.
fn json_to_struct(
    v: &serde_json::Value,
) -> Result<buffa_types::google::protobuf::Struct, TerminalClientError> {
    let json = serde_json::to_string(v)
        .map_err(|e| TerminalClientError::Decode(format!("params not serializable: {e}")))?;
    let out: buffa_types::google::protobuf::Struct = serde_json::from_str(&json)
        .map_err(|e| TerminalClientError::Decode(format!("params not a Struct: {e}")))?;
    Ok(out)
}

/// Convert a `google.protobuf.Struct` reply back into JSON.
fn struct_to_json(
    s: &buffa_types::google::protobuf::Struct,
) -> Result<serde_json::Value, TerminalClientError> {
    serde_json::to_value(s)
        .map_err(|e| TerminalClientError::Decode(format!("result not convertible to JSON: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_struct_round_trip() {
        let v = serde_json::json!({"asset": "USDT", "amount": "10", "nested": {"k": true}});
        let s = json_to_struct(&v).expect("to struct");
        let back = struct_to_json(&s).expect("to json");
        assert_eq!(back["asset"], "USDT");
        assert_eq!(back["amount"], "10");
        assert_eq!(back["nested"]["k"], true);
    }

    #[test]
    fn service_constants_match_the_contract() {
        assert_eq!(SERVICE_VENUE_OPS, "longtrader.ops.v1.VenueOpService");
        assert_eq!(SERVICE_UNIFIED_MARKET, "longtrader.market.v1.MarketDataService");
        assert_eq!(SERVICE_UNIFIED_TRADING, "longtrader.trading.v1.TradingService");
        assert_eq!(SERVICE_UNIFIED_STREAM, "longtrader.stream.v1.StreamService");
    }

    #[test]
    fn canonical_surface_uses_the_canonical_service_paths() {
        // The terminal no longer redefines market/trading: every market and
        // trading call must land on the canonical service, or a converged
        // backend would answer with `unimplemented`.
        assert_eq!(SERVICE_MARKET, SERVICE_UNIFIED_MARKET);
        assert_eq!(SERVICE_TRADING, SERVICE_UNIFIED_TRADING);
    }
}
