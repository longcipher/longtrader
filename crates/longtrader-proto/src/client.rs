//! Native (non-WASM) Connect-RPC client backed by `hpx`.
//!
//! Implements the `longtrader.terminal.v1` services over the Connect
//! protocol's binary (`application/connect+proto`) encoding. Streaming
//! (`RuntimeService::StreamUpdates`) is exposed as an NDJSON-free
//! length-prefixed message stream.

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
        terminal::v1 as proto, trading::v1 as utrading,
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

/// Connect-RPC client for the trading terminal services.
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

    // ---- MarketDataService ----

    /// List available symbols.
    pub async fn get_symbols(
        &self,
        venue: &str,
    ) -> Result<Vec<proto::Symbol>, TerminalClientError> {
        let req = proto::GetSymbolsRequest { venue: venue.to_string(), ..Default::default() };
        let resp: proto::GetSymbolsResponse = self.unary(SERVICE_MARKET, "GetSymbols", req).await?;
        Ok(resp.symbols)
    }

    /// Fetch OHLCV candles.
    pub async fn get_candles(
        &self,
        venue: &str,
        symbol: &str,
        timeframe: proto::Timeframe,
        limit: u32,
    ) -> Result<Vec<proto::Candle>, TerminalClientError> {
        self.get_candles_window(venue, symbol, timeframe, None, None, limit).await
    }

    /// Fetch OHLCV candles with optional time window.
    pub async fn get_candles_window(
        &self,
        venue: &str,
        symbol: &str,
        timeframe: proto::Timeframe,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: u32,
    ) -> Result<Vec<proto::Candle>, TerminalClientError> {
        let req = proto::GetCandlesRequest {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            timeframe: buffa::EnumValue::Known(timeframe),
            start_ms,
            end_ms,
            pagination: crate::proto::longtrader::common::v1::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let resp: proto::GetCandlesResponse = self.unary(SERVICE_MARKET, "GetCandles", req).await?;
        Ok(resp.candles)
    }

    /// Fetch the current order book snapshot.
    pub async fn get_book(
        &self,
        venue: &str,
        symbol: &str,
        depth: u32,
    ) -> Result<proto::Book, TerminalClientError> {
        let req = proto::GetBookRequest {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            depth,
            ..Default::default()
        };
        let resp: proto::GetBookResponse = self.unary(SERVICE_MARKET, "GetBook", req).await?;
        resp.book
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("book".to_string()))
    }

    /// Fetch tickers (empty `symbols` = all).
    pub async fn get_tickers(
        &self,
        venue: &str,
        symbols: &[String],
    ) -> Result<Vec<proto::Ticker>, TerminalClientError> {
        let req = proto::GetTickersRequest {
            venue: venue.to_string(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: proto::GetTickersResponse = self.unary(SERVICE_MARKET, "GetTickers", req).await?;
        Ok(resp.tickers)
    }

    /// Search symbols by query.
    pub async fn search_symbols(
        &self,
        venue: &str,
        query: &str,
        limit: u32,
    ) -> Result<Vec<proto::Symbol>, TerminalClientError> {
        let req = proto::SearchSymbolsRequest {
            venue: venue.to_string(),
            query: query.to_string(),
            pagination: crate::proto::longtrader::common::v1::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let resp: proto::SearchSymbolsResponse =
            self.unary(SERVICE_MARKET, "SearchSymbols", req).await?;
        Ok(resp.symbols)
    }

    // ---- TradingService ----

    /// Fetch account snapshot.
    pub async fn get_account(&self, venue: &str) -> Result<proto::Account, TerminalClientError> {
        let req = proto::GetAccountRequest { venue: venue.to_string(), ..Default::default() };
        let resp: proto::GetAccountResponse =
            self.unary(SERVICE_TRADING, "GetAccount", req).await?;
        resp.account
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("account".to_string()))
    }

    /// Fetch open positions.
    pub async fn get_positions(
        &self,
        venue: &str,
    ) -> Result<Vec<proto::Position>, TerminalClientError> {
        let req = proto::GetPositionsRequest { venue: venue.to_string(), ..Default::default() };
        let resp: proto::GetPositionsResponse =
            self.unary(SERVICE_TRADING, "GetPositions", req).await?;
        Ok(resp.positions)
    }

    /// Fetch open orders.
    pub async fn get_open_orders(
        &self,
        venue: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<proto::Order>, TerminalClientError> {
        let req = proto::GetOpenOrdersRequest {
            venue: venue.to_string(),
            symbol: symbol.map(String::from),
            ..Default::default()
        };
        let resp: proto::GetOpenOrdersResponse =
            self.unary(SERVICE_TRADING, "GetOpenOrders", req).await?;
        Ok(resp.orders)
    }

    /// Fetch order history.
    pub async fn get_order_history(
        &self,
        venue: &str,
        limit: u32,
    ) -> Result<Vec<proto::Order>, TerminalClientError> {
        let req = proto::GetOrderHistoryRequest {
            venue: venue.to_string(),
            pagination: crate::proto::longtrader::common::v1::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let resp: proto::GetOrderHistoryResponse =
            self.unary(SERVICE_TRADING, "GetOrderHistory", req).await?;
        Ok(resp.orders)
    }

    /// Fetch closed positions.
    pub async fn get_closed_positions(
        &self,
        venue: &str,
        limit: u32,
    ) -> Result<Vec<proto::ClosedPosition>, TerminalClientError> {
        let req = proto::GetClosedPositionsRequest {
            venue: venue.to_string(),
            pagination: crate::proto::longtrader::common::v1::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let resp: proto::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp.positions)
    }

    /// Place an order.
    ///
    /// `stop_price` carries the unified contract's `trigger_price` and
    /// `reduce_only` its risk flag. Both are mapped rather than dropped:
    /// omitting them silently turned a conditional order into a plain one and
    /// a risk-reducing close into one that could open opposite exposure.
    #[expect(clippy::too_many_arguments)]
    pub async fn place_order(
        &self,
        venue: &str,
        symbol: &str,
        side: proto::Side,
        order_type: proto::OrderType,
        quantity: &str,
        price: Option<&str>,
        stop_price: Option<&str>,
        take_profit: Option<&str>,
        stop_loss: Option<&str>,
        client_order_id: &str,
        reduce_only: bool,
    ) -> Result<proto::Order, TerminalClientError> {
        let req = build_place_order_request(
            venue,
            symbol,
            side,
            order_type,
            quantity,
            price,
            stop_price,
            take_profit,
            stop_loss,
            client_order_id,
            reduce_only,
        );
        let resp: proto::PlaceOrderResponse =
            self.unary(SERVICE_TRADING, "PlaceOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Current funding rates for `symbols` (empty = every symbol).
    pub async fn get_funding_rates(
        &self,
        venue: &str,
        symbols: &[String],
    ) -> Result<Vec<proto::FundingRate>, TerminalClientError> {
        let req = proto::GetFundingRatesRequest {
            venue: venue.to_string(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: proto::GetFundingRatesResponse =
            self.unary(SERVICE_MARKET, "GetFundingRates", req).await?;
        Ok(resp.funding_rates)
    }

    /// Recent funding settlements, newest first.
    pub async fn get_funding_rate_history(
        &self,
        venue: &str,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<proto::FundingRatePoint>, TerminalClientError> {
        let req = proto::GetFundingRateHistoryRequest {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            limit,
            ..Default::default()
        };
        let resp: proto::GetFundingRateHistoryResponse =
            self.unary(SERVICE_MARKET, "GetFundingRateHistory", req).await?;
        Ok(resp.points)
    }

    /// Place a venue-side conditional order that fires even if this process dies.
    pub async fn create_trigger_order(
        &self,
        venue: &str,
        order: proto::TriggerOrderRequest,
    ) -> Result<proto::TriggerOrder, TerminalClientError> {
        let req = proto::CreateTriggerOrderRequest {
            venue: venue.to_string(),
            order: order.into(),
            ..Default::default()
        };
        let resp: proto::CreateTriggerOrderResponse =
            self.unary(SERVICE_TRADING, "CreateTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Cancel a resting conditional order. Returns the updated order.
    pub async fn cancel_trigger_order(
        &self,
        venue: &str,
        order_id: &str,
        symbol: &str,
    ) -> Result<proto::TriggerOrder, TerminalClientError> {
        let req = proto::CancelTriggerOrderRequest {
            venue: venue.to_string(),
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: proto::CancelTriggerOrderResponse =
            self.unary(SERVICE_TRADING, "CancelTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Resting conditional orders, optionally filtered by symbol.
    pub async fn list_trigger_orders(
        &self,
        venue: &str,
        symbols: &[String],
    ) -> Result<Vec<proto::TriggerOrder>, TerminalClientError> {
        let req = proto::ListTriggerOrdersRequest {
            venue: venue.to_string(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: proto::ListTriggerOrdersResponse =
            self.unary(SERVICE_TRADING, "ListTriggerOrders", req).await?;
        Ok(resp.orders)
    }

    /// Wallet ledger rows, newest first, optionally filtered.
    pub async fn get_ledger_entries(
        &self,
        venue: &str,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<account::LedgerEntry>, TerminalClientError> {
        let req = proto::GetLedgerEntriesRequest {
            venue: venue.to_string(),
            currency: currency.to_string(),
            r#type: entry_type.to_string(),
            pagination: crate::proto::longtrader::common::v1::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let resp: proto::GetLedgerEntriesResponse =
            self.unary(SERVICE_TRADING, "GetLedgerEntries", req).await?;
        Ok(resp.entries)
    }

    /// Move `amount` of `asset` to `dest_label` on the same venue.
    ///
    /// `client_transfer_id` makes the call idempotent: a retry with the same
    /// id returns the first receipt instead of moving funds twice.
    pub async fn transfer(
        &self,
        venue: &str,
        asset: &str,
        amount: &str,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<proto::TransferResponse, TerminalClientError> {
        let req = proto::TransferRequest {
            venue: venue.to_string(),
            asset: asset.to_string(),
            amount: amount.to_string(),
            dest_label: dest_label.to_string(),
            client_transfer_id: client_transfer_id.to_string(),
            ..Default::default()
        };
        self.unary(SERVICE_TRADING, "Transfer", req).await
    }

    /// Cancel an open order.
    pub async fn cancel_order(
        &self,
        venue: &str,
        order_id: &str,
    ) -> Result<proto::Order, TerminalClientError> {
        let req = proto::CancelOrderRequest {
            venue: venue.to_string(),
            order_id: order_id.to_string(),
            ..Default::default()
        };
        let resp: proto::CancelOrderResponse =
            self.unary(SERVICE_TRADING, "CancelOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Close a position (full close, market).
    pub async fn close_position(
        &self,
        venue: &str,
        position_id: &str,
    ) -> Result<proto::Position, TerminalClientError> {
        let req = proto::ClosePositionRequest {
            venue: venue.to_string(),
            position_id: position_id.to_string(),
            close_bps: 10_000,
            price_choice: Some(proto::close_position_request::PriceChoice::Market(true)),
            ..Default::default()
        };
        let resp: proto::ClosePositionResponse =
            self.unary(SERVICE_TRADING, "ClosePosition", req).await?;
        resp.position
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("position".to_string()))
    }

    /// Cancel all open orders (optionally one symbol).
    pub async fn cancel_all(
        &self,
        venue: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<proto::Order>, TerminalClientError> {
        let req = proto::CancelAllRequest {
            venue: venue.to_string(),
            symbol: symbol.map(String::from),
            ..Default::default()
        };
        let resp: proto::CancelAllResponse = self.unary(SERVICE_TRADING, "CancelAll", req).await?;
        Ok(resp.orders)
    }

    /// Close all positions.
    pub async fn close_all_positions(&self, venue: &str) -> Result<(), TerminalClientError> {
        let req =
            proto::CloseAllPositionsRequest { venue: venue.to_string(), ..Default::default() };
        let _resp: proto::CloseAllPositionsResponse =
            self.unary(SERVICE_TRADING, "CloseAllPositions", req).await?;
        Ok(())
    }

    /// Modify the take-profit / stop-loss of an open position.
    pub async fn modify_position(
        &self,
        venue: &str,
        position_id: &str,
        take_profit: Option<&str>,
        stop_loss: Option<&str>,
    ) -> Result<proto::Position, TerminalClientError> {
        let req = proto::ModifyPositionRequest {
            venue: venue.to_string(),
            position_id: position_id.to_string(),
            take_profit: take_profit.map(String::from),
            stop_loss: stop_loss.map(String::from),
            ..Default::default()
        };
        let resp: proto::ModifyPositionResponse =
            self.unary(SERVICE_TRADING, "ModifyPosition", req).await?;
        resp.position
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("position".to_string()))
    }

    // ---- StrategyService ----

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

    // ---- RuntimeService ----

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
    /// Returns a stream of `UpdateEnvelope` messages. The response body is
    /// read incrementally with the Connect streaming encoding (no NDJSON).
    pub async fn stream_updates(
        &self,
        venues: &[String],
        symbols: &[String],
        topics: &[proto::TopicClass],
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<proto::UpdateEnvelope, TerminalClientError>> + Send>>,
        TerminalClientError,
    > {
        let url = self.url(SERVICE_RUNTIME, "StreamUpdates");
        let req = proto::StreamUpdatesRequest {
            venues: venues.to_vec(),
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
                            match proto::UpdateEnvelope::decode_from_slice(&payload) {
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
}

/// Build a `PlaceOrderRequest` from its parts.
///
/// Extracted from [`TerminalClient::place_order`] so the field mapping can be
/// asserted without a live venue — this is where a dropped field would
/// otherwise only show up as wrong behaviour on a real account.
#[expect(clippy::too_many_arguments)]
pub fn build_place_order_request(
    venue: &str,
    symbol: &str,
    side: proto::Side,
    order_type: proto::OrderType,
    quantity: &str,
    price: Option<&str>,
    stop_price: Option<&str>,
    take_profit: Option<&str>,
    stop_loss: Option<&str>,
    client_order_id: &str,
    reduce_only: bool,
) -> proto::PlaceOrderRequest {
    proto::PlaceOrderRequest {
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        side: buffa::EnumValue::Known(side),
        order_type: buffa::EnumValue::Known(order_type),
        quantity: quantity.to_string(),
        price: price.map(String::from),
        stop_price: stop_price.map(String::from),
        take_profit: take_profit.map(String::from),
        stop_loss: stop_loss.map(String::from),
        client_order_id: client_order_id.to_string(),
        reduce_only,
        ..Default::default()
    }
}

#[cfg(test)]
mod place_order_tests {
    use super::*;

    fn base() -> (proto::Side, proto::OrderType) {
        (proto::Side::Buy, proto::OrderType::Limit)
    }

    #[test]
    fn maps_every_optional_field() {
        let (side, order_type) = base();
        let req = build_place_order_request(
            "mock",
            "BTC/USDT",
            side,
            order_type,
            "1",
            Some("95000"),
            Some("94000"),
            Some("96000"),
            Some("93000"),
            "coid-1",
            true,
        );
        assert_eq!(req.venue, "mock");
        assert_eq!(req.symbol, "BTC/USDT");
        assert_eq!(req.quantity, "1");
        assert_eq!(req.price.as_deref(), Some("95000"));
        assert_eq!(req.stop_price.as_deref(), Some("94000"));
        assert_eq!(req.take_profit.as_deref(), Some("96000"));
        assert_eq!(req.stop_loss.as_deref(), Some("93000"));
        assert_eq!(req.client_order_id, "coid-1");
        assert!(req.reduce_only, "reduce_only must reach the venue");
    }

    #[test]
    fn absent_optionals_stay_unset() {
        let (side, order_type) = base();
        let req = build_place_order_request(
            "mock", "BTC/USDT", side, order_type, "1", None, None, None, None, "", false,
        );
        assert_eq!(req.price, None);
        assert_eq!(req.stop_price, None);
        assert!(!req.reduce_only);
    }

    #[test]
    fn stop_price_is_carried_not_dropped() {
        // Regression: `stop_price` was absent from the struct literal, so a
        // conditional order reached the venue with no trigger at all.
        let (side, order_type) = base();
        let req = build_place_order_request(
            "mock",
            "BTC/USDT",
            side,
            order_type,
            "1",
            Some("95000"),
            Some("94000"),
            None,
            None,
            "coid",
            false,
        );
        assert_eq!(req.stop_price.as_deref(), Some("94000"));
    }

    #[test]
    fn request_survives_a_protobuf_round_trip() {
        use buffa::Message;

        let (side, order_type) = base();
        let req = build_place_order_request(
            "mock",
            "BTC/USDT",
            side,
            order_type,
            "1",
            Some("95000"),
            Some("94000"),
            None,
            None,
            "coid",
            true,
        );
        let bytes = req.encode_to_vec();
        let back = proto::PlaceOrderRequest::decode_from_slice(&bytes).expect("decodes");
        assert_eq!(back.stop_price.as_deref(), Some("94000"));
        assert_eq!(back.price.as_deref(), Some("95000"));
        assert!(back.reduce_only);
    }
}

// ---- Unified surface: funding, conditional orders, wallet, venue ops ----
//
// These target the `longtrader.*.v1` services the `RemoteAdapter` speaks, as
// opposed to the `terminal.v1` methods above. `longtrader.terminal.v1` keeps
// decimals as strings (the desktop UI round-trips what it was sent); the
// unified contract uses typed `common.v1.Decimal`, so these convert.

/// `longtrader.ops.v1.VenueOpService` — the self-describing registry of
/// exchange-specific operations.
pub const SERVICE_VENUE_OPS: &str = "longtrader.ops.v1.VenueOpService";
/// `longtrader.market.v1.MarketDataService` (unified).
pub const SERVICE_UNIFIED_MARKET: &str = "longtrader.market.v1.MarketDataService";
/// `longtrader.trading.v1.TradingService` (unified).
pub const SERVICE_UNIFIED_TRADING: &str = "longtrader.trading.v1.TradingService";

/// Build the `common.v1.Decimal` message the unified contract expects.
///
/// All three fields are populated: the host's decoder trusts the numeric pair
/// when `raw_str` is empty, so writing only the string form arrives as zero.
fn unified_decimal(v: rust_decimal::Decimal) -> common::Decimal {
    longtrader_contract::ext::decimal_to_common(v)
}

fn unified_exchange_id(id: &common::ExchangeId) -> common::ExchangeId {
    id.clone()
}

impl TerminalClient {
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

    // ---- unified market: funding -----------------------------------------

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
            exchange_id: unified_exchange_id(exchange_id).into(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: umarket::FetchFundingRateResponse =
            self.unary(SERVICE_UNIFIED_MARKET, "FetchFundingRate", req).await?;
        Ok(resp.funding_rate.as_option().cloned())
    }

    /// Recent funding settlements, newest first.
    pub async fn fetch_funding_rate_history(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<umarket::FundingRatePoint>, TerminalClientError> {
        let req = umarket::FetchFundingRateHistoryRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            symbol: symbol.to_string(),
            limit,
            ..Default::default()
        };
        let resp: umarket::FetchFundingRateHistoryResponse =
            self.unary(SERVICE_UNIFIED_MARKET, "FetchFundingRateHistory", req).await?;
        Ok(resp.points)
    }

    // ---- unified trading: conditional orders -----------------------------

    /// Place a venue-side conditional order that fires even if this process dies.
    pub async fn create_trigger_order_unified(
        &self,
        exchange_id: &common::ExchangeId,
        order: utrading::TriggerOrderRequest,
    ) -> Result<utrading::TriggerOrder, TerminalClientError> {
        let req = utrading::CreateTriggerOrderRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            order: order.into(),
            ..Default::default()
        };
        let resp: utrading::CreateTriggerOrderResponse =
            self.unary(SERVICE_UNIFIED_TRADING, "CreateTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Cancel a resting conditional order. Returns the updated order.
    pub async fn cancel_trigger_order_unified(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<utrading::TriggerOrder, TerminalClientError> {
        let req = utrading::CancelTriggerOrderRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
            ..Default::default()
        };
        let resp: utrading::CancelTriggerOrderResponse =
            self.unary(SERVICE_UNIFIED_TRADING, "CancelTriggerOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
    }

    /// Resting conditional orders, optionally filtered by symbol.
    pub async fn list_trigger_orders_unified(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<utrading::TriggerOrder>, TerminalClientError> {
        let req = utrading::ListTriggerOrdersRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            symbols: symbols.to_vec(),
            ..Default::default()
        };
        let resp: utrading::ListTriggerOrdersResponse =
            self.unary(SERVICE_UNIFIED_TRADING, "ListTriggerOrders", req).await?;
        Ok(resp.orders)
    }

    // ---- unified trading: wallet -----------------------------------------

    /// Wallet ledger rows, newest first, optionally filtered.
    pub async fn fetch_ledger_entries(
        &self,
        exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<account::LedgerEntry>, TerminalClientError> {
        let req = utrading::FetchLedgerEntriesRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            currency: currency.to_string(),
            r#type: entry_type.to_string(),
            pagination: common::Pagination { limit: u64::from(limit), ..Default::default() }.into(),
            ..Default::default()
        };
        let resp: utrading::FetchLedgerEntriesResponse =
            self.unary(SERVICE_UNIFIED_TRADING, "FetchLedgerEntries", req).await?;
        Ok(resp.entries)
    }

    /// Move `amount` of `asset` to `dest_label` on the same venue.
    ///
    /// `client_transfer_id` makes the call idempotent.
    pub async fn transfer_unified(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: rust_decimal::Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<utrading::TransferResponse, TerminalClientError> {
        let req = utrading::TransferRequest {
            exchange_id: unified_exchange_id(exchange_id).into(),
            asset: asset.to_string(),
            amount: unified_decimal(amount).into(),
            dest_label: dest_label.to_string(),
            client_transfer_id: client_transfer_id.to_string(),
            ..Default::default()
        };
        self.unary(SERVICE_UNIFIED_TRADING, "Transfer", req).await
    }
}

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
mod extended_tests {
    use super::*;

    #[test]
    fn unified_decimal_is_lossless_through_the_host_decoder() {
        // The host decodes with `longtrader_contract::ext::common_to_decimal`,
        // which trusts the numeric pair when `raw_str` is empty. The invariant
        // that matters is therefore a lossless round trip, not that all three
        // representations are written.
        for value in [
            rust_decimal::Decimal::new(125, 2),   // 1.25
            rust_decimal::Decimal::new(1, 3),     // 0.001
            rust_decimal::Decimal::new(90000, 0), // 90000
            rust_decimal::Decimal::new(-5, 1),    // -0.5
            rust_decimal::Decimal::new(0, 0),
        ] {
            let wire = unified_decimal(value);
            let back = longtrader_contract::ext::common_to_decimal(&wire)
                .map_err(|e| format!("{value} must decode: {e}"));
            assert_eq!(back.expect("decodes"), value, "round trip must be lossless");
        }
    }

    #[test]
    fn unified_decimal_writes_the_numeric_pair() {
        let d = unified_decimal(rust_decimal::Decimal::new(125, 2));
        assert_eq!(d.unscaled, 125);
        assert_eq!(d.scale, 2);
    }

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
    }
}
