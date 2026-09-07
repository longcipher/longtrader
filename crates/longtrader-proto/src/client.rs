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
    client_core::{SERVICE_MARKET, SERVICE_RUNTIME, SERVICE_TRADING, service_url, trim_base_url},
    proto::longtrader::terminal::v1 as proto,
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
        if let Some(token) = &self.auth_token {
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
        let url = self.url(service, method);
        let body = req.encode_to_vec();
        let resp = self
            .request(&url, body, "application/proto")
            .send()
            .await
            .map_err(|e| TerminalClientError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let body_bytes = resp
                .bytes()
                .await
                .map_err(|e| TerminalClientError::Http(format!("read error: {e}")))?;
            let text = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(TerminalClientError::Rpc { code: status.as_u16().into(), message: text });
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| TerminalClientError::Http(format!("read error: {e}")))?;
        R::decode_from_slice(&body_bytes)
            .map_err(|e| TerminalClientError::Decode(format!("decode {method}: {e}")))
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
        let req = proto::GetCandlesRequest {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            timeframe: buffa::EnumValue::Known(timeframe),
            limit,
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
            limit,
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
        let req =
            proto::GetOrderHistoryRequest { venue: venue.to_string(), limit, ..Default::default() };
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
            limit,
            ..Default::default()
        };
        let resp: proto::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp.positions)
    }

    /// Place an order.
    pub async fn place_order(
        &self,
        venue: &str,
        symbol: &str,
        side: proto::Side,
        order_type: proto::OrderType,
        quantity: &str,
        price: Option<&str>,
        take_profit: Option<&str>,
        stop_loss: Option<&str>,
        client_order_id: &str,
    ) -> Result<proto::Order, TerminalClientError> {
        let req = proto::PlaceOrderRequest {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            side: buffa::EnumValue::Known(side),
            order_type: buffa::EnumValue::Known(order_type),
            quantity: quantity.to_string(),
            price: price.map(String::from),
            take_profit: take_profit.map(String::from),
            stop_loss: stop_loss.map(String::from),
            client_order_id: client_order_id.to_string(),
            ..Default::default()
        };
        let resp: proto::PlaceOrderResponse =
            self.unary(SERVICE_TRADING, "PlaceOrder", req).await?;
        resp.order
            .as_option()
            .cloned()
            .ok_or_else(|| TerminalClientError::MissingField("order".to_string()))
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

    /// Close a position.
    pub async fn close_position(
        &self,
        venue: &str,
        position_id: &str,
    ) -> Result<(), TerminalClientError> {
        let req = proto::ClosePositionRequest {
            venue: venue.to_string(),
            position_id: position_id.to_string(),
            close_bps: 10_000,
            price_choice: Some(proto::close_position_request::PriceChoice::Market(true)),
            ..Default::default()
        };
        let _resp: proto::ClosePositionResponse =
            self.unary(SERVICE_TRADING, "ClosePosition", req).await?;
        Ok(())
    }

    /// Close all positions.
    pub async fn close_all_positions(&self, venue: &str) -> Result<(), TerminalClientError> {
        let req =
            proto::CloseAllPositionsRequest { venue: venue.to_string(), ..Default::default() };
        let _resp: proto::CloseAllPositionsResponse =
            self.unary(SERVICE_TRADING, "CloseAllPositions", req).await?;
        Ok(())
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
            .request(&url, body, "application/connect+proto")
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
