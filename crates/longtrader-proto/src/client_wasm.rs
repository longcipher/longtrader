//! WASM-compatible Connect-RPC client backed by `gloo-net` (browser).
//!
//! Mirrors the native [`client`](crate::client) API surface so browser
//! frontends talk the same `longtrader.terminal.v1` protocol with identical
//! method names. Streaming (`RuntimeService::StreamUpdates`) uses `web-sys`
//! fetch + `ReadableStream` so chunked Connect framing works in the browser.

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
    proto::longtrader::{common::v1 as common, terminal::v1 as proto},
};

/// Build a `common.v1.Pagination` from a simple `limit`.
fn pagination(limit: u32) -> common::Pagination {
    common::Pagination { limit: u64::from(limit), ..Default::default() }
}

/// Client errors.
///
/// The `Rpc` code is `u32` on every transport so call sites can match on one
/// type regardless of native vs WASM.
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

/// Connect-RPC client for the trading terminal services (browser/WASM).
#[derive(Clone, Debug)]
pub struct TerminalClient {
    base_url: String,
}

impl TerminalClient {
    /// Create a new client targeting `base_url`.
    #[must_use]
    pub fn new(base_url: &str) -> Self {
        Self { base_url: trim_base_url(base_url) }
    }

    /// Create a client with a static bearer token (ignored pre-CORS; the
    /// server enforces auth via the same-origin session token when proxied).
    #[must_use]
    pub fn new_with_token(base_url: &str, _token: &str) -> Self {
        Self::new(base_url)
    }

    fn url(&self, service: &str, method: &str) -> String {
        service_url(&self.base_url, service, method)
    }

    async fn unary<Q: Message, R: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: Q,
    ) -> Result<R, TerminalClientError> {
        let body = req.encode_to_vec();
        let resp = gloo_net::http::Request::post(&self.url(service, method))
            .header("content-type", "application/proto")
            .body(body)
            .map_err(|e| TerminalClientError::Http(e.to_string()))?
            .send()
            .await
            .map_err(|e| TerminalClientError::Http(e.to_string()))?;

        let status = u32::from(resp.status());
        if !(200..300).contains(&status) {
            let text = resp
                .text()
                .await
                .map_err(|e| TerminalClientError::Http(format!("read error: {e}")))?;
            return Err(TerminalClientError::Rpc { code: status, message: text });
        }

        let bytes = resp
            .binary()
            .await
            .map_err(|e| TerminalClientError::Http(format!("read error: {e}")))?;
        R::decode_from_slice(&bytes)
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
            pagination: buffa::MessageField::some(pagination(limit)),
            ..Default::default()
        };
        let resp: proto::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp.positions)
    }

    /// Place an order.
    #[expect(clippy::too_many_arguments)]
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
    ///
    /// `None` leaves the corresponding bracket unchanged.
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
        self.unary(SERVICE_RUNTIME, "Health", req).await
    }

    /// List connected venues.
    pub async fn list_venues(&self) -> Result<Vec<proto::VenueStatus>, TerminalClientError> {
        let req = proto::ListVenuesRequest::default();
        let resp: proto::ListVenuesResponse =
            self.unary(SERVICE_RUNTIME, "ListVenues", req).await?;
        Ok(resp.venues)
    }

    /// Open the streaming updates channel.
    ///
    /// Returns a stream of `UpdateEnvelope` messages decoded from the Connect
    /// streaming framing (1-byte flag + 4-byte BE length + protobuf payload).
    pub async fn stream_updates(
        &self,
        venues: &[String],
        symbols: &[String],
        topics: &[proto::TopicClass],
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<proto::UpdateEnvelope, TerminalClientError>>>>,
        TerminalClientError,
    > {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;

        let url = self.url(SERVICE_RUNTIME, "StreamUpdates");
        let req_msg = proto::StreamUpdatesRequest {
            venues: venues.to_vec(),
            symbols: symbols.to_vec(),
            topics: topics.iter().map(|t| buffa::EnumValue::Known(*t)).collect(),
            ..Default::default()
        };
        let payload = req_msg.encode_to_vec();
        let mut body = Vec::with_capacity(5 + payload.len());
        body.push(0);
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(&payload);

        let opts = web_sys::RequestInit::new();
        opts.set_method("POST");
        let uint8 = js_sys::Uint8Array::from(body.as_slice());
        opts.set_body(&uint8);

        let headers =
            web_sys::Headers::new().map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;
        headers
            .set("content-type", "application/connect+proto")
            .map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;
        opts.set_headers(&headers);

        let request = web_sys::Request::new_with_str_and_init(&url, &opts)
            .map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;

        let window =
            web_sys::window().ok_or_else(|| TerminalClientError::Http("no window".into()))?;
        let resp_val = JsFuture::from(window.fetch_with_request(&request))
            .await
            .map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;
        let resp: web_sys::Response =
            resp_val.dyn_into().map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;

        if !resp.ok() {
            let status = u32::from(resp.status());
            let text_promise =
                resp.text().map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;
            let text = JsFuture::from(text_promise)
                .await
                .map_err(|e| TerminalClientError::Http(format!("{e:?}")))?;
            let msg = text.as_string().unwrap_or_default();
            return Err(TerminalClientError::Rpc { code: status, message: msg });
        }

        let body_stream =
            resp.body().ok_or_else(|| TerminalClientError::Http("no response body".into()))?;
        let reader: web_sys::ReadableStreamDefaultReader = body_stream
            .get_reader()
            .dyn_into()
            .map_err(|e| TerminalClientError::Http(format!("failed to get reader: {e:?}")))?;

        let stream = futures_util::stream::unfold(
            (reader, Vec::<u8>::new()),
            |(reader, mut buf)| async move {
                loop {
                    if buf.len() >= 5 {
                        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                        let total = 5 + len;
                        if buf.len() >= total {
                            let payload: Vec<u8> = buf[5..total].to_vec();
                            buf.drain(..total);
                            match proto::UpdateEnvelope::decode_from_slice(&payload) {
                                Ok(env) => return Some((Ok(env), (reader, buf))),
                                Err(e) => {
                                    return Some((
                                        Err(TerminalClientError::Decode(e.to_string())),
                                        (reader, buf),
                                    ));
                                }
                            }
                        }
                    }
                    let result = match JsFuture::from(reader.read()).await {
                        Ok(v) => v,
                        Err(e) => {
                            return Some((
                                Err(TerminalClientError::Http(format!("{e:?}"))),
                                (reader, buf),
                            ));
                        }
                    };
                    let done =
                        js_sys::Reflect::get(&result, &wasm_bindgen::JsValue::from_str("done"))
                            .ok()
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                    if done {
                        return None;
                    }
                    let value = match js_sys::Reflect::get(
                        &result,
                        &wasm_bindgen::JsValue::from_str("value"),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            return Some((
                                Err(TerminalClientError::Http(format!("{e:?}"))),
                                (reader, buf),
                            ));
                        }
                    };
                    if value.is_undefined() || value.is_null() {
                        return None;
                    }
                    let uint8 = js_sys::Uint8Array::new(&value);
                    let mut chunk = vec![0u8; uint8.length() as usize];
                    uint8.copy_to(&mut chunk);
                    buf.extend_from_slice(&chunk);
                }
            },
        );

        Ok(Box::pin(stream))
    }
}
