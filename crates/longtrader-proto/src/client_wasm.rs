//! WASM-compatible Connect-RPC client backed by `gloo-net` (browser).
//!
//! Mirrors the native [`client`](crate::client) API surface so browser
//! frontends talk the same canonical `longtrader.{market,trading}.v1` services
//! plus the terminal-only runtime/strategy surfaces, with identical method
//! names. Streaming (`RuntimeService::StreamUpdates`) uses `web-sys` fetch +
//! `ReadableStream` so chunked Connect framing works in the browser.

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
        common::v1 as common, market::v1 as umarket, stream::v1 as stream, terminal::v1 as proto,
        trading::v1 as utrading,
    },
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

/// Connect-RPC client for the trading terminal and canonical contract services
/// (browser/WASM).
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
            start_ms,
            end_ms,
            exchange_id: exchange_id.clone().into(),
            symbol: symbol.to_string(),
            timeframe: buffa::EnumValue::Known(timeframe),
            pagination: buffa::MessageField::some(pagination(limit)),
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
            pagination: buffa::MessageField::some(pagination(limit)),
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

    /// Batch ticker snapshots (empty `symbols` = all).
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

    /// Batch funding rates (empty `symbols` = all).
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
            pagination: buffa::MessageField::some(pagination(limit)),
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

    /// Fetch open positions (empty `symbols` = all).
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
            pagination: buffa::MessageField::some(pagination(limit)),
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
    /// Returns a stream of canonical `stream.v1.UpdateEnvelope` messages
    /// decoded from the Connect streaming framing (1-byte flag + 4-byte BE
    /// length + protobuf payload).
    pub async fn stream_updates(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
        topics: &[stream::TopicClass],
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<stream::UpdateEnvelope, TerminalClientError>>>>,
        TerminalClientError,
    > {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;

        let url = self.url(SERVICE_RUNTIME, "StreamUpdates");
        let req_msg = stream::StreamUpdatesRequest {
            exchange_id: exchange_id.clone().into(),
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
                            match stream::UpdateEnvelope::decode_from_slice(&payload) {
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
