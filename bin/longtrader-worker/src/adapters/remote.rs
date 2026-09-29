//! Open-source `RemoteAdapter`: speaks the public contract (`longtrader.*.v1`)
//! directly via Connect `application/proto` over `hpx`.
//!
//! The legacy `tradingcharts.terminal.v1` translation layer has been removed
//! for the open source repo; every method forwards the contract DTO without
//! string-decimal parsing or `terminal_core` timeframe mapping. Timeframes
//! are already strings in `market::GetCandlesRequest`, so no conversion is
//! needed. Extended capabilities (funding, conditional orders, wallet, venue
//! ops) are served from the same contract, so a unified backend can run every
//! in-tree strategy; a venue that lacks one surfaces as
//! [`PortError::NotFound`] or the backend's own error rather than a silent
//! empty result.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::{Message, MessageField};
use rust_decimal::Decimal;

use crate::{
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, ops, trading, worker},
};

const SERVICE_TRADING: &str = "longtrader.trading.v1.TradingService";
const SERVICE_MARKET: &str = "longtrader.market.v1.MarketDataService";

fn ts_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        // remainder <1000 so nanos <1e9 always fits i32
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).expect("ms remainder fits i32"),
        ..Default::default()
    }
}

fn ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Minimal Connect client over `hpx` – mirrors the former `TerminalClient`
/// but targets the open `longtrader.*.v1` services. One `RemoteAdapter`
/// per config, cloned as `Arc<dyn TradingGateway + MarketDataSource + ...>`.
///
/// A single adapter implements every strategy-facing port, so the worker keeps
/// exactly one backend connection per session.
#[derive(Clone)]
pub struct RemoteAdapter {
    base_url: String,
    token: String,
    http: hpx::Client,
    /// Monotonic snapshot watermark source for deterministic recovery
    /// (`ReconcileStateResponse.snapshot_sequence`, design doc §6.3).
    snapshot_seq: Arc<AtomicU64>,
}

impl RemoteAdapter {
    #[must_use]
    pub fn new(base_url: &str, token: &str) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        // HTTP/2 is enabled so the same client can carry server-streaming
        // (`StreamMarketData`/`StreamUpdates`) once the backend exposes it;
        // unary calls work over either version.
        let http = match hpx::Client::builder().build() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("hpx build failed, using default client: {e}");
                hpx::Client::new()
            }
        };
        Self { base_url, token: token.to_string(), http, snapshot_seq: Arc::new(AtomicU64::new(0)) }
    }

    async fn unary<Q: Message, R: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: Q,
    ) -> Result<R, PortError> {
        longtrader_proto::transport::unary(
            &self.http,
            &self.base_url,
            service,
            method,
            Some(self.token.as_str()),
            req,
        )
        .await
        .map_err(|e| match e {
            longtrader_proto::transport::TransportError::Http(m) => PortError::Transport(m),
            longtrader_proto::transport::TransportError::Rpc { code, message } => {
                PortError::Rpc { code, message }
            }
            longtrader_proto::transport::TransportError::Decode(m) => {
                PortError::Transport(format!("decode {method}: {m}"))
            }
        })
    }
}

#[async_trait]
impl TradingGateway for RemoteAdapter {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let resp: trading::CreateOrderResponse =
            self.unary(SERVICE_TRADING, "CreateOrder", req).await?;
        resp.order.into_option().ok_or_else(|| PortError::MissingField("order".to_string()))
    }

    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::CreateOrdersResponse =
            self.unary(SERVICE_TRADING, "CreateOrders", req).await?;
        Ok(resp.orders)
    }

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let resp: trading::CancelOrderResponse =
            self.unary(SERVICE_TRADING, "CancelOrder", req).await?;
        resp.order.into_option().ok_or_else(|| PortError::MissingField("order".to_string()))
    }

    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::CancelAllOrdersResponse =
            self.unary(SERVICE_TRADING, "CancelAllOrders", req).await?;
        Ok(resp.orders)
    }

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::FetchOpenOrdersResponse =
            self.unary(SERVICE_TRADING, "FetchOpenOrders", req).await?;
        Ok(resp.orders)
    }

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError> {
        let resp: trading::GetAccountResponse =
            self.unary(SERVICE_TRADING, "GetAccount", req).await?;
        Ok(resp)
    }

    async fn get_positions(
        &self,
        req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError> {
        let resp: trading::GetPositionsResponse =
            self.unary(SERVICE_TRADING, "GetPositions", req).await?;
        Ok(resp)
    }

    async fn get_order_history(
        &self,
        req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError> {
        let resp: trading::GetOrderHistoryResponse =
            self.unary(SERVICE_TRADING, "GetOrderHistory", req).await?;
        Ok(resp)
    }

    async fn get_closed_positions(
        &self,
        req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError> {
        let resp: trading::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp)
    }

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError> {
        let resp: trading::ClosePositionResponse =
            self.unary(SERVICE_TRADING, "ClosePosition", req).await?;
        Ok(resp)
    }

    async fn close_all_positions(
        &self,
        req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError> {
        let resp: trading::CloseAllPositionsResponse =
            self.unary(SERVICE_TRADING, "CloseAllPositions", req).await?;
        Ok(resp)
    }

    async fn modify_position(
        &self,
        req: trading::ModifyPositionRequest,
    ) -> Result<trading::ModifyPositionResponse, PortError> {
        let resp: trading::ModifyPositionResponse =
            self.unary(SERVICE_TRADING, "ModifyPosition", req).await?;
        Ok(resp)
    }

    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        // Best-effort snapshot: three sequential unary calls, NOT atomic.
        // Callers must treat `snapshot_sequence` as a local watermark and
        // replay deltas after it (see `SessionManager::complete_reconcile`).
        // A server-side single-RPC `ReconcileState` is the long-term fix.
        //
        // SECURITY NOTE: The snapshot is NOT atomic. There is a race window
        // between the three RPCs where the backend state may change. The
        // `snapshot_sequence` is a local monotonic counter, not a server-side
        // watermark. Strategies MUST replay deltas after `snapshot_sequence`
        // to converge to the correct state.
        let started_ms = ms_now();
        let account_resp: trading::GetAccountResponse = self
            .unary(
                SERVICE_TRADING,
                "GetAccount",
                trading::GetAccountRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;
        let account = account_resp.account.into_option().unwrap_or_default();
        // Project aggregate `trading::Account` onto a single `account::Balance` (USD) – matches
        // the old terminal adapter's projection, preserving strategy expectations until a
        // proper `AccountService` is exposed.
        let balances = vec![account::Balance {
            currency: "USD".to_string(),
            free: account.free_margin.clone(),
            used: account.margin_used.clone(),
            total: account.equity.clone(),
            ..Default::default()
        }];

        let positions_resp: trading::GetPositionsResponse = self
            .unary(
                SERVICE_TRADING,
                "GetPositions",
                trading::GetPositionsRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;

        let open_orders_resp: trading::FetchOpenOrdersResponse = self
            .unary(
                SERVICE_TRADING,
                "FetchOpenOrders",
                trading::FetchOpenOrdersRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;

        if started_ms != ms_now() {
            tracing::debug!(
                "sync_state spanned multiple millis; snapshot is best-effort, not atomic"
            );
        }
        Ok(worker::ReconcileStateResponse {
            // Local monotonic watermark (NOT server-atomic). Session discards
            // deltas at/below this point and replays the remainder.
            snapshot_sequence: self.snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1,
            snapshot_time: MessageField::some(ts_from_ms(started_ms)),
            balances,
            positions: positions_resp.positions,
            open_orders: open_orders_resp.orders,
            ..Default::default()
        })
    }
}

#[async_trait]
impl MarketDataSource for RemoteAdapter {
    async fn fetch_ticker(
        &self,
        req: market::FetchTickerRequest,
    ) -> Result<market::Ticker, PortError> {
        let resp: market::FetchTickerResponse =
            self.unary(SERVICE_MARKET, "FetchTicker", req).await?;
        resp.ticker.into_option().ok_or_else(|| PortError::MissingField("ticker".to_string()))
    }

    async fn fetch_order_book(
        &self,
        req: market::FetchOrderBookRequest,
    ) -> Result<market::OrderBook, PortError> {
        let resp: market::FetchOrderBookResponse =
            self.unary(SERVICE_MARKET, "FetchOrderBook", req).await?;
        resp.orderbook.into_option().ok_or_else(|| PortError::MissingField("orderbook".to_string()))
    }

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError> {
        let resp: market::GetCandlesResponse =
            self.unary(SERVICE_MARKET, "GetCandles", req).await?;
        Ok(resp)
    }

    async fn list_symbols(
        &self,
        req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError> {
        let resp: market::ListSymbolsResponse =
            self.unary(SERVICE_MARKET, "ListSymbols", req).await?;
        Ok(resp)
    }

    async fn subscribe_market_data(
        &self,
        req: market::StreamMarketDataRequest,
        policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError> {
        // Backed by periodic unary fetches through a backpressure-aware policy
        // channel, realising the design-doc per-session isolation until the
        // backend exposes server-streaming. Each subscription polls its snapshot
        // on a fixed cadence; the channel applies `policy` (DropOldest / Coalesce
        // / Block) so a slow consumer never stalls the worker or other sessions.
        let this = self.clone();
        Ok(crate::adapters::poll_market_data(
            req,
            policy,
            move |channel, exchange_id, symbol, seq| {
                let this = this.clone();
                async move { this.poll_snapshot(channel, &exchange_id, symbol, &seq).await }
            },
        ))
    }
}

impl RemoteAdapter {
    /// Fetch one snapshot for a subscription channel and wrap it as a
    /// `MarketDataEvent` with a monotonically increasing `header.sequence`.
    async fn poll_snapshot(
        &self,
        channel: market::StreamChannel,
        exchange_id: &common::ExchangeId,
        symbol: String,
        seq: &AtomicU64,
    ) -> Result<Option<market::MarketDataEvent>, PortError> {
        let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
        let header = common::EventHeader { sequence: next, ..Default::default() };
        // Ticker is the sensible default for any non-orderbook channel;
        // trades/ohlcv are not independently fetchable here, so they reuse
        // the ticker feed.
        let event = if channel == market::StreamChannel::Orderbook {
            let book = self
                .fetch_order_book(market::FetchOrderBookRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.clone(),
                    pagination: crate::proto::common::Pagination {
                        limit: 100,
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                })
                .await?;
            market::market_data_event::Event::Orderbook(Box::new(book))
        } else {
            let ticker = self
                .fetch_ticker(market::FetchTickerRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.clone(),
                    ..Default::default()
                })
                .await?;
            market::market_data_event::Event::Ticker(Box::new(ticker))
        };
        Ok(Some(market::MarketDataEvent {
            header: MessageField::some(header),
            event: Some(event),
            resume_token: next.to_string(),
            ..Default::default()
        }))
    }
}

// ---------------------------------------------------------------------------
// Extended capability ports, spoken over the unified contract.
//
// The `ops.v1.VenueOpService` and the funding / conditional-order / wallet RPCs
// all exist on the unified surface, so a backend that serves it supports the
// same strategies as the paper venue. Every conversion is explicit: a decimal
// the host could not encode is an error, and an `optional` field left unset
// stays unset rather than becoming a zero that reads as a real measurement.
// ---------------------------------------------------------------------------

use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TransferReceipt,
    TriggerOrder, TriggerOrderGateway, TriggerOrderRequest, TriggerOrderStatus, VenueOpDescriptor,
    VenueOpInvoker, VenueOpParam, VenueOpParamType, WalletGateway,
};

/// Service names on the unified surface.
const OPS_SERVICE: &str = "longtrader.ops.v1.VenueOpService";
const MARKET_SERVICE: &str = "longtrader.market.v1.MarketDataService";
const TRADING_SERVICE: &str = "longtrader.trading.v1.TradingService";

/// The default page size when a request does not set one.
const DEFAULT_LIMIT: u32 = 100;

/// Build a `google.protobuf.Timestamp` from Unix milliseconds.
///
/// Only the tests build wire messages directly; the read paths decode the
/// timestamps the host sent.
#[cfg(test)]
fn timestamp_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        // The ms remainder (< 1000) scaled to nanos always fits in i32.
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
        ..Default::default()
    }
}

fn dec(d: Option<&common::Decimal>, field: &str) -> Result<Decimal, PortError> {
    let d = d.ok_or_else(|| PortError::MissingField(field.to_string()))?;
    Ok(longtrader_contract::ext::common_to_decimal(d)?)
}

/// Map a unified `FundingRate` onto the port snapshot.
fn funding_rate_to_port(rate: &market::FundingRate) -> Result<FundingRateSnapshot, PortError> {
    Ok(FundingRateSnapshot {
        symbol: rate.symbol.clone(),
        rate: dec(rate.rate.as_option(), "funding_rate.rate")?,
        next_funding_time_ms: rate.next_funding_time_ms,
        // `mark_price` is `optional` on purpose: absent means the venue does
        // not report one, which is not the same as a mark of zero.
        mark_price: rate
            .mark_price
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?,
    })
}

/// Map a unified `TriggerOrder` onto the port record.
///
/// An unknown status is rejected rather than defaulted to `Open`: reporting a
/// dead backstop as live is the one failure this port cannot tolerate.
fn trigger_order_to_port(order: &trading::TriggerOrder) -> Result<TriggerOrder, PortError> {
    let is_buy = match order.side {
        buffa::EnumValue::Known(trading::OrderSide::Buy) => true,
        buffa::EnumValue::Known(trading::OrderSide::Sell) => false,
        buffa::EnumValue::Known(other) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has invalid side {other:?}",
                id = order.id
            )));
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has unknown side discriminant {v}",
                id = order.id
            )));
        }
    };
    let status = match order.status {
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Open) => TriggerOrderStatus::Open,
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Triggered) => {
            TriggerOrderStatus::Triggered
        }
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Canceled) => {
            TriggerOrderStatus::Canceled
        }
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Rejected) => {
            TriggerOrderStatus::Rejected
        }
        buffa::EnumValue::Known(other) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has invalid status {other:?}",
                id = order.id
            )));
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has unknown status discriminant {v}",
                id = order.id
            )));
        }
    };
    Ok(TriggerOrder {
        id: order.id.clone(),
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        is_buy,
        trigger_price: dec(order.trigger_price.as_option(), "trigger_price")?,
        qty: dec(order.qty.as_option(), "qty")?,
        reduce_only: order.reduce_only,
        status,
        created_at_ms: order
            .created_at
            .as_option()
            .map_or(0, |ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
        order_id: order.order_id.clone(),
        triggered_at_ms: order
            .triggered_at
            .as_option()
            .map(|ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
    })
}

/// Map an `ops.v1.OpDescriptor` onto the port descriptor, preserving
/// `mutating` and the parameter schema.
///
/// `mutating` is not cosmetic: a client uses it to decide whether a call needs a
/// confirmation prompt, so dropping it would report a fund-moving operation as
/// read-only.
fn op_descriptor_to_port(d: &ops::OpDescriptor) -> Result<VenueOpDescriptor, PortError> {
    Ok(VenueOpDescriptor {
        name: d.name.clone(),
        category: d.category.clone(),
        summary: d.summary.clone(),
        mutating: d.mutating,
        params: d
            .params
            .iter()
            .map(|p| VenueOpParam {
                name: p.name.clone(),
                r#type: param_type_from_wire(p.r#type),
                required: p.required,
                doc: p.doc.clone(),
                enum_values: p.enum_values.clone(),
            })
            .collect(),
    })
}

/// Map an `ops.v1.ParamType` onto the port enum.
///
/// An unknown discriminant degrades to `Unspecified` rather than being guessed
/// at: the parameter is still passed through by name, and a wrong claimed type
/// would be worse than an honest "not reported".
fn param_type_from_wire(t: buffa::EnumValue<ops::ParamType>) -> VenueOpParamType {
    use ops::ParamType as W;
    match t {
        buffa::EnumValue::Known(W::String) => VenueOpParamType::String,
        buffa::EnumValue::Known(W::Int64) => VenueOpParamType::Int64,
        buffa::EnumValue::Known(W::Decimal) => VenueOpParamType::Decimal,
        buffa::EnumValue::Known(W::Bool) => VenueOpParamType::Bool,
        buffa::EnumValue::Known(W::Enum) => VenueOpParamType::Enum,
        buffa::EnumValue::Known(W::List) => VenueOpParamType::List,
        buffa::EnumValue::Known(W::Map) => VenueOpParamType::Map,
        buffa::EnumValue::Known(W::Unspecified) | buffa::EnumValue::Unknown(_) => {
            VenueOpParamType::Unspecified
        }
    }
}

/// Map a unified `account.v1.LedgerEntry` onto the port record.
///
/// The sign comes from `direction`, not from the venue's amount: a venue that
/// reports withdrawals as positive would otherwise look like a deposit.
fn ledger_entry_to_port(entry: &account::LedgerEntry) -> Result<LedgerEntry, PortError> {
    let magnitude = dec(entry.amount.as_option(), "ledger_entry.amount")?;
    let out = entry.direction.eq_ignore_ascii_case("out");
    Ok(LedgerEntry {
        id: entry.id.clone(),
        currency: entry.currency.clone(),
        amount: if out { -magnitude } else { magnitude },
        entry_type: entry.r#type.clone(),
        completed: entry.status.eq_ignore_ascii_case("completed"),
        time_ms: entry
            .timestamp
            .as_option()
            .map_or(0, |ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
    })
}

/// Map a `google.protobuf.Struct` into JSON.
fn struct_to_json(
    s: &buffa_types::google::protobuf::Struct,
) -> Result<serde_json::Value, PortError> {
    serde_json::to_value(s).map_err(|e| PortError::Transport(format!("result not JSON: {e}")))
}

/// Map JSON into a `google.protobuf.Struct`.
fn json_to_struct(
    v: &serde_json::Value,
) -> Result<buffa_types::google::protobuf::Struct, PortError> {
    let json = serde_json::to_string(v)
        .map_err(|e| PortError::InvalidArgument(format!("params not serializable: {e}")))?;
    serde_json::from_str(&json)
        .map_err(|e| PortError::InvalidArgument(format!("params not a Struct: {e}")))
}

#[async_trait]
impl FundingRateSource for RemoteAdapter {
    async fn fetch_funding_rate(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError> {
        if symbol.trim().is_empty() {
            return Err(PortError::MissingField("symbol".to_string()));
        }
        let resp: market::FetchFundingRateResponse = self
            .unary(
                MARKET_SERVICE,
                "FetchFundingRate",
                market::FetchFundingRateRequest {
                    exchange_id: exchange_id.clone().into(),
                    symbol: symbol.to_string(),
                    ..Default::default()
                },
            )
            .await?;
        // An absent `funding_rate` means the venue tracks no perps for this
        // symbol, which is different from the venue having no funding surface
        // at all (that arrives as an error).
        resp.funding_rate
            .as_option()
            .ok_or_else(|| {
                PortError::NotFound(format!(
                    "no funding rate for {venue}/{symbol}",
                    venue = exchange_id.id
                ))
            })
            .and_then(funding_rate_to_port)
    }

    async fn fetch_funding_rate_history(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError> {
        if symbol.trim().is_empty() {
            return Err(PortError::MissingField("symbol".to_string()));
        }
        let resp: market::FetchFundingRateHistoryResponse = self
            .unary(
                MARKET_SERVICE,
                "FetchFundingRateHistory",
                market::FetchFundingRateHistoryRequest {
                    exchange_id: exchange_id.clone().into(),
                    symbol: symbol.to_string(),
                    limit: if limit == 0 { DEFAULT_LIMIT } else { limit },
                    ..Default::default()
                },
            )
            .await?;
        resp.points
            .iter()
            .map(|p| {
                Ok(FundingRatePoint {
                    rate: dec(p.rate.as_option(), "funding_rate_point.rate")?,
                    time_ms: p.funding_time_ms,
                })
            })
            .collect()
    }
}

#[async_trait]
impl TriggerOrderGateway for RemoteAdapter {
    async fn create_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<TriggerOrder, PortError> {
        if req.symbol.trim().is_empty() {
            return Err(PortError::MissingField("symbol".to_string()));
        }
        if req.qty.is_sign_negative() || req.qty.is_zero() {
            return Err(PortError::InvalidArgument(format!(
                "trigger order qty must be positive, got {:?}",
                req.qty
            )));
        }
        if req.trigger_price.is_sign_negative() || req.trigger_price.is_zero() {
            // A zero trigger fires immediately and degenerates into a market
            // order, which is the opposite of what a STOP is for.
            return Err(PortError::InvalidArgument(format!(
                "trigger order trigger_price must be positive, got {:?}",
                req.trigger_price
            )));
        }
        let order = trading::TriggerOrderRequest {
            client_order_id: req.client_order_id,
            symbol: req.symbol,
            side: buffa::EnumValue::Known(if req.is_buy {
                trading::OrderSide::Buy
            } else {
                trading::OrderSide::Sell
            }),
            trigger_price: longtrader_contract::ext::decimal_to_common(req.trigger_price).into(),
            qty: longtrader_contract::ext::decimal_to_common(req.qty).into(),
            reduce_only: req.reduce_only,
            trigger_type: buffa::EnumValue::Known(trading::TriggerPriceType::Last),
            // No post-trigger limit price: a protective stop should fire into
            // liquidity rather than rest unfilled at a price the market has
            // already passed through.
            order_price: buffa::MessageField::none(),
            order_type: buffa::EnumValue::Known(trading::OrderType::Market),
            ..Default::default()
        };
        let resp: trading::CreateTriggerOrderResponse = self
            .unary(
                TRADING_SERVICE,
                "CreateTriggerOrder",
                trading::CreateTriggerOrderRequest {
                    exchange_id: exchange_id.clone().into(),
                    order: order.into(),
                    ..Default::default()
                },
            )
            .await?;
        let order = resp
            .order
            .as_option()
            .ok_or_else(|| PortError::Transport("venue returned no trigger order".to_string()))?;
        if order.id.is_empty() {
            // An empty id leaves the caller unable to cancel the backstop it
            // just placed, which is the one thing it must be able to do.
            return Err(PortError::Transport(
                "venue returned a trigger order with no id".to_string(),
            ));
        }
        trigger_order_to_port(order)
    }

    async fn cancel_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<TriggerOrder, PortError> {
        if order_id.trim().is_empty() {
            return Err(PortError::MissingField("order_id".to_string()));
        }
        let resp: trading::CancelTriggerOrderResponse = self
            .unary(
                TRADING_SERVICE,
                "CancelTriggerOrder",
                trading::CancelTriggerOrderRequest {
                    exchange_id: exchange_id.clone().into(),
                    order_id: order_id.to_string(),
                    symbol: symbol.to_string(),
                    ..Default::default()
                },
            )
            .await?;
        // The venue's record, not a value synthesised from the request: a
        // response built from the caller's own inputs cannot tell it which
        // order was actually cancelled.
        resp.order
            .as_option()
            .ok_or_else(|| PortError::MissingField("order".to_string()))
            .and_then(trigger_order_to_port)
    }

    async fn list_trigger_orders(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<TriggerOrder>, PortError> {
        let resp: trading::ListTriggerOrdersResponse = self
            .unary(
                TRADING_SERVICE,
                "ListTriggerOrders",
                trading::ListTriggerOrdersRequest {
                    exchange_id: exchange_id.clone().into(),
                    symbols: symbols.to_vec(),
                    ..Default::default()
                },
            )
            .await?;
        resp.orders.iter().map(trigger_order_to_port).collect()
    }
}

#[async_trait]
impl VenueOpInvoker for RemoteAdapter {
    async fn invoke_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
        params: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, PortError> {
        if op.trim().is_empty() {
            return Err(PortError::MissingField("op".to_string()));
        }
        let params = json_to_struct(&serde_json::Value::Object(params))?;
        let resp: ops::InvokeVenueOpResponse = self
            .unary(
                OPS_SERVICE,
                "InvokeVenueOp",
                ops::InvokeVenueOpRequest {
                    exchange_id: exchange_id.id.clone(),
                    op: op.to_string(),
                    params: params.into(),
                    ..Default::default()
                },
            )
            .await?;
        resp.result
            .as_option()
            .ok_or_else(|| PortError::MissingField("result".to_string()))
            .and_then(struct_to_json)
    }

    async fn list_venue_ops(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<VenueOpDescriptor>, PortError> {
        let resp: ops::ListVenueOpsResponse = self
            .unary(
                OPS_SERVICE,
                "ListVenueOps",
                ops::ListVenueOpsRequest {
                    exchange_id: exchange_id.id.clone(),
                    ..Default::default()
                },
            )
            .await?;
        resp.ops.iter().map(op_descriptor_to_port).collect()
    }

    async fn describe_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
    ) -> Result<VenueOpDescriptor, PortError> {
        if op.trim().is_empty() {
            return Err(PortError::MissingField("op".to_string()));
        }
        let resp: ops::DescribeVenueOpResponse = self
            .unary(
                OPS_SERVICE,
                "DescribeVenueOp",
                ops::DescribeVenueOpRequest {
                    exchange_id: exchange_id.id.clone(),
                    op: op.to_string(),
                    ..Default::default()
                },
            )
            .await?;
        let descriptor = resp
            .op
            .as_option()
            .ok_or_else(|| PortError::NotFound(format!("venue operation {op}")))?;
        Ok(op_descriptor_to_port(descriptor)?)
    }
}

#[async_trait]
impl WalletGateway for RemoteAdapter {
    async fn list_ledger_entries(
        &self,
        exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        let resp: trading::FetchLedgerEntriesResponse = self
            .unary(
                TRADING_SERVICE,
                "FetchLedgerEntries",
                trading::FetchLedgerEntriesRequest {
                    exchange_id: exchange_id.clone().into(),
                    currency: currency.to_string(),
                    r#type: entry_type.to_string(),
                    pagination: common::Pagination {
                        limit: u64::from(if limit == 0 { DEFAULT_LIMIT } else { limit }),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
            )
            .await?;
        resp.entries.iter().map(ledger_entry_to_port).collect()
    }

    async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<TransferReceipt, PortError> {
        if asset.trim().is_empty() {
            return Err(PortError::MissingField("asset".to_string()));
        }
        if amount.is_sign_negative() || amount.is_zero() {
            return Err(PortError::InvalidArgument(format!(
                "transfer amount must be positive, got {amount:?}"
            )));
        }
        if dest_label.trim().is_empty() {
            return Err(PortError::MissingField("dest_label".to_string()));
        }
        let resp: trading::TransferResponse = self
            .unary(
                TRADING_SERVICE,
                "Transfer",
                trading::TransferRequest {
                    exchange_id: exchange_id.clone().into(),
                    asset: asset.to_string(),
                    amount: longtrader_contract::ext::decimal_to_common(amount).into(),
                    dest_label: dest_label.to_string(),
                    // Transforms move funds, so an idempotency key is forwarded
                    // verbatim: an empty key means the caller is not retrying.
                    client_transfer_id: client_transfer_id.to_string(),
                    ..Default::default()
                },
            )
            .await?;
        Ok(TransferReceipt {
            transfer_id: resp.transfer_id,
            entry: resp.entry.as_option().map(ledger_entry_to_port).transpose()?,
        })
    }
}

#[cfg(test)]
mod extended_capability_tests {
    use rust_decimal_macros::dec;

    use super::*;

    /// These conversions sit between the unified contract and the port types.
    /// The dangerous outcome in each case is a silent default: a zeroed price
    /// or a defaulted status looks like valid data to a strategy.

    #[test]
    fn funding_rate_maps_the_numeric_decimal_pair() {
        let rate = market::FundingRate {
            symbol: "BTC/USDT".to_string(),
            rate: longtrader_contract::ext::decimal_to_common(dec!(0.0001)).into(),
            next_funding_time_ms: 1_700_000_000_000,
            // `mark_price` deliberately unset: the venue does not report one.
            ..Default::default()
        };
        let snapshot = funding_rate_to_port(&rate).expect("mapped");
        assert_eq!(snapshot.rate, dec!(0.0001));
        assert_eq!(snapshot.symbol, "BTC/USDT");
        assert_eq!(
            snapshot.mark_price, None,
            "an unreported mark price must stay None, not become 0"
        );
    }

    #[test]
    fn funding_rate_rejects_a_missing_decimal() {
        let rate = market::FundingRate { symbol: "BTC/USDT".to_string(), ..Default::default() };
        // A zeroed rate would make a carry strategy think funding is free.
        assert!(funding_rate_to_port(&rate).is_err());
    }

    #[test]
    fn trigger_order_maps_status_and_geometry() {
        let order = trading::TriggerOrder {
            id: "t-1".to_string(),
            symbol: "BTC/USDT".to_string(),
            side: buffa::EnumValue::Known(trading::OrderSide::Sell),
            trigger_price: longtrader_contract::ext::decimal_to_common(dec!(95000)).into(),
            qty: longtrader_contract::ext::decimal_to_common(dec!(0.001)).into(),
            status: buffa::EnumValue::Known(trading::TriggerOrderStatus::Open),
            reduce_only: true,
            created_at: timestamp_from_ms(1_700_000_000_123).into(),
            ..Default::default()
        };
        let port = trigger_order_to_port(&order).expect("mapped");
        assert_eq!(port.status, TriggerOrderStatus::Open);
        assert!(!port.is_buy);
        assert!(port.reduce_only);
        assert_eq!(port.trigger_price, dec!(95000));
        assert_eq!(port.qty, dec!(0.001));
        // Milliseconds must survive, not be truncated to whole seconds.
        assert_eq!(port.created_at_ms, 1_700_000_000_123);
        assert_eq!(port.order_id, None);
    }

    #[test]
    fn trigger_order_with_an_unknown_status_is_rejected() {
        // Defaulting to `Open` would tell a strategy its backstop is live when
        // the venue says something it does not understand.
        let order = trading::TriggerOrder {
            id: "t-1".to_string(),
            status: buffa::EnumValue::Unknown(99),
            ..Default::default()
        };
        assert!(trigger_order_to_port(&order).is_err());
    }

    #[test]
    fn trigger_order_with_an_unknown_side_is_rejected() {
        // Defaulting to Buy would silently invert a stop.
        let order = trading::TriggerOrder {
            id: "t-1".to_string(),
            side: buffa::EnumValue::Known(trading::OrderSide::Unspecified),
            ..Default::default()
        };
        assert!(trigger_order_to_port(&order).is_err());
    }

    #[test]
    fn ledger_entry_sign_comes_from_direction_not_the_amount() {
        let amount = longtrader_contract::ext::decimal_to_common(dec!(100));
        let mk = |direction: &str, amount: common::Decimal| account::LedgerEntry {
            id: "l-1".to_string(),
            currency: "USDT".to_string(),
            direction: direction.to_string(),
            r#type: "transfer".to_string(),
            amount: amount.into(),
            timestamp: timestamp_from_ms(1_700_000_000_000).into(),
            status: "completed".to_string(),
            ..Default::default()
        };
        let entry = ledger_entry_to_port(&mk("out", amount.clone())).expect("mapped");
        assert_eq!(entry.amount, dec!(-100), "a withdrawal must be negative");
        assert!(entry.completed);
        assert_eq!(entry.time_ms, 1_700_000_000_000);

        let into = mk("in", amount);
        assert_eq!(
            ledger_entry_to_port(&into).expect("mapped").amount,
            dec!(100),
            "a deposit must be positive"
        );
    }

    #[test]
    fn ledger_entry_without_an_amount_is_rejected() {
        let entry = account::LedgerEntry {
            id: "l-1".to_string(),
            currency: "USDT".to_string(),
            direction: "in".to_string(),
            ..Default::default()
        };
        assert!(ledger_entry_to_port(&entry).is_err());
    }

    #[test]
    fn json_struct_round_trip_preserves_decimal_strings() {
        let v = serde_json::json!({"asset": "USDT", "amount": "10.5", "flag": true});
        let back = struct_to_json(&json_to_struct(&v).expect("to struct")).expect("to json");
        // Amounts stay strings: venues are inconsistent about number vs string,
        // and re-serialising "10.5" as 10.5 would change its decimal scale.
        assert_eq!(back["amount"], "10.5");
        assert_eq!(back["asset"], "USDT");
        assert_eq!(back["flag"], true);
    }

    #[test]
    fn service_names_match_the_contract() {
        assert_eq!(OPS_SERVICE, "longtrader.ops.v1.VenueOpService");
        assert_eq!(MARKET_SERVICE, "longtrader.market.v1.MarketDataService");
        assert_eq!(TRADING_SERVICE, "longtrader.trading.v1.TradingService");
    }
}
