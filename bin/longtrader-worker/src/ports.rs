//! Strategy-facing ports whose DTOs are generated protobuf types.
//!
//! Strategies depend on these traits only; transport and backend selection
//! live in [`crate::adapters`]. This is the hexagonal boundary of the worker.

use async_trait::async_trait;
use longtrader_contract::ext::DecimalConvertError;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::proto::{common, market, trading, worker};

// ---------------------------------------------------------------------------
// Decimal dual-representation
//
// The authoritative encode/decode lives in `longtrader_contract::ext`
// (`decimal_to_common` / `common_to_decimal`). The previous duplicate helpers
// here were removed so the representation split has a single owner; the old
// fast path silently zeroed an out-of-range `scale`, a latent data-corruption
// bug. Strategy code should call `longtrader_contract::ext::decimal_to_common`.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Sequence gap detection (design doc §6.1: per-stream gap-free `EventHeader.sequence`)
// ---------------------------------------------------------------------------

/// Returns `true` when `next` does NOT follow `prev` by exactly one.
/// Consumers MUST treat a gap as a trigger for resync: orderbook streams
/// re-fetch a fresh snapshot, private streams re-run `ReconcileState`.
#[inline]
pub fn is_sequence_gap(prev: u64, next: u64) -> bool {
    next != prev.wrapping_add(1)
}

/// Sequence gap helper on top of raw headers; `None` means no previous
/// watermark (e.g. first event), so no gap.
#[inline]
pub fn has_sequence_gap(prev: Option<u64>, header: &common::EventHeader) -> bool {
    prev.is_some_and(|p| is_sequence_gap(p, header.sequence))
}

/// Backpressure policy applied to streamed events (see design doc §6.5).
///
/// Each session owns an independent `mpsc` channel per logical stream;
/// overflowing one session never blocks another. The policy is chosen per
/// stream kind (doc constants below).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop oldest, keep newest — tickers, L1 quotes, OHLCV.
    /// Use for idempotent, high-frequency market data where newest value
    /// subsumes older ticks. Maps to `ticker` channels.
    DropOldest,
    /// Fold updates into latest state per key — book deltas collapse to the
    /// newest view per symbol/channel. Maps to `book` (orderbook) channels;
    /// stale intermediates are merged, unrecoverable lag surfaces as a
    /// sequence gap → automatic resync (§6.1).
    Coalesce,
    /// Bounded block — order/balance/position events are NEVER dropped;
    /// sustained blockage backpressures the upstream pump and ultimately
    /// trips the lease → kill-switch (final backstop). Maps to `orders`
    /// channels.
    Block,
}

/// Per-channel default overflow policies (design doc §6.5 table).
/// Ticker/trades/ohlcv → `DropOldest`; orderbook → `Coalesce`;
/// orders/balances/positions/my-trades → `Block`.
pub const OVERFLOW_TICKER: OverflowPolicy = OverflowPolicy::DropOldest;
pub const OVERFLOW_ORDERBOOK: OverflowPolicy = OverflowPolicy::Coalesce;
pub const OVERFLOW_ORDERS: OverflowPolicy = OverflowPolicy::Block;
/// Generic alias kept for callers that already know the mapping.
pub const OVERFLOW_TRADES: OverflowPolicy = OverflowPolicy::DropOldest;
pub const OVERFLOW_OHLCV: OverflowPolicy = OverflowPolicy::DropOldest;
pub const OVERFLOW_BALANCES: OverflowPolicy = OverflowPolicy::Block;
pub const OVERFLOW_POSITIONS: OverflowPolicy = OverflowPolicy::Block;

/// Resolve the OverflowPolicy for a market `StreamChannel`.
#[inline]
pub fn overflow_policy_for_channel(channel: market::StreamChannel) -> OverflowPolicy {
    match channel {
        market::StreamChannel::Orderbook => OVERFLOW_ORDERBOOK,
        market::StreamChannel::Ticker |
        market::StreamChannel::Trades |
        market::StreamChannel::Ohlcv => OVERFLOW_TICKER,
        _ => OVERFLOW_TICKER,
    }
}

/// Errors surfaced by port implementations.
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("rpc error ({code}): {message}")]
    Rpc { code: u32, message: String },
    #[error("operation unsupported by this backend: {0}")]
    Unsupported(String),
    #[error("missing required field: {0}")]
    MissingField(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The backend answered, but has no such object. Distinct from
    /// `Unsupported` (the backend can never have it) and from an empty
    /// successful result (which a caller would read as "none exist").
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Decimal(#[from] DecimalConvertError),
}

// ponytail: tradingcharts_proto removed for open source; former From impl deleted.
// RemoteAdapter now uses hpx+contract directly, so no TerminalClientError mapping needed.

/// Streamed market data events delivered under an [`OverflowPolicy`].
pub type MarketEventStream = mpsc::Receiver<market::MarketDataEvent>;

/// Order execution port.
#[async_trait]
pub trait TradingGateway: Send + Sync {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError>;

    /// Batch placement (grids / market making); contract-level batch RPC.
    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError>;

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError>;

    /// Cancel every open order for the exchange (optionally one symbol);
    /// kill-switch execution path.
    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError>;

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError>;

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError>;

    async fn get_positions(
        &self,
        req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError>;

    async fn get_order_history(
        &self,
        req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError>;

    async fn get_closed_positions(
        &self,
        req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError>;

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError>;

    /// Close every open position; kill-switch execution path.
    async fn close_all_positions(
        &self,
        req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError>;

    /// Modify the take-profit / stop-loss of an open position.
    async fn modify_position(
        &self,
        req: trading::ModifyPositionRequest,
    ) -> Result<trading::ModifyPositionResponse, PortError>;

    /// Best-effort snapshot for deterministic recovery. Adapters aggregate
    /// their backend's balance/position/open-order reads; the three reads are
    /// NOT atomic on the open backend (see `RemoteAdapter::sync_state`).
    /// Callers must replay deltas after `snapshot_sequence` to converge.
    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError>;
}

/// Market data port.
#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn fetch_ticker(
        &self,
        req: market::FetchTickerRequest,
    ) -> Result<market::Ticker, PortError>;

    async fn fetch_order_book(
        &self,
        req: market::FetchOrderBookRequest,
    ) -> Result<market::OrderBook, PortError>;

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError>;

    async fn list_symbols(
        &self,
        req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError>;

    /// Subscribe with backpressure protection; the returned receiver yields
    /// events already filtered through `policy`.
    async fn subscribe_market_data(
        &self,
        req: market::StreamMarketDataRequest,
        policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError>;
}

// ---------------------------------------------------------------------------
// Extended capability ports (P2): funding rates, trigger orders, venue ops,
// wallet operations. Backends that do not implement a capability return
// [`PortError::Unsupported`].
// ---------------------------------------------------------------------------

/// One funding-rate snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateSnapshot {
    pub symbol: String,
    pub rate: Decimal,
    pub next_funding_time_ms: i64,
    pub mark_price: Option<Decimal>,
}

/// One historical funding-rate point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRatePoint {
    pub rate: Decimal,
    pub time_ms: i64,
}

/// Perpetual funding-rate data port.
#[async_trait]
pub trait FundingRateSource: Send + Sync {
    async fn fetch_funding_rate(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError>;

    async fn fetch_funding_rate_history(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError>;
}

/// A venue-side conditional (trigger) order request.
#[derive(Debug, Clone)]
pub struct TriggerOrderRequest {
    pub client_order_id: String,
    pub symbol: String,
    pub is_buy: bool,
    pub trigger_price: Decimal,
    pub qty: Decimal,
    pub reduce_only: bool,
}

/// Venue-side protective order port (crash-safe stop-loss backstop).
#[async_trait]
pub trait TriggerOrderGateway: Send + Sync {
    /// Places the order and returns it as the venue now sees it.
    ///
    /// Returning the record (not just an id) means the caller can confirm the
    /// venue accepted the intended trigger price and status, instead of
    /// discovering a mismatch when it later tries to cancel.
    async fn create_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<TriggerOrder, PortError>;

    /// Cancels a resting conditional order, returning it as the venue now
    /// sees it.
    ///
    /// The record matters: a caller confirming the cancellation must be able to
    /// see which order was cancelled (price, quantity, side), because a
    /// response synthesised from the request cannot distinguish "I cancelled
    /// the stop I meant to" from "I cancelled something else with this id".
    async fn cancel_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<TriggerOrder, PortError>;

    /// Resting conditional orders, filtered by `symbols` (empty = every
    /// symbol).
    async fn list_trigger_orders(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<TriggerOrder>, PortError>;
}

/// Lifecycle of a venue-side conditional order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TriggerOrderStatus {
    /// Resting at the venue, waiting for the trigger price.
    #[default]
    Open,
    /// Trigger fired; `order_id` names the resulting live order.
    Triggered,
    Canceled,
    Rejected,
}

impl std::fmt::Display for TriggerOrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TriggerOrderStatus {
    /// The wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Triggered => "triggered",
            Self::Canceled => "canceled",
            Self::Rejected => "rejected",
        }
    }
}

/// A resting or fired venue-side conditional order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerOrder {
    pub id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub is_buy: bool,
    pub trigger_price: Decimal,
    pub qty: Decimal,
    pub reduce_only: bool,
    pub status: TriggerOrderStatus,
    pub created_at_ms: i64,
    /// Set once the trigger fired; names the resulting live order.
    pub order_id: Option<String>,
    pub triggered_at_ms: Option<i64>,
}

/// Type of a venue-op parameter, mirroring `ops.v1.ParamType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VenueOpParamType {
    #[default]
    Unspecified,
    String,
    Int64,
    Decimal,
    Bool,
    Enum,
    List,
    Map,
}

impl VenueOpParamType {
    /// The `ops.v1.ParamType` enum name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "PARAM_TYPE_UNSPECIFIED",
            Self::String => "PARAM_TYPE_STRING",
            Self::Int64 => "PARAM_TYPE_INT64",
            Self::Decimal => "PARAM_TYPE_DECIMAL",
            Self::Bool => "PARAM_TYPE_BOOL",
            Self::Enum => "PARAM_TYPE_ENUM",
            Self::List => "PARAM_TYPE_LIST",
            Self::Map => "PARAM_TYPE_MAP",
        }
    }
}

/// One parameter of a venue operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueOpParam {
    /// Parameter name exactly as the venue API expects it.
    pub name: String,
    pub r#type: VenueOpParamType,
    pub required: bool,
    /// Human-readable documentation, empty when the venue supplies none.
    pub doc: String,
    /// Populated when `type` is `Enum`.
    pub enum_values: Vec<String>,
}

/// Self-describing metadata for one venue operation.
///
/// Carrying the full descriptor rather than just a name matters: `mutating` is
/// what a client uses to decide whether a call needs a confirmation prompt, and
/// reporting `false` for `account.transfer` would tell it a fund-moving
/// operation is read-only.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VenueOpDescriptor {
    pub name: String,
    pub category: String,
    pub summary: String,
    /// True when the operation moves funds or changes account state.
    pub mutating: bool,
    pub params: Vec<VenueOpParam>,
}

/// Generic venue-operation invocation port backed by the self-describing
/// venue-ops registry (`account.transfer`, `margin.borrow`, ...).
#[async_trait]
pub trait VenueOpInvoker: Send + Sync {
    /// Invokes `op` with JSON params; returns the venue's JSON response.
    async fn invoke_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
        params: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, PortError>;

    /// Lists the operations this backend exposes, with their full descriptors.
    async fn list_venue_ops(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<VenueOpDescriptor>, PortError>;

    /// Full schema of one operation.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] when the venue does not expose `op`. A caller
    /// must be able to tell "no such operation" from "exists but undocumented".
    async fn describe_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
    ) -> Result<VenueOpDescriptor, PortError> {
        let ops = self.list_venue_ops(exchange_id).await?;
        ops.into_iter()
            .find(|o| o.name == op)
            .ok_or_else(|| PortError::NotFound(format!("venue operation {op}")))
    }
}

/// One wallet ledger entry (deposit / withdrawal / transfer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub id: String,
    pub currency: String,
    /// **Signed**: a withdrawal is negative, a deposit positive. Keeping the
    /// sign here (rather than trusting a venue's unsigned `direction` field)
    /// means a caller cannot read a withdrawal as a deposit.
    pub amount: Decimal,
    pub entry_type: String,
    pub completed: bool,
    pub time_ms: i64,
}

/// A venue's answer to a [`WalletGateway::transfer`] call.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransferReceipt {
    /// Venue-assigned transfer id; empty when the venue reports none.
    pub transfer_id: String,
    /// The resulting ledger row, when the venue reports one.
    pub entry: Option<LedgerEntry>,
}

/// Wallet operations port: ledger scanning and internal transfers.
#[async_trait]
pub trait WalletGateway: Send + Sync {
    /// Ledger rows newest first, filtered by `currency` and `entry_type`
    /// (either empty meaning "no filter").
    async fn list_ledger_entries(
        &self,
        exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError>;

    /// Completed deposits, newest first.
    ///
    /// A default over [`Self::list_ledger_entries`] so a backend that can
    /// serve a filtered ledger does not have to implement this separately.
    async fn fetch_deposits(
        &self,
        exchange_id: &common::ExchangeId,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        self.list_ledger_entries(exchange_id, "", "deposit", limit).await
    }

    /// Transfers `amount` of `asset` to the destination account label.
    ///
    /// `client_transfer_id` is an optional idempotency key. Transfers move
    /// funds, so a retry without one risks a double spend; supply it whenever
    /// the caller can retry.
    async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<TransferReceipt, PortError>;
}
