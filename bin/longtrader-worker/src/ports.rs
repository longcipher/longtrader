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
    async fn create_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<String, PortError>;

    async fn cancel_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<(), PortError>;
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

    /// Lists the operation names this backend exposes.
    async fn list_venue_ops(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<String>, PortError>;
}

/// One wallet ledger entry (deposit / withdrawal / transfer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub id: String,
    pub currency: String,
    pub amount: Decimal,
    pub entry_type: String,
    pub completed: bool,
    pub time_ms: i64,
}

/// Wallet operations port: deposits scanning and internal transfers.
#[async_trait]
pub trait WalletGateway: Send + Sync {
    async fn fetch_deposits(
        &self,
        exchange_id: &common::ExchangeId,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError>;

    /// Transfers `amount` of `asset` to the destination account label.
    async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
    ) -> Result<(), PortError>;
}
