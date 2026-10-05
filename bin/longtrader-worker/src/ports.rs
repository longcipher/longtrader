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
// Decimal encode/decode
//
// The wire representation of a decimal is a single base-10 string, and
// `longtrader_contract::ext` owns the only encode/decode pair for it
// (`decimal_to_common` / `common_to_decimal`). The duplicate helpers that used
// to live here were removed so that pair has one owner. Strategy code should
// call `longtrader_contract::ext::decimal_to_common` directly.
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

    /// Case-insensitive substring search over symbol names.
    async fn search_symbols(
        &self,
        req: market::SearchSymbolsRequest,
    ) -> Result<market::SearchSymbolsResponse, PortError>;

    /// Batch ticker snapshots (empty `symbols` = every symbol).
    async fn list_tickers(
        &self,
        req: market::ListTickersRequest,
    ) -> Result<market::ListTickersResponse, PortError>;

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

    /// Batch funding snapshots (empty `symbols` = every symbol the venue
    /// reports funding for). One call, one venue: a batch spanning venues would
    /// make a cross-venue basis comparison unattributable.
    async fn list_funding_rates(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<FundingRateSnapshot>, PortError>;
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
    #[allow(clippy::too_many_arguments)]
    async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<TransferReceipt, PortError>;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use longtrader_contract::ext::common_to_decimal;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    /// The decode failure for a payload no `Decimal` can hold.
    ///
    /// `Decimal` carries one base-10 string, so "cannot represent" is a property
    /// of that payload rather than of a numeric pair. Built by running the real
    /// decoder so the fixture cannot drift from what the parser does.
    fn out_of_range(payload: &str) -> DecimalConvertError {
        let wire = common::Decimal { value: payload.to_string(), ..Default::default() };
        match common_to_decimal(&wire) {
            Err(err) => err,
            Ok(_) => unreachable!("{payload} must not decode"),
        }
    }

    // -----------------------------------------------------------------------
    // Sequence-gap detection
    // -----------------------------------------------------------------------

    #[test]
    fn consecutive_sequences_are_not_a_gap() {
        assert!(!is_sequence_gap(0, 1));
        assert!(!is_sequence_gap(1, 2));
        assert!(!is_sequence_gap(u64::MAX - 1, u64::MAX));
    }

    /// `wrapping_add` is what makes `u64::MAX -> 0` contiguous rather than a
    /// gap; a saturating add would have reported a false resync here.
    #[test]
    fn the_sequence_wraps_through_zero_without_a_gap() {
        assert!(!is_sequence_gap(u64::MAX, 0), "u64::MAX -> 0 is contiguous");
    }

    #[test]
    fn skipped_duplicate_and_regressed_sequences_are_gaps() {
        assert!(is_sequence_gap(0, 0), "a duplicate is not an increment");
        assert!(is_sequence_gap(0, 2), "a skip is a gap");
        assert!(is_sequence_gap(5, 1), "a regression is a gap");
        assert!(is_sequence_gap(1, 0), "one backwards is a gap");
    }

    #[test]
    fn a_missing_watermark_is_never_a_gap() {
        let header = common::EventHeader { sequence: 4_242, ..Default::default() };
        assert!(!has_sequence_gap(None, &header), "the first event cannot gap");
    }

    #[test]
    fn has_sequence_gap_forwards_the_watermark() {
        let header = common::EventHeader { sequence: 10, ..Default::default() };
        assert!(!has_sequence_gap(Some(9), &header));
        assert!(has_sequence_gap(Some(10), &header));
        assert!(has_sequence_gap(Some(3), &header));
    }

    // -----------------------------------------------------------------------
    // Overflow policy mapping
    // -----------------------------------------------------------------------

    #[test]
    fn orderbook_channels_coalesce_and_everything_else_drops_the_oldest() {
        assert_eq!(
            overflow_policy_for_channel(market::StreamChannel::Orderbook),
            OverflowPolicy::Coalesce
        );
        for channel in [
            market::StreamChannel::Ticker,
            market::StreamChannel::Trades,
            market::StreamChannel::Ohlcv,
            market::StreamChannel::Unspecified,
        ] {
            assert_eq!(
                overflow_policy_for_channel(channel),
                OverflowPolicy::DropOldest,
                "{channel:?} must drop the oldest"
            );
        }
    }

    /// The policy constants are the documented §6.5 table; pin every one so a
    /// refactor cannot quietly repoint a channel at a lossy policy.
    #[test]
    fn the_documented_policy_constants_are_stable() {
        assert_eq!(OVERFLOW_TICKER, OverflowPolicy::DropOldest);
        assert_eq!(OVERFLOW_ORDERBOOK, OverflowPolicy::Coalesce);
        assert_eq!(OVERFLOW_ORDERS, OverflowPolicy::Block);
        assert_eq!(OVERFLOW_TRADES, OverflowPolicy::DropOldest);
        assert_eq!(OVERFLOW_OHLCV, OverflowPolicy::DropOldest);
        assert_eq!(OVERFLOW_BALANCES, OverflowPolicy::Block);
        assert_eq!(OVERFLOW_POSITIONS, OverflowPolicy::Block);
    }

    // -----------------------------------------------------------------------
    // PortError
    // -----------------------------------------------------------------------

    #[test]
    fn every_port_error_renders_a_distinct_message() {
        let errors = [
            PortError::Transport("socket closed".into()),
            PortError::Rpc { code: 5, message: "not found".into() },
            PortError::Unsupported("trigger orders".into()),
            PortError::MissingField("last".into()),
            PortError::InvalidArgument("qty must be positive".into()),
            PortError::NotFound("venue operation nope".into()),
        ];
        let rendered: Vec<String> = errors.iter().map(ToString::to_string).collect();
        for pair in rendered.iter().zip(rendered.iter().skip(1)) {
            assert_ne!(pair.0, pair.1, "two errors share the message {}", pair.0);
        }
        for message in &rendered {
            assert!(!message.is_empty(), "an error rendered as an empty string");
        }
        assert!(rendered.iter().any(|m| m.contains('5') && m.contains("not found")));
    }

    /// `NotFound` must read as "the backend answered, no such object" rather
    /// than as the "this backend can never have it" `Unsupported`.
    #[test]
    fn not_found_and_unsupported_are_distinguishable_in_the_message() {
        let not_found = PortError::NotFound("thing".into()).to_string();
        let unsupported = PortError::Unsupported("thing".into()).to_string();
        assert_ne!(not_found, unsupported);
        assert!(not_found.contains("not found"));
        assert!(unsupported.contains("unsupported"));
    }

    #[test]
    fn a_decimal_convert_error_converts_into_a_port_error() {
        // 29 nines is inside the contract grammar but wider than a `Decimal`.
        let payload = "9".repeat(29);
        let err: PortError = out_of_range(&payload).into();
        assert!(matches!(err, PortError::Decimal(_)));
        assert!(err.to_string().contains(&payload), "the payload must be echoed: {err}");
    }

    // -----------------------------------------------------------------------
    // Wire-name mappings
    // -----------------------------------------------------------------------

    #[test]
    fn trigger_order_status_wire_names() {
        assert_eq!(TriggerOrderStatus::Open.as_str(), "open");
        assert_eq!(TriggerOrderStatus::Triggered.as_str(), "triggered");
        assert_eq!(TriggerOrderStatus::Canceled.as_str(), "canceled");
        assert_eq!(TriggerOrderStatus::Rejected.as_str(), "rejected");
    }

    #[test]
    fn trigger_order_status_display_matches_as_str() {
        for status in [
            TriggerOrderStatus::Open,
            TriggerOrderStatus::Triggered,
            TriggerOrderStatus::Canceled,
            TriggerOrderStatus::Rejected,
        ] {
            assert_eq!(status.to_string(), status.as_str(), "{status:?}");
        }
    }

    #[test]
    fn trigger_order_status_defaults_to_open() {
        assert_eq!(TriggerOrderStatus::default(), TriggerOrderStatus::Open);
    }

    /// `VenueOpParamType::as_str` mirrors `ops.v1.ParamType`; the proxy maps
    /// these straight onto the wire, so a drift would publish an unknown enum
    /// name to clients.
    #[test]
    fn venue_op_param_type_wire_names() {
        assert_eq!(VenueOpParamType::Unspecified.as_str(), "PARAM_TYPE_UNSPECIFIED");
        assert_eq!(VenueOpParamType::String.as_str(), "PARAM_TYPE_STRING");
        assert_eq!(VenueOpParamType::Int64.as_str(), "PARAM_TYPE_INT64");
        assert_eq!(VenueOpParamType::Decimal.as_str(), "PARAM_TYPE_DECIMAL");
        assert_eq!(VenueOpParamType::Bool.as_str(), "PARAM_TYPE_BOOL");
        assert_eq!(VenueOpParamType::Enum.as_str(), "PARAM_TYPE_ENUM");
        assert_eq!(VenueOpParamType::List.as_str(), "PARAM_TYPE_LIST");
        assert_eq!(VenueOpParamType::Map.as_str(), "PARAM_TYPE_MAP");
    }

    #[test]
    fn venue_op_param_type_defaults_to_unspecified() {
        assert_eq!(VenueOpParamType::default(), VenueOpParamType::Unspecified);
    }

    /// The wire names are part of the published contract, so they must be
    /// unique across every variant.
    #[test]
    fn every_param_type_name_is_unique() {
        let all = [
            VenueOpParamType::Unspecified,
            VenueOpParamType::String,
            VenueOpParamType::Int64,
            VenueOpParamType::Decimal,
            VenueOpParamType::Bool,
            VenueOpParamType::Enum,
            VenueOpParamType::List,
            VenueOpParamType::Map,
        ];
        let mut names: Vec<&str> = all.iter().map(|t| t.as_str()).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "two param types share a wire name");
    }

    // -----------------------------------------------------------------------
    // Defaulted trait methods
    // -----------------------------------------------------------------------

    /// A backend that only implements `list_venue_ops` must still get the
    /// documented `describe_venue_op` behaviour for free.
    struct StubOps {
        ops: Vec<VenueOpDescriptor>,
    }

    #[async_trait]
    impl VenueOpInvoker for StubOps {
        async fn invoke_venue_op(
            &self,
            _exchange_id: &common::ExchangeId,
            _op: &str,
            _params: serde_json::Map<String, serde_json::Value>,
        ) -> Result<serde_json::Value, PortError> {
            Err(PortError::Unsupported("invoke".into()))
        }

        async fn list_venue_ops(
            &self,
            _exchange_id: &common::ExchangeId,
        ) -> Result<Vec<VenueOpDescriptor>, PortError> {
            Ok(self.ops.clone())
        }
    }

    fn op(name: &str) -> VenueOpDescriptor {
        VenueOpDescriptor {
            name: name.into(),
            category: "account".into(),
            summary: String::new(),
            mutating: false,
            params: Vec::new(),
        }
    }

    #[tokio::test]
    async fn the_default_describe_venue_op_finds_a_known_operation() {
        let stub = StubOps { ops: vec![op("account.balance"), op("account.transfer")] };
        let exchange = common::ExchangeId { id: "mock".into(), ..Default::default() };
        let found = stub.describe_venue_op(&exchange, "account.transfer").await.expect("found");
        assert_eq!(found.name, "account.transfer");
    }

    #[tokio::test]
    async fn the_default_describe_venue_op_reports_not_found() {
        let stub = StubOps { ops: vec![op("account.balance")] };
        let exchange = common::ExchangeId { id: "mock".into(), ..Default::default() };
        let err = stub.describe_venue_op(&exchange, "nope").await.expect_err("must not resolve");
        assert!(matches!(err, PortError::NotFound(_)), "got {err:?}");
        assert!(err.to_string().contains("nope"), "the message must name the op: {err}");
    }

    #[tokio::test]
    async fn the_default_describe_venue_op_on_an_empty_registry_is_not_found() {
        let stub = StubOps { ops: Vec::new() };
        let exchange = common::ExchangeId { id: "mock".into(), ..Default::default() };
        let err = stub.describe_venue_op(&exchange, "account.balance").await.expect_err("empty");
        assert!(matches!(err, PortError::NotFound(_)), "got {err:?}");
    }

    /// The default `fetch_deposits` forwards an empty currency filter and the
    /// `deposit` entry type; a backend that ignores the filter would report
    /// withdrawals as deposits.
    #[tokio::test]
    async fn the_default_fetch_deposits_forwards_the_deposit_filter() {
        struct RecordingWallet {
            seen: std::sync::Mutex<Option<(String, String, u32)>>,
        }

        #[async_trait]
        impl WalletGateway for RecordingWallet {
            async fn list_ledger_entries(
                &self,
                _exchange_id: &common::ExchangeId,
                currency: &str,
                entry_type: &str,
                limit: u32,
            ) -> Result<Vec<LedgerEntry>, PortError> {
                *self.seen.lock().expect("wallet mutex") =
                    Some((currency.to_string(), entry_type.to_string(), limit));
                Ok(Vec::new())
            }

            async fn transfer(
                &self,
                _exchange_id: &common::ExchangeId,
                _asset: &str,
                _amount: Decimal,
                _dest_label: &str,
                _client_transfer_id: &str,
            ) -> Result<TransferReceipt, PortError> {
                Err(PortError::Unsupported("transfer".into()))
            }
        }

        let wallet = RecordingWallet { seen: std::sync::Mutex::new(None) };
        let exchange = common::ExchangeId { id: "mock".into(), ..Default::default() };
        let rows = wallet.fetch_deposits(&exchange, 42).await.expect("deposits");
        assert!(rows.is_empty());
        let seen = wallet.seen.lock().expect("wallet mutex").clone();
        assert_eq!(
            seen,
            Some((String::new(), "deposit".to_string(), 42)),
            "the default must forward an empty currency filter and the deposit type"
        );
    }

    // -----------------------------------------------------------------------
    // Ledger signing
    // -----------------------------------------------------------------------

    /// `LedgerEntry::amount` is signed on purpose: a caller must not be able to
    /// read a withdrawal as a deposit.
    #[test]
    fn ledger_entries_keep_the_direction_in_the_sign() {
        let deposit = LedgerEntry {
            id: "d1".into(),
            currency: "USDT".into(),
            amount: dec!(100),
            entry_type: "deposit".into(),
            completed: true,
            time_ms: 0,
        };
        let withdrawal = LedgerEntry { amount: dec!(-100), ..deposit.clone() };
        assert!(deposit.amount.is_sign_positive());
        assert!(withdrawal.amount.is_sign_negative());
        assert_ne!(deposit, withdrawal);
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// `is_sequence_gap(prev, next)` is gap-free exactly when `next` is
        /// `prev + 1` in wrapping arithmetic.
        #[test]
        fn a_sequence_is_gap_free_exactly_when_it_wraps_to_prev_plus_one(
            prev in any::<u64>(),
            delta in 0u64..8,
        ) {
            let next = prev.wrapping_add(delta);
            prop_assert_eq!(is_sequence_gap(prev, next), delta != 1, "prev={} delta={}", prev, delta );
        }

        /// Adding a sequence and then checking the next one is always
        /// gap-free: the detector must agree with its own producer.
        #[test]
        fn incrementing_then_checking_is_never_a_gap(prev in any::<u64>()) {
            let next = prev.wrapping_add(1);
            let header = common::EventHeader { sequence: next, ..Default::default() };
            prop_assert!(!is_sequence_gap(prev, next));
            prop_assert!(!has_sequence_gap(Some(prev), &header));
        }

        /// Presenting the same sequence twice always reports a gap.
        #[test]
        fn repeating_a_sequence_is_always_a_gap(sequence in any::<u64>()) {
            let header = common::EventHeader { sequence, ..Default::default() };
            prop_assert!(is_sequence_gap(sequence, sequence));
            prop_assert!(has_sequence_gap(Some(sequence), &header));
        }

        /// Orderbook is the only channel that coalesces — the §6.5 table. Any
        /// other channel, present or future, must drop the oldest instead.
        #[test]
        fn only_the_orderbook_channel_coalesces(
            channel in prop::sample::select(&[
                market::StreamChannel::Unspecified,
                market::StreamChannel::Ticker,
                market::StreamChannel::Orderbook,
                market::StreamChannel::Trades,
                market::StreamChannel::Ohlcv,
            ]),
        ) {
            let policy = overflow_policy_for_channel(channel);
            prop_assert_eq!(
                policy == OverflowPolicy::Coalesce,
                channel == market::StreamChannel::Orderbook, "{:?} mapped to {:?}", channel, policy
            );
        }

        /// Every status renders as a non-empty, unique, lowercase wire name.
        #[test]
        fn trigger_order_status_names_are_stable(idx in 0u8..4) {
            let status = match idx {
                0 => TriggerOrderStatus::Open,
                1 => TriggerOrderStatus::Triggered,
                2 => TriggerOrderStatus::Canceled,
                _ => TriggerOrderStatus::Rejected,
            };
            let name = status.as_str();
            prop_assert!(!name.is_empty());
            prop_assert_eq!(name.to_lowercase(), name);
            prop_assert_eq!(status.to_string(), name);
        }

        /// `Decimal` converts into `PortError` and renders the offending payload.
        ///
        /// Nine digits past what the 96-bit mantissa holds: the payload is
        /// grammar-clean, so only the width can reject it.
        #[test]
        fn an_out_of_range_decimal_always_converts(digits in 29usize..40) {
            let payload = "9".repeat(digits);
            let err: PortError = out_of_range(&payload).into();
            prop_assert!(matches!(err, PortError::Decimal(_)));
            prop_assert!(err.to_string().contains(&payload), "{err}");
        }
    }
}
