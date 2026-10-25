//! Pre-trade risk gate.
//!
//! The gate sits between a strategy and its [`TradingGateway`]: every
//! submission is measured against the configured [`RiskLimits`] before it
//! reaches a venue, and a refusal comes back to the strategy as a
//! [`PortError`] instead of an order id.
//!
//! Every mainstream strategy host leaves this layer out or charges for it:
//! Hummingbot runs one bot per install and cannot reconcile net exposure across
//! them, and NautilusTrader ships its risk engine as a paid component. Here it
//! is part of the host, and it is off until configured.
//!
//! Two classes of limit, split by what each one costs:
//!
//! - **Local** — `max_order_qty`, `max_order_notional`, `max_orders_per_minute`. Decided from the
//!   request alone, so they are free.
//! - **Venue-backed** — `max_open_orders`, `max_net_qty_per_symbol`. Decided against the venue's
//!   own resting orders, read once per submission. Exact, because the venue is the only authority
//!   on what is resting, at the cost of one extra `FetchOpenOrders` round-trip.
//!
//! Cancel and close paths are never gated: a gate that could refuse a
//! cancel-all would be able to hold the kill switch shut, which is the one
//! outcome a risk control must never cause.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use buffa::{EnumValue, MessageField};
use rust_decimal::Decimal;

use crate::{
    ports::{PortError, TradingGateway},
    proto::{common, trading, worker},
};

/// One order's submission, in the only terms the gate measures: which symbol,
/// which direction, how much, and at what price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    pub symbol: String,
    pub is_buy: bool,
    pub qty: Decimal,
    /// Absent for a market order, which carries no price and therefore has no
    /// computable notional.
    pub price: Option<Decimal>,
}

impl OrderIntent {
    /// Buy is positive, sell is negative. A market maker resting both sides of
    /// one symbol nets toward zero, which is exactly what a net-exposure limit
    /// is meant to permit and a gross limit would not.
    #[must_use]
    pub fn signed_qty(&self) -> Decimal {
        if self.is_buy { self.qty } else { -self.qty }
    }

    /// `price * qty`. `None` for an order with no price: refusing to guess is
    /// better than sizing a market order against a stale number.
    #[must_use]
    pub fn notional(&self) -> Option<Decimal> {
        self.price.map(|price| price * self.qty)
    }
}

/// A resting order as the venue reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestingOrder {
    pub symbol: String,
    pub is_buy: bool,
    /// Unfilled quantity, not the original amount.
    pub qty: Decimal,
}

impl RestingOrder {
    #[must_use]
    pub fn signed_qty(&self) -> Decimal {
        if self.is_buy { self.qty } else { -self.qty }
    }
}

/// Order-submission limits. Every field defaults to `None`, and an all-`None`
/// set is a pass-through.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RiskLimits {
    /// Largest quantity one order may carry.
    pub max_order_qty: Option<Decimal>,
    /// Largest `price * qty` one order may carry. A market order has no price,
    /// so its notional is unknown and this limit does not apply to it —
    /// `max_order_qty` is the market-order equivalent.
    pub max_order_notional: Option<Decimal>,
    /// Largest number of submissions accepted per minute.
    pub max_orders_per_minute: Option<u32>,
    /// Largest number of resting orders across every symbol.
    pub max_open_orders: Option<u32>,
    /// Largest absolute net resting quantity on one symbol.
    pub max_net_qty_per_symbol: Option<Decimal>,
}

impl RiskLimits {
    /// Whether anything is configured.
    ///
    /// An all-`None` set must cost nothing on the hot path, so the caller skips
    /// the decorator entirely rather than paying for a wrapper that always
    /// answers "allowed".
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.max_order_qty.is_some() ||
            self.max_order_notional.is_some() ||
            self.max_orders_per_minute.is_some() ||
            self.max_open_orders.is_some() ||
            self.max_net_qty_per_symbol.is_some()
    }

    /// Whether a decision needs the venue's resting orders.
    #[must_use]
    pub const fn needs_book(&self) -> bool {
        self.max_open_orders.is_some() || self.max_net_qty_per_symbol.is_some()
    }
}

/// Why a submission was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RiskViolation {
    /// The venue's resting orders were required and not supplied. Failing
    /// closed is the point: admitting an unmeasured order is the one outcome a
    /// risk gate exists to prevent.
    #[error("risk limits need the venue's resting orders, which were not read")]
    BookUnavailable,
    #[error("order quantity {actual} on {symbol} exceeds the limit {limit}")]
    OrderQty { symbol: String, actual: Decimal, limit: Decimal },
    #[error("order notional {actual} on {symbol} exceeds the limit {limit}")]
    OrderNotional { symbol: String, actual: Decimal, limit: Decimal },
    #[error("submission rate exceeds {limit} orders/minute")]
    Rate { limit: u32 },
    #[error("open orders {actual} exceed the limit {limit}")]
    OpenOrders { actual: usize, limit: u32 },
    #[error("net quantity {actual} on {symbol} exceeds the limit {limit}")]
    NetQty { symbol: String, actual: Decimal, limit: Decimal },
}

/// Token-bucket unit: one order costs [`TOKENS_PER_ORDER`], and a bucket only
/// ever holds whole units, so the rate is exact at any limit. A float rate
/// would make `max_orders_per_minute = 1` refill slower than a minute.
const TOKENS_PER_ORDER: u128 = 60_000;

/// The only mutable part of the gate: how much submission budget is left, and
/// when it was last measured.
#[derive(Debug)]
struct Bucket {
    tokens: u128,
    last: Instant,
}

/// Decides submissions against [`RiskLimits`].
///
/// Pure with respect to the venue: it never calls out. The caller supplies the
/// resting book when one is needed, which is what makes the venue-backed limits
/// exact and the gate testable without a backend.
#[derive(Debug)]
pub struct RiskGate {
    limits: RiskLimits,
    bucket: Mutex<Bucket>,
}

impl RiskGate {
    #[must_use]
    pub fn new(limits: RiskLimits, now: Instant) -> Self {
        let capacity = u128::from(limits.max_orders_per_minute.unwrap_or(0)) * TOKENS_PER_ORDER;
        Self { limits, bucket: Mutex::new(Bucket { tokens: capacity, last: now }) }
    }

    #[must_use]
    pub const fn limits(&self) -> RiskLimits {
        self.limits
    }

    /// Decide a submission of `intents`.
    ///
    /// `resting` must be `Some` whenever [`RiskLimits::needs_book`] is true.
    /// A batch is all-or-nothing, matching the batch RPC's own gate: one bad leg
    /// refuses the whole batch, so a strategy never sees a half-placed grid.
    ///
    /// Nothing is consumed on a refusal — otherwise a strategy rejected for a
    /// size limit would also burn its rate budget, and a loop that is already
    /// misbehaving would be throttled into silence without ever being told why.
    ///
    /// # Errors
    ///
    /// Returns the first [`RiskViolation`] found.
    pub fn admit(
        &self,
        intents: &[OrderIntent],
        resting: Option<&[RestingOrder]>,
        now: Instant,
    ) -> Result<(), RiskViolation> {
        for intent in intents {
            if let Some(limit) = self.limits.max_order_qty &&
                intent.qty > limit
            {
                return Err(RiskViolation::OrderQty {
                    symbol: intent.symbol.clone(),
                    actual: intent.qty,
                    limit,
                });
            }
            if let (Some(limit), Some(actual)) = (self.limits.max_order_notional, intent.notional()) &&
                actual > limit
            {
                return Err(RiskViolation::OrderNotional {
                    symbol: intent.symbol.clone(),
                    actual,
                    limit,
                });
            }
        }

        if self.limits.needs_book() {
            let resting = resting.ok_or(RiskViolation::BookUnavailable)?;
            let open = resting.len() + intents.len();
            if let Some(limit) = self.limits.max_open_orders &&
                open > usize::try_from(limit).unwrap_or(usize::MAX)
            {
                return Err(RiskViolation::OpenOrders { actual: open, limit });
            }
            if let Some(limit) = self.limits.max_net_qty_per_symbol {
                let mut net: BTreeMap<&str, Decimal> = BTreeMap::new();
                for resting in resting {
                    *net.entry(resting.symbol.as_str()).or_insert(Decimal::ZERO) +=
                        resting.signed_qty();
                }
                for intent in intents {
                    *net.entry(intent.symbol.as_str()).or_insert(Decimal::ZERO) +=
                        intent.signed_qty();
                }
                for (symbol, net) in net {
                    if net.abs() > limit {
                        return Err(RiskViolation::NetQty {
                            symbol: symbol.to_string(),
                            actual: net,
                            limit,
                        });
                    }
                }
            }
        }

        self.consume(intents.len(), now)
    }

    /// Spend one minute's worth of budget per order. Integer-only: the refill
    /// is `elapsed_ms * limit` units, which is exact for every limit including
    /// one order per minute.
    fn consume(&self, orders: usize, now: Instant) -> Result<(), RiskViolation> {
        let Some(limit) = self.limits.max_orders_per_minute else {
            return Ok(());
        };
        let capacity = u128::from(limit) * TOKENS_PER_ORDER;
        let mut bucket = self.bucket.lock().expect("the gate bucket lock is not poisoned");
        if let Some(elapsed) = now.checked_duration_since(bucket.last) {
            let refill = elapsed.as_millis() * u128::from(limit);
            bucket.tokens = bucket.tokens.saturating_add(refill).min(capacity);
            bucket.last = now;
        }
        let cost = u128::try_from(orders).unwrap_or(u128::MAX).saturating_mul(TOKENS_PER_ORDER);
        if bucket.tokens < cost {
            return Err(RiskViolation::Rate { limit });
        }
        bucket.tokens -= cost;
        Ok(())
    }
}

/// A [`TradingGateway`] that refuses submissions the configured limits reject
/// and forwards everything else unchanged.
pub struct RiskGateway {
    inner: Arc<dyn TradingGateway>,
    gate: RiskGate,
}

impl RiskGateway {
    #[must_use]
    pub fn new(inner: Arc<dyn TradingGateway>, limits: RiskLimits) -> Self {
        Self { inner, gate: RiskGate::new(limits, Instant::now()) }
    }

    /// Measure `intents`, reading the venue's resting orders only when a
    /// configured limit needs them.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::InvalidArgument`] carrying the violated limit, or
    /// whatever the venue read failed with.
    async fn admit(
        &self,
        intents: &[OrderIntent],
        exchange_id: &common::ExchangeId,
    ) -> Result<(), PortError> {
        let resting = if self.gate.limits().needs_book() {
            // Unscoped: a batch can span symbols, and per-symbol reads would
            // miss every leg outside the first one.
            let req = trading::FetchOpenOrdersRequest {
                exchange_id: MessageField::some(exchange_id.clone()),
                ..Default::default()
            };
            let orders = self.inner.fetch_open_orders(req).await?;
            let mut resting = Vec::with_capacity(orders.len());
            for order in &orders {
                resting.push(resting_of(order)?);
            }
            Some(resting)
        } else {
            None
        };
        self.gate
            .admit(intents, resting.as_deref(), Instant::now())
            .map_err(|violation| PortError::InvalidArgument(violation.to_string()))
    }
}

/// The submission shape of one [`trading::OrderRequest`].
///
/// # Errors
///
/// Returns [`PortError::MissingField`] when the request carries no quantity,
/// and a decimal error when a populated decimal is outside the documented
/// grammar.
fn intent_of(order: &trading::OrderRequest) -> Result<OrderIntent, PortError> {
    let amount = order
        .amount
        .as_option()
        .ok_or_else(|| PortError::MissingField("order.amount".to_string()))?;
    Ok(OrderIntent {
        symbol: order.symbol.clone(),
        is_buy: matches!(order.side, EnumValue::Known(trading::OrderSide::Buy)),
        qty: longtrader_contract::ext::common_to_decimal(amount)?,
        price: order
            .price
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?,
    })
}

/// The exposure shape of one resting [`trading::Order`].
///
/// # Errors
///
/// Returns [`PortError::MissingField`] when the venue reports an order with
/// neither a remaining nor an original quantity.
fn resting_of(order: &trading::Order) -> Result<RestingOrder, PortError> {
    let qty = order
        .remaining
        .as_option()
        .or(order.amount.as_option())
        .ok_or_else(|| PortError::MissingField("order.remaining".to_string()))?;
    Ok(RestingOrder {
        symbol: order.symbol.clone(),
        is_buy: matches!(order.side, EnumValue::Known(trading::OrderSide::Buy)),
        qty: longtrader_contract::ext::common_to_decimal(qty)?,
    })
}

#[async_trait]
impl TradingGateway for RiskGateway {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let exchange_id =
            req.exchange_id.as_option().cloned().unwrap_or_else(common::ExchangeId::default);
        let order =
            req.order.as_option().ok_or_else(|| PortError::MissingField("order".to_string()))?;
        self.admit(&[intent_of(order)?], &exchange_id).await?;
        self.inner.create_order(req).await
    }

    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let exchange_id =
            req.exchange_id.as_option().cloned().unwrap_or_else(common::ExchangeId::default);
        let mut intents = Vec::with_capacity(req.orders.len());
        for order in &req.orders {
            intents.push(intent_of(order)?);
        }
        self.admit(&intents, &exchange_id).await?;
        self.inner.batch_create_orders(req).await
    }

    // --- Never gated: a risk control must not be able to hold the kill switch
    // shut, and none of these change exposure upward.

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError> {
        self.inner.cancel_order(req).await
    }

    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        self.inner.cancel_all_orders(req).await
    }

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        self.inner.fetch_open_orders(req).await
    }

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError> {
        self.inner.get_account(req).await
    }

    async fn get_positions(
        &self,
        req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError> {
        self.inner.get_positions(req).await
    }

    async fn get_order_history(
        &self,
        req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError> {
        self.inner.get_order_history(req).await
    }

    async fn get_closed_positions(
        &self,
        req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError> {
        self.inner.get_closed_positions(req).await
    }

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError> {
        self.inner.close_position(req).await
    }

    async fn close_all_positions(
        &self,
        req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError> {
        self.inner.close_all_positions(req).await
    }

    async fn modify_position(
        &self,
        req: trading::ModifyPositionRequest,
    ) -> Result<trading::ModifyPositionResponse, PortError> {
        self.inner.modify_position(req).await
    }

    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        self.inner.sync_state(exchange_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::adapters::MockAdapter;

    /// The gate's clock origin.
    ///
    /// Shared, not sampled per call: if the gate and the test each sampled
    /// `Instant::now()`, the gate's origin would sit a few microseconds after
    /// the test's, and every "one minute later" assertion would measure one
    /// minute minus that drift — one refill unit short of what the limit
    /// promises.
    fn clock() -> Instant {
        static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        *ORIGIN.get_or_init(Instant::now)
    }

    fn buy(symbol: &str, qty: Decimal, price: Option<Decimal>) -> OrderIntent {
        OrderIntent { symbol: symbol.to_string(), is_buy: true, qty, price }
    }

    fn sell(symbol: &str, qty: Decimal, price: Option<Decimal>) -> OrderIntent {
        OrderIntent { symbol: symbol.to_string(), is_buy: false, qty, price }
    }

    fn resting(symbol: &str, is_buy: bool, qty: Decimal) -> RestingOrder {
        RestingOrder { symbol: symbol.to_string(), is_buy, qty }
    }

    /// A gate whose bucket starts at the shared [`clock`] origin.
    fn gate(limits: RiskLimits) -> RiskGate {
        RiskGate::new(limits, clock())
    }

    // ---- limits -----------------------------------------------------------

    /// An unconfigured gate must admit anything: the whole point of the
    /// decorator being skipped in `main` is that a default config behaves
    /// exactly as it did before the gate existed.
    #[test]
    fn an_unconfigured_gate_admits_everything() {
        let gate = gate(RiskLimits::default());
        assert!(!gate.limits().is_enabled());
        assert!(!gate.limits().needs_book());
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1_000_000), None)], None, clock()).is_ok());
    }

    /// Each limit must enable the gate on its own, so enabling cannot depend on
    /// a field someone forgot to add to `is_enabled`.
    #[test]
    fn every_limit_enables_the_gate() {
        let any = RiskLimits {
            max_order_qty: Some(dec!(1)),
            max_order_notional: Some(dec!(1)),
            max_orders_per_minute: Some(1),
            max_open_orders: Some(1),
            max_net_qty_per_symbol: Some(dec!(1)),
        };
        assert!(any.is_enabled());
        assert!(any.needs_book());

        let local_only = RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() };
        assert!(local_only.is_enabled());
        assert!(!local_only.needs_book(), "a local-only gate must not pay for a book read");

        let book_only = RiskLimits { max_open_orders: Some(1), ..RiskLimits::default() };
        assert!(book_only.needs_book());

        // Each limit on its own, not just all five together: an `is_enabled`
        // that short-circuits on one field would still answer `true` for the
        // combined set and silently leave every other limit unarmed.
        for single in [
            RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() },
            RiskLimits { max_order_notional: Some(dec!(1)), ..RiskLimits::default() },
            RiskLimits { max_orders_per_minute: Some(1), ..RiskLimits::default() },
            RiskLimits { max_open_orders: Some(1), ..RiskLimits::default() },
            RiskLimits { max_net_qty_per_symbol: Some(dec!(1)), ..RiskLimits::default() },
        ] {
            assert!(single.is_enabled(), "{single:?} must arm the gate on its own");
        }
    }

    // ---- local limits -----------------------------------------------------

    #[test]
    fn a_quantity_above_the_limit_is_refused() {
        let gate = gate(RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, clock()).is_ok());
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1.0001), None)], None, clock()),
            Err(RiskViolation::OrderQty {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(1.0001),
                limit: dec!(1),
            })
        );
    }

    /// The limit is inclusive: an operator who sets `max_order_qty = 1` expects
    /// an order of exactly 1 to trade.
    #[test]
    fn the_quantity_limit_is_inclusive() {
        let gate = gate(RiskLimits { max_order_qty: Some(dec!(2)), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(2), None)], None, clock()).is_ok());
    }

    #[test]
    fn a_notional_above_the_limit_is_refused() {
        let gate =
            gate(RiskLimits { max_order_notional: Some(dec!(100)), ..RiskLimits::default() });
        let at = buy("BTC/USDT", dec!(2), Some(dec!(50)));
        let over = buy("BTC/USDT", dec!(2), Some(dec!(50.01)));
        assert!(gate.admit(&[at], None, clock()).is_ok());
        assert_eq!(
            gate.admit(&[over], None, clock()),
            Err(RiskViolation::OrderNotional {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(100.02),
                limit: dec!(100),
            })
        );
    }

    /// A market order carries no price, so its notional is unknown. It must not
    /// be refused on a guess; `max_order_qty` is the market-order control.
    #[test]
    fn an_order_without_a_price_is_not_measured_by_notional() {
        let gate =
            gate(RiskLimits { max_order_notional: Some(dec!(0.0001)), ..RiskLimits::default() });
        let market = buy("BTC/USDT", dec!(1000), None);
        assert!(market.notional().is_none());
        assert!(gate.admit(&[market], None, clock()).is_ok());
    }

    // ---- venue-backed limits ----------------------------------------------

    /// Without the book, a book-backed limit cannot be decided. Admitting an
    /// unmeasured order is the one outcome the gate exists to prevent.
    #[test]
    fn a_book_backed_limit_without_the_book_fails_closed() {
        let gate = gate(RiskLimits { max_open_orders: Some(10), ..RiskLimits::default() });
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, clock()),
            Err(RiskViolation::BookUnavailable)
        );
    }

    #[test]
    fn an_open_order_count_above_the_limit_is_refused() {
        let gate = gate(RiskLimits { max_open_orders: Some(2), ..RiskLimits::default() });
        let resting = vec![resting("BTC/USDT", true, dec!(1))];
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], Some(&resting), clock()).is_ok());
        let batch = [buy("BTC/USDT", dec!(1), None), sell("BTC/USDT", dec!(1), None)];
        assert_eq!(
            gate.admit(&batch, Some(&resting), clock()),
            Err(RiskViolation::OpenOrders { actual: 3, limit: 2 })
        );
    }

    /// Net, not gross: a two-sided quote on one symbol cancels out, which is
    /// what lets a market maker keep quoting under a net-exposure limit.
    #[test]
    fn net_quantity_is_measured_signed_so_two_sided_quotes_cancel() {
        let gate =
            gate(RiskLimits { max_net_qty_per_symbol: Some(dec!(1)), ..RiskLimits::default() });
        let flat = vec![resting("BTC/USDT", true, dec!(5)), resting("BTC/USDT", false, dec!(5))];
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], Some(&flat), clock()).is_ok());
        assert!(gate.admit(&[sell("BTC/USDT", dec!(1), None)], Some(&flat), clock()).is_ok());
    }

    #[test]
    fn a_net_quantity_above_the_limit_is_refused_in_either_direction() {
        let gate =
            gate(RiskLimits { max_net_qty_per_symbol: Some(dec!(2)), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(2), None)], Some(&[]), clock()).is_ok());
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(2.5), None)], Some(&[]), clock()),
            Err(RiskViolation::NetQty {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(2.5),
                limit: dec!(2),
            })
        );
        assert_eq!(
            gate.admit(&[sell("BTC/USDT", dec!(2.5), None)], Some(&[]), clock()),
            Err(RiskViolation::NetQty {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(-2.5),
                limit: dec!(2),
            })
        );
    }

    /// The limit is per symbol, not portfolio-wide: one symbol at its cap must
    /// not consume another symbol's headroom.
    #[test]
    fn the_net_quantity_limit_is_per_symbol() {
        let gate =
            gate(RiskLimits { max_net_qty_per_symbol: Some(dec!(1)), ..RiskLimits::default() });
        let one_at_cap = vec![resting("BTC/USDT", true, dec!(1))];
        assert!(gate.admit(&[buy("ETH/USDT", dec!(1), None)], Some(&one_at_cap), clock()).is_ok());
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(0.5), None)], Some(&one_at_cap), clock()),
            Err(RiskViolation::NetQty {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(1.5),
                limit: dec!(1),
            })
        );
    }

    // ---- rate limit --------------------------------------------------------

    #[test]
    fn the_rate_limit_refuses_the_order_that_exceeds_it() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(2), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok());
        assert!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now + Duration::from_secs(1))
                .is_ok()
        );
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now + Duration::from_secs(2)),
            Err(RiskViolation::Rate { limit: 2 })
        );
    }

    /// A minute of idleness restores the full budget, and only the full budget:
    /// the bucket must not accumulate beyond capacity, or one quiet hour would
    /// license a burst large enough to be the incident the limit prevents.
    #[test]
    fn a_minute_of_idleness_refills_the_bucket_to_the_limit_and_no_further() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(1), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok());
        assert!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now + Duration::from_secs(61))
                .is_ok()
        );
        assert_eq!(
            gate.admit(
                &[buy("BTC/USDT", dec!(1), None)],
                None,
                now + Duration::from_secs(61) + Duration::from_secs(61)
            ),
            Ok(()),
            "one order per minute must refill exactly, not drift"
        );
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now + Duration::from_secs(123)),
            Err(RiskViolation::Rate { limit: 1 }),
            "two refilled orders back to back still exceed one per minute"
        );
    }

    /// A limit of one order per minute must still refill: a rate scaled by
    /// integer division would floor to zero and wedge the gate shut forever.
    #[test]
    fn the_slowest_rate_still_refills() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(1), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok());
        assert!(
            gate.admit(
                &[buy("BTC/USDT", dec!(1), None)],
                None,
                now + Duration::from_millis(60_000)
            )
            .is_ok()
        );
    }

    /// The refill is proportional to the limit, not to elapsed time alone:
    /// half a minute at two per minute restores one order.
    #[test]
    fn the_refill_scales_with_the_limit() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(2), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok());
        let half_minute = now + Duration::from_millis(30_000);
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, half_minute).is_ok());
        assert!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, half_minute).is_ok(),
            "thirty seconds at two per minute must restore exactly one more order"
        );
    }

    /// A refusal must not spend budget. Otherwise a strategy rejected on size
    /// would also lose its rate allowance and be throttled without being told
    /// the real reason.
    #[test]
    fn a_refusal_does_not_consume_rate_budget() {
        let now = clock();
        let gate = gate(RiskLimits {
            max_order_qty: Some(dec!(1)),
            max_orders_per_minute: Some(1),
            ..RiskLimits::default()
        });
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(5), None)], None, now),
            Err(RiskViolation::OrderQty {
                symbol: "BTC/USDT".to_string(),
                actual: dec!(5),
                limit: dec!(1),
            })
        );
        assert!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok(),
            "the refused order must not have spent the single available slot"
        );
    }

    /// A batch spends one slot per leg, so a grid cannot multiply its rate by
    /// batching.
    #[test]
    fn a_batch_consumes_one_rate_slot_per_leg() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(3), ..RiskLimits::default() });
        let three = vec![
            buy("BTC/USDT", dec!(1), None),
            sell("BTC/USDT", dec!(1), None),
            buy("BTC/USDT", dec!(1), None),
        ];
        assert!(gate.admit(&three, None, now).is_ok());
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now),
            Err(RiskViolation::Rate { limit: 3 })
        );
    }

    /// A batch larger than the whole budget is refused outright rather than
    /// partially, matching the batch RPC's own all-or-nothing gate.
    #[test]
    fn a_batch_larger_than_the_rate_budget_is_refused_whole() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(2), ..RiskLimits::default() });
        let three = vec![
            buy("BTC/USDT", dec!(1), None),
            sell("BTC/USDT", dec!(1), None),
            buy("BTC/USDT", dec!(1), None),
        ];
        assert_eq!(gate.admit(&three, None, now), Err(RiskViolation::Rate { limit: 2 }));
    }

    /// A clock that goes backwards must not refill: `checked_duration_since`
    /// yields `None` and the bucket is left alone rather than gaining budget.
    #[test]
    fn a_non_monotonic_clock_does_not_refill() {
        let now = clock();
        let gate = gate(RiskLimits { max_orders_per_minute: Some(1), ..RiskLimits::default() });
        assert!(gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now).is_ok());
        assert_eq!(
            gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, now - Duration::from_millis(1)),
            Err(RiskViolation::Rate { limit: 1 })
        );
    }

    // ---- intent / resting extraction ---------------------------------------

    fn common_dec(value: Decimal) -> common::Decimal {
        longtrader_contract::ext::decimal_to_common(value)
    }

    fn request(
        symbol: &str,
        side: trading::OrderSide,
        qty: Decimal,
        price: Option<Decimal>,
    ) -> trading::OrderRequest {
        trading::OrderRequest {
            symbol: symbol.to_string(),
            side: EnumValue::Known(side),
            amount: MessageField::some(common_dec(qty)),
            price: price.map(|p| MessageField::some(common_dec(p))).unwrap_or_default(),
            ..Default::default()
        }
    }

    #[test]
    fn an_order_request_maps_to_its_intent() {
        let intent =
            intent_of(&request("BTC/USDT", trading::OrderSide::Buy, dec!(2), Some(dec!(50))))
                .expect("a populated request must map");
        assert_eq!(intent, buy("BTC/USDT", dec!(2), Some(dec!(50))));
        assert_eq!(intent.signed_qty(), dec!(2));
        assert_eq!(intent.notional(), Some(dec!(100)));

        let short =
            intent_of(&request("BTC/USDT", trading::OrderSide::Sell, dec!(2), Some(dec!(50))))
                .expect("a populated request must map");
        assert_eq!(short.signed_qty(), dec!(-2));
    }

    /// An unpriced request must map with `price: None` rather than as zero: a
    /// zero price would be a real (and catastrophic) limit order.
    #[test]
    fn an_unpriced_request_has_no_price() {
        let intent = intent_of(&request("BTC/USDT", trading::OrderSide::Buy, dec!(2), None))
            .expect("a populated request must map");
        assert_eq!(intent.price, None);
        assert_eq!(intent.notional(), None);
    }

    /// A request with no quantity cannot be measured, so it is refused.
    #[test]
    fn a_request_without_an_amount_is_refused() {
        let err = intent_of(&trading::OrderRequest {
            symbol: "BTC/USDT".to_string(),
            ..Default::default()
        })
        .expect_err("a quantityless request cannot be measured");
        assert!(matches!(err, PortError::MissingField(_)), "{err}");
    }

    /// A resting order is measured by what is left, not by what was placed.
    #[test]
    fn a_resting_order_is_measured_by_its_remaining_quantity() {
        let order = trading::Order {
            symbol: "BTC/USDT".to_string(),
            side: EnumValue::Known(trading::OrderSide::Sell),
            amount: MessageField::some(common_dec(dec!(5))),
            remaining: MessageField::some(common_dec(dec!(1))),
            ..Default::default()
        };
        assert_eq!(resting_of(&order).expect("maps"), resting("BTC/USDT", false, dec!(1)));
    }

    /// A venue that omits `remaining` is measured by the original amount rather
    /// than refused: refusing would take the strategy's whole book offline
    /// because one venue's response was sparse.
    #[test]
    fn a_resting_order_without_remaining_falls_back_to_its_amount() {
        let order = trading::Order {
            symbol: "BTC/USDT".to_string(),
            side: EnumValue::Known(trading::OrderSide::Buy),
            amount: MessageField::some(common_dec(dec!(5))),
            ..Default::default()
        };
        assert_eq!(resting_of(&order).expect("maps"), resting("BTC/USDT", true, dec!(5)));
    }

    // ---- the decorator, over the offline venue -----------------------------

    fn create_req(symbol: &str, qty: Decimal, price: Decimal) -> trading::CreateOrderRequest {
        trading::CreateOrderRequest {
            exchange_id: MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            }),
            order: MessageField::some(request(symbol, trading::OrderSide::Buy, qty, Some(price))),
            ..Default::default()
        }
    }

    async fn open_count(gateway: &dyn TradingGateway) -> usize {
        gateway
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("the offline venue answers")
            .len()
    }

    /// The whole point is that a refused order never becomes an order: a gate
    /// that logged and forwarded would be a monitor, not a control.
    #[tokio::test]
    async fn the_decorator_refuses_an_order_before_the_venue_sees_it() {
        let mock = Arc::new(MockAdapter::new(dec!(95_000)));
        let gated = RiskGateway::new(
            mock.clone(),
            RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() },
        );
        let err = gated
            .create_order(create_req("BTC/USDT", dec!(5), dec!(95_000)))
            .await
            .expect_err("an oversized order must be refused");
        assert!(matches!(err, PortError::InvalidArgument(_)), "{err}");
        assert_eq!(open_count(&*mock).await, 0, "the refused order must never reach the venue");
    }

    #[tokio::test]
    async fn the_decorator_admits_an_order_inside_the_limits() {
        let mock = Arc::new(MockAdapter::new(dec!(95_000)));
        let gated = RiskGateway::new(
            mock.clone(),
            RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() },
        );
        gated.create_order(create_req("BTC/USDT", dec!(1), dec!(95_000))).await.expect("fits");
        assert_eq!(open_count(&*mock).await, 1);
    }

    /// A book-backed limit is measured against what is actually resting, which
    /// is why the decorator pays for one read per submission.
    #[tokio::test]
    async fn a_book_backed_limit_is_measured_against_the_venue() {
        let mock = Arc::new(MockAdapter::new(dec!(95_000)));
        let gated = RiskGateway::new(
            mock.clone(),
            RiskLimits { max_open_orders: Some(1), ..RiskLimits::default() },
        );
        gated
            .create_order(create_req("BTC/USDT", dec!(1), dec!(95_000)))
            .await
            .expect("first fits");
        let err = gated
            .create_order(create_req("BTC/USDT", dec!(1), dec!(95_000)))
            .await
            .expect_err("the second exceeds one resting order");
        assert!(matches!(err, PortError::InvalidArgument(_)), "{err}");
        assert_eq!(open_count(&*mock).await, 1, "only the admitted order is resting");
    }

    /// A batch is all-or-nothing: one oversized leg refuses the whole grid
    /// rather than placing the part that happened to fit.
    #[tokio::test]
    async fn a_batch_is_refused_whole_when_one_leg_exceeds_a_limit() {
        let mock = Arc::new(MockAdapter::new(dec!(95_000)));
        let gated = RiskGateway::new(
            mock.clone(),
            RiskLimits { max_order_qty: Some(dec!(1)), ..RiskLimits::default() },
        );
        let err = gated
            .batch_create_orders(trading::CreateOrdersRequest {
                exchange_id: MessageField::some(common::ExchangeId {
                    id: "mock".to_string(),
                    ..Default::default()
                }),
                orders: vec![
                    request("BTC/USDT", trading::OrderSide::Buy, dec!(1), Some(dec!(95_000))),
                    request("BTC/USDT", trading::OrderSide::Buy, dec!(5), Some(dec!(95_000))),
                ],
                ..Default::default()
            })
            .await
            .expect_err("the oversized leg must refuse the batch");
        assert!(matches!(err, PortError::InvalidArgument(_)), "{err}");
        assert_eq!(open_count(&*mock).await, 0, "no leg of a refused batch may be placed");
    }

    /// The kill switch must never be gated. Even with a limit the venue is
    /// already over, cancel-all has to reach it — a gate that could refuse a
    /// cancel would be able to hold the one safety path shut.
    #[tokio::test]
    async fn the_decorator_never_blocks_a_cancel_all() {
        let mock = Arc::new(MockAdapter::new(dec!(95_000)));
        mock.create_order(create_req("BTC/USDT", dec!(1), dec!(95_000)))
            .await
            .expect("place through the ungated venue");
        let gated = RiskGateway::new(
            mock.clone(),
            RiskLimits { max_open_orders: Some(0), ..RiskLimits::default() },
        );
        let canceled = gated
            .cancel_all_orders(trading::CancelAllOrdersRequest::default())
            .await
            .expect("a cancel-all must never be gated");
        assert_eq!(canceled.len(), 1);
    }

    /// A gateway that records every call that reached it, and with which venue
    /// identity, then forwards to the offline venue.
    ///
    /// The decorator's whole contract is "gate the submissions, forward
    /// everything else untouched". A forwarded call that stops forwarding —
    /// returning a default answer instead — is invisible to every other test
    /// here, because a default answer is indistinguishable from an empty venue
    /// until something counts the calls.
    struct RecordingGateway {
        inner: Arc<MockAdapter>,
        calls: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl RecordingGateway {
        fn new(inner: Arc<MockAdapter>) -> Self {
            Self { inner, calls: std::sync::Mutex::new(Vec::new()) }
        }

        fn note(&self, method: &str, venue: Option<&common::ExchangeId>) {
            let id = venue.map_or_else(String::new, |e| e.id.clone());
            self.calls.lock().expect("the call log is not poisoned").push((method.to_string(), id));
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().expect("the call log is not poisoned").clone()
        }
    }

    #[async_trait]
    impl TradingGateway for RecordingGateway {
        async fn create_order(
            &self,
            req: trading::CreateOrderRequest,
        ) -> Result<trading::Order, PortError> {
            self.note("create_order", req.exchange_id.as_option());
            self.inner.create_order(req).await
        }

        async fn batch_create_orders(
            &self,
            req: trading::CreateOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.note("batch_create_orders", req.exchange_id.as_option());
            self.inner.batch_create_orders(req).await
        }

        async fn cancel_order(
            &self,
            req: trading::CancelOrderRequest,
        ) -> Result<trading::Order, PortError> {
            self.note("cancel_order", req.exchange_id.as_option());
            self.inner.cancel_order(req).await
        }

        async fn cancel_all_orders(
            &self,
            req: trading::CancelAllOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.note("cancel_all_orders", req.exchange_id.as_option());
            self.inner.cancel_all_orders(req).await
        }

        async fn fetch_open_orders(
            &self,
            req: trading::FetchOpenOrdersRequest,
        ) -> Result<Vec<trading::Order>, PortError> {
            self.note("fetch_open_orders", req.exchange_id.as_option());
            self.inner.fetch_open_orders(req).await
        }

        async fn get_account(
            &self,
            req: trading::GetAccountRequest,
        ) -> Result<trading::GetAccountResponse, PortError> {
            self.note("get_account", req.exchange_id.as_option());
            self.inner.get_account(req).await
        }

        async fn get_positions(
            &self,
            req: trading::GetPositionsRequest,
        ) -> Result<trading::GetPositionsResponse, PortError> {
            self.note("get_positions", req.exchange_id.as_option());
            self.inner.get_positions(req).await
        }

        async fn get_order_history(
            &self,
            req: trading::GetOrderHistoryRequest,
        ) -> Result<trading::GetOrderHistoryResponse, PortError> {
            self.note("get_order_history", req.exchange_id.as_option());
            self.inner.get_order_history(req).await
        }

        async fn get_closed_positions(
            &self,
            req: trading::GetClosedPositionsRequest,
        ) -> Result<trading::GetClosedPositionsResponse, PortError> {
            self.note("get_closed_positions", req.exchange_id.as_option());
            self.inner.get_closed_positions(req).await
        }

        async fn close_position(
            &self,
            req: trading::ClosePositionRequest,
        ) -> Result<trading::ClosePositionResponse, PortError> {
            self.note("close_position", req.exchange_id.as_option());
            self.inner.close_position(req).await
        }

        async fn close_all_positions(
            &self,
            req: trading::CloseAllPositionsRequest,
        ) -> Result<trading::CloseAllPositionsResponse, PortError> {
            self.note("close_all_orders", req.exchange_id.as_option());
            self.inner.close_all_positions(req).await
        }

        async fn modify_position(
            &self,
            req: trading::ModifyPositionRequest,
        ) -> Result<trading::ModifyPositionResponse, PortError> {
            self.note("modify_position", req.exchange_id.as_option());
            self.inner.modify_position(req).await
        }

        async fn sync_state(
            &self,
            exchange_id: &common::ExchangeId,
        ) -> Result<worker::ReconcileStateResponse, PortError> {
            self.note("sync_state", Some(exchange_id));
            self.inner.sync_state(exchange_id).await
        }
    }

    /// Every method the gate does not decide must reach the venue, on the venue
    /// the caller named.
    ///
    /// This is what separates "forwarded" from "silently answered with a
    /// default": the default is a valid, empty, successful response, so only
    /// counting the call — and checking which venue it was addressed to — can
    /// tell them apart.
    #[tokio::test]
    async fn every_forwarded_call_reaches_the_venue_on_the_same_identity() {
        let recorder = Arc::new(RecordingGateway::new(Arc::new(MockAdapter::new(dec!(95_000)))));
        let inner = Arc::clone(&recorder) as Arc<dyn TradingGateway>;
        // A book-backed limit, so the gate reads the venue on every submission
        // and its read is part of what is being checked.
        let gated = RiskGateway::new(
            Arc::clone(&inner),
            RiskLimits { max_open_orders: Some(100), ..RiskLimits::default() },
        );

        let venue = common::ExchangeId { id: "mock".to_string(), ..common::ExchangeId::default() };
        let _ = gated
            .cancel_order(trading::CancelOrderRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .cancel_all_orders(trading::CancelAllOrdersRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .get_account(trading::GetAccountRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .get_positions(trading::GetPositionsRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .get_order_history(trading::GetOrderHistoryRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .get_closed_positions(trading::GetClosedPositionsRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .close_position(trading::ClosePositionRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .close_all_positions(trading::CloseAllPositionsRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated
            .modify_position(trading::ModifyPositionRequest {
                exchange_id: MessageField::some(venue.clone()),
                ..Default::default()
            })
            .await;
        let _ = gated.sync_state(&venue).await;
        let _ = gated.create_order(create_req("BTC/USDT", dec!(1), dec!(95_000))).await;

        let calls = recorder.calls();
        for method in [
            "cancel_order",
            "cancel_all_orders",
            "fetch_open_orders",
            "get_account",
            "get_positions",
            "get_order_history",
            "get_closed_positions",
            "close_position",
            "close_all_orders",
            "modify_position",
            "sync_state",
            "create_order",
        ] {
            assert!(
                calls.iter().any(|(seen, _)| seen == method),
                "{method} must reach the venue, saw {calls:?}"
            );
        }
        for (method, seen) in &calls {
            assert_eq!(seen, "mock", "{method} must name the venue it was addressed to");
        }
    }

    // ---- properties --------------------------------------------------------

    proptest::proptest! {
        /// A limit exactly at the measured quantity is inclusive, for every
        /// quantity: `admit(limit)` succeeds and `admit(limit + eps)` fails, so
        /// no quantity can slip past a limit it equals.
        #[test]
        fn the_quantity_limit_is_inclusive_everywhere(qty in 1u32..1_000) {
            let limit = Decimal::from(qty);
            let gate = gate(RiskLimits { max_order_qty: Some(limit), ..RiskLimits::default() });
            let at = buy("BTC/USDT", limit, None);
            // The step must stay inside `Decimal`'s 28 significant digits, or
            // it rounds away and `over` silently equals `at`.
            let over = buy("BTC/USDT", limit + dec!(0.00000000000000000001), None);
            proptest::prop_assert!(gate.admit(&[at], None, clock()).is_ok());
            proptest::prop_assert!(gate.admit(&[over], None, clock()).is_err());
        }

        /// Netting is exact: a signed sum over buys and sells never exceeds the
        /// largest single leg, whatever the mix.
        #[test]
        fn signed_netting_never_exceeds_the_largest_leg(
            buys in 0u32..50, sells in 0u32..50,
        ) {
            let qty = dec!(0.5);
            let mut intents: Vec<OrderIntent> = Vec::new();
            for _ in 0..buys {
                intents.push(buy("BTC/USDT", qty, None));
            }
            for _ in 0..sells {
                intents.push(sell("BTC/USDT", qty, None));
            }
            let mut net = Decimal::ZERO;
            for intent in &intents {
                net += intent.signed_qty();
            }
            proptest::prop_assert!(net.abs() <= qty * Decimal::from(buys.max(sells)));
        }

        /// The bucket never exceeds capacity, however long the idle gap: an
        /// unbounded bucket would turn quiet time into a burst license.
        #[test]
        fn the_bucket_never_exceeds_capacity(limit in 1u32..600, idle_ms in 0u64..7_200_000) {
            let now = clock();
            let gate = gate(RiskLimits {
                max_orders_per_minute: Some(limit),
                ..RiskLimits::default()
            });
            let later = now + Duration::from_millis(idle_ms);
            let mut admitted = 0u32;
            for _ in 0..limit + 1 {
                if gate.admit(&[buy("BTC/USDT", dec!(1), None)], None, later).is_ok() {
                    admitted += 1;
                }
            }
            proptest::prop_assert_eq!(admitted, limit);
        }
    }
}
