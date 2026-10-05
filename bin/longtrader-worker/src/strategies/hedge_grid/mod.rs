//! Hedged grid: a grid on the primary venue with an opposite market hedge
//! on the hedge venue for every detected fill, keeping net delta ~zero.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order, market_order},
};

/// Config for the hedged grid.
#[derive(Debug, Clone, Deserialize)]
pub struct HedgeGridConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    pub lower_price: Decimal,
    pub upper_price: Decimal,
    pub num_levels: u32,
    pub qty_per_level: Decimal,
}

/// Hedged grid strategy.
pub struct HedgeGrid {
    config: HedgeGridConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    /// Ladder prices the strategy believes are resting, mapped to the side the
    /// venue reports for each.
    ///
    /// The hedge decision needs to compare this poll's book against the previous
    /// poll's, and a single `fetch_open_orders` read cannot express that
    /// difference — so the grid has to remember what it placed. Empty on the
    /// first poll, which is exactly why the first poll never hedges.
    believed: Mutex<HashMap<Decimal, bool>>,
}

impl HedgeGrid {
    pub fn new(
        config: HedgeGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, believed: Mutex::new(HashMap::new()) }
    }

    /// One poll cycle: hedge levels that left the book, then seed the levels
    /// that have no resting order.
    pub async fn tick(&self) -> Result<()> {
        let primary = self.config.venues.primary.proto();
        let hedge = self.config.venues.hedge.proto();
        let symbol = self.config.venues.symbol.clone();

        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(primary.clone()),
                symbol: symbol.clone(),
                ..Default::default()
            })
            .await?;
        let mid = ticker
            .last
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .unwrap_or_default();
        // The mid has to be strictly positive. A zero mid has no side to take,
        // and a negative one makes `is_buy = price < mid` false for every level
        // of a band above zero — an all-sell ladder, i.e. marketable orders at
        // prices the strategy believes are far below the market.
        if mid.is_sign_negative() || mid.is_zero() {
            return Ok(());
        }

        // A zero level count would divide by zero inside `rust_decimal`, and an
        // inverted span would produce a negative step that walks the seeding
        // loop away from the band. Both mean "nothing to do".
        if self.config.num_levels == 0 {
            return Ok(());
        }
        let step = (self.config.upper_price - self.config.lower_price) /
            Decimal::from(self.config.num_levels);
        if step <= Decimal::ZERO {
            return Ok(());
        }
        let open = self
            .gateway
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(primary.clone()),
                symbol: symbol.clone(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await?;
        // Resting price -> the side the venue reports for it.
        let mut live: HashMap<Decimal, bool> = HashMap::with_capacity(open.len());
        for order in &open {
            if let Some(Ok(price)) =
                order.price.as_option().map(longtrader_contract::ext::common_to_decimal)
            {
                let is_buy =
                    order.side == buffa::EnumValue::Known(crate::proto::trading::OrderSide::Buy);
                live.insert(price, is_buy);
            }
        }

        // Hedge on fills, never on a resting-order count.
        //
        // A count of resting bids against resting asks is not a fill signal:
        // seeding the ladder creates that imbalance itself (a `90..110` band
        // around a mid of 100 rests two bids against three asks), so a
        // count-driven hedge fires on the second poll with nothing having
        // filled. The only fill evidence these ports offer is a level this
        // strategy placed that is no longer on the book, so the hedge covers
        // the difference between the ladder we believed live and the one the
        // venue reports.
        //
        // Absence cannot distinguish a fill from a venue-side cancel. That is a
        // real imprecision, but it is strictly better than comparing counts: it
        // can only fire on a level this grid placed, and the seeding pass below
        // restores whatever left the book.
        let previous = {
            let mut believed = self.believed.lock().await;
            std::mem::take(&mut *believed)
        };
        // Hedge before re-quoting: the hedge is the risk-reducing leg, so a
        // ladder level the venue refuses must not be able to suppress it.
        for (price, was_buy) in previous.iter().filter(|(price, _)| !live.contains_key(*price)) {
            let coid = format!("hg-hedge-{}", ulid::Ulid::generate());
            self.gateway
                .create_order(market_order(
                    &hedge,
                    &symbol,
                    coid,
                    !*was_buy,
                    self.config.qty_per_level,
                ))
                .await?;
            tracing::info!(%price, hedge_is_buy = !*was_buy, "grid level left the book; hedged");
        }

        // Seed one leg per level with no resting order, and record what the
        // strategy now believes is live. A level the venue still shows keeps the
        // venue's side rather than the side this mid would pick today: a resting
        // order is not re-seeded when the mid crosses it.
        let mut believed = HashMap::new();
        let mut price = self.config.lower_price;
        while price <= self.config.upper_price {
            if let Some(resting_is_buy) = live.get(&price) {
                believed.insert(price, *resting_is_buy);
            } else {
                let is_buy = price < mid;
                let coid = format!("hg-{}", ulid::Ulid::generate());
                let request =
                    limit_order(&primary, &symbol, coid, is_buy, price, self.config.qty_per_level);
                // A refused leg ends the cycle. The next poll reconciles against
                // the open-order set and re-seeds whatever is missing, so
                // aborting repairs the ladder without hammering a venue that is
                // rejecting.
                self.gateway.create_order(request).await?;
                believed.insert(price, is_buy);
            }
            price += step;
        }
        *self.believed.lock().await = believed;
        Ok(())
    }
}

#[async_trait]
impl Strategy for HedgeGrid {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "hedge-grid tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use longtrader_contract::ext::{DecimalConvertError, decimal_to_common};
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::PortError,
        proto::{common, market, trading, worker},
        strategies::VenueRef,
    };

    const SYMBOL: &str = "BTC/USDT";

    fn config() -> HedgeGridConfig {
        HedgeGridConfig {
            venues: CrossVenueParams {
                primary: VenueRef { exchange_id: "primary".into(), label: String::new() },
                hedge: VenueRef { exchange_id: "hedge".into(), label: String::new() },
                symbol: SYMBOL.into(),
                poll_secs: 30,
            },
            lower_price: dec!(90),
            upper_price: dec!(110),
            num_levels: 4,
            qty_per_level: dec!(0.1),
        }
    }

    /// A config whose ladder walks `[lower, upper]` in `levels` steps.
    fn config_in_band(lower: Decimal, upper: Decimal, levels: u32) -> HedgeGridConfig {
        let mut config = config();
        config.lower_price = lower;
        config.upper_price = upper;
        config.num_levels = levels;
        config
    }

    /// A ladder of `levels` steps of exactly `step` each, so the band is
    /// `[lower, lower + levels * step]` and every rung lands on an exact decimal.
    ///
    /// Exact rungs matter for the properties: a band whose step does not divide
    /// evenly forces `rust_decimal` to a scale-28 mantissa, where repeated
    /// addition can overflow the 96-bit mantissa for a large `lower`.
    fn ladder(lower: i64, step: i64, levels: u32) -> HedgeGridConfig {
        let base = Decimal::from(lower);
        config_in_band(base, base + Decimal::from(i64::from(levels) * step), levels)
    }

    /// The mock venue, addressed as a market-data source at a fixed mid.
    fn mock_market(mid: Decimal) -> Arc<dyn MarketDataSource> {
        Arc::new(MockAdapter::new(mid))
    }

    // -----------------------------------------------------------------------
    // Stubs
    // -----------------------------------------------------------------------

    /// Which `PortError` a scripted venue call fails with.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Scripted {
        Transport,
        Rpc,
        Unsupported,
        MissingField,
        InvalidArgument,
        NotFound,
        Decimal,
    }

    /// Every [`PortError`] variant, rebuilt per call so the stub stays `Sync`
    /// (the enum is deliberately not `Clone`).
    fn scripted_error(kind: Scripted) -> PortError {
        match kind {
            Scripted::Transport => PortError::Transport("venue read failed".into()),
            Scripted::Rpc => PortError::Rpc { code: 8, message: "rate limited".into() },
            Scripted::Unsupported => PortError::Unsupported("open orders".into()),
            Scripted::MissingField => PortError::MissingField("symbol".into()),
            Scripted::InvalidArgument => PortError::InvalidArgument("bad symbol".into()),
            Scripted::NotFound => PortError::NotFound("open orders".into()),
            Scripted::Decimal => PortError::Decimal(DecimalConvertError::NotBase10 {
                value: "1e3".into(),
                reason: "exponents are not accepted",
            }),
        }
    }

    /// A gateway that records every submission and answers reads from a
    /// script.
    ///
    /// [`MockAdapter`] cannot be used for this: it never fails a read and it
    /// does not record `exchange_id`, so neither the error paths nor "the hedge
    /// leg went to the hedge venue" would be observable.
    #[derive(Default)]
    struct ScriptedGateway {
        open_orders: Mutex<Vec<trading::Order>>,
        open_failure: Mutex<Option<Scripted>>,
        /// `(first failing index, kind)`; `None` never fails a submission.
        create_failure: Mutex<Option<(usize, Scripted)>>,
        submitted: Mutex<Vec<trading::CreateOrderRequest>>,
        open_calls: AtomicUsize,
    }

    impl ScriptedGateway {
        /// The resting orders the next `fetch_open_orders` reports.
        fn resting(&self, orders: Vec<trading::Order>) -> &Self {
            *self.open_orders.lock().expect("gateway mutex") = orders;
            self
        }

        /// Drop every resting order at `price`, as a venue would after a fill.
        ///
        /// This is the only fill signal the strategy has: a level it placed that
        /// is no longer on the book.
        fn filled(&self, price: Decimal) -> &Self {
            self.open_orders.lock().expect("gateway mutex").retain(|order| {
                order
                    .price
                    .as_option()
                    .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok()) !=
                    Some(price)
            });
            self
        }

        /// Make every open-order read fail with `kind`.
        fn fail_open(&self, kind: Scripted) -> &Self {
            *self.open_failure.lock().expect("gateway mutex") = Some(kind);
            self
        }

        /// Make every submission fail with `kind`.
        fn fail_create(&self, kind: Scripted) -> &Self {
            self.fail_create_from(0, kind)
        }

        /// Make the submission at `index` and every later one fail.
        fn fail_create_from(&self, index: usize, kind: Scripted) -> &Self {
            *self.create_failure.lock().expect("gateway mutex") = Some((index, kind));
            self
        }

        /// Let every submission succeed again, whatever was scripted before.
        ///
        /// The index is a position in `submitted`, so a script left armed spans a
        /// `clear_submitted` and keeps refusing the next cycle's first leg.
        fn heal_creates(&self) -> &Self {
            *self.create_failure.lock().expect("gateway mutex") = None;
            self
        }

        /// Every resting order the venue is currently holding.
        fn resting_orders(&self) -> Vec<trading::Order> {
            self.open_orders.lock().expect("gateway mutex").clone()
        }

        fn submitted(&self) -> Vec<trading::CreateOrderRequest> {
            self.submitted.lock().expect("gateway mutex").clone()
        }

        /// Forget the recorded submissions so the next poll can be read alone.
        fn clear_submitted(&self) {
            self.submitted.lock().expect("gateway mutex").clear();
        }

        /// How many times the open-order set was read.
        fn open_reads(&self) -> usize {
            self.open_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl TradingGateway for ScriptedGateway {
        /// A venue that remembers what it accepted: every accepted order joins
        /// the resting set, so the *next* poll sees it. Without that echo a
        /// second cycle could never observe a resting order at all.
        async fn create_order(
            &self,
            req: trading::CreateOrderRequest,
        ) -> std::result::Result<trading::Order, PortError> {
            let index = {
                let mut submitted = self.submitted.lock().expect("gateway mutex");
                submitted.push(req.clone());
                submitted.len() - 1
            };
            if let Some((from, kind)) = *self.create_failure.lock().expect("gateway mutex") &&
                index >= from
            {
                return Err(scripted_error(kind));
            }
            let inner = req.order.as_option().cloned().unwrap_or_default();
            let accepted = trading::Order {
                id: format!("placed-{}", inner.client_order_id),
                client_order_id: inner.client_order_id,
                symbol: inner.symbol,
                r#type: inner.r#type,
                side: inner.side,
                price: inner.price,
                ..Default::default()
            };
            self.open_orders.lock().expect("gateway mutex").push(accepted.clone());
            Ok(accepted)
        }

        async fn batch_create_orders(
            &self,
            _req: trading::CreateOrdersRequest,
        ) -> std::result::Result<Vec<trading::Order>, PortError> {
            Err(PortError::Unsupported("batch_create_orders".into()))
        }

        async fn cancel_order(
            &self,
            _req: trading::CancelOrderRequest,
        ) -> std::result::Result<trading::Order, PortError> {
            Err(PortError::Unsupported("cancel_order".into()))
        }

        async fn cancel_all_orders(
            &self,
            _req: trading::CancelAllOrdersRequest,
        ) -> std::result::Result<Vec<trading::Order>, PortError> {
            Err(PortError::Unsupported("cancel_all_orders".into()))
        }

        async fn fetch_open_orders(
            &self,
            _req: trading::FetchOpenOrdersRequest,
        ) -> std::result::Result<Vec<trading::Order>, PortError> {
            self.open_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(kind) = *self.open_failure.lock().expect("gateway mutex") {
                return Err(scripted_error(kind));
            }
            Ok(self.open_orders.lock().expect("gateway mutex").clone())
        }

        async fn get_account(
            &self,
            _req: trading::GetAccountRequest,
        ) -> std::result::Result<trading::GetAccountResponse, PortError> {
            Err(PortError::Unsupported("get_account".into()))
        }

        async fn get_positions(
            &self,
            _req: trading::GetPositionsRequest,
        ) -> std::result::Result<trading::GetPositionsResponse, PortError> {
            Err(PortError::Unsupported("get_positions".into()))
        }

        async fn get_order_history(
            &self,
            _req: trading::GetOrderHistoryRequest,
        ) -> std::result::Result<trading::GetOrderHistoryResponse, PortError> {
            Err(PortError::Unsupported("get_order_history".into()))
        }

        async fn get_closed_positions(
            &self,
            _req: trading::GetClosedPositionsRequest,
        ) -> std::result::Result<trading::GetClosedPositionsResponse, PortError> {
            Err(PortError::Unsupported("get_closed_positions".into()))
        }

        async fn close_position(
            &self,
            _req: trading::ClosePositionRequest,
        ) -> std::result::Result<trading::ClosePositionResponse, PortError> {
            Err(PortError::Unsupported("close_position".into()))
        }

        async fn close_all_positions(
            &self,
            _req: trading::CloseAllPositionsRequest,
        ) -> std::result::Result<trading::CloseAllPositionsResponse, PortError> {
            Err(PortError::Unsupported("close_all_positions".into()))
        }

        async fn modify_position(
            &self,
            _req: trading::ModifyPositionRequest,
        ) -> std::result::Result<trading::ModifyPositionResponse, PortError> {
            Err(PortError::Unsupported("modify_position".into()))
        }

        async fn sync_state(
            &self,
            _exchange_id: &common::ExchangeId,
        ) -> std::result::Result<worker::ReconcileStateResponse, PortError> {
            Ok(worker::ReconcileStateResponse::default())
        }
    }

    /// A ticker source with a scripted answer; `None` is a dead feed.
    struct TickerStub(Option<market::Ticker>);

    impl TickerStub {
        fn of(ticker: market::Ticker) -> Self {
            Self(Some(ticker))
        }

        fn down() -> Self {
            Self(None)
        }
    }

    #[async_trait]
    impl MarketDataSource for TickerStub {
        async fn fetch_ticker(
            &self,
            req: market::FetchTickerRequest,
        ) -> std::result::Result<market::Ticker, PortError> {
            self.0
                .clone()
                .ok_or_else(|| PortError::Transport("ticker feed down".into()))
                .map(|ticker| market::Ticker { symbol: req.symbol, ..ticker })
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> std::result::Result<market::OrderBook, PortError> {
            Err(PortError::Unsupported("fetch_order_book".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> std::result::Result<market::GetCandlesResponse, PortError> {
            Err(PortError::Unsupported("get_candles".into()))
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> std::result::Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Unsupported("list_symbols".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> std::result::Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Unsupported("search_symbols".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> std::result::Result<market::ListTickersResponse, PortError> {
            Err(PortError::Unsupported("list_tickers".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: crate::ports::OverflowPolicy,
        ) -> std::result::Result<crate::ports::MarketEventStream, PortError> {
            Err(PortError::Unsupported("subscribe_market_data".into()))
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn grid_with(
        config: HedgeGridConfig,
        source: Arc<dyn MarketDataSource>,
    ) -> (Arc<ScriptedGateway>, HedgeGrid) {
        let gateway = Arc::new(ScriptedGateway::default());
        let grid = HedgeGrid::new(config, gateway.clone(), source);
        (gateway, grid)
    }

    /// One polling cycle on a throwaway runtime (`proptest!` cases are sync).
    fn tick_once(
        config: HedgeGridConfig,
        source: Arc<dyn MarketDataSource>,
    ) -> (Arc<ScriptedGateway>, Result<()>) {
        let (gateway, grid) = grid_with(config, source);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        let outcome = runtime.block_on(grid.tick());
        (gateway, outcome)
    }

    /// One resting limit order as `fetch_open_orders` would report it.
    fn resting(side: trading::OrderSide, price: Decimal) -> trading::Order {
        trading::Order {
            id: format!("resting-{side:?}-{price}"),
            symbol: SYMBOL.into(),
            r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
            side: buffa::EnumValue::Known(side),
            status: buffa::EnumValue::Known(trading::OrderStatus::Open),
            price: buffa::MessageField::some(decimal_to_common(price)),
            ..Default::default()
        }
    }

    /// `(price, is_buy)` of every submitted grid leg, in submission order.
    fn legs(requests: &[trading::CreateOrderRequest]) -> Vec<(Decimal, bool)> {
        requests
            .iter()
            .filter_map(|req: &trading::CreateOrderRequest| -> Option<(Decimal, bool)> {
                let inner = req.order.as_option()?;
                if inner.r#type != buffa::EnumValue::Known(trading::OrderType::Limit) {
                    return None;
                }
                let price =
                    longtrader_contract::ext::common_to_decimal(inner.price.as_option()?).ok()?;
                Some((price, inner.side == buffa::EnumValue::Known(trading::OrderSide::Buy)))
            })
            .collect()
    }

    /// The venue of every submitted request, in submission order.
    fn venues(requests: &[trading::CreateOrderRequest]) -> Vec<String> {
        requests
            .iter()
            .map(|req| req.exchange_id.as_option().map_or_else(String::new, |id| id.id.clone()))
            .collect()
    }

    /// Whether a submitted request is a hedge leg, which is a market order.
    fn is_hedge_leg(req: &trading::CreateOrderRequest) -> bool {
        req.order
            .as_option()
            .is_some_and(|o| o.r#type == buffa::EnumValue::Known(trading::OrderType::Market))
    }

    /// The single market order — the hedge leg — if one was submitted.
    fn hedge_leg(requests: &[trading::CreateOrderRequest]) -> Option<&trading::CreateOrderRequest> {
        requests.iter().find(|req| is_hedge_leg(req))
    }

    /// The quantity an order asked for. Takes the wire field rather than a whole
    /// order so it works for both a submitted `OrderRequest` and a venue `Order`.
    fn amount_of(amount: Option<&common::Decimal>) -> Decimal {
        let raw = amount.expect("every request carries an amount");
        longtrader_contract::ext::common_to_decimal(raw).expect("a representable amount")
    }

    // -----------------------------------------------------------------------
    // The zero-level guard
    // -----------------------------------------------------------------------

    /// `num_levels == 0` would divide by zero inside `rust_decimal`, which
    /// panics. The guard turns that into a no-op, and it runs before the
    /// open-order read so a misconfigured grid costs the venue nothing.
    #[tokio::test]
    async fn a_zero_level_count_is_a_no_op_instead_of_a_divide_by_zero() {
        let mut config = config();
        config.num_levels = 0;
        let (gateway, grid) = grid_with(config, mock_market(dec!(100)));
        grid.tick().await.expect("a zero level count is not an error");
        assert!(gateway.submitted().is_empty(), "the guard must place no order");
        assert_eq!(gateway.open_reads(), 0, "the guard must run before any venue read");
    }

    /// An inverted span gives a negative step, which would walk the seeding loop
    /// away from the band forever. The `step <= 0` guard rejects it.
    #[tokio::test]
    async fn an_inverted_span_is_a_no_op() {
        let mut config = config();
        config.lower_price = dec!(110);
        config.upper_price = dec!(90);
        let (gateway, grid) = grid_with(config, mock_market(dec!(100)));
        grid.tick().await.expect("an inverted span is not an error");
        assert!(gateway.submitted().is_empty(), "an inverted band must place no order");
        assert_eq!(gateway.open_reads(), 0);
    }

    /// A collapsed span has a zero step: no ladder, and no infinite
    /// `price += 0` walk either.
    #[tokio::test]
    async fn a_collapsed_span_is_a_no_op() {
        let mut config = config();
        config.lower_price = dec!(100);
        config.upper_price = dec!(100);
        let (gateway, grid) = grid_with(config, mock_market(dec!(100)));
        grid.tick().await.expect("a collapsed span is not an error");
        assert!(gateway.submitted().is_empty(), "a zero-width band must place no order");
        assert_eq!(gateway.open_reads(), 0);
    }

    /// The `num_levels` guard is checked before the step arithmetic, so a
    /// config that is *both* degenerate takes the cheap path.
    #[tokio::test]
    async fn the_level_count_is_checked_before_the_step_is_computed() {
        let mut config = config();
        config.num_levels = 0;
        config.lower_price = dec!(110);
        config.upper_price = dec!(90);
        let (gateway, grid) = grid_with(config, mock_market(dec!(100)));
        grid.tick().await.expect("degenerate configs are not errors");
        assert!(gateway.submitted().is_empty());
        assert_eq!(gateway.open_reads(), 0);
    }

    // -----------------------------------------------------------------------
    // Seeding
    // -----------------------------------------------------------------------

    /// One leg per level, on the primary venue, and `is_buy = price < mid`.
    /// `90..110` in 4 steps is 5 inclusive price points.
    #[tokio::test]
    async fn the_first_tick_seeds_one_leg_per_level_below_or_above_the_mid() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        let submitted = gateway.submitted();
        assert_eq!(
            legs(&submitted),
            vec![
                (dec!(90), true),
                (dec!(95), true),
                (dec!(100), false),
                (dec!(105), false),
                (dec!(110), false),
            ],
            "a level below the mid buys and every other level sells"
        );
        assert_eq!(venues(&submitted), vec!["primary".to_string(); 5], "the grid rests on primary");
        assert_eq!(hedge_leg(&submitted), None, "nothing has left the book yet");
    }

    /// A level that already has a resting order at that price must not be
    /// seeded again. `90..100` in 5 steps is 6 levels; the second poll finds all
    /// six still resting, so it places nothing at all.
    #[tokio::test]
    async fn a_level_that_already_has_a_resting_order_is_not_seeded_again() {
        let mut config = config();
        config.upper_price = dec!(100);
        config.num_levels = 5;
        let (gateway, grid) = grid_with(config, mock_market(dec!(95)));

        grid.tick().await.expect("first poll");
        let first = legs(&gateway.submitted());
        assert_eq!(first.len(), 6, "the first poll seeds the whole ladder");
        assert_eq!(gateway.open_reads(), 1);

        gateway.clear_submitted();
        grid.tick().await.expect("second poll");
        assert_eq!(legs(&gateway.submitted()), Vec::new(), "every level already rests an order");
        assert_eq!(hedge_leg(&gateway.submitted()), None, "a resting ladder has no fill to hedge");
        assert_eq!(gateway.open_reads(), 2);
    }

    /// The matching side of the same rule: only the *price* of a resting order
    /// suppresses a leg, and a book resting entirely off the ladder seeds all of
    /// it.
    #[tokio::test]
    async fn a_resting_order_at_an_off_ladder_price_suppresses_nothing() {
        let gateway = Arc::new(ScriptedGateway::default());
        gateway.resting(vec![
            resting(trading::OrderSide::Buy, dec!(1)),
            resting(trading::OrderSide::Sell, dec!(2)),
        ]);
        let grid = HedgeGrid::new(config(), gateway.clone(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        assert_eq!(legs(&gateway.submitted()).len(), 5, "no ladder level is covered");
    }

    // -----------------------------------------------------------------------
    // The fill hedge
    // -----------------------------------------------------------------------

    /// A bid that left the book means the grid bought: the position is long, so
    /// the hedge leg SELLS on the hedge venue. `95` is a ladder buy under the
    /// default `90..110` band around a mid of 100.
    #[tokio::test]
    async fn a_bid_that_left_the_book_sells_the_hedge_on_the_hedge_venue() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        gateway.clear_submitted();
        gateway.filled(dec!(95));

        grid.tick().await.expect("hedged");
        let submitted = gateway.submitted();
        let hedge = hedge_leg(&submitted).expect("a filled level must be hedged");
        assert_eq!(hedge.exchange_id.as_option().map(|id| id.id.as_str()), Some("hedge"));
        let order = hedge.order.as_option().expect("a request always carries an order");
        assert_eq!(order.side, buffa::EnumValue::Known(trading::OrderSide::Sell));
        assert!(order.price.as_option().is_none(), "a hedge is a market order");
        assert_eq!(order.symbol, SYMBOL, "both venues trade the same symbol");
        assert_eq!(amount_of(order.amount.as_option()), dec!(0.1), "covers exactly one level");
    }

    /// An ask that left the book means the grid sold: the position is short, so
    /// the hedge leg BUYS on the hedge venue.
    #[tokio::test]
    async fn an_ask_that_left_the_book_buys_the_hedge_on_the_hedge_venue() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        gateway.clear_submitted();
        gateway.filled(dec!(105));

        grid.tick().await.expect("hedged");
        let submitted = gateway.submitted();
        let hedge = hedge_leg(&submitted).expect("a filled level must be hedged");
        let order = hedge.order.as_option().expect("a request always carries an order");
        assert_eq!(order.side, buffa::EnumValue::Known(trading::OrderSide::Buy));
    }

    /// A level the venue still shows is not a fill, so a fully resting ladder
    /// places neither a hedge nor a fresh leg — whatever the bid/ask split is.
    #[tokio::test]
    async fn a_ladder_that_is_still_fully_resting_places_no_hedge_order() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        gateway.clear_submitted();

        grid.tick().await.expect("second poll");
        assert_eq!(hedge_leg(&gateway.submitted()), None, "nothing left the book");
        assert_eq!(legs(&gateway.submitted()), Vec::new(), "and nothing was re-seeded");
    }

    /// Each vanished level is hedged on its own, because each one is a separate
    /// fill: two bids leaving the book are two longs to neutralise.
    #[tokio::test]
    async fn every_level_that_left_the_book_is_hedged_separately() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        gateway.clear_submitted();
        gateway.filled(dec!(90));
        gateway.filled(dec!(95));

        grid.tick().await.expect("hedged");
        let submitted = gateway.submitted();
        let hedges: Vec<&trading::CreateOrderRequest> =
            submitted.iter().filter(|req| is_hedge_leg(req)).collect();
        assert_eq!(hedges.len(), 2, "two fills are two hedges");
        for hedge in &hedges {
            assert_eq!(hedge.exchange_id.as_option().map(|id| id.id.as_str()), Some("hedge"));
        }
    }

    /// The seeded ladder is itself lopsided — `90..110` around a mid of 100 rests
    /// two bids against three asks — so a count-based hedge would fire on the
    /// second poll with nothing having filled. Pinned so the strategy can never
    /// hedge the imbalance it created itself.
    #[tokio::test]
    async fn the_grid_does_not_hedge_the_imbalance_it_seeded_itself() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("first poll");
        gateway.clear_submitted();
        grid.tick().await.expect("second poll");
        assert_eq!(
            hedge_leg(&gateway.submitted()),
            None,
            "2 resting bids vs 3 resting asks is the ladder's own shape, not a fill"
        );
    }

    // -----------------------------------------------------------------------
    // The mid
    // -----------------------------------------------------------------------

    /// An absent mid has no side to compare a level against, so the cycle
    /// returns before it reads the book or places anything.
    #[tokio::test]
    async fn a_zero_mid_returns_before_any_venue_work() {
        let (gateway, grid) = grid_with(config(), mock_market(Decimal::ZERO));
        grid.tick().await.expect("a flat ticker is not an error");
        assert!(gateway.submitted().is_empty(), "there is no side to take without a mid");
        assert_eq!(gateway.open_reads(), 0);
    }

    /// A ticker with no `last` decodes to a zero mid rather than to a usable
    /// price, so it takes the same early return.
    #[tokio::test]
    async fn a_ticker_without_a_last_price_is_read_as_a_zero_mid() {
        let blank = market::Ticker::default();
        let (gateway, grid) = grid_with(config(), Arc::new(TickerStub::of(blank)));
        grid.tick().await.expect("no last price is not an error");
        assert!(gateway.submitted().is_empty());
        assert_eq!(gateway.open_reads(), 0);
    }

    /// The guard is `mid > 0`, not merely "not zero". A negative mid makes
    /// `is_buy = price < mid` false for every level of a band above zero, so
    /// accepting one would place an all-sell ladder — marketable orders at
    /// prices the strategy believes are far below the market.
    #[tokio::test]
    async fn a_negative_mid_is_rejected_before_any_venue_work() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(-5)));
        grid.tick().await.expect("a negative ticker is not an error");
        assert!(gateway.submitted().is_empty(), "no level may be priced off a negative mid");
        assert_eq!(gateway.open_reads(), 0, "the guard runs before any venue read");
    }

    // -----------------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------------

    /// An undecodable `last` aborts the cycle: falling through to "no price"
    /// would silently skip the grid instead of reporting the broken feed.
    #[tokio::test]
    async fn an_undecodable_last_price_propagates_before_any_order_work() {
        let ticker = market::Ticker {
            last: buffa::MessageField::some(common::Decimal {
                value: "not-a-number".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (gateway, grid) = grid_with(config(), Arc::new(TickerStub::of(ticker)));
        let err = grid.tick().await.expect_err("an undecodable price must not read as flat");
        // The failure echoes the payload, so the broken feed is identifiable.
        assert!(err.to_string().contains("not-a-number"), "{err}");
        assert!(gateway.submitted().is_empty());
        assert_eq!(gateway.open_reads(), 0);
    }

    #[tokio::test]
    async fn a_ticker_failure_propagates_and_places_no_order() {
        let (gateway, grid) = grid_with(config(), Arc::new(TickerStub::down()));
        let err = grid.tick().await.expect_err("a dead ticker feed must not read as a flat tick");
        assert!(err.to_string().contains("ticker feed down"), "{err}");
        assert!(gateway.submitted().is_empty());
        assert_eq!(gateway.open_reads(), 0);
    }

    /// Every `PortError` the open-order read can produce surfaces, and nothing
    /// is placed on the way out: seeding only runs once the book is known.
    #[tokio::test]
    async fn every_fetch_open_orders_failure_propagates_and_places_no_order() {
        for kind in [
            Scripted::Transport,
            Scripted::Rpc,
            Scripted::Unsupported,
            Scripted::MissingField,
            Scripted::InvalidArgument,
            Scripted::NotFound,
            Scripted::Decimal,
        ] {
            let gateway = Arc::new(ScriptedGateway::default());
            gateway.fail_open(kind);
            let grid = HedgeGrid::new(config(), gateway.clone(), mock_market(dec!(100)));
            let err = grid.tick().await.expect_err("an unreadable open-order set must surface");
            let expected = scripted_error(kind).to_string();
            assert!(err.to_string().contains(&expected), "{kind:?} surfaced as {err}");
            assert!(gateway.submitted().is_empty(), "{kind:?} must place no order");
        }
    }

    /// The hedge leg is placed before the seeding pass, so it is submission
    /// zero on a poll that also has a level to re-quote. A venue that rejects it
    /// has to reach the caller rather than be logged and dropped.
    #[tokio::test]
    async fn a_failed_hedge_placement_propagates() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        grid.tick().await.expect("seeded");
        gateway.clear_submitted();
        gateway.filled(dec!(95));
        gateway.fail_create_from(0, Scripted::Rpc);

        let err = grid.tick().await.expect_err("a rejected hedge must surface");
        let expected = scripted_error(Scripted::Rpc).to_string();
        assert!(err.to_string().contains(&expected), "{err}");
        assert_eq!(gateway.submitted().len(), 1, "the cycle stops at the refused hedge");
    }

    /// A refused grid leg ends the cycle instead of being logged and dropped: the
    /// caller of a successful cycle could not otherwise tell a partly seeded grid
    /// from a full one. The next poll re-seeds whatever the venue is missing.
    #[tokio::test]
    async fn a_rejected_grid_leg_propagates_and_stops_the_cycle() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        gateway.fail_create(Scripted::Transport);

        let err = grid.tick().await.expect_err("a rejected leg must not read as a full grid");
        let expected = scripted_error(Scripted::Transport).to_string();
        assert!(err.to_string().contains(&expected), "{err}");
        assert_eq!(gateway.submitted().len(), 1, "the cycle stops at the first refusal");
    }

    /// A leg the venue refuses must not stop the *next* poll from repairing the
    /// ladder: the recovery is the open-order read, not the error.
    #[tokio::test]
    async fn a_refused_leg_is_re_seeded_by_the_next_poll() {
        let (gateway, grid) = grid_with(config(), mock_market(dec!(100)));
        gateway.fail_create_from(1, Scripted::Transport);

        let err = grid.tick().await.expect_err("the first refused leg ends the cycle");
        assert!(err.to_string().contains("venue"), "the refusal must reach the caller, got {err}");
        // `submitted` records *attempts*, refusals included, so two legs were
        // tried; only the first reached the venue.
        assert_eq!(
            legs(&gateway.submitted()).len(),
            2,
            "two legs were attempted before the first refusal"
        );
        assert_eq!(gateway.resting_orders().len(), 1, "only the accepted leg is live on the venue");
        gateway.clear_submitted();
        gateway.heal_creates();
        grid.tick().await.expect("the venue is healthy again");
        assert_eq!(
            legs(&gateway.submitted()).len(),
            4,
            "the four levels that never landed are re-seeded"
        );
        assert_eq!(hedge_leg(&gateway.submitted()), None, "repairing is not hedging");
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// A positive ladder seeds every rung exactly once, both bounds inclusive, and
        /// every rung lands inside the configured band.
        #[test]
        fn a_positive_ladder_seeds_every_rung_once_inside_the_band(
            lower in 1i64..1_000_000,
            step in 1i64..1_000,
            levels in 1u32..=8,
        ) {
            let mid = Decimal::from(lower);
            let (gateway, outcome) = tick_once(ladder(lower, step, levels), mock_market(mid));
            outcome.expect("a positive step must seed");
            let lower = Decimal::from(lower);
            let upper = lower + Decimal::from(i64::from(levels) * step);
            let prices: Vec<Decimal> =
                legs(&gateway.submitted()).into_iter().map(|(price, _)| price).collect();
            prop_assert_eq!(prices.len(), usize::try_from(levels).expect("small") + 1);
            prop_assert_eq!(prices.first().copied(), Some(lower));
            prop_assert_eq!(prices.last().copied(), Some(upper));
            for price in &prices {
                prop_assert!(*price >= lower, "{} sits below the band {}", price, lower);
                prop_assert!(*price <= upper, "{} sits above the band {}", price, upper);
            }
            prop_assert!(
                prices.windows(2).all(|w| (w[1] - w[0]) == Decimal::from(step)),
                "every rung must be exactly one step above the previous: {:?}",
                prices
            );
        }

        /// A span that is zero or negative is "nothing to do" whatever the level
        /// count: no leg, and not even a read of the open-order set.
        #[test]
        fn a_non_positive_span_is_always_a_no_op(
            lower in 1i64..1_000_000,
            delta in -1_000i64..1,
            levels in 1u32..=8,
        ) {
            let lower = Decimal::from(lower);
            let band = config_in_band(lower, lower + Decimal::from(delta), levels);
            let (gateway, outcome) = tick_once(band, mock_market(lower));
            outcome.expect("a degenerate span is not an error");
            prop_assert!(gateway.submitted().is_empty(), "a degenerate span places no order");
            prop_assert_eq!(gateway.open_reads(), 0);
        }

        /// `is_buy = price < mid` and nothing else: a mid above the whole band
        /// buys every rung, a mid below it sells every rung.
        #[test]
        fn the_side_of_every_leg_follows_the_mid_alone(
            lower in 1i64..1_000_000,
            step in 1i64..1_000,
            levels in 1u32..=8,
            offset in 1i64..1_000,
        ) {
            let lower = Decimal::from(lower);
            let upper = lower + Decimal::from(i64::from(levels) * step);
            let offset = Decimal::from(offset);
            // A non-positive mid short-circuits before any side is taken, so
            // that case belongs to the guard, not to the side rule.
            prop_assume!(lower > offset, "a non-positive mid returns before the ladder");
            let band = || config_in_band(lower, upper, levels);

            let (buying, outcome) = tick_once(band(), mock_market(upper + offset));
            outcome.expect("seeded");
            let bought = legs(&buying.submitted());
            prop_assert!(!bought.is_empty(), "a positive span always has a first rung");
            prop_assert!(
                bought.iter().all(|(_, is_buy)| *is_buy),
                "a mid above the band must buy every rung"
            );

            let (selling, outcome) = tick_once(band(), mock_market(lower - offset));
            outcome.expect("seeded");
            let sold = legs(&selling.submitted());
            prop_assert!(!sold.is_empty(), "a positive span always has a first rung");
            prop_assert!(
                sold.iter().all(|(_, is_buy)| !*is_buy),
                "a mid below the band must sell every rung"
            );
        }
    }
}
