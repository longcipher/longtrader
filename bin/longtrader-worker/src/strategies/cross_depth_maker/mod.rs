//! Cross-venue depth-based maker: fair value from the hedge venue's order
//! book mid; quotes a bid/ask pair on the primary venue each cycle.
//!
//! `spread` is a **fraction of the mid**, not a price offset: the bid is
//! `mid * (1 - spread)` and the ask is `mid * (1 + spread)`. A mid of 100 with
//! `spread = "0.002"` therefore quotes 99.8 / 100.2 — a 0.2 price distance, not
//! a 0.002 one. The quoted distance scales with the price, so the same config
//! means something different on a 10 000 instrument.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order},
};

/// Config for the cross depth maker.
#[derive(Debug, Clone, Deserialize)]
pub struct CrossDepthMakerConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Half-spread as a fraction of the mid: the bid sits at
    /// `mid * (1 - spread)` and the ask at `mid * (1 + spread)`.
    #[serde(default = "default_spread")]
    pub spread: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_spread() -> Decimal {
    Decimal::new(2, 3)
}

/// Cross depth maker strategy.
pub struct CrossDepthMaker {
    config: CrossDepthMakerConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl CrossDepthMaker {
    pub fn new(
        config: CrossDepthMakerConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// Hedge-venue book mid: the mean of the **first level of each side exactly
    /// as the venue returned it**.
    ///
    /// No sorting and no depth walk is performed, so the caller must supply a
    /// venue that returns bids best-first and asks best-first. The level-1
    /// request below is what a venue is documented to answer that way, but a
    /// venue that returns its levels the other way round would have its deepest
    /// level mistaken for the best one.
    pub async fn hedge_mid(&self) -> Result<Decimal> {
        let hedge = self.config.venues.hedge.proto();
        let book = self
            .market
            .fetch_order_book(crate::proto::market::FetchOrderBookRequest {
                exchange_id: buffa::MessageField::some(hedge),
                symbol: self.config.venues.symbol.clone(),
                pagination: crate::proto::common::Pagination { limit: 1, ..Default::default() }
                    .into(),
                ..Default::default()
            })
            .await?;
        let bid = book
            .bids
            .first()
            .and_then(|l| l.price.as_option())
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?;
        let ask = book
            .asks
            .first()
            .and_then(|l| l.price.as_option())
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?;
        match (bid, ask) {
            (Some(b), Some(a)) if b > Decimal::ZERO && a > Decimal::ZERO => {
                Ok((b + a) / Decimal::from(2))
            }
            _ => color_eyre::eyre::bail!("hedge book lacks both sides"),
        }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let primary = self.config.venues.primary.proto();
        let symbol = self.config.venues.symbol.clone();
        let fair = self.hedge_mid().await?;
        for (price, is_buy) in [
            (fair * (Decimal::ONE - self.config.spread), true),
            (fair * (Decimal::ONE + self.config.spread), false),
        ] {
            let coid = format!("xdm-{}", ulid::Ulid::generate());
            let request = limit_order(&primary, &symbol, coid, is_buy, price, self.config.qty);
            // A refused leg ends the cycle rather than being logged and dropped.
            // The two legs exist as a pair: quoting one side alone leaves a naked
            // quote whose inventory the other leg was there to bound, so
            // placing it anyway would widen the exposure instead of reducing it.
            self.gateway.create_order(request).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for CrossDepthMaker {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "cross-depth-maker tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use longtrader_contract::ext::decimal_to_common;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        ports::PortError,
        proto::{common, market, trading, worker},
        strategies::VenueRef,
    };

    const SYMBOL: &str = "BTC/USDT";

    fn config() -> CrossDepthMakerConfig {
        CrossDepthMakerConfig {
            venues: CrossVenueParams {
                primary: VenueRef { exchange_id: "primary".into(), label: String::new() },
                hedge: VenueRef { exchange_id: "hedge".into(), label: String::new() },
                symbol: SYMBOL.into(),
                poll_secs: 30,
            },
            spread: dec!(0.002),
            qty: dec!(0.001),
        }
    }

    // -----------------------------------------------------------------------
    // Stubs
    // -----------------------------------------------------------------------

    /// A gateway that records every quote; `reject` makes each one fail.
    #[derive(Default)]
    struct RecordingGateway {
        submitted: Mutex<Vec<trading::CreateOrderRequest>>,
        reject: AtomicBool,
    }

    impl RecordingGateway {
        /// Every subsequent quote is refused by the venue.
        fn rejecting(&self) -> &Self {
            self.reject.store(true, Ordering::Relaxed);
            self
        }

        fn submitted(&self) -> Vec<trading::CreateOrderRequest> {
            self.submitted.lock().expect("gateway mutex").clone()
        }
    }

    #[async_trait]
    impl TradingGateway for RecordingGateway {
        async fn create_order(
            &self,
            req: trading::CreateOrderRequest,
        ) -> std::result::Result<trading::Order, PortError> {
            self.submitted.lock().expect("gateway mutex").push(req);
            if self.reject.load(Ordering::Relaxed) {
                return Err(PortError::Transport("venue rejected the quote".into()));
            }
            Ok(trading::Order::default())
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
            Err(PortError::Unsupported("fetch_open_orders".into()))
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

    /// An order-book source with a scripted answer; `None` is a dead feed.
    ///
    /// Every request is recorded, so the venue and depth a caller asked for are
    /// observable — neither is visible from the answer alone.
    #[derive(Default)]
    struct BookStub {
        answer: Option<market::OrderBook>,
        requests: Mutex<Vec<market::FetchOrderBookRequest>>,
    }

    impl BookStub {
        fn of(book: market::OrderBook) -> Self {
            Self { answer: Some(book), requests: Mutex::new(Vec::new()) }
        }

        fn down() -> Self {
            Self { answer: None, requests: Mutex::new(Vec::new()) }
        }

        fn requests(&self) -> Vec<market::FetchOrderBookRequest> {
            self.requests.lock().expect("book mutex").clone()
        }
    }

    #[async_trait]
    impl MarketDataSource for BookStub {
        async fn fetch_order_book(
            &self,
            req: market::FetchOrderBookRequest,
        ) -> std::result::Result<market::OrderBook, PortError> {
            self.requests.lock().expect("book mutex").push(req.clone());
            self.answer
                .clone()
                .ok_or_else(|| PortError::Transport("book feed down".into()))
                .map(|book| market::OrderBook { symbol: req.symbol, ..book })
        }

        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> std::result::Result<market::Ticker, PortError> {
            Err(PortError::Unsupported("fetch_ticker".into()))
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

    /// A maker quoting against `book` with the default config.
    fn maker_with(book: market::OrderBook) -> (Arc<RecordingGateway>, CrossDepthMaker) {
        maker_against(config(), Arc::new(BookStub::of(book)))
    }

    /// A maker quoting `source` under `config`.
    ///
    /// The config is a parameter rather than a call to [`config`] so a test can
    /// vary the spread: threading it through here is what makes
    /// `tick_once(config, book)` mean what its signature says.
    fn maker_against(
        config: CrossDepthMakerConfig,
        source: Arc<dyn MarketDataSource>,
    ) -> (Arc<RecordingGateway>, CrossDepthMaker) {
        let gateway = Arc::new(RecordingGateway::default());
        let maker = CrossDepthMaker::new(config, gateway.clone(), source);
        (gateway, maker)
    }

    /// One polling cycle on a throwaway runtime (`proptest!` cases are sync).
    fn tick_once(
        config: CrossDepthMakerConfig,
        book: market::OrderBook,
    ) -> (Arc<RecordingGateway>, Result<()>) {
        let (gateway, maker) = maker_against(config, Arc::new(BookStub::of(book)));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        let outcome = runtime.block_on(maker.tick());
        (gateway, outcome)
    }

    /// One price level; `None` leaves the price unset.
    fn level(price: Option<Decimal>) -> market::PriceLevel {
        market::PriceLevel {
            price: price.map(decimal_to_common).map_or_default(buffa::MessageField::some),
            amount: buffa::MessageField::some(decimal_to_common(Decimal::ONE)),
            ..Default::default()
        }
    }

    /// A maker reading `book` through a recording stub, so the request it sends
    /// is observable as well as its answer.
    fn recording_maker(
        book: market::OrderBook,
    ) -> (Arc<BookStub>, Arc<RecordingGateway>, CrossDepthMaker) {
        let stub = Arc::new(BookStub::of(book));
        let source: Arc<dyn MarketDataSource> = stub.clone();
        let gateway = Arc::new(RecordingGateway::default());
        let maker = CrossDepthMaker::new(config(), gateway.clone(), source);
        (stub, gateway, maker)
    }

    /// A level whose price the decoder must refuse.
    ///
    /// `Decimal` carries one base-10 string, so there is no numeric pair to be
    /// out of range — the payload itself is outside the grammar.
    fn broken_level() -> market::PriceLevel {
        market::PriceLevel {
            price: buffa::MessageField::some(common::Decimal {
                value: "not-a-number".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn book(bids: Vec<market::PriceLevel>, asks: Vec<market::PriceLevel>) -> market::OrderBook {
        market::OrderBook { symbol: SYMBOL.into(), bids, asks, ..Default::default() }
    }

    /// `(is_buy, price)` of every submitted limit order, in submission order.
    fn quotes(requests: &[trading::CreateOrderRequest]) -> Vec<(bool, Decimal)> {
        requests
            .iter()
            .filter_map(|req: &trading::CreateOrderRequest| -> Option<(bool, Decimal)> {
                let inner = req.order.as_option()?;
                let price =
                    longtrader_contract::ext::common_to_decimal(inner.price.as_option()?).ok()?;
                Some((inner.side == buffa::EnumValue::Known(trading::OrderSide::Buy), price))
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // hedge_mid
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn the_mid_is_the_average_of_the_first_level_of_each_side() {
        let (_gateway, maker) =
            maker_with(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        assert_eq!(maker.hedge_mid().await.expect("a two-sided book has a mid"), dec!(100));
    }

    /// Fair value must come from the hedge venue at level one, for the configured
    /// symbol — a level-1 read of the *quoting* venue's own book would be the
    /// strategy marking its own inventory.
    #[tokio::test]
    async fn fair_value_is_read_from_the_hedge_venue_at_level_one() {
        let (stub, _gateway, maker) =
            recording_maker(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        maker.hedge_mid().await.expect("a two-sided book has a mid");

        let seen = stub.requests();
        assert_eq!(seen.len(), 1, "one book read per mid");
        let req = seen.first().expect("one book read");
        assert_eq!(
            req.exchange_id.as_option().map(|id| id.id.as_str()),
            Some("hedge"),
            "fair value must come from the hedge venue, not the quoting one"
        );
        assert_eq!(req.symbol, SYMBOL);
        assert_eq!(req.pagination.as_option().map(|p| p.limit), Some(1), "level-1 only");
    }

    /// A missing bid side has no mid: fair value from one side only would be
    /// the venue's own price, not a mid.
    #[tokio::test]
    async fn a_book_without_a_bid_side_has_no_mid() {
        let (_gateway, maker) = maker_with(book(Vec::new(), vec![level(Some(dec!(101)))]));
        let err = maker.hedge_mid().await.expect_err("a one-sided book has no mid");
        assert!(err.to_string().contains("lacks both sides"), "{err}");
    }

    /// Symmetrically, a missing ask side has no mid either.
    #[tokio::test]
    async fn a_book_without_an_ask_side_has_no_mid() {
        let (_gateway, maker) = maker_with(book(vec![level(Some(dec!(99)))], Vec::new()));
        let err = maker.hedge_mid().await.expect_err("a one-sided book has no mid");
        assert!(err.to_string().contains("lacks both sides"), "{err}");
    }

    #[tokio::test]
    async fn an_entirely_empty_book_has_no_mid() {
        let (_gateway, maker) = maker_with(book(Vec::new(), Vec::new()));
        let err = maker.hedge_mid().await.expect_err("an empty book has no mid");
        assert!(err.to_string().contains("lacks both sides"), "{err}");
    }

    /// The guard is `> 0`, not merely "present": a zero or negative side is not
    /// a usable price, and averaging one against the other would quote around a
    /// number the venue never offered.
    #[tokio::test]
    async fn a_side_that_is_not_strictly_positive_has_no_mid() {
        for (bid, ask) in [
            (Some(dec!(0)), Some(dec!(101))),
            (Some(dec!(-1)), Some(dec!(101))),
            (Some(dec!(99)), Some(dec!(0))),
            (Some(dec!(99)), Some(dec!(-1))),
            (Some(dec!(0)), Some(dec!(0))),
            (Some(dec!(-1)), Some(dec!(-2))),
        ] {
            let (_gateway, maker) = maker_with(book(vec![level(bid)], vec![level(ask)]));
            let err = maker.hedge_mid().await.expect_err("a non-positive side has no mid");
            assert!(err.to_string().contains("lacks both sides"), "bid={bid:?} ask={ask:?}: {err}");
        }
    }

    /// A level with no price at all is treated exactly like a missing side.
    #[tokio::test]
    async fn a_level_without_a_price_counts_as_a_missing_side() {
        for (bids, asks) in [
            (vec![level(None)], vec![level(Some(dec!(101)))]),
            (vec![level(Some(dec!(99)))], vec![level(None)]),
        ] {
            let (_gateway, maker) = maker_with(book(bids, asks));
            let err = maker.hedge_mid().await.expect_err("an unpriced level is not a side");
            assert!(err.to_string().contains("lacks both sides"), "{err}");
        }
    }

    /// An undecodable price surfaces as an error rather than silently reading as
    /// a missing side — the two have very different causes.
    #[tokio::test]
    async fn an_undecodable_bid_price_propagates_instead_of_reading_as_missing() {
        let (_gateway, maker) =
            maker_with(book(vec![broken_level()], vec![level(Some(dec!(101)))]));
        let err = maker.hedge_mid().await.expect_err("an undecodable price must surface");
        // The failure echoes the payload that arrived, so the cause is visible.
        assert!(err.to_string().contains("not-a-number"), "{err}");
        assert!(!err.to_string().contains("lacks both sides"), "{err}");
    }

    #[tokio::test]
    async fn an_undecodable_ask_price_propagates_instead_of_reading_as_missing() {
        let (_gateway, maker) = maker_with(book(vec![level(Some(dec!(99)))], vec![broken_level()]));
        let err = maker.hedge_mid().await.expect_err("an undecodable price must surface");
        assert!(err.to_string().contains("not-a-number"), "{err}");
        assert!(!err.to_string().contains("lacks both sides"), "{err}");
    }

    /// A dead book feed propagates; it is not read as an empty book, which would
    /// read as "no mid available" and stop the cycle for the wrong reason.
    #[tokio::test]
    async fn a_book_failure_propagates() {
        let (_gateway, maker) = maker_against(config(), Arc::new(BookStub::down()));
        let err = maker.hedge_mid().await.expect_err("a dead book feed must surface");
        assert!(err.to_string().contains("book feed down"), "{err}");
    }

    /// Only the first element of each side is read, so the deeper levels cannot
    /// drift the mid — and nothing sorts them, which is the assumption the
    /// caller has to satisfy.
    #[tokio::test]
    async fn only_the_first_level_of_each_side_feeds_the_mid() {
        let (_gateway, maker) = maker_with(book(
            vec![level(Some(dec!(99))), level(Some(dec!(95)))],
            vec![level(Some(dec!(101))), level(Some(dec!(105)))],
        ));
        assert_eq!(maker.hedge_mid().await.expect("a two-sided book has a mid"), dec!(100));
    }

    // -----------------------------------------------------------------------
    // tick
    // -----------------------------------------------------------------------

    /// Both legs, on the primary venue, at `mid * (1 -/+ spread)`.
    #[tokio::test]
    async fn a_cycle_quotes_a_bid_below_and_an_ask_above_the_mid() {
        let (gateway, maker) =
            maker_with(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        maker.tick().await.expect("quoted");
        let submitted = gateway.submitted();
        assert_eq!(submitted.len(), 2, "exactly one bid and one ask per cycle");
        assert_eq!(
            quotes(&submitted),
            vec![(true, dec!(99.8)), (false, dec!(100.2))],
            "the bid sits below the mid and the ask above it"
        );
    }

    /// The spread multiplies the mid rather than offsetting the price: the bid
    /// sits at `mid * (1 - spread)` and the ask at `mid * (1 + spread)`, so a
    /// mid of 100 with a spread of 0.002 quotes 99.8 / 100.2 — a 0.2 distance.
    /// Pinned so the two readings cannot be confused.
    #[tokio::test]
    async fn the_spread_scales_with_the_mid_rather_than_being_a_price_offset() {
        let (gateway, maker) =
            maker_with(book(vec![level(Some(dec!(100)))], vec![level(Some(dec!(100)))]));
        maker.tick().await.expect("quoted");
        let mid = dec!(100);
        let quotes = quotes(&gateway.submitted());
        assert_eq!(quotes[0].1, mid * (Decimal::ONE - dec!(0.002)));
        assert_eq!(quotes[1].1, mid * (Decimal::ONE + dec!(0.002)));
        assert_eq!(quotes[0].1, dec!(99.8));
        assert_eq!(quotes[1].1, dec!(100.2));
    }

    /// Both legs name the configured symbol, the primary venue, limit type, and
    /// the configured quantity. A quote to the wrong venue or with the wrong
    /// size is a live trade against the strategy's intent.
    #[tokio::test]
    async fn both_quotes_rest_on_the_primary_venue_for_the_configured_symbol() {
        let (gateway, maker) =
            maker_with(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        maker.tick().await.expect("quoted");
        let submitted = gateway.submitted();
        assert_eq!(submitted.len(), 2);
        for req in &submitted {
            assert_eq!(
                req.exchange_id.as_option().map(|id| id.id.as_str()),
                Some("primary"),
                "quotes rest on the primary venue"
            );
            let order = req.order.as_option().expect("a request always carries an order");
            assert_eq!(order.symbol, SYMBOL);
            assert_eq!(order.r#type, buffa::EnumValue::Known(trading::OrderType::Limit));
            let amount = order.amount.as_option().expect("a quote carries an amount");
            assert_eq!(
                longtrader_contract::ext::common_to_decimal(amount).expect("decodes"),
                dec!(0.001)
            );
        }
    }

    /// The quote client id is namespaced per cycle so a venue can tell this
    /// strategy's orders from anyone else's.
    #[tokio::test]
    async fn every_quote_carries_a_distinct_namespaced_client_order_id() {
        let (gateway, maker) =
            maker_with(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        maker.tick().await.expect("quoted");
        let ids: Vec<String> = gateway
            .submitted()
            .iter()
            .filter_map(|req| req.order.as_option().map(|o| o.client_order_id.clone()))
            .collect();
        assert_eq!(ids.len(), 2);
        for id in &ids {
            assert!(id.starts_with("xdm-"), "unexpected client_order_id {id}");
        }
        assert_ne!(ids[0], ids[1], "two quotes must not share a client_order_id");
    }

    /// A quote the venue refuses ends the cycle: the second leg is not placed,
    /// because a lone ask (or a lone bid) is exactly the exposure the pair exists
    /// to bound. The caller has to be able to tell a cycle that quoted nothing
    /// from one that quoted both legs.
    #[tokio::test]
    async fn a_refused_quote_propagates_and_the_second_leg_is_not_placed() {
        let (gateway, maker) =
            maker_with(book(vec![level(Some(dec!(99)))], vec![level(Some(dec!(101)))]));
        gateway.rejecting();
        let err = maker.tick().await.expect_err("a refused quote is not a successful cycle");
        assert!(err.to_string().contains("venue rejected the quote"), "{err}");
        assert_eq!(gateway.submitted().len(), 1, "the pair is never half-quoted");
    }

    /// With no mid there is nothing to quote around, so the cycle stops before
    /// touching the gateway at all.
    #[tokio::test]
    async fn a_book_without_a_mid_stops_the_cycle_before_any_quote() {
        let (gateway, maker) = maker_with(book(vec![level(Some(dec!(99)))], Vec::new()));
        let err = maker.tick().await.expect_err("no fair value means no quotes");
        assert!(err.to_string().contains("lacks both sides"), "{err}");
        assert!(gateway.submitted().is_empty(), "no quote may be sent without a fair value");
    }

    #[tokio::test]
    async fn a_book_failure_stops_the_cycle_before_any_quote() {
        let (gateway, maker) = maker_against(config(), Arc::new(BookStub::down()));
        let err = maker.tick().await.expect_err("a dead book feed must surface");
        assert!(err.to_string().contains("book feed down"), "{err}");
        assert!(gateway.submitted().is_empty());
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Whatever the two-sided book, a non-zero spread always straddles the mid: the
        /// bid below it and the ask above it. Stated relative to the mid, so the
        /// identity holds for any spread below one, not only a small one.
        #[test]
        fn a_non_zero_spread_always_straddles_the_mid(
            bid in 1i64..1_000_000,
            ask in 1i64..1_000_000,
            spread_units in prop::sample::select(&[1i64, 2, 999, 1_000, 4_999, 9_999]),
        ) {
            let mut config = config();
            config.spread = Decimal::new(spread_units, 6);
            let book = book(
                vec![level(Some(Decimal::from(bid)))],
                vec![level(Some(Decimal::from(ask)))],
            );
            let (gateway, outcome) = tick_once(config, book);
            outcome.expect("a two-sided book has a mid");
            let quotes = quotes(&gateway.submitted());
            prop_assert_eq!(quotes.len(), 2, "one bid and one ask per cycle");
            let mid = (Decimal::from(bid) + Decimal::from(ask)) / Decimal::from(2);
            prop_assert!(quotes[0].1 < mid, "bid {} is not below mid {}", quotes[0].1, mid);
            prop_assert!(quotes[1].1 > mid, "ask {} is not above mid {}", quotes[1].1, mid);
        }

        /// A negative spread inverts the pair: the "bid" lands above the mid and
        /// the "ask" below it. Pinned because a misconfigured spread would
        /// otherwise quietly cross the book in the wrong direction.
        #[test]
        fn a_negative_spread_inverts_the_pair(
            bid in 1i64..1_000_000,
            ask in 1i64..1_000_000,
            spread_units in -9_999i64..0,
        ) {
            prop_assume!(spread_units != 0, "a zero spread is not an inversion");
            let mut config = config();
            config.spread = Decimal::new(spread_units, 6);
            let book = book(
                vec![level(Some(Decimal::from(bid)))],
                vec![level(Some(Decimal::from(ask)))],
            );
            let (gateway, outcome) = tick_once(config, book);
            outcome.expect("a two-sided book has a mid");
            let quotes = quotes(&gateway.submitted());
            prop_assert_eq!(quotes.len(), 2, "one bid and one ask per cycle");
            let mid = (Decimal::from(bid) + Decimal::from(ask)) / Decimal::from(2);
            prop_assert!(quotes[0].1 > mid, "bid {} is not above mid {}", quotes[0].1, mid);
            prop_assert!(quotes[1].1 < mid, "ask {} is not below mid {}", quotes[1].1, mid);
        }

        /// The sides are unconditional: the first leg is always a buy and the
        /// second always a sell, whatever the fair value came out as.
        #[test]
        fn the_first_leg_always_buys_and_the_second_always_sells(
            bid in 1i64..1_000_000,
            ask in 1i64..1_000_000,
        ) {
            let book = book(
                vec![level(Some(Decimal::from(bid)))],
                vec![level(Some(Decimal::from(ask)))],
            );
            let (gateway, outcome) = tick_once(config(), book);
            outcome.expect("a two-sided book has a mid");
            let quotes = quotes(&gateway.submitted());
            prop_assert_eq!(quotes.len(), 2);
            prop_assert!(quotes[0].0, "the first leg is the bid");
            prop_assert!(!quotes[1].0, "the second leg is the ask");
        }

        /// `hedge_mid` yields a mid exactly when both sides are present and
        /// strictly positive; every other shape is an error naming the cause.
        #[test]
        fn a_mid_exists_exactly_when_both_sides_are_positive(
            bid in -1_000i64..1_001,
            ask in -1_000i64..1_001,
        ) {
            let (_stub, _gateway, maker) = recording_maker(book(
                vec![level(Some(Decimal::from(bid)))],
                vec![level(Some(Decimal::from(ask)))],
            ));
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("a current-thread runtime");
            if bid > 0 && ask > 0 {
                let expected = (Decimal::from(bid) + Decimal::from(ask)) / Decimal::from(2);
                let mid = runtime
                    .block_on(maker.hedge_mid())
                    .expect("two positive sides have a mid");
                prop_assert_eq!(mid, expected, "bid={} ask={}", bid, ask);
            } else {
                let err = runtime
                    .block_on(maker.hedge_mid())
                    .expect_err("a non-positive side has no mid");
                prop_assert!(
                    err.to_string().contains("lacks both sides"),
                    "bid={} ask={} produced {}",
                    bid,
                    ask,
                    err
                );
            }
        }
    }
}
