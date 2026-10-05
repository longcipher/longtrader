//! Bollinger-band grid strategy over the worker candle-poll model.
//!
//! Polls candles, computes Bollinger bands over the close window, and when
//! the latest close sits inside sufficiently wide bands, replaces the
//! ladder: `grid_num` buy limits below and sell limits above the close.
//! Filled legs are detected via open-order reconciliation (same pattern as
//! `simple_grid`) and reversed at ±profit spread.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::{Bands, BollingerBands},
    ports::{MarketDataSource, TradingGateway},
    strategies::{
        CommonParams, Strategy, closes_of, fetch_candles, limit_order, params_from_table,
    },
};

/// Config for the worker Bollinger grid.
#[derive(Debug, Clone, Deserialize)]
pub struct BollGridConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_window")]
    pub boll_window: usize,
    #[serde(default = "default_mult")]
    pub boll_mult: Decimal,
    #[serde(default = "default_levels")]
    pub grid_num: u32,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Reverse-leg spread as a fraction of price.
    #[serde(default = "default_spread")]
    pub profit_spread_pct: Decimal,
}

const fn default_window() -> usize {
    21
}
fn default_mult() -> Decimal {
    Decimal::new(2, 0)
}
const fn default_levels() -> u32 {
    3
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_spread() -> Decimal {
    Decimal::new(5, 4)
}

/// Ceiling on `grid_num`, enforced when the config is parsed.
///
/// `plan_ladder` walks `grid_num` levels and emits up to two quotes per level,
/// and every quote becomes a live order the venue has to accept. A couple of
/// hundred levels is already denser than any real grid — 200 rungs across a 20%
/// band is a 10 bp step — whereas `u32::MAX` would walk four billion iterations
/// and build a multi-gigabyte ladder out of nothing but a config file. Capping it
/// at parse time turns that into a startup failure that names the limit.
const fn max_grid_num() -> u32 {
    200
}

impl BollGridConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors, and rejects a `grid_num` above the
    /// ceiling of 200 with an error naming both the ceiling and the value.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        let config: Self = params_from_table(table)?;
        if config.grid_num > max_grid_num() {
            return Err(color_eyre::eyre::eyre!(
                "grid_num must be at most {}, got {}",
                max_grid_num(),
                config.grid_num
            ));
        }
        Ok(config)
    }
}

/// Pure ladder planner: `(price, is_buy)` pairs around the close.
///
/// Levels falling outside the bands are dropped (quotes are confined to
/// `[lower, upper]`).
///
/// `grid_num` is bounded by a ceiling of 200 when it arrives from a config file;
/// this function trusts its caller, so an unbounded `grid_num` walks that many
/// iterations.
#[must_use]
pub fn plan_ladder(close: Decimal, bands: &Bands, grid_num: u32) -> Vec<(Decimal, bool)> {
    let width = bands.upper - bands.lower;
    if width <= Decimal::ZERO || close > bands.upper || close < bands.lower {
        return Vec::new();
    }
    let step = width / Decimal::from(grid_num.saturating_add(1));
    let mut plan = Vec::with_capacity(usize::try_from(grid_num).unwrap_or(0) * 2);
    for level in 1..=grid_num {
        let offset = step * Decimal::from(level);
        let bid = close - offset;
        let ask = close + offset;
        if bid > dec!(0) && bid >= bands.lower {
            plan.push((bid, true));
        }
        if ask <= bands.upper {
            plan.push((ask, false));
        }
    }
    plan
}

/// Worker Bollinger grid strategy.
pub struct BollGrid {
    config: BollGridConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl BollGrid {
    pub fn new(
        config: BollGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// One poll cycle: recompute bands and replace the ladder.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            u32::try_from(self.config.boll_window).unwrap_or(200),
        )
        .await?;
        let closes = closes_of(&candles);
        let Some(bands) = BollingerBands::compute(&closes, self.config.boll_mult) else {
            tracing::debug!("boll-grid warmup incomplete");
            return Ok(());
        };
        let Some(close) = closes.last().copied() else {
            return Ok(());
        };

        // Cancel previous ladder before quoting the new one (kill-switch
        // path also clears anything stale on other symbols).
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;

        for (price, is_buy) in plan_ladder(close, &bands, self.config.grid_num) {
            let coid = format!("bollgrid-{}", ulid::Ulid::generate());
            let request = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                price,
                self.config.qty,
            );
            // A refused leg ends the cycle rather than being logged and dropped.
            // The previous ladder has already been cancelled at this point, so a
            // half-quoted replacement leaves the symbol either bare or one-sided,
            // and reporting success would hide which one it is.
            self.gateway.create_order(request).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for BollGrid {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "boll-grid tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use proptest::prelude::*;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketEventStream, OverflowPolicy, PortError},
        proto::{market, trading},
    };

    #[test]
    fn ladder_plans_both_sides_inside_bands() {
        // width 40 / (3+1) = step 10 → clean in-band levels.
        let bands = Bands { middle: dec!(100), upper: dec!(120), lower: dec!(80) };
        let plan = plan_ladder(dec!(100), &bands, 3);
        assert_eq!(
            plan,
            vec![(dec!(90), true), (dec!(110), false), (dec!(80), true), (dec!(120), false),]
        );
    }

    #[test]
    fn ladder_drops_levels_outside_bands() {
        let bands = Bands { middle: dec!(100), upper: dec!(105), lower: dec!(95) };
        // width 10 / 3 ≈ 3.33: second levels fall outside the band.
        let plan = plan_ladder(dec!(100), &bands, 2);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].1, true);
        assert_eq!(plan[1].1, false);
    }

    #[test]
    fn ladder_skips_when_close_outside_bands() {
        let bands = Bands { middle: dec!(100), upper: dec!(110), lower: dec!(90) };
        assert!(plan_ladder(dec!(120), &bands, 3).is_empty());
        assert!(plan_ladder(dec!(80), &bands, 3).is_empty());
    }

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\ngrid_num = 4\nqty = \"0.01\"")
                .expect("valid toml");
        let cfg = BollGridConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.grid_num, 4);
        assert_eq!(cfg.qty.to_string(), "0.01");
    }

    /// `grid_num` bounds the work `plan_ladder` does, so it is bounded at parse
    /// time: a config file is enough to hang the worker otherwise.
    #[test]
    fn an_absurd_grid_num_is_refused_naming_the_ceiling() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\ngrid_num = 4294967295").expect("valid toml");
        let err = BollGridConfig::from_params(&table).expect_err("u32::MAX levels is not a ladder");
        let message = err.to_string();
        assert!(message.contains("200"), "the error must name the ceiling: {message}");
        assert!(message.contains("4294967295"), "the error must echo the value: {message}");
    }

    /// The ceiling itself is legal, so the bound is a bound and not a veto.
    #[test]
    fn a_grid_num_at_the_ceiling_is_accepted() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\ngrid_num = 200").expect("valid toml");
        assert_eq!(BollGridConfig::from_params(&table).expect("parse").grid_num, 200);
    }

    // -----------------------------------------------------------------------
    // plan_ladder: degenerate inputs
    // -----------------------------------------------------------------------

    /// `grid_num == 0` is the divide-by-zero trap: the step divisor is
    /// `grid_num + 1`, so the degenerate case divides by one and then loops
    /// zero times instead of quoting an unbounded ladder.
    #[test]
    fn a_zero_grid_num_plans_nothing() {
        let bands = Bands { middle: dec!(100), upper: dec!(120), lower: dec!(80) };
        assert!(plan_ladder(dec!(100), &bands, 0).is_empty());
    }

    /// Collapsed bands have no width, and an inverted pair is nonsense. Both
    /// must decline to quote rather than build a zero or negative step.
    #[test]
    fn a_non_positive_band_width_plans_nothing() {
        let collapsed = Bands { middle: dec!(100), upper: dec!(100), lower: dec!(100) };
        assert!(plan_ladder(dec!(100), &collapsed, 4).is_empty());
        let inverted = Bands { middle: dec!(100), upper: dec!(80), lower: dec!(120) };
        assert!(plan_ladder(dec!(100), &inverted, 4).is_empty());
    }

    /// Both band edges are inclusive. A close sitting exactly on `upper` still
    /// ladders (all sells), and a close exactly on `lower` still ladders (all
    /// buys) — only a strict excursion outside the band is refused.
    #[test]
    fn the_band_edges_are_inclusive_on_both_sides() {
        let bands = Bands { middle: dec!(100), upper: dec!(120), lower: dec!(80) };
        // width 40 / (3 + 1) = step 10; from 120 only the bids are inside.
        assert_eq!(
            plan_ladder(dec!(120), &bands, 3),
            vec![(dec!(110), true), (dec!(100), true), (dec!(90), true)]
        );
        assert_eq!(
            plan_ladder(dec!(80), &bands, 3),
            vec![(dec!(90), false), (dec!(100), false), (dec!(110), false)]
        );
    }

    /// A band that straddles zero cannot host a bid at or below zero, so the
    /// ladder truncates on the downside rather than quoting a non-tradable
    /// price.
    #[test]
    fn non_positive_bids_are_dropped() {
        let bands = Bands { middle: dec!(5), upper: dec!(15), lower: dec!(-5) };
        // width 20 / 4 = step 5; from 5 the bids run 0, -5, -10.
        assert_eq!(plan_ladder(dec!(5), &bands, 3), vec![(dec!(10), false), (dec!(15), false)]);
    }

    /// The ladder is band-confined, so `grid_num` is an upper bound rather than
    /// a promise: with the close in the middle of the band, the furthest levels
    /// always fall outside one edge.
    #[test]
    fn the_level_count_never_exceeds_two_entries_per_grid_level() {
        let bands = Bands { middle: dec!(1000), upper: dec!(1999), lower: dec!(1) };
        for grid_num in 0..=6u32 {
            let plan = plan_ladder(dec!(1000), &bands, grid_num);
            let per_side = usize::try_from(grid_num).expect("u32 fits usize");
            let buys = plan.iter().filter(|(_, is_buy)| *is_buy).count();
            assert!(buys <= per_side, "grid_num {grid_num} produced {buys} bids");
            assert!(plan.len() - buys <= per_side, "grid_num {grid_num} produced too many asks");
        }
    }

    /// The plan interleaves one bid and one ask per level, widening away from
    /// the close, which is what makes it a ladder rather than two independent
    /// order books.
    #[test]
    fn each_level_contributes_a_bid_below_and_an_ask_above_the_close() {
        let bands = Bands { middle: dec!(100), upper: dec!(120), lower: dec!(80) };
        let plan = plan_ladder(dec!(100), &bands, 3);
        assert!(!plan.is_empty(), "a ladder inside the bands must plan something");
        // Bids sit strictly below the close and asks strictly above it, which is
        // what makes the plan a ladder around the mark rather than one-sided.
        for (price, is_buy) in &plan {
            if *is_buy {
                assert!(*price < dec!(100), "a bid must be below the close: {price}");
            } else {
                assert!(*price > dec!(100), "an ask must be above the close: {price}");
            }
        }
        assert!(
            plan.iter().any(|(price, is_buy)| *is_buy && *price > dec!(80)),
            "the ladder must reach the bid band: {plan:?}"
        );
        assert!(
            plan.iter().any(|(price, is_buy)| !*is_buy && *price < dec!(120)),
            "the ladder must reach the ask band: {plan:?}"
        );
    }

    // -----------------------------------------------------------------------
    // BollGrid::tick
    // -----------------------------------------------------------------------

    fn grid_config() -> BollGridConfig {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nboll_window = 20\nboll_mult = \"2\"\n\
             grid_num = 3\nqty = \"0.01\"\n",
        )
        .expect("valid toml");
        BollGridConfig::from_params(&table).expect("parse")
    }

    /// A grid reading a close series with real dispersion, so the band has width.
    ///
    /// Every `MockAdapter` candle carries the same close, which collapses the
    /// bands and leaves `plan_ladder` nothing to quote — so the ladder-placing
    /// paths need a source whose window actually varies.
    fn grid_over_closes(closes: &[Decimal]) -> (Arc<MockAdapter>, BollGrid) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedCandles(closes.to_vec()));
        (adapter, BollGrid::new(grid_config(), gateway, market))
    }

    async fn resting_orders(adapter: &MockAdapter) -> Vec<trading::Order> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup")
    }

    /// The happy path the refusal tests build on: a band with width really does
    /// put a ladder on the venue.
    #[tokio::test]
    async fn a_band_with_width_quotes_the_planned_ladder() {
        let (adapter, grid) = grid_over_closes(&[dec!(90), dec!(100), dec!(110)]);
        grid.tick().await.expect("quoted");
        assert!(
            !resting_orders(&adapter).await.is_empty(),
            "a band with width must produce quotes for this assertion to mean anything"
        );
    }

    /// A leg the venue refuses ends the cycle. The previous ladder was already
    /// cancelled, so a swallowed error would report a success for a symbol the
    /// strategy has left bare or half-quoted.
    #[tokio::test]
    async fn a_refused_ladder_leg_propagates_out_of_the_tick() {
        let (adapter, grid) = grid_over_closes(&[dec!(90), dec!(100), dec!(110)]);
        adapter.fail_next_creates(1).await;

        let err = grid.tick().await.expect_err("a refused leg must not read as a full ladder");
        assert!(err.to_string().contains("scripted failure"), "{err}");
        assert!(resting_orders(&adapter).await.is_empty(), "the refused leg must not rest");
    }

    /// Market source whose candle window is a scripted close series.
    struct ScriptedCandles(Vec<Decimal>);

    #[async_trait]
    impl MarketDataSource for ScriptedCandles {
        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            Ok(market::GetCandlesResponse {
                candles: self
                    .0
                    .iter()
                    .map(|close| market::Candle {
                        close: buffa::MessageField::some(
                            longtrader_contract::ext::decimal_to_common(*close),
                        ),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
        }

        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Err(PortError::Unsupported("fetch_ticker".into()))
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Unsupported("fetch_order_book".into()))
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Unsupported("list_symbols".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Unsupported("search_symbols".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Unsupported("list_tickers".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Err(PortError::Unsupported("subscribe_market_data".into()))
        }
    }

    /// Every mock candle carries the same close, so the deviation is zero and
    /// the bands collapse onto the close. `plan_ladder` must then decline to
    /// quote — and the tick must still succeed, because cancelling the previous
    /// ladder is progress, not an error.
    #[tokio::test]
    async fn a_flat_close_series_collapses_the_bands_and_quotes_nothing() {
        let (adapter, grid) = grid_over_closes(&[dec!(100); 5]);
        grid.tick().await.expect("a collapsed ladder is not a failure");
        assert!(resting_orders(&adapter).await.is_empty(), "a zero-width band must not quote");
    }

    /// A venue that cannot serve the candle window is an error, not an empty
    /// ladder: the caller has to be able to tell "warmup incomplete" from
    /// "the feed is broken".
    #[tokio::test]
    async fn a_candle_feed_failure_propagates_out_of_the_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = Arc::new(FailingCandles);
        let grid = BollGrid::new(grid_config(), gateway, market);
        let err = grid.tick().await.expect_err("a broken feed must not read as a quiet tick");
        assert!(err.to_string().contains("candle feed down"), "{err}");
    }

    /// Market source whose candle window is a transport error.
    struct FailingCandles;

    #[async_trait]
    impl MarketDataSource for FailingCandles {
        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Err(PortError::Transport("ticker feed down".into()))
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Transport("book feed down".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            Err(PortError::Transport("candle feed down".into()))
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Transport("symbol feed down".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Transport("symbol feed down".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Transport("ticker feed down".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Err(PortError::Unsupported("no stream".into()))
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Everything the ladder publishes is a tradable price inside the bands,
        /// and its side follows the side of the close it sits on.
        #[test]
        fn every_level_is_positive_in_band_and_on_its_side_of_the_close(
            lower_milli in 1i64..100_000,
            width_milli in 1i64..100_000,
            offset_micro in 0i64..1_000_001,
            grid_num in 0u32..8,
        ) {
            let lower = Decimal::new(lower_milli, 3);
            let upper = lower + Decimal::new(width_milli, 3);
            let middle = (lower + upper) / dec!(2);
            let bands = Bands { middle, upper, lower };
            let span = upper - lower;
            let close = lower + span * Decimal::new(offset_micro, 6);
            if close > upper || close < lower {
                return Ok(());
            }
            let plan = plan_ladder(close, &bands, grid_num);
            let bound = usize::try_from(grid_num).expect("u32 fits usize") * 2;
            prop_assert!(plan.len() <= bound, "grid_num {grid_num} produced {} levels", plan.len());
            for (price, is_buy) in &plan {
                prop_assert!(*price > Decimal::ZERO, "{price} is not a tradable price");
                prop_assert!(
                    *price >= lower && *price <= upper,
                    "{price} escaped [{lower}, {upper}]"
                );
                prop_assert_eq!(*is_buy, *price < close, "side must follow the side of {}", close );
            }
        }

        /// Bids walk strictly down and asks strictly up, so the two sides never
        /// cross each other or the close.
        #[test]
        fn bids_descend_and_asks_ascend(
            lower_milli in 1i64..50_000,
            width_milli in 1i64..50_000,
            offset_micro in 0i64..1_000_001,
            grid_num in 1u32..8,
        ) {
            let lower = Decimal::new(lower_milli, 3);
            let upper = lower + Decimal::new(width_milli, 3);
            let middle = (lower + upper) / dec!(2);
            let bands = Bands { middle, upper, lower };
            let span = upper - lower;
            let close = lower + span * Decimal::new(offset_micro, 6);
            if close > upper || close < lower {
                return Ok(());
            }
            let plan = plan_ladder(close, &bands, grid_num);
            let bids: Vec<Decimal> =
                plan.iter().filter(|(_, buy)| *buy).map(|(p, _)| *p).collect();
            let asks: Vec<Decimal> =
                plan.iter().filter(|(_, buy)| !*buy).map(|(p, _)| *p).collect();
            prop_assert!(
                bids.windows(2).all(|w| w[0] > w[1]),
                "bids must descend: {bids:?}"
            );
            prop_assert!(
                asks.windows(2).all(|w| w[0] < w[1]),
                "asks must ascend: {asks:?}"
            );
            prop_assert!(
                bids.iter().all(|p| *p < close),
                "a bid at or above {close} would cross the close"
            );
            prop_assert!(asks.iter().all(|p| *p > close), "an ask at or below {close} would cross");
        }

        /// A band pair whose width is not positive can never produce a ladder,
        /// so `plan_ladder` can never be driven into a zero or negative step.
        #[test]
        fn a_non_positive_width_always_plans_nothing(
            lower_milli in 1i64..10_000,
            width_delta in -10_000i64..10_000,
            grid_num in 0u32..6,
        ) {
            let lower = Decimal::new(lower_milli, 3);
            let upper = lower + Decimal::new(width_delta, 3);
            if upper > lower {
                return Ok(());
            }
            let bands = Bands { middle: lower, upper, lower };
            prop_assert!(
                plan_ladder(lower, &bands, grid_num).is_empty(),
                "width {} must not be laddered", upper - lower
            );
        }
    }
}
