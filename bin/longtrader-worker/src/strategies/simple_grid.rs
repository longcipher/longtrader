//! Simple grid strategy written against the unified ports.
//!
//! Replaces the former daemon-specific and terminal-specific grids; the
//! backend is selected by configuration, not by strategy type.

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use longtrader_contract::ext::{common_to_decimal, decimal_to_common};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, PortError, TradingGateway},
    proto::{common, market, trading},
    strategies::Strategy,
};

/// Configuration for the unified simple grid.
#[derive(Debug, Clone, Deserialize)]
pub struct SimpleGridConfig {
    pub exchange_id: common::ExchangeId,
    pub symbol: String,
    pub lower_price: Decimal,
    pub upper_price: Decimal,
    pub num_levels: u32,
    pub qty_per_level: Decimal,
    /// Reverse-leg spread as a fraction of the fill price; `None` uses one
    /// grid step.
    #[serde(default)]
    pub profit_spread_pct: Option<Decimal>,
    /// Optional state file. The ladder is written here after every seed and after
    /// the kill-switch clears it, and is restored from it on the first
    /// `rebalance` after a restart. A missing file is a normal first start.
    #[serde(default)]
    pub state_file: Option<String>,
}

impl SimpleGridConfig {
    /// Build the unified grid configuration from strategy params.
    ///
    /// `state_file` is optional and read from the merged parameter table, so an
    /// operator can turn restart recovery on by naming a path; without it the
    /// grid keeps its ladder in memory only.
    ///
    /// # Errors
    /// Returns an error when required fields are missing.
    pub fn from_config(config: &crate::config::Config) -> color_eyre::Result<Self> {
        let params = &config.strategy.params;
        let required = |value: Option<Decimal>, name: &str| -> color_eyre::Result<Decimal> {
            value.ok_or_else(|| color_eyre::eyre::eyre!("{name} is required"))
        };
        Ok(Self {
            profit_spread_pct: None,
            state_file: params
                .table()
                .get("state_file")
                .and_then(toml::Value::as_str)
                .map(ToString::to_string),
            exchange_id: crate::adapters::exchange_id(
                params.exchange_id.as_deref().unwrap_or("mock"),
                params.label.as_deref().unwrap_or_default(),
            ),
            symbol: params
                .symbol
                .clone()
                .ok_or_else(|| color_eyre::eyre::eyre!("symbol is required"))?,
            lower_price: required(params.lower_price, "lower_price")?,
            upper_price: required(params.upper_price, "upper_price")?,
            num_levels: params
                .num_levels
                .ok_or_else(|| color_eyre::eyre::eyre!("num_levels is required"))?,
            qty_per_level: required(params.qty_per_level, "qty_per_level")?,
        })
    }
}

#[derive(Debug, Clone)]
struct GridLevel {
    price: Decimal,
    side: trading::OrderSide,
    client_order_id: String,
    /// Venue-assigned order id.
    ///
    /// `CancelOrderRequest.order_id` addresses the order the *venue* issued;
    /// the client id only lives in `Order.client_order_id`. Cancelling by the
    /// client id therefore asks the venue to cancel an order it never created,
    /// which fails and (because the kill-switch only warns) leaves the level
    /// resting while the grid believes it is flat.
    order_id: String,
}

/// Grid strategy with fill-flip rebalancing and an error kill-switch.
pub struct SimpleGrid {
    config: SimpleGridConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    levels: Mutex<Vec<GridLevel>>,
    grid_step: Decimal,
    max_consecutive_errors: u32,
}

impl SimpleGrid {
    pub fn new(
        config: SimpleGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let grid_step = if config.num_levels > 0 {
            (config.upper_price - config.lower_price) / Decimal::from(config.num_levels)
        } else {
            dec!(0)
        };
        Self {
            config,
            gateway,
            market,
            levels: Mutex::new(Vec::new()),
            grid_step,
            max_consecutive_errors: 5,
        }
    }

    /// Persists grid levels when `state_file` is configured.
    fn persist_levels(&self, levels: &[GridLevel]) {
        if let Some(path) = &self.config.state_file {
            let store = crate::state_store::StateStore::new(path);
            let json = serde_json::json!(levels
                .iter()
                .map(|l| serde_json::json!({
                    "price": l.price.to_string(),
                    "side": if matches!(l.side, trading::OrderSide::Buy) { "buy" } else { "sell" },
                    "client_order_id": l.client_order_id,
                    // The venue order id travels with the level so a restored
                    // ladder can still be addressed by the kill-switch. It is
                    // only a hint: an id the venue has since retired fails the
                    // cancel, which the kill-switch already treats as a warning.
                    "order_id": l.order_id,
                }))
                .collect::<Vec<_>>());
            let _ = store.save(&json).inspect_err(|err| {
                tracing::warn!(error = %err, "grid state save failed");
            });
        }
    }

    /// Restores the ladder from `state_file`. An empty vector means "nothing to
    /// restore": no file configured, no file present, or a file holding an empty
    /// ladder — which is exactly what the kill-switch writes, and what the
    /// in-memory path does with an empty ladder anyway.
    ///
    /// A read or parse failure is an error rather than a silent seed-from-scratch.
    /// The seeding path quotes every level without reading the venue, so a
    /// restored-and-then-dropped ladder leaves the previous process's orders
    /// resting *and* a second ladder on top of them — twice the intended
    /// inventory, with the flip logic unable to tell the copies apart. Refusing
    /// the cycle is loud and costs no money; guessing is silent and does.
    fn restore_levels(&self) -> color_eyre::Result<Vec<GridLevel>> {
        let Some(path) = self.config.state_file.as_deref() else {
            return Ok(Vec::new());
        };
        let Some(document) = crate::state_store::StateStore::new(path).load()? else {
            return Ok(Vec::new());
        };
        let rows = document.as_array().ok_or_else(|| {
            color_eyre::eyre::eyre!("grid state at {path} is not a list of levels")
        })?;
        let mut levels = Vec::with_capacity(rows.len());
        let mut off_band = 0usize;
        for row in rows {
            let level = Self::level_from_json(row)?;
            // A level the current band no longer contains can neither be
            // re-quoted nor flipped sensibly, so it is dropped rather than left to
            // linger as a level the strategy can never act on.
            if level.price < self.config.lower_price || level.price > self.config.upper_price {
                off_band += 1;
                continue;
            }
            levels.push(level);
        }
        if off_band > 0 {
            tracing::warn!(off_band, "dropped restored grid levels outside the configured band");
        }
        Ok(levels)
    }

    /// One persisted level row.
    ///
    /// A row the strategy wrote always carries a price and a side. A missing
    /// client order id is replaced with a fresh one rather than refused: a
    /// restored level is matched against the venue by price, and it is given a new
    /// id the moment it is flipped anyway.
    fn level_from_json(row: &serde_json::Value) -> color_eyre::Result<GridLevel> {
        let price = row
            .get("price")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("grid level row has no price"))?
            .parse::<Decimal>()
            .map_err(|err| color_eyre::eyre::eyre!("grid level price is not a decimal: {err}"))?;
        let side = match row.get("side").and_then(serde_json::Value::as_str) {
            Some("buy") => trading::OrderSide::Buy,
            Some("sell") => trading::OrderSide::Sell,
            other => {
                return Err(color_eyre::eyre::eyre!("grid level side is not buy/sell: {other:?}"));
            }
        };
        let text = |key: &str| {
            row.get(key).and_then(serde_json::Value::as_str).unwrap_or_default().to_string()
        };
        let stored_coid = text("client_order_id");
        let client_order_id = if stored_coid.is_empty() {
            format!("grid-{}", ulid::Ulid::generate())
        } else {
            stored_coid
        };
        Ok(GridLevel { price, side, client_order_id, order_id: text("order_id") })
    }

    /// Latest price from ticker last/close, falling back to bid-ask mid.
    ///
    /// A rung that is absent, undecodable, or not strictly positive is skipped
    /// rather than treated as fatal: one malformed `last` must not abort a grid
    /// whose `close` or book is perfectly usable. The ladder only fails when
    /// every rung is unusable.
    async fn current_price(&self) -> color_eyre::Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                symbol: self.config.symbol.clone(),
                ..Default::default()
            })
            .await?;
        for candidate in [ticker.last.as_option(), ticker.close.as_option()] {
            if let Some(price) = Self::usable_price(candidate) {
                return Ok(price);
            }
        }
        let bid = Self::usable_price(ticker.bid.as_option());
        let ask = Self::usable_price(ticker.ask.as_option());
        if let (Some(bid), Some(ask)) = (bid, ask) {
            return Ok((bid + ask) / dec!(2));
        }
        color_eyre::eyre::bail!("no usable price in ticker for {}", self.config.symbol);
    }

    /// A ticker rung as a price: `None` when it is absent, cannot be decoded, or
    /// is not strictly positive.
    fn usable_price(candidate: Option<&common::Decimal>) -> Option<Decimal> {
        let price = common_to_decimal(candidate?).ok()?;
        (price > Decimal::ZERO).then_some(price)
    }

    fn side_for(price: Decimal, current: Decimal) -> trading::OrderSide {
        if price < current { trading::OrderSide::Buy } else { trading::OrderSide::Sell }
    }

    fn calculate_levels(&self, current_price: Decimal) -> Vec<GridLevel> {
        let mut levels = Vec::new();
        if self.grid_step <= dec!(0) {
            return levels;
        }
        let max_levels = usize::try_from(self.config.num_levels).unwrap_or(usize::MAX);
        let mut price = self.config.lower_price;
        while price <= self.config.upper_price && levels.len() <= max_levels {
            levels.push(GridLevel {
                price,
                side: Self::side_for(price, current_price),
                client_order_id: format!("grid-{}", ulid::Ulid::generate()),
                order_id: String::new(),
            });
            price += self.grid_step;
        }
        levels
    }

    /// Places the level and records the venue-assigned order id on it so the
    /// kill-switch can address the order the venue actually created.
    async fn place_level_order(&self, level: &mut GridLevel) -> Result<(), PortError> {
        let order_req = trading::OrderRequest {
            client_order_id: level.client_order_id.clone(),
            symbol: self.config.symbol.clone(),
            r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
            side: buffa::EnumValue::Known(level.side),
            amount: buffa::MessageField::some(decimal_to_common(self.config.qty_per_level)),
            price: buffa::MessageField::some(decimal_to_common(level.price)),
            trigger_price: buffa::MessageField::none(),
            time_in_force: buffa::EnumValue::Known(trading::TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            params: Default::default(),
            ..Default::default()
        };
        let order = self
            .gateway
            .create_order(trading::CreateOrderRequest {
                exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                order: buffa::MessageField::some(order_req),
                ..Default::default()
            })
            .await?;
        level.order_id = order.id;
        Ok(())
    }

    /// Cancel every tracked order (kill-switch path).
    ///
    /// A level that was never placed (or whose placement failed) has no venue
    /// order id; there is nothing to cancel for it, so it is skipped rather
    /// than sending an empty `order_id`.
    async fn cancel_tracked_orders(&self, levels: &[GridLevel]) {
        for level in levels {
            if level.order_id.is_empty() {
                continue;
            }
            if let Err(err) = self
                .gateway
                .cancel_order(trading::CancelOrderRequest {
                    exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                    order_id: level.order_id.clone(),
                    symbol: self.config.symbol.clone(),
                    ..Default::default()
                })
                .await
            {
                tracing::warn!(
                    order_id = %level.order_id,
                    coid = %level.client_order_id,
                    error = %err,
                    "kill-switch cancel failed"
                );
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn rebalance(&self) -> color_eyre::Result<()> {
        let current = self.current_price().await?;
        // ponytail: snapshot under a short lock; all RPCs run lock-free.
        let snapshot: Vec<GridLevel> = self.levels.lock().await.clone();
        let mut errors: u32 = 0;

        if snapshot.is_empty() {
            // A restart resumes the ladder it persisted instead of seeding a second
            // one on top of whatever the previous process left resting. The
            // restored ladder is reconciled against the venue on the next poll,
            // which is where a level that is no longer resting gets flipped.
            let restored = self.restore_levels()?;
            if !restored.is_empty() {
                tracing::info!(
                    levels = restored.len(),
                    "restored grid levels; the next poll reconciles them against the venue"
                );
                *self.levels.lock().await = restored;
                return Ok(());
            }
            let mut fresh = self.calculate_levels(current);
            tracing::info!("initialized {} grid levels on {}", fresh.len(), self.config.symbol);
            for level in &mut fresh {
                if let Err(err) = self.place_level_order(level).await {
                    errors += 1;
                    tracing::error!(price = %level.price, error = %err, "initial placement failed");
                }
            }
            if errors >= self.max_consecutive_errors {
                tracing::error!(
                    errors,
                    "error threshold reached during init; tripping grid kill-switch"
                );
                self.cancel_tracked_orders(&fresh).await;
                fresh.clear();
            }
            self.persist_levels(&fresh);
            *self.levels.lock().await = fresh;
            return Ok(());
        }

        // Authoritative open-order view keyed by client_order_id and price.
        // Lock-free: uses the snapshot taken above.
        let open_orders = self
            .gateway
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                symbol: self.config.symbol.clone(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await?;
        let mut live_coids = HashSet::with_capacity(open_orders.len());
        let mut live_prices = HashSet::with_capacity(open_orders.len());
        for order in &open_orders {
            live_coids.insert(order.client_order_id.clone());
            if let Some(Ok(price)) = order.price.as_option().map(common_to_decimal) {
                live_prices.insert(price);
            }
        }

        let mut updated = snapshot;
        for level in &mut updated {
            // Prefer exact client_order_id matching; fall back to price-level
            // matching for backends that do not echo client_order_id.
            let alive =
                live_coids.contains(&level.client_order_id) || live_prices.contains(&level.price);
            if alive {
                continue;
            }
            // Filled or gone: place the opposite leg one step away.
            let opposite_side = match level.side {
                trading::OrderSide::Sell => trading::OrderSide::Buy,
                _ => trading::OrderSide::Sell,
            };
            let opposite_price = match level.side {
                trading::OrderSide::Sell => level.price - self.grid_step,
                _ => level.price + self.grid_step,
            };
            if opposite_price < self.config.lower_price || opposite_price > self.config.upper_price
            {
                continue;
            }
            *level = GridLevel {
                price: opposite_price,
                side: opposite_side,
                client_order_id: format!("grid-{}", ulid::Ulid::generate()),
                order_id: String::new(),
            };
            if let Err(err) = self.place_level_order(level).await {
                errors += 1;
                tracing::error!(price = %level.price, error = %err, "flip placement failed");
            }
        }

        if errors >= self.max_consecutive_errors {
            tracing::error!(
                errors,
                "error threshold reached; tripping grid kill-switch and cancelling tracked orders"
            );
            self.cancel_tracked_orders(&updated).await;
            updated.clear();
            self.persist_levels(&updated);
        }
        *self.levels.lock().await = updated;
        Ok(())
    }
}

#[async_trait]
impl Strategy for SimpleGrid {
    async fn run(&self) -> color_eyre::Result<()> {
        tracing::info!(
            "starting unified grid on {} ({}-{}, {} levels, qty={}/level)",
            self.config.symbol,
            self.config.lower_price,
            self.config.upper_price,
            self.config.num_levels,
            self.config.qty_per_level
        );
        loop {
            match self.rebalance().await {
                Ok(()) => {}
                Err(err) => tracing::error!("grid rebalance error: {err}"),
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use async_trait::async_trait;
    use longtrader_contract::ext::decimal_to_common;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketEventStream, OverflowPolicy, PortError},
    };

    /// A ticker decimal that is present on the wire but cannot be decoded.
    ///
    /// `Decimal` carries one base-10 string and nothing beside it, so there is no
    /// numeric pair left to be out of range: the payload itself has to be the
    /// thing that is wrong. Outside the grammar is how.
    fn undecodable() -> common::Decimal {
        common::Decimal { value: "not-a-number".to_string(), ..Default::default() }
    }

    fn grid_config() -> SimpleGridConfig {
        SimpleGridConfig {
            exchange_id: common::ExchangeId {
                id: "mock".to_string(),
                label: String::new(),
                ..Default::default()
            },
            symbol: "BTC/USDT".to_string(),
            lower_price: dec!(90),
            upper_price: dec!(110),
            num_levels: 4,
            qty_per_level: dec!(0.1),
            profit_spread_pct: None,
            state_file: None,
        }
    }

    fn build() -> (StdArc<MockAdapter>, SimpleGrid) {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let grid = SimpleGrid::new(grid_config(), gateway, market);
        (adapter, grid)
    }

    async fn open_orders(grid: &SimpleGrid) -> Vec<trading::Order> {
        grid.gateway
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(grid.config.exchange_id.clone()),
                symbol: grid.config.symbol.clone(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("test setup")
    }

    #[tokio::test]
    async fn first_rebalance_seeds_the_grid() {
        let (_adapter, grid) = build();
        grid.rebalance().await.expect("test setup");
        let open = open_orders(&grid).await;
        assert_eq!(open.len(), 5, "num_levels=4 spans 5 inclusive price points");
        let levels = grid.levels.lock().await;
        assert_eq!(levels.len(), 5);
        // Every level carries a unique ULID-based client_order_id.
        let coids: HashSet<_> = levels.iter().map(|l| l.client_order_id.clone()).collect();
        assert_eq!(coids.len(), 5);
    }

    #[tokio::test]
    async fn filled_level_flips_to_the_opposite_leg() {
        let (adapter, grid) = build();
        grid.rebalance().await.expect("test setup");

        // Simulate a fill of the lowest (buy) level by cancelling its order.
        let lowest = {
            let levels = grid.levels.lock().await;
            levels.iter().min_by_key(|l| l.price).expect("test setup").clone()
        };
        let filled_order = open_orders(&grid)
            .await
            .into_iter()
            .find(|o| o.client_order_id == lowest.client_order_id)
            .expect("seeded order must exist");
        adapter
            .cancel_order(trading::CancelOrderRequest {
                exchange_id: buffa::MessageField::some(grid.config.exchange_id.clone()),
                order_id: filled_order.id.clone(),
                symbol: String::new(),
                ..Default::default()
            })
            .await
            .expect("test setup");

        grid.rebalance().await.expect("test setup");

        // The flipped leg is one step ABOVE the filled buy price.
        let levels = grid.levels.lock().await;
        let flipped = levels.iter().find(|l| l.price == lowest.price + grid.grid_step);
        assert!(flipped.is_some(), "opposite leg must be placed one step up");
        let flipped = flipped.expect("test setup");
        assert_eq!(flipped.side, trading::OrderSide::Sell);
        assert_ne!(
            flipped.client_order_id, lowest.client_order_id,
            "flip must use a fresh client_order_id"
        );

        // Total live orders returns to the seeded count.
        let open = open_orders(&grid).await;
        assert_eq!(open.len(), 5);
    }

    #[tokio::test]
    async fn repeated_failures_trip_the_kill_switch_and_clear_levels() {
        let (adapter, grid) = build();
        adapter.fail_next_creates(6).await; // more than max_consecutive_errors (5)
        let _ = grid.rebalance().await;
        let levels = grid.levels.lock().await;
        assert!(levels.is_empty(), "kill-switch clears the grid after repeated failures");
    }

    // -----------------------------------------------------------------------
    // side_for
    // -----------------------------------------------------------------------

    /// A level strictly below the market price buys; everything else sells.
    /// Equality is the boundary and it sells, so a level resting exactly at the
    /// current price is never crossed for a bid it did not intend.
    #[test]
    fn a_level_below_the_current_price_buys_and_everything_else_sells() {
        assert_eq!(SimpleGrid::side_for(dec!(99), dec!(100)), trading::OrderSide::Buy);
        assert_eq!(SimpleGrid::side_for(dec!(100), dec!(100)), trading::OrderSide::Sell);
        assert_eq!(SimpleGrid::side_for(dec!(101), dec!(100)), trading::OrderSide::Sell);
    }

    #[test]
    fn the_side_rule_holds_for_degenerate_prices() {
        assert_eq!(SimpleGrid::side_for(Decimal::ZERO, dec!(100)), trading::OrderSide::Buy);
        assert_eq!(SimpleGrid::side_for(dec!(0.00000001), Decimal::ZERO), trading::OrderSide::Sell);
        assert_eq!(SimpleGrid::side_for(Decimal::ZERO, Decimal::ZERO), trading::OrderSide::Sell);
        assert_eq!(SimpleGrid::side_for(dec!(-2), dec!(-1)), trading::OrderSide::Buy);
        assert_eq!(SimpleGrid::side_for(dec!(-1), dec!(-2)), trading::OrderSide::Sell);
        assert_eq!(SimpleGrid::side_for(dec!(-1), dec!(-1)), trading::OrderSide::Sell);
    }

    // -----------------------------------------------------------------------
    // calculate_levels / grid_step
    // -----------------------------------------------------------------------

    fn grid_with(lower: Decimal, upper: Decimal, num_levels: u32) -> SimpleGrid {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let mut config = grid_config();
        config.lower_price = lower;
        config.upper_price = upper;
        config.num_levels = num_levels;
        SimpleGrid::new(config, gateway, adapter as StdArc<dyn MarketDataSource>)
    }

    /// `num_levels == 0` is the divide-by-zero trap in the grid step. The guard
    /// pins the step at zero, which in turn makes the ladder empty — no panic,
    /// no infinite `price += 0` loop.
    #[test]
    fn a_zero_level_grid_has_a_zero_step_and_no_levels() {
        let grid = grid_with(dec!(90), dec!(110), 0);
        assert_eq!(grid.grid_step, Decimal::ZERO);
        assert!(grid.calculate_levels(dec!(100)).is_empty());
    }

    /// A collapsed span has no width to step across, so there is nothing to
    /// quote even though the level count is positive.
    #[test]
    fn a_collapsed_span_produces_no_levels() {
        let grid = grid_with(dec!(100), dec!(100), 4);
        assert_eq!(grid.grid_step, Decimal::ZERO);
        assert!(grid.calculate_levels(dec!(100)).is_empty());
    }

    /// An inverted span yields a negative step, which the `grid_step <= 0` guard
    /// rejects. Without it the ladder would walk away from the band.
    #[test]
    fn an_inverted_span_produces_no_levels() {
        let grid = grid_with(dec!(110), dec!(90), 4);
        assert!(grid.grid_step < Decimal::ZERO);
        assert!(grid.calculate_levels(dec!(100)).is_empty());
    }

    /// Both bounds are inclusive and every level is exactly one step above the
    /// previous one.
    #[test]
    fn the_ladder_is_inclusive_of_both_bounds_and_evenly_spaced() {
        let grid = grid_with(dec!(90), dec!(110), 4);
        assert_eq!(grid.grid_step, dec!(5));
        let levels = grid.calculate_levels(dec!(100));
        let prices: Vec<Decimal> = levels.iter().map(|l| l.price).collect();
        assert_eq!(prices, vec![dec!(90), dec!(95), dec!(100), dec!(105), dec!(110)]);
        assert!(levels.windows(2).all(|w| w[1].price - w[0].price == grid.grid_step));
    }

    /// `side_for` is what `calculate_levels` applies to each price: below the
    /// market price buys, at or above it sells.
    #[test]
    fn ladder_sides_follow_the_current_price() {
        let grid = grid_with(dec!(90), dec!(110), 4);
        let levels = grid.calculate_levels(dec!(100));
        let sides: Vec<trading::OrderSide> = levels.iter().map(|l| l.side).collect();
        assert_eq!(
            sides,
            vec![
                trading::OrderSide::Buy,
                trading::OrderSide::Buy,
                trading::OrderSide::Sell,
                trading::OrderSide::Sell,
                trading::OrderSide::Sell,
            ]
        );
    }

    /// The level count is bounded by `num_levels + 1`: the inclusive walk emits
    /// one level per price point, and the `levels.len() <= max_levels` guard caps
    /// it. Accumulated `Decimal` rounding on a span that does not divide evenly
    /// can only ever shrink the ladder, never lengthen it.
    #[test]
    fn the_ladder_never_exceeds_the_level_count_plus_one() {
        for num_levels in 1..=8u32 {
            let grid = grid_with(dec!(90), dec!(110), num_levels);
            let levels = grid.calculate_levels(dec!(100));
            let bound = usize::try_from(num_levels).expect("u32 fits usize") + 1;
            assert!(
                levels.len() <= bound,
                "num_levels={num_levels} produced {} levels",
                levels.len()
            );
            for level in &levels {
                assert!(
                    level.price >= grid.config.lower_price &&
                        level.price <= grid.config.upper_price,
                    "{} escaped the configured band",
                    level.price
                );
            }
        }
    }

    /// A grid configured with no levels never places an order and never
    /// converges to a seeded state, so the poll loop degrades to a no-op rather
    /// than spinning on an empty ladder.
    #[tokio::test]
    async fn a_zero_level_grid_never_places_orders() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let mut config = grid_config();
        config.num_levels = 0;
        let grid = SimpleGrid::new(config, gateway, market);
        grid.rebalance().await.expect("an empty ladder is not a failure");
        grid.rebalance().await.expect("an empty ladder is not a failure");
        assert!(grid.levels.lock().await.is_empty());
        assert!(open_orders(&grid).await.is_empty());
    }

    // -----------------------------------------------------------------------
    // current_price fallback ladder
    // -----------------------------------------------------------------------

    fn grid_against(ticker: market::Ticker) -> SimpleGrid {
        let gateway: StdArc<dyn TradingGateway> = StdArc::new(MockAdapter::new(dec!(100)));
        let market: StdArc<dyn MarketDataSource> = StdArc::new(ScriptedTicker::of(ticker));
        SimpleGrid::new(grid_config(), gateway, market)
    }

    #[tokio::test]
    async fn a_positive_last_price_is_used_directly() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.last = buffa::MessageField::some(decimal_to_common(dec!(101)));
            t.close = buffa::MessageField::some(decimal_to_common(dec!(55)));
        }));
        assert_eq!(grid.current_price().await.expect("price"), dec!(101));
    }

    /// A venue that reports a zero `last` (a dead pair, or a field it never
    /// fills) must not poison the grid: the close is the next candidate.
    #[tokio::test]
    async fn a_zero_last_price_falls_through_to_the_close() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.last = buffa::MessageField::some(decimal_to_common(Decimal::ZERO));
            t.close = buffa::MessageField::some(decimal_to_common(dec!(97)));
        }));
        assert_eq!(grid.current_price().await.expect("price"), dec!(97));
    }

    #[tokio::test]
    async fn an_absent_last_price_falls_through_to_the_close() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.close = buffa::MessageField::some(decimal_to_common(dec!(97)));
        }));
        assert_eq!(grid.current_price().await.expect("price"), dec!(97));
    }

    /// With no last and no close the bid-ask midpoint is the last resort.
    #[tokio::test]
    async fn an_absent_last_and_close_falls_back_to_the_bid_ask_midpoint() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.bid = buffa::MessageField::some(decimal_to_common(dec!(99)));
            t.ask = buffa::MessageField::some(decimal_to_common(dec!(101)));
        }));
        assert_eq!(grid.current_price().await.expect("price"), dec!(100));
    }

    /// Every rung of the ladder rejects a non-positive quote: a half-empty or
    /// one-sided book would place the grid at zero.
    #[tokio::test]
    async fn a_non_positive_book_is_not_a_usable_price() {
        let cases = [
            ScriptedTicker::build(|t| {
                t.bid = buffa::MessageField::some(decimal_to_common(Decimal::ZERO));
                t.ask = buffa::MessageField::some(decimal_to_common(dec!(101)));
            }),
            ScriptedTicker::build(|t| {
                t.bid = buffa::MessageField::some(decimal_to_common(dec!(99)));
                t.ask = buffa::MessageField::some(decimal_to_common(Decimal::ZERO));
            }),
            ScriptedTicker::build(|t| {
                t.bid = buffa::MessageField::some(decimal_to_common(dec!(99)));
            }),
            ScriptedTicker::build(|t| {
                t.ask = buffa::MessageField::some(decimal_to_common(dec!(101)));
            }),
            ScriptedTicker::build(|_| {}),
        ];
        for ticker in cases {
            let grid = grid_against(ticker);
            let err = grid.current_price().await.expect_err("no usable price must be an error");
            assert!(err.to_string().contains("no usable price"), "{err}");
            assert!(err.to_string().contains("BTC/USDT"), "the error must name the symbol: {err}");
        }
    }

    /// An undecodable `last` is an unusable rung, not a fatal one: the ladder must
    /// fall through to a perfectly usable rung below it instead of aborting the
    /// whole cycle on a malformed field.
    #[tokio::test]
    async fn an_undecodable_last_price_falls_through_to_the_close() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.last = buffa::MessageField::some(undecodable());
            t.close = buffa::MessageField::some(decimal_to_common(dec!(97)));
            t.bid = buffa::MessageField::some(decimal_to_common(dec!(99)));
            t.ask = buffa::MessageField::some(decimal_to_common(dec!(101)));
        }));
        assert_eq!(grid.current_price().await.expect("the close is usable"), dec!(97));
    }

    /// The same for a malformed `close`: the book is the last rung and it works.
    #[tokio::test]
    async fn an_undecodable_close_falls_through_to_the_book() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.close = buffa::MessageField::some(undecodable());
            t.bid = buffa::MessageField::some(decimal_to_common(dec!(99)));
            t.ask = buffa::MessageField::some(decimal_to_common(dec!(101)));
        }));
        assert_eq!(grid.current_price().await.expect("the book is usable"), dec!(100));
    }

    /// With every rung unusable there is genuinely no price, so the error has to
    /// survive — an undecodable field is not the same as an absent one.
    #[tokio::test]
    async fn an_undecodable_last_and_close_still_have_no_usable_price() {
        let grid = grid_against(ScriptedTicker::build(|t| {
            t.last = buffa::MessageField::some(undecodable());
            t.close = buffa::MessageField::some(undecodable());
            t.bid = buffa::MessageField::some(undecodable());
            t.ask = buffa::MessageField::some(undecodable());
        }));
        let err = grid.current_price().await.expect_err("no usable rung is an error");
        assert!(err.to_string().contains("no usable price"), "{err}");
    }

    /// A ticker read failure is fatal to the rebalance rather than being read as
    /// "no price available".
    #[tokio::test]
    async fn a_ticker_failure_propagates_out_of_the_rebalance() {
        let gateway: StdArc<dyn TradingGateway> = StdArc::new(MockAdapter::new(dec!(100)));
        let market: StdArc<dyn MarketDataSource> = StdArc::new(FailingTicker);
        let grid = SimpleGrid::new(grid_config(), gateway, market);
        let err = grid.rebalance().await.expect_err("a broken ticker must not read as a flat tick");
        assert!(err.to_string().contains("ticker feed down"), "{err}");
    }

    /// Ticker source with a scripted `Ticker`, so each rung of the fallback
    /// ladder is reachable.
    struct ScriptedTicker {
        ticker: market::Ticker,
    }

    impl ScriptedTicker {
        fn build(build: impl FnOnce(&mut market::Ticker)) -> market::Ticker {
            let mut ticker =
                market::Ticker { symbol: "BTC/USDT".to_string(), ..Default::default() };
            build(&mut ticker);
            ticker
        }

        fn of(ticker: market::Ticker) -> Self {
            Self { ticker }
        }
    }

    #[async_trait]
    impl MarketDataSource for ScriptedTicker {
        async fn fetch_ticker(
            &self,
            req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Ok(market::Ticker { symbol: req.symbol, ..self.ticker.clone() })
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Unsupported("no book".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            Err(PortError::Unsupported("no candles".into()))
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Unsupported("no tickers".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Err(PortError::Unsupported("no stream".into()))
        }
    }

    /// Ticker source whose every read is a transport error.
    struct FailingTicker;

    #[async_trait]
    impl MarketDataSource for FailingTicker {
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
    // persist_levels
    // -----------------------------------------------------------------------

    fn grid_with_state_file(path: &std::path::Path) -> SimpleGrid {
        let gateway: StdArc<dyn TradingGateway> = StdArc::new(MockAdapter::new(dec!(100)));
        let market: StdArc<dyn MarketDataSource> = StdArc::new(MockAdapter::new(dec!(100)));
        let mut config = grid_config();
        config.state_file = Some(path.to_string_lossy().into_owned());
        SimpleGrid::new(config, gateway, market)
    }

    /// A level's identity as a restart would find it: the coid proves which
    /// process minted it, the price and side prove the ladder itself.
    fn level_identity(level: &GridLevel) -> (String, Decimal, trading::OrderSide) {
        (level.client_order_id.clone(), level.price, level.side)
    }

    /// The persisted document is what a restart reads back, so it has to name
    /// every level's price, side, and client order id.
    #[tokio::test]
    async fn persisted_levels_record_price_side_and_client_order_id() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("nested/grid.json");
        let grid = grid_with_state_file(&path);
        grid.rebalance().await.expect("test setup");

        let store = crate::state_store::StateStore::new(&path);
        let saved = store.load().expect("load").expect("the grid persisted its state");
        let rows = saved.as_array().expect("the document is an array of levels");
        assert_eq!(rows.len(), 5);
        for (index, row) in rows.iter().enumerate() {
            let level = grid.levels.lock().await[index].clone();
            assert_eq!(row["price"], serde_json::Value::String(level.price.to_string()));
            let side = if matches!(level.side, trading::OrderSide::Buy) { "buy" } else { "sell" };
            assert_eq!(row["side"], serde_json::Value::String(side.to_string()));
            assert_eq!(row["client_order_id"], serde_json::Value::String(level.client_order_id));
        }
    }

    // -----------------------------------------------------------------------
    // Restart recovery
    //
    // Writing a ladder is only half of persistence: nothing read it back, so the
    // documented "levels survive restarts" behaviour did not exist. These tests
    // pin the read path, which is what makes a restart resume the ladder instead
    // of quoting a second one on top of the orders the previous process left.
    // -----------------------------------------------------------------------

    /// A second process over the same state file resumes the first one's ladder
    /// rather than seeding a fresh one — the coids are the proof, since a seeded
    /// ladder always mints new ones.
    #[tokio::test]
    async fn a_restart_resumes_the_persisted_ladder_instead_of_reseeding_it() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");

        let first = grid_with_state_file(&path);
        first.rebalance().await.expect("seeded");
        let seeded: Vec<(String, Decimal, trading::OrderSide)> =
            first.levels.lock().await.iter().map(level_identity).collect();
        assert_eq!(seeded.len(), 5);

        let second = grid_with_state_file(&path);
        second.rebalance().await.expect("restored");
        let resumed: Vec<(String, Decimal, trading::OrderSide)> =
            second.levels.lock().await.iter().map(level_identity).collect();
        assert_eq!(resumed, seeded, "the restored ladder is the persisted one");
        assert!(
            open_orders(&second).await.is_empty(),
            "a resumed ladder must not quote again before it has been reconciled"
        );
    }

    /// The resumed ladder is reconciled against the venue on the next poll: the
    /// orders the previous process left resting are adopted rather than replaced.
    #[tokio::test]
    async fn a_resumed_ladder_adopts_the_orders_the_venue_still_holds() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let mut config = grid_config();
        config.state_file = Some(path.to_string_lossy().into_owned());
        let first = SimpleGrid::new(config.clone(), gateway.clone(), market.clone());
        first.rebalance().await.expect("seeded");
        let seeded_coids: Vec<String> =
            first.levels.lock().await.iter().map(|l| l.client_order_id.clone()).collect();

        // Same venue, same state file: the previous run's orders are still resting.
        let second = SimpleGrid::new(config, gateway, market);
        second.rebalance().await.expect("restored");
        second.rebalance().await.expect("reconciled");
        let levels = second.levels.lock().await;
        let live: Vec<String> = levels.iter().map(|l| l.client_order_id.clone()).collect();
        assert_eq!(live, seeded_coids, "every level is still the venue's own order");
        let resting = open_orders(&second).await;
        assert_eq!(resting.len(), 5, "reconciling a resting ladder places nothing");
    }

    /// No file is a first start, not a failure: the ladder is seeded as usual.
    #[tokio::test]
    async fn a_missing_state_file_is_a_normal_first_start() {
        let dir = tempfile::tempdir().expect("tmp");
        let grid = grid_with_state_file(&dir.path().join("absent/grid.json"));
        grid.rebalance().await.expect("a missing file is not an error");
        assert_eq!(open_orders(&grid).await.len(), 5, "the ladder seeds from scratch");
        assert_eq!(grid.levels.lock().await.len(), 5);
    }

    /// An unreadable state file stops the cycle instead of seeding a second
    /// ladder on top of the orders the previous process left resting. The seeding
    /// path never reads the venue, so "just start fresh" is the one answer that
    /// doubles the inventory.
    #[tokio::test]
    async fn a_state_file_that_cannot_be_read_stops_the_grid_instead_of_reseeding() {
        let dir = tempfile::tempdir().expect("tmp");
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "i am a file").expect("write");
        let grid = grid_with_state_file(&blocker.join("grid.json"));
        let err = grid.rebalance().await.expect_err("an unreadable ladder must not be guessed at");
        assert!(err.to_string().contains("read"), "{err}");
        assert!(open_orders(&grid).await.is_empty(), "no order may be quoted blind");
        assert!(grid.levels.lock().await.is_empty());
    }

    /// A corrupt document is the same failure: it is not an empty ladder, and
    /// treating it as one would re-quote over a live one.
    #[tokio::test]
    async fn a_corrupt_state_file_stops_the_grid_instead_of_reseeding() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        std::fs::write(&path, "{not json").expect("write");
        let grid = grid_with_state_file(&path);
        let err = grid.rebalance().await.expect_err("a corrupt ladder must not be guessed at");
        assert!(err.to_string().contains("deserialize state"), "{err}");
        assert!(open_orders(&grid).await.is_empty());
    }

    /// A state file holding an object instead of a list of levels is a shape the
    /// strategy cannot read, and is reported rather than treated as no ladder.
    #[tokio::test]
    async fn a_state_file_that_is_not_a_list_of_levels_is_reported() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        crate::state_store::StateStore::new(&path)
            .save(&serde_json::json!({"levels": []}))
            .expect("write");
        let grid = grid_with_state_file(&path);
        let err = grid.rebalance().await.expect_err("an unreadable shape must not be guessed at");
        assert!(err.to_string().contains("not a list of levels"), "{err}");
    }

    /// A level the operator's new band no longer contains is dropped rather than
    /// left to linger: it can be neither re-quoted nor flipped inside the band.
    #[tokio::test]
    async fn a_restored_level_outside_the_current_band_is_dropped() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        crate::state_store::StateStore::new(&path)
            .save(&serde_json::json!([
                {"price": "50", "side": "buy", "client_order_id": "grid-low"},
                {"price": "95", "side": "buy", "client_order_id": "grid-in"},
            ]))
            .expect("write");
        let grid = grid_with_state_file(&path);
        grid.rebalance().await.expect("restored");
        let levels = grid.levels.lock().await;
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].client_order_id, "grid-in", "only the in-band level survives");
    }

    /// A row with no client order id is still a level: the venue match falls back
    /// to price, and a fresh id is minted for the flip that replaces it.
    #[tokio::test]
    async fn a_restored_level_without_a_client_order_id_gets_a_fresh_one() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        crate::state_store::StateStore::new(&path)
            .save(&serde_json::json!([{"price": "95", "side": "buy"}]))
            .expect("write");
        let grid = grid_with_state_file(&path);
        grid.rebalance().await.expect("restored");
        let levels = grid.levels.lock().await;
        assert_eq!(levels.len(), 1);
        assert!(levels[0].client_order_id.starts_with("grid-"));
    }

    /// A row whose side is neither `buy` nor `sell` cannot be acted on, so it is
    /// an error rather than a level the strategy guesses at.
    #[tokio::test]
    async fn a_restored_level_with_an_unknown_side_is_reported() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        crate::state_store::StateStore::new(&path)
            .save(&serde_json::json!([{"price": "95", "side": "long"}]))
            .expect("write");
        let grid = grid_with_state_file(&path);
        let err = grid.rebalance().await.expect_err("an unknown side must not be guessed at");
        assert!(err.to_string().contains("buy/sell"), "{err}");
    }

    /// The kill-switch must address the venue-assigned order id, not the client id.
    ///
    /// `CancelOrderRequest.order_id` names the order the *venue* created; the
    /// client id only appears in `Order.client_order_id`. Cancelling by the
    /// client id asks the venue to cancel an order it never issued, which fails
    /// with `InvalidArgument` — and because the kill-switch only warns, the grid
    /// would clear its state while its orders stayed resting.
    #[tokio::test]
    async fn the_kill_switch_cancels_by_the_venue_order_id() {
        let (_adapter, grid) = build();
        grid.rebalance().await.expect("test setup");
        assert_eq!(open_orders(&grid).await.len(), 5);

        let levels = grid.levels.lock().await.clone();
        assert!(
            levels.iter().all(|level| !level.order_id.is_empty()),
            "every placed level must carry the venue order id"
        );

        grid.cancel_tracked_orders(&levels).await;
        assert_eq!(
            open_orders(&grid).await.len(),
            0,
            "every tracked level must actually be cancelled"
        );
    }

    /// Regression guard for the identifier mix-up: the two ids are distinct, so
    /// using one where the other belongs silently cancels nothing.
    #[tokio::test]
    async fn the_venue_order_id_is_not_the_client_order_id() {
        let (adapter, grid) = build();
        grid.rebalance().await.expect("test setup");
        let levels = grid.levels.lock().await.clone();

        let seeded = open_orders(&grid).await;
        let first = seeded.first().expect("a seeded order");
        assert_ne!(first.id, first.client_order_id, "the venue must assign its own id");

        let coid = levels.first().expect("a level").client_order_id.clone();
        let err = adapter
            .cancel_order(trading::CancelOrderRequest {
                exchange_id: buffa::MessageField::some(grid.config.exchange_id.clone()),
                order_id: coid.clone(),
                symbol: grid.config.symbol.clone(),
                ..Default::default()
            })
            .await
            .expect_err("the client order id is not the venue order id");
        assert!(matches!(err, PortError::InvalidArgument(_)), "{err:?}");
        assert_eq!(
            open_orders(&grid).await.len(),
            5,
            "a client-id cancel must not remove anything"
        );
    }

    /// A level whose placement never succeeded has no venue order id; the
    /// kill-switch must skip it rather than send an empty `order_id`.
    #[tokio::test]
    async fn the_kill_switch_skips_levels_that_were_never_placed() {
        let (_adapter, grid) = build();
        grid.rebalance().await.expect("test setup");
        let mut levels = grid.levels.lock().await.clone();
        let total = levels.len();
        assert_eq!(total, 5, "the fixture seeds five levels");
        levels[0].order_id = String::new();
        grid.cancel_tracked_orders(&levels).await;
        let remaining = open_orders(&grid).await;
        assert_eq!(
            remaining.len(),
            1,
            "only the level that was never placed stays resting, and it must be \
             that level rather than an arbitrary one"
        );
        assert_eq!(
            remaining[0].client_order_id, levels[0].client_order_id,
            "the survivor must be the level whose venue id was never recorded"
        );
    }

    /// After the kill switch clears the ladder, the persisted document must be
    /// the cleared one. A restart that read a stale file would re-quote a grid
    /// the venue never accepted.
    #[tokio::test]
    async fn the_kill_switch_persists_an_empty_ladder() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let market: StdArc<dyn MarketDataSource> = adapter.clone();
        let mut config = grid_config();
        config.state_file = Some(path.to_string_lossy().into_owned());
        let grid = SimpleGrid::new(config, gateway, market);
        adapter.fail_next_creates(6).await;
        let _ = grid.rebalance().await;
        assert!(grid.levels.lock().await.is_empty());

        let store = crate::state_store::StateStore::new(&path);
        let saved = store.load().expect("load").expect("the kill switch persisted a state");
        assert_eq!(saved, serde_json::json!([]));
    }

    /// A save that cannot reach the filesystem is only a warning: the grid keeps
    /// quoting from memory. Losing future restart state must not stop the strategy.
    #[tokio::test]
    async fn a_state_file_that_cannot_be_written_back_does_not_stop_the_grid() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("grid.json");
        // An empty persisted ladder is what the kill-switch writes, so this run
        // has nothing to resume: it seeds, then tries to persist into a directory
        // that no longer accepts writes.
        crate::state_store::StateStore::new(&path).save(&serde_json::json!([])).expect("write");
        #[cfg(unix)]
        make_dir_read_only(dir.path());

        let grid = grid_with_state_file(&path);
        grid.rebalance().await.expect("an unwritable state file is not fatal");
        assert_eq!(open_orders(&grid).await.len(), 5, "the ladder is still quoted");
        assert_eq!(grid.levels.lock().await.len(), 5);

        #[cfg(unix)]
        make_dir_writable(dir.path());
    }

    /// Drops write permission on `dir` so a state save fails while reads still
    /// succeed. No-op off unix, where the mode bits do not exist.
    #[cfg(unix)]
    fn make_dir_read_only(dir: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dir).expect("stat").permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(dir, perms).expect("chmod");
    }

    /// Restores write permission so the temp directory can be cleaned up.
    #[cfg(unix)]
    fn make_dir_writable(dir: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dir).expect("stat").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(dir, perms).expect("chmod");
    }

    /// With no `state_file` configured, persistence is skipped entirely: the grid
    /// still seeds from memory.
    #[tokio::test]
    async fn no_state_file_means_no_persistence() {
        let (_adapter, grid) = build();
        grid.rebalance().await.expect("test setup");
        assert_eq!(open_orders(&grid).await.len(), 5);
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    fn config_from(params: &str) -> crate::config::Config {
        let doc = format!(
            "daemon_endpoint = \"http://localhost:1\"\n\
             [strategy]\ntype = \"simple_grid\"\n[strategy.params]\n{params}\n"
        );
        toml::from_str(&doc).expect("valid toml")
    }

    #[test]
    fn a_config_file_must_supply_every_grid_bound() {
        let cfg = SimpleGridConfig::from_config(&config_from(
            "symbol = \"BTC/USDT\"\nlower_price = \"90\"\nupper_price = \"110\"\n\
             num_levels = 4\nqty_per_level = \"0.1\"\n",
        ))
        .expect("all required fields present");
        assert_eq!(cfg.symbol, "BTC/USDT");
        assert_eq!(cfg.lower_price, dec!(90));
        assert_eq!(cfg.upper_price, dec!(110));
        assert_eq!(cfg.num_levels, 4);
        assert_eq!(cfg.qty_per_level, dec!(0.1));
        assert_eq!(cfg.exchange_id.id, "mock", "the default venue is the mock backend");
        assert!(cfg.state_file.is_none(), "state_file is optional");
        assert!(cfg.profit_spread_pct.is_none(), "profit_spread_pct is optional");
    }

    /// `state_file` is the switch for restart recovery, so a path named in the
    /// config file has to reach the strategy — otherwise the documented
    /// persistence is unreachable in production.
    #[test]
    fn the_state_file_is_carried_through_from_the_config() {
        let cfg = SimpleGridConfig::from_config(&config_from(
            "symbol = \"BTC/USDT\"\nlower_price = \"90\"\nupper_price = \"110\"\n\
             num_levels = 4\nqty_per_level = \"0.1\"\nstate_file = \"/var/lib/lt/grid.json\"\n",
        ))
        .expect("all required fields present");
        assert_eq!(cfg.state_file.as_deref(), Some("/var/lib/lt/grid.json"));
    }

    /// Every required grid parameter is checked by name, so an operator can
    /// see which key the config file is missing.
    #[test]
    fn each_missing_required_param_is_named_in_the_error() {
        let fields = [
            "symbol = \"BTC/USDT\"",
            "lower_price = \"90\"",
            "upper_price = \"110\"",
            "num_levels = 4",
            "qty_per_level = \"0.1\"",
        ];
        for omitted in 0..fields.len() {
            let body = fields
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != omitted)
                .map(|(_, line)| *line)
                .collect::<Vec<_>>()
                .join("\n");
            let err = SimpleGridConfig::from_config(&config_from(&body))
                .expect_err("a missing bound must fail startup");
            let key = fields[omitted].split('=').next().expect("a key is present").trim();
            assert!(err.to_string().contains(key), "the error must name {key}: {err}");
        }
    }

    /// The venue defaults to the mock backend, but an explicit venue and label
    /// are carried through so a real session addresses the right account.
    #[test]
    fn the_venue_and_label_are_carried_through_from_the_config() {
        let cfg = SimpleGridConfig::from_config(&config_from(
            "exchange_id = \"binance\"\nlabel = \"sub-1\"\nsymbol = \"BTC/USDT\"\n\
             lower_price = \"90\"\nupper_price = \"110\"\nnum_levels = 4\n\
             qty_per_level = \"0.1\"\n",
        ))
        .expect("all required fields present");
        assert_eq!(cfg.exchange_id.id, "binance");
        assert_eq!(cfg.exchange_id.label, "sub-1");
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// For any positive span the ladder is empty or it is an ordered
        /// one-step-per-level ladder wholly inside the configured band, and its
        /// sides follow `side_for` exactly.
        ///
        /// Prices are kept at cent scale and below 100 so the step's 28-decimal
        /// expansion never pushes the running price past `Decimal`'s 96-bit
        /// mantissa, which would abort the walk with an arithmetic overflow.
        #[test]
        fn the_ladder_is_ordered_in_band_and_correctly_sided(
            lower_cent in 100i64..10_000,
            width_cent in 1i64..10_000,
            num_levels in 1u32..8,
            current_cent in 100i64..20_000,
        ) {
            let lower = Decimal::new(lower_cent, 2);
            let upper = lower + Decimal::new(width_cent, 2);
            let current = Decimal::new(current_cent, 2);
            let grid = grid_with(lower, upper, num_levels);
            prop_assert!(grid.grid_step > Decimal::ZERO, "a positive span needs a positive step");
            let levels = grid.calculate_levels(current);
            let bound = usize::try_from(num_levels).expect("u32 fits usize") + 1;
            prop_assert!(
                levels.len() <= bound,
                "num_levels {num_levels} emitted {} levels",
                levels.len()
            );
            for level in &levels {
                prop_assert!(level.price >= lower, "{} fell below {lower}", level.price);
                prop_assert!(level.price <= upper, "{} rose above {upper}", level.price);
                prop_assert_eq!(
                    level.side,
                    SimpleGrid::side_for(level.price, current),
                    "side must follow side_for at {}",
                    level.price
                );
                prop_assert!(level.client_order_id.starts_with("grid-"));
            }
            // `price += grid_step` accumulates, so the spacing is exact only
            // while the running sum stays representable; allow one ulp.
            let ulp = Decimal::new(1, 18);
            prop_assert!(
                levels.windows(2).all(|w| {
                    (w[1].price - w[0].price - grid.grid_step).abs() <= ulp
                }),
                "levels must be evenly spaced"
            );
        }

        /// A span that is not positive can never yield a ladder, which is what
        /// keeps the `price += grid_step` walk from running forever.
        #[test]
        fn a_non_positive_span_never_produces_a_ladder(
            lower_cent in 100i64..10_000,
            width_cent in -10_000i64..0,
            num_levels in 1u32..8,
        ) {
            let lower = Decimal::new(lower_cent, 2);
            let upper = lower + Decimal::new(width_cent, 2);
            let grid = grid_with(lower, upper, num_levels);
            prop_assert!(grid.grid_step <= Decimal::ZERO);
            prop_assert!(
                grid.calculate_levels(lower).is_empty(),
                "width {width_cent} must not be laddered"
            );
        }

        /// Client order ids are minted per level, so a seeded ladder can never
        /// carry a duplicate id into the venue's reconciliation view.
        #[test]
        fn every_seeded_level_gets_its_own_client_order_id(
            num_levels in 1u32..6,
        ) {
            let grid = grid_with(dec!(90), dec!(110), num_levels);
            let levels = grid.calculate_levels(dec!(100));
            let mut ids: Vec<String> = levels.iter().map(|l| l.client_order_id.clone()).collect();
            let before = ids.len();
            ids.sort();
            ids.dedup();
            prop_assert_eq!(ids.len(), before, "a client order id was reused");
        }
    }
}
