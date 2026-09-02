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
    /// Optional state persistence path (grid levels survive restarts).
    #[serde(default)]
    pub state_file: Option<String>,
}

#[derive(Debug, Clone)]
struct GridLevel {
    price: Decimal,
    side: trading::OrderSide,
    client_order_id: String,
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
                }))
                .collect::<Vec<_>>());
            let _ = store.save(&json).inspect_err(|err| {
                tracing::warn!(error = %err, "grid state save failed");
            });
        }
    }

    /// Latest price from ticker last/close, falling back to bid-ask mid.
    async fn current_price(&self) -> color_eyre::Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                symbol: self.config.symbol.clone(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let price = common_to_decimal(last)?;
            if price > Decimal::ZERO {
                return Ok(price);
            }
        }
        if let Some(close) = ticker.close.as_option() {
            let price = common_to_decimal(close)?;
            if price > Decimal::ZERO {
                return Ok(price);
            }
        }
        let bid = ticker.bid.as_option().map(common_to_decimal).transpose()?;
        let ask = ticker.ask.as_option().map(common_to_decimal).transpose()?;
        if let (Some(bid), Some(ask)) = (bid, ask) &&
            bid > Decimal::ZERO &&
            ask > Decimal::ZERO
        {
            return Ok((bid + ask) / dec!(2));
        }
        color_eyre::eyre::bail!("no usable price in ticker for {}", self.config.symbol);
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
            });
            price += self.grid_step;
        }
        levels
    }

    async fn place_level_order(&self, level: &GridLevel) -> Result<(), PortError> {
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
        self.gateway
            .create_order(trading::CreateOrderRequest {
                exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                order: buffa::MessageField::some(order_req),
                ..Default::default()
            })
            .await
            .map(|_| ())
    }

    /// Cancel every tracked order (kill-switch path).
    async fn cancel_tracked_orders(&self, levels: &[GridLevel]) {
        for level in levels {
            if let Err(err) = self
                .gateway
                .cancel_order(trading::CancelOrderRequest {
                    exchange_id: buffa::MessageField::some(self.config.exchange_id.clone()),
                    order_id: level.client_order_id.clone(),
                    symbol: self.config.symbol.clone(),
                    ..Default::default()
                })
                .await
            {
                tracing::warn!(
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
        let mut levels = self.levels.lock().await;
        let mut errors: u32 = 0;

        if levels.is_empty() {
            *levels = self.calculate_levels(current);
            tracing::info!("initialized {} grid levels on {}", levels.len(), self.config.symbol);
            for level in &mut *levels {
                if let Err(err) = self.place_level_order(&*level).await {
                    errors += 1;
                    tracing::error!(price = %level.price, error = %err, "initial placement failed");
                }
            }
            if errors >= self.max_consecutive_errors {
                tracing::error!(
                    errors,
                    "error threshold reached during init; tripping grid kill-switch"
                );
                self.cancel_tracked_orders(&levels).await;
                levels.clear();
            }
            self.persist_levels(&levels);
            return Ok(());
        }

        // Authoritative open-order view keyed by client_order_id and price.
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

        for level in &mut *levels {
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
            };
            if let Err(err) = self.place_level_order(&*level).await {
                errors += 1;
                tracing::error!(price = %level.price, error = %err, "flip placement failed");
            }
        }

        if errors >= self.max_consecutive_errors {
            tracing::error!(
                errors,
                "error threshold reached; tripping grid kill-switch and cancelling tracked orders"
            );
            self.cancel_tracked_orders(&levels).await;
            levels.clear();
            self.persist_levels(&levels);
        }
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

    use rust_decimal_macros::dec;

    use super::*;
    use crate::adapters::MockAdapter;

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
        adapter.fail_next_creates(6); // more than max_consecutive_errors (5)
        let _ = grid.rebalance().await;
        let levels = grid.levels.lock().await;
        assert!(levels.is_empty(), "kill-switch clears the grid after repeated failures");
    }
}
