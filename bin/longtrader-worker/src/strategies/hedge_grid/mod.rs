//! Hedged grid: a grid on the primary venue with an opposite market hedge
//! on the hedge venue for every detected fill, keeping net delta ~zero.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

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
}

impl HedgeGrid {
    pub fn new(
        config: HedgeGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// One poll cycle: seed missing grid legs on primary; hedge any fill.
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
        if mid.is_zero() {
            return Ok(());
        }

        let step = (self.config.upper_price - self.config.lower_price) /
            Decimal::from(self.config.num_levels);
        let open = self
            .gateway
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(primary.clone()),
                symbol: symbol.clone(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await?;
        let mut live_prices = std::collections::HashSet::new();
        let mut bids = 0usize;
        let mut asks = 0usize;
        for order in &open {
            if let Some(Ok(price)) =
                order.price.as_option().map(longtrader_contract::ext::common_to_decimal)
            {
                live_prices.insert(price);
            }
            match order.side {
                buffa::EnumValue::Known(crate::proto::trading::OrderSide::Buy) => bids += 1,
                _ => asks += 1,
            }
        }

        // Seed one leg per grid level that has no resting order.
        let mut price = self.config.lower_price;
        while price <= self.config.upper_price {
            if !live_prices.contains(&price) {
                let is_buy = price < mid;
                let coid = format!("hg-{}", ulid::Ulid::generate());
                let request =
                    limit_order(&primary, &symbol, coid, is_buy, price, self.config.qty_per_level);
                let _ = self.gateway.create_order(request).await;
            }
            price += step;
        }

        // Fill imbalance → hedge on the hedge venue.
        if bids != asks {
            // Fewer resting bids ⇒ a bid filled ⇒ long ⇒ sell on hedge.
            let hedge_is_buy = bids > asks;
            let coid = format!("hg-hedge-{}", ulid::Ulid::generate());
            self.gateway
                .create_order(market_order(
                    &hedge,
                    &symbol,
                    coid,
                    hedge_is_buy,
                    self.config.qty_per_level,
                ))
                .await?;
            tracing::info!(hedge_is_buy, "grid fill hedged");
        }
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
