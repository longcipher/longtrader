//! Cross-venue market maker with inventory skew.
//!
//! Fair value comes from the hedge venue; quote sizes scale with a signed
//! inventory ratio so the strategy quotes more aggressively on the side
//! that reduces inventory (classic Avellaneda-style skew, simplified).

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order},
};

/// Config for the cross maker.
#[derive(Debug, Clone, Deserialize)]
pub struct CrossMakerConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    #[serde(default = "default_spread")]
    pub spread: Decimal,
    /// Inventory cap in base units; skew saturates at ±cap.
    pub inventory_cap: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_spread() -> Decimal {
    Decimal::new(2, 3)
}

/// Cross maker strategy.
pub struct CrossMaker {
    config: CrossMakerConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    /// Signed inventory estimate (fills minus hedges).
    inventory: Mutex<Decimal>,
}

impl CrossMaker {
    pub fn new(
        config: CrossMakerConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, inventory: Mutex::new(Decimal::ZERO) }
    }

    /// Quote size skewed by inventory: long inventory boosts the ask,
    /// short inventory boosts the bid.
    pub fn skewed_sizes(inventory: Decimal, cap: Decimal, qty: Decimal) -> (Decimal, Decimal) {
        if cap.is_zero() {
            return (qty, qty);
        }
        let skew = (inventory / cap).clamp(dec!(-1), dec!(1));
        let bid = qty * (Decimal::ONE - skew) / Decimal::from(2);
        let ask = qty * (Decimal::ONE + skew) / Decimal::from(2);
        (bid.max(Decimal::ZERO), ask.max(Decimal::ZERO))
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let primary = self.config.venues.primary.proto();
        let hedge = self.config.venues.hedge.proto();
        let symbol = self.config.venues.symbol.clone();
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(hedge),
                symbol: symbol.clone(),
                ..Default::default()
            })
            .await?;
        let fair = ticker
            .last
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .unwrap_or_default();
        if fair.is_zero() {
            return Ok(());
        }
        let inv = *self.inventory.lock().await;
        let (bid_size, ask_size) =
            Self::skewed_sizes(inv, self.config.inventory_cap, self.config.qty);
        for (price, is_buy, size) in [
            (fair * (Decimal::ONE - self.config.spread), true, bid_size),
            (fair * (Decimal::ONE + self.config.spread), false, ask_size),
        ] {
            if size.is_zero() {
                continue;
            }
            let coid = format!("xmk-{}", ulid::Ulid::generate());
            let request = limit_order(&primary, &symbol, coid, is_buy, price, size);
            let _ = self.gateway.create_order(request).await;
        }
        Ok(())
    }

    /// Records an executed fill/hedge pair (test and host hook).
    pub async fn apply_fill(&self, signed_qty: Decimal) {
        *self.inventory.lock().await += signed_qty;
    }
}

#[async_trait]
impl Strategy for CrossMaker {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "cross-maker tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros as rm;

    use super::*;

    #[test]
    fn skew_shifts_size_toward_inventory_reducing_side() {
        let (bid, ask) = CrossMaker::skewed_sizes(rm::dec!(5), rm::dec!(10), rm::dec!(1));
        assert!(ask > bid, "long inventory must boost the ask");
        let (bid, ask) = CrossMaker::skewed_sizes(rm::dec!(-5), rm::dec!(10), rm::dec!(1));
        assert!(bid > ask, "short inventory must boost the bid");
        let (bid, ask) = CrossMaker::skewed_sizes(Decimal::ZERO, rm::dec!(10), rm::dec!(1));
        assert_eq!(bid, ask);
    }
}
