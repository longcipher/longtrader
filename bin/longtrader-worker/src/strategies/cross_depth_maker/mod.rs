//! Cross-venue depth-based maker: fair value from the hedge venue's order
//! book mid; quotes a bid/ask pair on the primary venue each cycle.

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

    /// Hedge-venue book mid (best bid + best ask) / 2.
    pub async fn hedge_mid(&self) -> Result<Decimal> {
        let hedge = self.config.venues.hedge.proto();
        let book = self
            .market
            .fetch_order_book(crate::proto::market::FetchOrderBookRequest {
                exchange_id: buffa::MessageField::some(hedge),
                symbol: self.config.venues.symbol.clone(),
                limit: 1,
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
            if let Err(err) = self.gateway.create_order(request).await {
                tracing::error!(error = %err, "depth-maker quote failed");
            }
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
