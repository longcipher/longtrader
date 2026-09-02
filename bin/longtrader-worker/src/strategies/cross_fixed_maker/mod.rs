//! Cross-venue fixed-spread maker.
//!
//! Quotes a bid/ask pair on the primary venue around the hedge venue's
//! mid price (fair value). When a primary leg disappears from the open
//! orders (filled), a market hedge order for the same side is sent on the
//! hedge venue, keeping inventory flat.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order, market_order},
};

/// Config for the cross fixed maker.
#[derive(Debug, Clone, Deserialize)]
pub struct CrossFixedMakerConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Bid/ask spread from fair value (fraction).
    #[serde(default = "default_spread")]
    pub spread: Decimal,
    /// Order quantity.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_spread() -> Decimal {
    Decimal::new(2, 3)
}

/// Cross fixed maker strategy.
pub struct CrossFixedMaker {
    config: CrossFixedMakerConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

async fn ticker_mid(
    market: &dyn MarketDataSource,
    exchange_id: &crate::proto::common::ExchangeId,
    symbol: &str,
) -> Result<Decimal> {
    let ticker = market
        .fetch_ticker(crate::proto::market::FetchTickerRequest {
            exchange_id: buffa::MessageField::some(exchange_id.clone()),
            symbol: symbol.to_string(),
            ..Default::default()
        })
        .await?;
    if let Some(last) = ticker.last.as_option() {
        let price = longtrader_contract::ext::common_to_decimal(last)?;
        if price > Decimal::ZERO {
            return Ok(price);
        }
    }
    color_eyre::eyre::bail!("no usable ticker price");
}

impl CrossFixedMaker {
    pub fn new(
        config: CrossFixedMakerConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// One poll cycle: re-quote on primary; hedge fills detected by
    /// missing open orders.
    pub async fn tick(&self) -> Result<()> {
        let primary = self.config.venues.primary.proto();
        let hedge = self.config.venues.hedge.proto();
        let symbol = self.config.venues.symbol.clone();
        let fair = ticker_mid(self.market.as_ref(), &hedge, &symbol).await?;

        // Detect previously quoted legs that are gone → filled → hedge.
        let open = self
            .gateway
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest {
                exchange_id: buffa::MessageField::some(primary.clone()),
                symbol: symbol.clone(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await?;
        let live_bids = open
            .iter()
            .filter(|o| o.side == buffa::EnumValue::Known(crate::proto::trading::OrderSide::Buy));
        let live_asks = open
            .iter()
            .filter(|o| o.side == buffa::EnumValue::Known(crate::proto::trading::OrderSide::Sell));
        let _ = (live_bids.count(), live_asks.count());

        // Re-quote both sides at ±spread from fair value.
        let bid = fair * (Decimal::ONE - self.config.spread);
        let ask = fair * (Decimal::ONE + self.config.spread);
        for (price, is_buy) in [(bid, true), (ask, false)] {
            let coid = format!("xfm-{}", ulid::Ulid::generate());
            let request = limit_order(&primary, &symbol, coid, is_buy, price, self.config.qty);
            if let Err(err) = self.gateway.create_order(request).await {
                tracing::error!(error = %err, "cross quote failed");
            }
        }

        // Hedge one leg per cycle when imbalance detected: count mismatch
        // between bids and asks means one leg filled — flatten on hedge.
        let bids = open
            .iter()
            .filter(|o| o.side == buffa::EnumValue::Known(crate::proto::trading::OrderSide::Buy))
            .count();
        let asks = open
            .iter()
            .filter(|o| o.side == buffa::EnumValue::Known(crate::proto::trading::OrderSide::Sell))
            .count();
        if bids != asks {
            // More asks resting ⇒ the bid filled ⇒ we hold long ⇒ sell hedge.
            let hedge_buy = asks > bids;
            let coid = format!("xfm-hedge-{}", ulid::Ulid::generate());
            self.gateway
                .create_order(market_order(&hedge, &symbol, coid, hedge_buy, self.config.qty))
                .await?;
            tracing::info!(hedge_buy, "cross hedge executed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for CrossFixedMaker {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "cross-fixed-maker tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_params_parse() {
        use crate::strategies::params_from_table;
        let table: toml::Table = toml::from_str(
            r#"
            primary = { exchange_id = "binance" }
            hedge = { exchange_id = "okx" }
            symbol = "BTCUSDT"
            spread = "0.001"
            "#,
        )
        .expect("toml");
        let cfg: CrossFixedMakerConfig = params_from_table(&table).expect("parse");
        assert_eq!(cfg.venues.primary.exchange_id, "binance");
        assert_eq!(cfg.venues.hedge.exchange_id, "okx");
    }
}
