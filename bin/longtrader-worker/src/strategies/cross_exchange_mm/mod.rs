//! Cross-Exchange Market Making (XEMM).
//!
//! Takes on venue A (the "taker" venue) and makes on venue B (the
//! "maker" venue). Observes the order book on the taker venue and
//! quotes a wider half-spread on the maker venue such that a fill on
//! the maker side can be profitably hedged on the taker side. Profits
//! from the spread difference between venues.
//!
//! ```text
//!   maker_bid  = taker_bid · (1 − min_profitability)
//!   maker_ask  = taker_ask · (1 + min_profitability)
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/cross-exchange-market-making/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CrossVenueParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the XEMM strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct XemmConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Minimum profitability fraction required to quote.
    #[serde(default = "default_min_profit")]
    pub min_profitability: Decimal,
    /// Quote quantity per side on the maker venue.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Number of maker levels per side.
    #[serde(default = "default_levels")]
    pub levels: u32,
    /// Step size as fraction between maker levels.
    #[serde(default = "default_step_size")]
    pub step_size: Decimal,
}

fn default_min_profit() -> Decimal {
    Decimal::new(1, 4)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_levels() -> u32 {
    3
}
fn default_step_size() -> Decimal {
    Decimal::new(5, 4)
}

impl XemmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// XEMM quote plan: `(bid_prices, ask_prices)`.
///
/// `skip = true` when the spread between venues is insufficient.
#[derive(Debug, Clone)]
pub struct XemmQuotePlan {
    pub bids: Vec<Decimal>,
    pub asks: Vec<Decimal>,
    pub skip: bool,
}

/// Compute the maker-venue quote ladder from the taker-venue best bid/ask.
///
/// `None` when inputs are invalid.
#[must_use]
pub fn plan_xemm_quotes(
    taker_bid: Decimal,
    taker_ask: Decimal,
    min_profitability: Decimal,
    levels: u32,
    step_size: Decimal,
) -> Option<XemmQuotePlan> {
    if taker_bid <= Decimal::ZERO ||
        taker_ask <= Decimal::ZERO ||
        taker_bid >= taker_ask ||
        min_profitability < Decimal::ZERO
    {
        return None;
    }
    // The taker-venue spread must exceed the profit margin.
    let taker_spread = (taker_ask - taker_bid) / taker_bid;
    if taker_spread < min_profitability * dec!(2) {
        return Some(XemmQuotePlan { bids: Vec::new(), asks: Vec::new(), skip: true });
    }
    let center = (taker_bid + taker_ask) / dec!(2);
    let mut bids = Vec::with_capacity(levels as usize);
    let mut asks = Vec::with_capacity(levels as usize);
    for i in 1..=levels {
        let offset = step_size * Decimal::from(i);
        // Maker bids: slightly below center, widening outward.
        let bid = center * (Decimal::ONE - min_profitability - offset).max(Decimal::ZERO);
        // Maker asks: slightly above center, widening outward.
        let ask = center * (Decimal::ONE + min_profitability + offset);
        bids.push(bid);
        asks.push(ask);
    }
    Some(XemmQuotePlan { bids, asks, skip: false })
}

/// Check whether a fill at `maker_price` is hedgeable at a profit.
#[must_use]
pub fn is_hedgeable(
    maker_price: Decimal,
    taker_bid: Decimal,
    taker_ask: Decimal,
    is_buy: bool,
    min_profitability: Decimal,
) -> bool {
    if is_buy {
        // Maker buy fill: hedge by selling on the taker venue.
        maker_price < taker_bid * (Decimal::ONE - min_profitability)
    } else {
        // Maker sell fill: hedge by buying on the taker venue.
        maker_price > taker_ask * (Decimal::ONE + min_profitability)
    }
}

/// Cross-exchange market maker strategy.
pub struct Xemm {
    config: XemmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl Xemm {
    pub fn new(
        config: XemmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    async fn fetch_taker_best(&self, symbol: &str) -> Result<(Decimal, Decimal)> {
        let req = crate::proto::market::FetchOrderBookRequest {
            exchange_id: buffa::MessageField::some(self.config.venues.hedge.proto()),
            symbol: symbol.to_string(),
            pagination: buffa::MessageField::some(crate::proto::common::Pagination {
                limit: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let book = self.market.fetch_order_book(req).await?;
        let bid = book
            .bids
            .first()
            .and_then(|l| l.price.as_option())
            .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
            .ok_or_else(|| color_eyre::eyre::eyre!("no taker bid"))?;
        let ask = book
            .asks
            .first()
            .and_then(|l| l.price.as_option())
            .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
            .ok_or_else(|| color_eyre::eyre::eyre!("no taker ask"))?;
        Ok((bid, ask))
    }

    pub async fn tick(&self) -> Result<()> {
        let (taker_bid, taker_ask) = self.fetch_taker_best(&self.config.venues.symbol).await?;
        let Some(plan) = plan_xemm_quotes(
            taker_bid,
            taker_ask,
            self.config.min_profitability,
            self.config.levels,
            self.config.step_size,
        ) else {
            return Ok(());
        };
        if plan.skip {
            return Ok(());
        }
        let exchange = self.config.venues.primary.proto();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.venues.symbol.clone(),
                ..Default::default()
            })
            .await?;
        for (i, bid) in plan.bids.iter().enumerate() {
            let coid = format!("xemm-b-{i}-{}", ulid::Ulid::generate());
            if let Err(err) = self
                .gateway
                .create_order(limit_order(
                    &exchange,
                    &self.config.venues.symbol,
                    coid,
                    true,
                    *bid,
                    self.config.qty,
                ))
                .await
            {
                tracing::error!(error = %err, "XEMM bid placement failed");
            }
        }
        for (i, ask) in plan.asks.iter().enumerate() {
            let coid = format!("xemm-a-{i}-{}", ulid::Ulid::generate());
            if let Err(err) = self
                .gateway
                .create_order(limit_order(
                    &exchange,
                    &self.config.venues.symbol,
                    coid,
                    false,
                    *ask,
                    self.config.qty,
                ))
                .await
            {
                tracing::error!(error = %err, "XEMM ask placement failed");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for Xemm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.venues.symbol,
            maker = %self.config.venues.primary.exchange_id,
            taker = %self.config.venues.hedge.exchange_id,
            min_profitability = %self.config.min_profitability,
            "starting cross-exchange market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "XEMM tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn plan_xemm_quotes_spreads_correctly() {
        let plan =
            plan_xemm_quotes(dec!(100), dec!(102), dec!(0.001), 3, dec!(0.005)).expect("quotes");
        assert!(!plan.skip);
        assert_eq!(plan.bids.len(), 3);
        assert_eq!(plan.asks.len(), 3);
        // Bids are below center (101), asks above.
        assert!(plan.bids.iter().all(|p| *p < dec!(101)));
        assert!(plan.asks.iter().all(|p| *p > dec!(101)));
    }

    #[test]
    fn plan_xemm_quotes_skips_tight_spread() {
        // Spread = 0.01%, less than 2 * 0.001 = 0.002%.
        // Function returns Some with skip=true (not None) when spread is insufficient.
        let plan = plan_xemm_quotes(dec!(100), dec!(100) + dec!(0.01), dec!(0.001), 3, dec!(0.005))
            .expect("plan");
        assert!(plan.skip);
    }

    #[test]
    fn plan_xemm_quotes_rejects_bad_input() {
        assert!(plan_xemm_quotes(dec!(0), dec!(102), dec!(0.001), 3, dec!(0.005)).is_none());
        assert!(plan_xemm_quotes(dec!(100), dec!(99), dec!(0.001), 3, dec!(0.005)).is_none());
        assert!(plan_xemm_quotes(dec!(100), dec!(102), dec!(-0.001), 3, dec!(0.005)).is_none());
    }

    #[test]
    fn is_hedgeable_buy_requires_discount() {
        assert!(is_hedgeable(dec!(99), dec!(100), dec!(102), true, dec!(0.001)));
        assert!(!is_hedgeable(dec!(100), dec!(100), dec!(102), true, dec!(0.001)));
    }

    #[test]
    fn is_hedgeable_sell_requires_premium() {
        assert!(is_hedgeable(dec!(103), dec!(100), dec!(102), false, dec!(0.001)));
        assert!(!is_hedgeable(dec!(102), dec!(100), dec!(102), false, dec!(0.001)));
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nmin_profitability = \"0.002\"\n\
             qty = \"0.01\"\nlevels = 5\n[primary]\nexchange_id = \"binance\"\n\
             [hedge]\nexchange_id = \"okx\"",
        )
        .expect("toml");
        let cfg = XemmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.venues.primary.exchange_id, "binance");
        assert_eq!(cfg.venues.hedge.exchange_id, "okx");
        assert_eq!(cfg.min_profitability, dec!(0.002));
        assert_eq!(cfg.qty, dec!(0.01));
        assert_eq!(cfg.levels, 5);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nstep_size = \"0.01\"\n[primary]\nexchange_id = \"a\"\n\
             [hedge]\nexchange_id = \"b\"",
        )
        .expect("toml");
        let cfg = XemmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.step_size, dec!(0.01));
        assert_eq!(cfg.qty, dec!(1) / dec!(1000));
        assert_eq!(cfg.levels, 3);
    }
}
