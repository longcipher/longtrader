//! Grid Strike — grid-based directional strategy with strike prices.
//!
//! Places a grid of orders between a lower and upper price band. Each
//! filled order is reversed at a profit spread. Unlike a pure market
//! maker, Grid Strike is directional and holds inventory, aiming to
//! accumulate base assets in a downtrend and distribute in an uptrend.
//!
//! Reference: <https://hummingbot.org/strategies/grid-strike/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the Grid Strike strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct GridStrikeConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Lower price of the grid.
    pub lower_price: Decimal,
    /// Upper price of the grid.
    pub upper_price: Decimal,
    /// Number of grid levels.
    #[serde(default = "default_levels")]
    pub levels: u32,
    /// Quantity per grid order.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Profit spread as a fraction (reverse at fill + spread).
    #[serde(default = "default_profit_spread")]
    pub profit_spread: Decimal,
}

fn default_levels() -> u32 {
    5
}
fn default_qty() -> Decimal {
    dec!(1)
}
fn default_profit_spread() -> Decimal {
    dec!(0.01)
}

impl GridStrikeConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Plan grid levels (prices only, evenly spaced).
#[must_use]
pub fn plan_grid_strike(lower: Decimal, upper: Decimal, levels: u32) -> Option<Vec<Decimal>> {
    if lower <= Decimal::ZERO || upper <= lower || levels == 0 {
        return None;
    }
    let step = (upper - lower) / Decimal::from(levels + 1);
    let mut prices = Vec::with_capacity(levels as usize);
    for i in 1..=levels {
        prices.push(lower + step * Decimal::from(i));
    }
    Some(prices)
}

/// Reverse price for a filled grid order.
#[must_use]
pub fn reverse_price(side: bool, fill_price: Decimal, profit_spread: Decimal) -> Decimal {
    if side {
        // Buy fill: place sell at fill * (1 + spread).
        fill_price * (Decimal::ONE + profit_spread)
    } else {
        // Sell fill: place buy at fill * (1 - spread).
        fill_price * (Decimal::ONE - profit_spread)
    }
}

/// Grid Strike strategy.
pub struct GridStrike {
    config: GridStrikeConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl GridStrike {
    pub fn new(
        config: GridStrikeConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    async fn current_mid(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let p = longtrader_contract::ext::common_to_decimal(last)?;
            if p > Decimal::ZERO {
                return Ok(p);
            }
        }
        color_eyre::eyre::bail!("no usable mid")
    }

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let Some(prices) =
            plan_grid_strike(self.config.lower_price, self.config.upper_price, self.config.levels)
        else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;

        // Place buy orders below mid, sell orders above mid.
        for price in &prices {
            let coid = format!("gridst-{}", ulid::Ulid::generate());
            let is_buy = *price < mid;
            if let Err(err) = self
                .gateway
                .create_order(limit_order(
                    &exchange,
                    &self.config.common.symbol,
                    coid,
                    is_buy,
                    *price,
                    self.config.qty,
                ))
                .await
            {
                tracing::error!(error = %err, "grid strike order failed");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for GridStrike {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            lower = %self.config.lower_price,
            upper = %self.config.upper_price,
            levels = %self.config.levels,
            "starting grid strike"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "grid strike tick failed");
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
    fn plan_grid_strike_evenly_spaced() {
        let prices = plan_grid_strike(dec!(90), dec!(110), 4).expect("prices");
        assert_eq!(prices.len(), 4);
        assert_eq!(prices[0], dec!(94));
        assert_eq!(prices[3], dec!(106));
    }

    #[test]
    fn plan_grid_strike_rejects_bad_input() {
        assert!(plan_grid_strike(dec!(0), dec!(100), 4).is_none());
        assert!(plan_grid_strike(dec!(100), dec!(90), 4).is_none());
        assert!(plan_grid_strike(dec!(90), dec!(110), 0).is_none());
    }

    #[test]
    fn reverse_price_buy_fills_above() {
        assert_eq!(reverse_price(true, dec!(100), dec!(0.01)), dec!(101));
    }

    #[test]
    fn reverse_price_sell_fills_below() {
        assert_eq!(reverse_price(false, dec!(100), dec!(0.01)), dec!(99));
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nlower_price = \"90\"\nupper_price = \"110\"\nlevels = 6",
        )
        .expect("toml");
        let cfg = GridStrikeConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.lower_price, dec!(90));
        assert_eq!(cfg.upper_price, dec!(110));
        assert_eq!(cfg.levels, 6);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nlower_price = \"1800\"\nupper_price = \"2200\"\nqty = \"0.5\"\nprofit_spread = \"0.02\"",
        )
        .expect("toml");
        let cfg = GridStrikeConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.5));
        assert_eq!(cfg.profit_spread, dec!(0.02));
    }
}
