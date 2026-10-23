//! VWAP (Volume-Weighted Average Price) order executor.
//!
//! Reads the order book and slices orders proportionally to the displayed
//! volume at each price level within a price cap. Buys/sells more where
//! there is deeper liquidity, reducing slippage relative to simple TWAP.
//!
//! Reference: <https://hummingbot.org/strategies/vwap/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    proto,
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the VWAP executor.
#[derive(Debug, Clone, Deserialize)]
pub struct VwapConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Total amount to execute (in quote units).
    pub total_amount: Decimal,
    /// Maximum price spread from mid to consider (fraction).
    #[serde(default = "default_price_spread")]
    pub price_spread: Decimal,
    /// Percentage of order book volume to take per level.
    #[serde(default = "default_volume_perc")]
    pub volume_perc: Decimal,
    /// Delay between orders (seconds).
    #[serde(default = "default_order_delay")]
    pub order_delay_secs: u64,
    /// True = buy, false = sell.
    #[serde(default = "default_is_buy")]
    pub is_buy: bool,
}

fn default_price_spread() -> Decimal {
    dec!(0.001)
}
fn default_volume_perc() -> Decimal {
    dec!(0.001)
}
fn default_order_delay() -> u64 {
    10
}
fn default_is_buy() -> bool {
    true
}

impl VwapConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// One VWAP child order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VwapOrder {
    pub price: Decimal,
    pub amount: Decimal,
}

/// Plan VWAP child orders from the order book.
///
/// Reads the book up to `price_spread` from mid, then allocates
/// `volume_perc` of the displayed volume at each level.
#[must_use]
pub fn plan_vwap_orders(
    mid: Decimal,
    price_spread: Decimal,
    volume_perc: Decimal,
    total_amount: Decimal,
    book_levels: &[(Decimal, Decimal)], // (price, size) pairs
) -> Option<Vec<VwapOrder>> {
    if mid <= Decimal::ZERO || price_spread < Decimal::ZERO || volume_perc <= Decimal::ZERO {
        return None;
    }
    let cap = mid * price_spread;
    let mut orders = Vec::new();
    let mut remaining = total_amount;
    for (price, size) in book_levels {
        if (*price - mid).abs() > cap {
            continue;
        }
        let amount = *size * volume_perc;
        if amount <= Decimal::ZERO {
            continue;
        }
        let actual = amount.min(remaining);
        orders.push(VwapOrder { price: *price, amount: actual });
        remaining -= actual;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if orders.is_empty() {
        return None;
    }
    Some(orders)
}

pub struct VwapExecutor {
    config: VwapConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    remaining: tokio::sync::Mutex<Decimal>,
    last_order: tokio::sync::Mutex<u64>,
}

impl VwapExecutor {
    pub fn new(
        config: VwapConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let total_amount = config.total_amount;
        Self {
            config,
            gateway,
            market,
            remaining: tokio::sync::Mutex::new(total_amount),
            last_order: tokio::sync::Mutex::new(0),
        }
    }

    async fn current_mid(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(proto::market::FetchTickerRequest {
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

    async fn fetch_book_levels(&self) -> Result<Vec<(Decimal, Decimal)>> {
        let req = proto::market::FetchOrderBookRequest {
            exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
            symbol: self.config.common.symbol.clone(),
            pagination: buffa::MessageField::some(crate::proto::common::Pagination {
                limit: 20,
                ..Default::default()
            }),
            ..Default::default()
        };
        let book = self.market.fetch_order_book(req).await?;
        let mut levels = Vec::new();
        for level in book.bids.iter().chain(book.asks.iter()) {
            if let (Some(price), Some(amount)) = (level.price.as_option(), level.amount.as_option()) &&
                let (Ok(p), Ok(a)) = (
                    longtrader_contract::ext::common_to_decimal(price),
                    longtrader_contract::ext::common_to_decimal(amount),
                ) &&
                p > Decimal::ZERO &&
                a > Decimal::ZERO
            {
                levels.push((p, a));
            }
        }
        Ok(levels)
    }

    pub async fn tick(&self) -> Result<()> {
        let mut remaining = self.remaining.lock().await;
        if *remaining <= Decimal::ZERO {
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut last = self.last_order.lock().await;
        if now < *last + self.config.order_delay_secs {
            return Ok(());
        }

        let mid = self.current_mid().await?;
        let levels = self.fetch_book_levels().await?;
        let Some(orders) = plan_vwap_orders(
            mid,
            self.config.price_spread,
            self.config.volume_perc,
            *remaining,
            &levels,
        ) else {
            return Ok(());
        };

        let exchange = self.config.common.proto_exchange_id();
        for order in &orders {
            let coid = format!("vwap-{}", ulid::Ulid::generate());
            let req = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                self.config.is_buy,
                order.price,
                order.amount,
            );
            if let Err(err) = self.gateway.create_order(req).await {
                tracing::error!(error = %err, "VWAP order failed");
            }
            *remaining -= order.amount;
        }
        *last = now;
        Ok(())
    }
}

#[async_trait]
impl Strategy for VwapExecutor {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            total_amount = %self.config.total_amount,
            price_spread = %self.config.price_spread,
            "starting VWAP executor"
        );
        let poll = std::time::Duration::from_secs(1);
        loop {
            let remaining = *self.remaining.lock().await;
            if remaining <= Decimal::ZERO {
                return Ok(());
            }
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "VWAP tick failed");
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
    fn plan_vwap_orders_allocates_proportionally() {
        let levels = vec![(dec!(100), dec!(10)), (dec!(101), dec!(20)), (dec!(102), dec!(30))];
        let orders =
            plan_vwap_orders(dec!(100), dec!(0.01), dec!(0.1), dec!(100), &levels).expect("orders");
        assert!(!orders.is_empty());
        // First level: 10 * 0.1 = 1.0
        assert_eq!(orders[0].amount, dec!(1));
    }

    #[test]
    fn plan_vwap_respects_price_cap() {
        let levels = vec![(dec!(100), dec!(10)), (dec!(110), dec!(20))];
        let orders =
            plan_vwap_orders(dec!(100), dec!(0.01), dec!(0.1), dec!(100), &levels).expect("orders");
        // Only the first level is within 1% of mid.
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn plan_vwap_rejects_bad_input() {
        assert!(plan_vwap_orders(dec!(0), dec!(0.01), dec!(0.1), dec!(100), &[]).is_none());
        assert!(plan_vwap_orders(dec!(100), dec!(-0.01), dec!(0.1), dec!(100), &[]).is_none());
        assert!(plan_vwap_orders(dec!(100), dec!(0.01), dec!(0), dec!(100), &[]).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ntotal_amount = \"1000\"\nprice_spread = \"0.002\"",
        )
        .expect("toml");
        let cfg = VwapConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.total_amount, dec!(1000));
        assert_eq!(cfg.price_spread, dec!(0.002));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\ntotal_amount = \"500\"\nvolume_perc = \"0.005\"\n\
             order_delay_secs = 5\nis_buy = false",
        )
        .expect("toml");
        let cfg = VwapConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.total_amount, dec!(500));
        assert_eq!(cfg.volume_perc, dec!(5) / dec!(1000));
        assert_eq!(cfg.order_delay_secs, 5);
        assert!(!cfg.is_buy);
    }
}
