//! TWAP (Time-Weighted Average Price) order executor.
//!
//! Splits a large order into N equal child orders placed at regular time
//! intervals to minimize market impact. Each child is sized identically:
//! `total_amount / N`. The interval between orders is `total_time / N`.
//!
//! Supports two modes: taker (market orders) and maker (limit orders with
//! a configurable buffer below/above mid). Used for accumulating or
//! distributing large positions without moving the market.
//!
//! Reference: <https://hummingbot.org/strategies/twap/>

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

/// Configuration for the TWAP executor.
#[derive(Debug, Clone, Deserialize)]
pub struct TwapConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Total amount to execute (in base units).
    pub total_amount: Decimal,
    /// Total time horizon (seconds).
    #[serde(default = "default_duration")]
    pub duration_secs: u64,
    /// Number of child orders.
    #[serde(default = "default_num_orders")]
    pub num_orders: u32,
    /// True = buy, false = sell.
    #[serde(default = "default_is_buy")]
    pub is_buy: bool,
    /// Place as maker limit orders (true) or taker market orders (false).
    #[serde(default = "default_is_maker")]
    pub is_maker: bool,
    /// Limit-order buffer as fraction of mid (for maker mode).
    #[serde(default = "default_limit_buffer")]
    pub limit_buffer: Decimal,
}

fn default_duration() -> u64 {
    600
}
fn default_num_orders() -> u32 {
    10
}
fn default_is_buy() -> bool {
    true
}
fn default_is_maker() -> bool {
    true
}
fn default_limit_buffer() -> Decimal {
    Decimal::new(5, 4)
}

impl TwapConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// One step of the TWAP execution plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TwapStep {
    pub index: u32,
    pub amount: Decimal,
    pub interval_secs: Decimal,
}

/// Build the TWAP execution plan.
///
/// `None` when parameters are invalid.
#[must_use]
pub fn plan_twap(
    total_amount: Decimal,
    num_orders: u32,
    duration_secs: u64,
) -> Option<Vec<TwapStep>> {
    if total_amount <= Decimal::ZERO || num_orders == 0 || duration_secs == 0 {
        return None;
    }
    let amount_per_order = total_amount / Decimal::from(num_orders);
    let interval = Decimal::from(duration_secs) / Decimal::from(num_orders);
    let mut steps = Vec::with_capacity(num_orders as usize);
    for i in 0..num_orders {
        steps.push(TwapStep { index: i, amount: amount_per_order, interval_secs: interval });
    }
    Some(steps)
}

/// TWAP execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwapState {
    Active,
    Complete,
}

pub struct TwapExecutor {
    config: TwapConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    state: tokio::sync::Mutex<TwapState>,
    next_index: tokio::sync::Mutex<u32>,
    next_time: tokio::sync::Mutex<u64>,
}

impl TwapExecutor {
    pub fn new(
        config: TwapConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            state: tokio::sync::Mutex::new(TwapState::Active),
            next_index: tokio::sync::Mutex::new(0),
            next_time: tokio::sync::Mutex::new(0),
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

    pub async fn tick(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if *state == TwapState::Complete {
            return Ok(());
        }
        let mut next_index = self.next_index.lock().await;
        let mut next_time = self.next_time.lock().await;

        if *next_index >= self.config.num_orders {
            *state = TwapState::Complete;
            return Ok(());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now < *next_time {
            return Ok(());
        }

        let step_amount = self.config.total_amount / Decimal::from(self.config.num_orders);
        let exchange = self.config.common.proto_exchange_id();
        let coid = format!("twap-{}-{}", *next_index, ulid::Ulid::generate());

        let mid = self.current_mid().await?;
        if self.config.is_maker {
            let price = if self.config.is_buy {
                mid * (Decimal::ONE - self.config.limit_buffer)
            } else {
                mid * (Decimal::ONE + self.config.limit_buffer)
            };
            let req = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                self.config.is_buy,
                price,
                step_amount,
            );
            if let Err(err) = self.gateway.create_order(req).await {
                tracing::error!(error = %err, "TWAP limit order failed");
            }
        } else {
            // Taker: market order via a very aggressive limit.
            let price = if self.config.is_buy {
                mid * (Decimal::ONE + dec!(0.05))
            } else {
                mid * (Decimal::ONE - dec!(0.05))
            };
            let req = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                self.config.is_buy,
                price,
                step_amount,
            );
            if let Err(err) = self.gateway.create_order(req).await {
                tracing::error!(error = %err, "TWAP market order failed");
            }
        }

        *next_index += 1;
        *next_time = now + self.config.duration_secs / u64::from(self.config.num_orders);
        Ok(())
    }
}

#[async_trait]
impl Strategy for TwapExecutor {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            total_amount = %self.config.total_amount,
            num_orders = %self.config.num_orders,
            duration_secs = %self.config.duration_secs,
            "starting TWAP executor"
        );
        let poll = std::time::Duration::from_secs(1);
        loop {
            let state = *self.state.lock().await;
            if state == TwapState::Complete {
                return Ok(());
            }
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "TWAP tick failed");
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
    fn plan_twap_splits_evenly() {
        let steps = plan_twap(dec!(10), 5, 600).expect("steps");
        assert_eq!(steps.len(), 5);
        assert_eq!(steps[0].amount, dec!(2));
        assert_eq!(steps[0].interval_secs, dec!(120));
    }

    #[test]
    fn plan_twap_rejects_bad_input() {
        assert!(plan_twap(dec!(0), 5, 600).is_none());
        assert!(plan_twap(dec!(10), 0, 600).is_none());
        assert!(plan_twap(dec!(10), 5, 0).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ntotal_amount = \"1.0\"\nnum_orders = 20\nduration_secs = 3600",
        )
        .expect("toml");
        let cfg = TwapConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.total_amount, dec!(1));
        assert_eq!(cfg.num_orders, 20);
        assert_eq!(cfg.duration_secs, 3600);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\ntotal_amount = \"0.5\"\nis_buy = false\n\
             is_maker = false\nlimit_buffer = \"0.01\"",
        )
        .expect("toml");
        let cfg = TwapConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.total_amount, dec!(0.5));
        assert!(!cfg.is_buy);
        assert!(!cfg.is_maker);
        assert_eq!(cfg.limit_buffer, dec!(0.01));
    }
}
