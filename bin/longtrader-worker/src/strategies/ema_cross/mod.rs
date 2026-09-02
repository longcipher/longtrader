//! EMA crossover strategy over the worker candle-poll model.
//!
//! Polls candles, computes fast/slow EMAs, and submits a market order when
//! the pair crosses. Cross state is edge-triggered in-memory: after a
//! restart the strategy waits for the next fresh cross instead of replaying
//! the last one.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_closes_of, market_order, params_from_table},
};

/// Config for the worker EMA-cross strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct EmaCrossConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_fast")]
    pub fast_window: usize,
    #[serde(default = "default_slow")]
    pub slow_window: usize,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_fast() -> usize {
    9
}
const fn default_slow() -> usize {
    21
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl EmaCrossConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrossState {
    Unknown,
    FastAbove,
    FastBelow,
}

/// Worker EMA-cross strategy.
pub struct EmaCross {
    config: EmaCrossConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    cross_state: Mutex<CrossState>,
}

impl EmaCross {
    pub fn new(
        config: EmaCrossConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, cross_state: Mutex::new(CrossState::Unknown) }
    }

    /// One poll cycle: fetch candles, detect a fresh cross, trade it.
    pub async fn tick(&self) -> Result<()> {
        let candles = fetch_closes_of(
            self.market.as_ref(),
            &self.config.common.proto_exchange_id(),
            &self.config.common.symbol,
            &self.config.common.timeframe,
        )
        .await?;
        let mut fast = Ema::new(self.config.fast_window);
        let mut slow = Ema::new(self.config.slow_window);
        for close in &candles {
            fast.push(*close);
            slow.push(*close);
        }
        let (Some(f), Some(s)) = (fast.value(), slow.value()) else {
            tracing::debug!("ema-cross warmup incomplete");
            return Ok(());
        };
        let new_state = match f.cmp(&s) {
            std::cmp::Ordering::Greater => CrossState::FastAbove,
            std::cmp::Ordering::Less => CrossState::FastBelow,
            std::cmp::Ordering::Equal => return Ok(()),
        };

        let mut state = self.cross_state.lock().await;
        let crossed_up = *state != CrossState::FastAbove && new_state == CrossState::FastAbove;
        let crossed_down = *state != CrossState::FastBelow && new_state == CrossState::FastBelow;
        *state = new_state;
        drop(state);

        let is_buy = if crossed_up {
            true
        } else if crossed_down {
            false
        } else {
            return Ok(());
        };

        let coid = format!("emacross-{}", ulid::Ulid::generate());
        let request = market_order(
            &self.config.common.proto_exchange_id(),
            &self.config.common.symbol,
            coid,
            is_buy,
            self.config.qty,
        );
        self.gateway.create_order(request).await?;
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, "ema-cross signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for EmaCross {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "ema-cross tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table = toml::from_str(
            r#"
            exchange_id = "binance"
            symbol = "BTCUSDT"
            timeframe = "5m"
            poll_secs = 15
            fast_window = 5
            slow_window = 20
            qty = "0.002"
            "#,
        )
        .expect("valid toml");
        let cfg = EmaCrossConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.common.symbol, "BTCUSDT");
        assert_eq!(cfg.fast_window, 5);
        assert_eq!(cfg.qty.to_string(), "0.002");
        assert_eq!(cfg.common.poll_secs, 15);
    }

    #[test]
    fn missing_symbol_fails_config() {
        let table: toml::Table = toml::from_str("qty = \"1\"").expect("valid toml");
        assert!(EmaCrossConfig::from_params(&table).is_err());
    }
}
