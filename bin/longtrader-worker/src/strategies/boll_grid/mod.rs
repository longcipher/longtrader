//! Bollinger-band grid strategy over the worker candle-poll model.
//!
//! Polls candles, computes Bollinger bands over the close window, and when
//! the latest close sits inside sufficiently wide bands, replaces the
//! ladder: `grid_num` buy limits below and sell limits above the close.
//! Filled legs are detected via open-order reconciliation (same pattern as
//! `simple_grid`) and reversed at ±profit spread.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::{Bands, BollingerBands},
    ports::{MarketDataSource, TradingGateway},
    strategies::{
        CommonParams, Strategy, closes_of, fetch_candles, limit_order, params_from_table,
    },
};

/// Config for the worker Bollinger grid.
#[derive(Debug, Clone, Deserialize)]
pub struct BollGridConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_window")]
    pub boll_window: usize,
    #[serde(default = "default_mult")]
    pub boll_mult: Decimal,
    #[serde(default = "default_levels")]
    pub grid_num: u32,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Reverse-leg spread as a fraction of price.
    #[serde(default = "default_spread")]
    pub profit_spread_pct: Decimal,
}

const fn default_window() -> usize {
    21
}
fn default_mult() -> Decimal {
    Decimal::new(2, 0)
}
const fn default_levels() -> u32 {
    3
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_spread() -> Decimal {
    Decimal::new(5, 4)
}

impl BollGridConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Pure ladder planner: `(price, is_buy)` pairs around the close.
///
/// Levels falling outside the bands are dropped (quotes are confined to
/// `[lower, upper]`).
#[must_use]
pub fn plan_ladder(close: Decimal, bands: &Bands, grid_num: u32) -> Vec<(Decimal, bool)> {
    let width = bands.upper - bands.lower;
    if width <= Decimal::ZERO || close > bands.upper || close < bands.lower {
        return Vec::new();
    }
    let step = width / Decimal::from(grid_num.saturating_add(1));
    let mut plan = Vec::with_capacity(usize::try_from(grid_num).unwrap_or(0) * 2);
    for level in 1..=grid_num {
        let offset = step * Decimal::from(level);
        let bid = close - offset;
        let ask = close + offset;
        if bid > dec!(0) && bid >= bands.lower {
            plan.push((bid, true));
        }
        if ask <= bands.upper {
            plan.push((ask, false));
        }
    }
    plan
}

/// Worker Bollinger grid strategy.
pub struct BollGrid {
    config: BollGridConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl BollGrid {
    pub fn new(
        config: BollGridConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// One poll cycle: recompute bands and replace the ladder.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            u32::try_from(self.config.boll_window).unwrap_or(200),
        )
        .await?;
        let closes = closes_of(&candles);
        let Some(bands) = BollingerBands::compute(&closes, self.config.boll_mult) else {
            tracing::debug!("boll-grid warmup incomplete");
            return Ok(());
        };
        let Some(close) = closes.last().copied() else {
            return Ok(());
        };

        // Cancel previous ladder before quoting the new one (kill-switch
        // path also clears anything stale on other symbols).
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;

        for (price, is_buy) in plan_ladder(close, &bands, self.config.grid_num) {
            let coid = format!("bollgrid-{}", ulid::Ulid::generate());
            let request = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                price,
                self.config.qty,
            );
            if let Err(err) = self.gateway.create_order(request).await {
                tracing::error!(price = %price, error = %err, "ladder leg failed");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for BollGrid {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "boll-grid tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_plans_both_sides_inside_bands() {
        // width 40 / (3+1) = step 10 → clean in-band levels.
        let bands = Bands { middle: dec!(100), upper: dec!(120), lower: dec!(80) };
        let plan = plan_ladder(dec!(100), &bands, 3);
        assert_eq!(
            plan,
            vec![(dec!(90), true), (dec!(110), false), (dec!(80), true), (dec!(120), false),]
        );
    }

    #[test]
    fn ladder_drops_levels_outside_bands() {
        let bands = Bands { middle: dec!(100), upper: dec!(105), lower: dec!(95) };
        // width 10 / 3 ≈ 3.33: second levels fall outside the band.
        let plan = plan_ladder(dec!(100), &bands, 2);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].1, true);
        assert_eq!(plan[1].1, false);
    }

    #[test]
    fn ladder_skips_when_close_outside_bands() {
        let bands = Bands { middle: dec!(100), upper: dec!(110), lower: dec!(90) };
        assert!(plan_ladder(dec!(120), &bands, 3).is_empty());
        assert!(plan_ladder(dec!(80), &bands, 3).is_empty());
    }

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\ngrid_num = 4\nqty = \"0.01\"")
                .expect("valid toml");
        let cfg = BollGridConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.grid_num, 4);
        assert_eq!(cfg.qty.to_string(), "0.01");
    }
}
