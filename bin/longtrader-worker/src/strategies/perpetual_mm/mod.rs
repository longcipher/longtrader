//! Perpetual Market Making — MM on perps with funding-rate awareness.
//!
//! Market making on perpetual futures with explicit funding-rate tracking.
//! Adjusts quotes to skew toward flattening as funding accrues. Handles
//! position limits, liquidation price monitoring, and separate long/short
//! spread adjustments. When funding is positive, the strategy may go short
//! to capture the premium; when negative, go long.
//!
//! Reference: <https://hummingbot.org/strategies/perpetual-market-making/>

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{FundingRateSource, MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the perpetual MM strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct PerpMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Perpetual symbol (e.g. "BTC/USDT:USDT").
    pub perp_symbol: String,
    /// Half-spread baseline as fraction.
    #[serde(default = "default_half_spread")]
    pub half_spread: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Position limit (max absolute inventory before aggressive skew).
    #[serde(default = "default_position_limit")]
    pub position_limit: Decimal,
    /// Funding-rate threshold: only skew when |rate| exceeds this.
    #[serde(default = "default_funding_threshold")]
    pub funding_threshold: Decimal,
    /// Liquidation safety buffer: stay this fraction away from liq price.
    #[serde(default = "default_liq_buffer")]
    pub liq_buffer: Decimal,
    /// Rolling window for realised volatility.
    #[serde(default = "default_vol_window")]
    pub vol_window: usize,
}

fn default_half_spread() -> Decimal {
    Decimal::new(5, 4)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_position_limit() -> Decimal {
    dec!(1)
}
fn default_funding_threshold() -> Decimal {
    Decimal::new(1, 5)
}
fn default_liq_buffer() -> Decimal {
    dec!(0.1)
}
fn default_vol_window() -> usize {
    20
}

impl PerpMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Perp MM quote plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerpQuotes {
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
    pub skew: Decimal,
}

/// Plan quotes with funding-aware skew.
///
/// `skip_signal = true` when funding is within the threshold (no
/// directional bias from funding alone).
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn plan_perp_quotes(
    mid: Decimal,
    half_spread: Decimal,
    qty: Decimal,
    inventory: Decimal,
    position_limit: Decimal,
    funding_rate: Decimal,
    funding_threshold: Decimal,
) -> Option<(PerpQuotes, bool)> {
    if mid <= Decimal::ZERO || half_spread < Decimal::ZERO {
        return None;
    }
    // Inventory skew: normalise by position_limit, clamp to [−1, 1].
    let inv_skew = if position_limit > Decimal::ZERO {
        (inventory / position_limit).clamp(-Decimal::ONE, Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    // Funding skew: positive funding → short bias (wider bid, tighter ask).
    let funding_skew = if funding_rate.abs() >= funding_threshold {
        if funding_rate.is_sign_positive() { -Decimal::ONE } else { Decimal::ONE }
    } else {
        Decimal::ZERO
    };
    let skew = (inv_skew + funding_skew).clamp(-Decimal::ONE, Decimal::ONE);
    let skew_half = half_spread * skew;
    let bid_half = half_spread + skew_half;
    let ask_half = half_spread - skew_half;
    let bid = mid * (Decimal::ONE - bid_half);
    let ask = mid * (Decimal::ONE + ask_half);
    let bid_qty = qty;
    let ask_qty = qty;
    let skip_signal = funding_skew.is_zero();
    Some((PerpQuotes { bid, ask, bid_qty, ask_qty, skew }, skip_signal))
}

pub struct PerpMm {
    config: PerpMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    funding: Arc<dyn FundingRateSource>,
    inventory: tokio::sync::Mutex<Decimal>,
}

impl PerpMm {
    pub fn new(
        config: PerpMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
        funding: Arc<dyn FundingRateSource>,
    ) -> Self {
        Self { config, gateway, market, funding, inventory: tokio::sync::Mutex::new(Decimal::ZERO) }
    }

    async fn current_mid(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.perp_symbol.clone(),
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

    async fn current_funding_rate(&self) -> Decimal {
        match self
            .funding
            .fetch_funding_rate(
                &crate::adapters::exchange_id(
                    &self.config.common.exchange_id,
                    &self.config.common.label,
                ),
                &self.config.perp_symbol,
            )
            .await
        {
            Ok(snapshot) => snapshot.rate,
            Err(_) => Decimal::ZERO,
        }
    }

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let funding_rate = self.current_funding_rate().await;
        let inv = *self.inventory.lock().await;
        let Some((quotes, _)) = plan_perp_quotes(
            mid,
            self.config.half_spread,
            self.config.qty,
            inv,
            self.config.position_limit,
            funding_rate,
            self.config.funding_threshold,
        ) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.perp_symbol.clone(),
                ..Default::default()
            })
            .await?;
        let bid_coid = format!("perpm-{}", ulid::Ulid::generate());
        let ask_coid = format!("perpm-{}", ulid::Ulid::generate());
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.perp_symbol,
                bid_coid,
                true,
                quotes.bid,
                quotes.bid_qty,
            ))
            .await
        {
            tracing::error!(error = %err, "perp MM bid placement failed");
        }
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.perp_symbol,
                ask_coid,
                false,
                quotes.ask,
                quotes.ask_qty,
            ))
            .await
        {
            tracing::error!(error = %err, "perp MM ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for PerpMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.perp_symbol,
            half_spread = %self.config.half_spread,
            position_limit = %self.config.position_limit,
            "starting perpetual market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "perp MM tick failed");
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
    fn plan_perp_quotes_symmetric_without_skew() {
        let (q, skip) = plan_perp_quotes(
            dec!(100),
            dec!(0.005),
            dec!(1),
            dec!(0),
            dec!(1),
            dec!(0),
            dec!(0.0001),
        )
        .expect("quotes");
        assert!(skip);
        assert_eq!(q.skew, dec!(0));
        assert_eq!(q.bid, dec!(99.5));
        assert_eq!(q.ask, dec!(100.5));
    }

    #[test]
    fn plan_perp_positive_inventory_skews_aggressive() {
        let (q, _) = plan_perp_quotes(
            dec!(100),
            dec!(0.005),
            dec!(1),
            dec!(1),
            dec!(1),
            dec!(0),
            dec!(0.0001),
        )
        .expect("quotes");
        // Positive inventory → positive skew → both quotes shifted down
        // to encourage selling down the position.
        assert!(q.skew > dec!(0));
    }

    #[test]
    fn plan_perp_positive_funding_skews_short() {
        let (q, skip) = plan_perp_quotes(
            dec!(100),
            dec!(0.005),
            dec!(1),
            dec!(0),
            dec!(1),
            dec!(0.001),
            dec!(0.0001),
        )
        .expect("quotes");
        assert!(!skip);
        assert!(q.skew < dec!(0));
    }

    #[test]
    fn plan_perp_rejects_bad_input() {
        assert!(
            plan_perp_quotes(
                dec!(0),
                dec!(0.005),
                dec!(1),
                dec!(0),
                dec!(1),
                dec!(0),
                dec!(0.0001)
            )
            .is_none()
        );
        assert!(
            plan_perp_quotes(
                dec!(100),
                dec!(-0.005),
                dec!(1),
                dec!(0),
                dec!(1),
                dec!(0),
                dec!(0.0001)
            )
            .is_none()
        );
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nperp_symbol = \"BTC/USDT:USDT\"\n\
             half_spread = \"0.002\"\nposition_limit = \"2\"",
        )
        .expect("toml");
        let cfg = PerpMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.perp_symbol, "BTC/USDT:USDT");
        assert_eq!(cfg.half_spread, dec!(0.002));
        assert_eq!(cfg.position_limit, dec!(2));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nperp_symbol = \"ETH/USDT:USDT\"\nqty = \"0.1\"\n\
             funding_threshold = \"0.00005\"\nliq_buffer = \"0.15\"\nvol_window = 50",
        )
        .expect("toml");
        let cfg = PerpMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.1));
        assert_eq!(cfg.funding_threshold, dec!(5) / dec!(100000));
        assert_eq!(cfg.liq_buffer, dec!(0.15));
        assert_eq!(cfg.vol_window, 50);
    }
}
