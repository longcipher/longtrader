//! Order Book Imbalance (OBI) market maker.
//!
//! Predicts short-term price movement from the relative pressure of bid vs ask
//! sizes at the top of book. The OBI signal feeds a fair-value adjustment
//! around which a tight two-sided quote is centred.
//!
//! ```text
//!   OBI = (bid_size − ask_size) / (bid_size + ask_size)       ∈ [−1, 1]
//!   fair = mid + α · OBI · mid
//!   bid  = fair · (1 − half_spread)
//!   ask  = fair · (1 + half_spread)
//! ```
//!
//! `α` is the ``signal_weight'', how aggressively the strategy tilts on the
//! imbalance. A positive OBI (more bid depth) signals upward pressure, so the
//! fair price moves up and the bid tightens to capture the expected rise before
//! the ask profits on the reversion.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    proto::market,
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the OBI market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct ObiMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Half-spread as a fraction of the fair price.
    #[serde(default = "default_half_spread")]
    pub half_spread: Decimal,
    /// Signal strength: fair = mid + weight · OBI · mid.
    #[serde(default = "default_signal_weight")]
    pub signal_weight: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Look depth (number of book levels) from which OBI is accumulated.
    #[serde(default = "default_depth")]
    pub depth: u32,
    /// Quote size boost factor on the favoured side (the side OBI agrees
    /// with). `1.0` = no boost.
    #[serde(default = "default_size_skew")]
    pub size_skew: Decimal,
}

fn default_half_spread() -> Decimal {
    Decimal::new(5, 4) // 0.5 bp
}
fn default_signal_weight() -> Decimal {
    dec!(0.5)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_depth() -> u32 {
    5
}
fn default_size_skew() -> Decimal {
    dec!(1)
}

impl ObiMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Raw order-book imbalance from the top `depth` levels.
///
/// `None` when both sides are empty — an empty book has no imbalance.
#[must_use]
pub fn compute_obi(bids: &[Decimal], asks: &[Decimal], depth: usize) -> Option<Decimal> {
    let n = depth.min(bids.len()).min(asks.len());
    if n == 0 {
        return None;
    }
    let mut bid_total = Decimal::ZERO;
    let mut ask_total = Decimal::ZERO;
    for i in 0..n {
        bid_total += bids[i];
        ask_total += asks[i];
    }
    let denom = bid_total + ask_total;
    if denom.is_zero() {
        return None;
    }
    Some((bid_total - ask_total) / denom)
}

/// Fair price given mid, OBI, and signal weight.
#[must_use]
pub fn fair_price(mid: Decimal, obi: Decimal, signal_weight: Decimal) -> Decimal {
    mid + signal_weight * obi * mid
}

/// Calculated quotes for the OBI MM strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObiQuotes {
    pub fair: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
}

#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn plan_obi_quotes(
    mid: Decimal,
    obi: Decimal,
    half_spread: Decimal,
    base_qty: Decimal,
    size_skew: Decimal,
) -> Option<ObiQuotes> {
    if mid <= Decimal::ZERO || half_spread < Decimal::ZERO {
        return None;
    }
    let fair = fair_price(mid, obi, size_skew);
    let bid = fair * (Decimal::ONE - half_spread);
    let ask = fair * (Decimal::ONE + half_spread);
    // Skew size: when OBI is positive (bid pressure), boost the bid.
    let skew_factor = if obi.is_sign_positive() {
        Decimal::ONE + size_skew * obi
    } else {
        Decimal::ONE - size_skew * obi.abs()
    };
    let bid_qty = base_qty * skew_factor.max(Decimal::ZERO);
    let ask_qty = base_qty;
    Some(ObiQuotes { fair, bid, ask, bid_qty, ask_qty })
}

pub struct ObiMm {
    config: ObiMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl ObiMm {
    pub fn new(
        config: ObiMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// Fetch the top-of-book snapshot and extract the first `depth` price
    /// levels as size vectors.
    async fn fetch_book_sizes(&self) -> Result<(Vec<Decimal>, Vec<Decimal>)> {
        let req = market::FetchOrderBookRequest {
            exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
            symbol: self.config.common.symbol.clone(),
            pagination: buffa::MessageField::some(crate::proto::common::Pagination {
                limit: u64::from(self.config.depth),
                ..Default::default()
            }),
            ..Default::default()
        };
        let book = self.market.fetch_order_book(req).await?;
        let bids = Self::extract_sizes(&book.bids);
        let asks = Self::extract_sizes(&book.asks);
        Ok((bids, asks))
    }

    fn extract_sizes(levels: &[crate::proto::market::PriceLevel]) -> Vec<Decimal> {
        levels
            .iter()
            .filter_map(|l| l.amount.as_option())
            .filter_map(|d| longtrader_contract::ext::common_to_decimal(d).ok())
            .filter(|q| *q > Decimal::ZERO)
            .collect()
    }

    /// Latest mid from book or ticker.
    async fn current_mid(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let price = longtrader_contract::ext::common_to_decimal(last)?;
            if price > Decimal::ZERO {
                return Ok(price);
            }
        }
        color_eyre::eyre::bail!("no usable mid price")
    }

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let (bids, asks) = self.fetch_book_sizes().await?;
        let Some(obi) = compute_obi(&bids, &asks, self.config.depth as usize) else {
            tracing::debug!("empty book — skipping OBI quote");
            return Ok(());
        };
        let Some(quotes) = plan_obi_quotes(
            mid,
            obi,
            self.config.half_spread,
            self.config.qty,
            self.config.signal_weight,
        ) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        // Cancel-and-replace: in a production venue this would be an amend, but
        // the worker's ports only offer cancel-all, so we cancel and re-place.
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        let bid_coid = format!("obimm-{}", ulid::Ulid::generate());
        let ask_coid = format!("obimm-{}", ulid::Ulid::generate());
        let req_bid = limit_order(
            &exchange,
            &self.config.common.symbol,
            bid_coid,
            true,
            quotes.bid,
            quotes.bid_qty,
        );
        let req_ask = limit_order(
            &exchange,
            &self.config.common.symbol,
            ask_coid,
            false,
            quotes.ask,
            quotes.ask_qty,
        );
        if let Err(err) = self.gateway.create_order(req_bid).await {
            tracing::error!(error = %err, "OBI bid placement failed");
        }
        if let Err(err) = self.gateway.create_order(req_ask).await {
            tracing::error!(error = %err, "OBI ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for ObiMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            half_spread = %self.config.half_spread,
            signal_weight = %self.config.signal_weight,
            "starting OBI market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "OBI tick failed");
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
    fn obi_balanced_book_is_zero() {
        assert_eq!(compute_obi(&[dec!(10), dec!(10)], &[dec!(10), dec!(10)], 2), Some(dec!(0)));
    }

    #[test]
    fn obi_bid_heavy_is_positive() {
        assert_eq!(compute_obi(&[dec!(20)], &[dec!(10)], 1), Some(dec!(1) / dec!(3)));
    }

    #[test]
    fn obi_ask_heavy_is_negative() {
        assert_eq!(compute_obi(&[dec!(10)], &[dec!(20)], 1), Some(-dec!(1) / dec!(3)));
    }

    #[test]
    fn obi_empty_book_is_none() {
        assert!(compute_obi(&[], &[], 5).is_none());
    }

    #[test]
    fn obi_respects_depth_cap() {
        // Depth=1 → only the first level counts.
        assert_eq!(
            compute_obi(&[dec!(100), dec!(1)], &[dec!(1), dec!(100)], 1),
            Some(dec!(99) / dec!(101))
        );
    }

    #[test]
    fn fair_price_tilts_with_obi() {
        // OBI = +0.5, weight = 0.5 → fair = 100 + 0.5*0.5*100 = 125.
        assert_eq!(fair_price(dec!(100), dec!(0.5), dec!(0.5)), dec!(125));
        // OBI = −0.3, weight = 0.5 → fair = 100 − 0.5*0.3*100 = 85.
        assert_eq!(fair_price(dec!(100), -dec!(0.3), dec!(0.5)), dec!(85));
    }

    #[test]
    fn plan_obi_quotes_centre_on_fair() {
        let Some(q) = plan_obi_quotes(dec!(100), dec!(0.5), dec!(0.01), dec!(1), dec!(0.5)) else {
            panic!("plan should succeed");
        };
        assert_eq!(q.fair, dec!(125));
        assert_eq!(q.bid, dec!(123.75));
        assert_eq!(q.ask, dec!(126.25));
    }

    #[test]
    fn plan_obi_quotes_rejects_bad_input() {
        assert!(plan_obi_quotes(dec!(0), dec!(0.5), dec!(0.01), dec!(1), dec!(0.5)).is_none());
        assert!(plan_obi_quotes(dec!(100), dec!(0.5), dec!(-0.01), dec!(1), dec!(0.5)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nhalf_spread = \"0.001\"\nsignal_weight = \"0.3\"",
        )
        .expect("toml");
        let cfg = ObiMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.half_spread, dec!(0.001));
        assert_eq!(cfg.signal_weight, dec!(0.3));
        assert_eq!(cfg.qty, Decimal::new(1, 3));
        assert_eq!(cfg.depth, 5);
    }

    #[test]
    fn config_fields_override_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"ETH/USDT\"\nhalf_spread = \"0.002\"\nsignal_weight = \"0.8\"\
             \nqty = \"0.5\"\ndepth = 10\nsize_skew = \"0.3\"",
        )
        .expect("toml");
        let cfg = ObiMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.5));
        assert_eq!(cfg.depth, 10);
        assert_eq!(cfg.size_skew, dec!(0.3));
    }
}
