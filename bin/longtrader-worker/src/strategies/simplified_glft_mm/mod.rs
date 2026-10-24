//! Simplified GLFT market maker.
//!
//! A lighter variant of the full GLFT model that skips the order-arrival
//! calibration. The half-spread is taken directly from realised volatility:
//!
//! ```text
//!   half_spread = vol_to_spread · σ
//! ```
//!
//! and the reservation price is obtained by combining a micro-price (the
//! bid-ask weighted midpoint) with an Avellaneda-Style inventory skew:
//!
//! ```text
//!   mid_micro = (bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)
//!   R        = mid_micro − q · γ · σ² · (T − t)
//!   bid      = R − half_spread
//!   ask      = R + half_spread
//! ```
//!
//! Calibration-free `vol_to_spread` replaces the full arrival/decay model
//! and is practically robust across a wide range of assets; the inventory
//! term keeps exposure bounded.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::Ema,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, cancel_all_orders, limit_order, params_from_table},
};

/// Configuration for the simplified GLFT market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct SimplifiedGlftMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Half-spread as `vol_to_spread · σ`.
    #[serde(default = "default_vol_to_spread")]
    pub vol_to_spread: Decimal,
    /// Risk-aversion γ (inventory penalty).
    #[serde(default = "default_gamma")]
    pub gamma: Decimal,
    /// Annualised/per-bar volatility σ (used as a fallback before the EMA
    /// estimator warms up).
    #[serde(default = "default_sigma")]
    pub sigma: Decimal,
    /// Remaining horizon in matching units.
    #[serde(default = "default_horizon")]
    pub time_horizon: Decimal,
    /// Quote quantity per side.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Rolling window for realised volatility estimation.
    #[serde(default = "default_vol_window")]
    pub vol_window: usize,
    /// Micro-price weighting: how much to tilt mid towards the side with
    /// more depth. `0` = simple mid, `1` = purely micro-price.
    #[serde(default = "default_micro_weight")]
    pub micro_weight: Decimal,
}

fn default_vol_to_spread() -> Decimal {
    dec!(0.5)
}
fn default_gamma() -> Decimal {
    dec!(0.1)
}
fn default_sigma() -> Decimal {
    dec!(1)
}
fn default_horizon() -> Decimal {
    dec!(1)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_vol_window() -> usize {
    20
}
fn default_micro_weight() -> Decimal {
    dec!(0.5)
}

impl SimplifiedGlftMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Micro-price: the size-weighted midpoint between best bid and ask.
///
/// ```text
///   mid_micro = (bid_price · ask_qty + ask_price · bid_qty) / (bid_qty + ask_qty)
/// ```
///
/// When one side is heavier, the micro-price tilts toward it, anticipating a
/// short-term move. Returns `None` when either quantity is zero (empty book).
#[must_use]
pub fn micro_price(
    bid: Decimal,
    ask: Decimal,
    bid_qty: Decimal,
    ask_qty: Decimal,
) -> Option<Decimal> {
    let total_qty = bid_qty + ask_qty;
    if total_qty.is_zero() || bid <= Decimal::ZERO || ask <= Decimal::ZERO {
        return None;
    }
    Some((bid * ask_qty + ask * bid_qty) / total_qty)
}

/// Blend between simple mid and micro-price.
#[must_use]
pub fn blended_mid(simple_mid: Decimal, micro_mid: Option<Decimal>, weight: Decimal) -> Decimal {
    match micro_mid {
        Some(micro_mid) => simple_mid * (Decimal::ONE - weight) + micro_mid * weight,
        None => simple_mid,
    }
}

/// Inventory skew: the same AS control but parameterised by realised
/// volatility instead of calibration.
#[must_use]
pub fn simplified_skew(
    inventory: Decimal,
    gamma: Decimal,
    sigma: Decimal,
    horizon: Decimal,
) -> Decimal {
    inventory * gamma * sigma * sigma * horizon
}

/// Quote plan for the simplified GLFT MM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimplifiedGlftQuotes {
    pub bid: Decimal,
    pub ask: Decimal,
    pub half_spread: Decimal,
}

#[must_use]
pub fn plan_simplified_glft_quotes(
    mid: Decimal,
    inventory: Decimal,
    gamma: Decimal,
    sigma: Decimal,
    horizon: Decimal,
    vol_to_spread: Decimal,
) -> Option<SimplifiedGlftQuotes> {
    if mid <= Decimal::ZERO || sigma < Decimal::ZERO || vol_to_spread < Decimal::ZERO {
        return None;
    }
    let half_spread = vol_to_spread * sigma;
    let skew = simplified_skew(inventory, gamma, sigma, horizon);
    let reservation = mid - skew;
    Some(SimplifiedGlftQuotes {
        bid: reservation - half_spread,
        ask: reservation + half_spread,
        half_spread,
    })
}

pub struct SimplifiedGlftMm {
    config: SimplifiedGlftMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    inventory: tokio::sync::Mutex<Decimal>,
    last_mid: tokio::sync::Mutex<Option<Decimal>>,
    vol_ema: tokio::sync::Mutex<Ema>,
}

impl SimplifiedGlftMm {
    pub fn new(
        config: SimplifiedGlftMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            inventory: tokio::sync::Mutex::new(Decimal::ZERO),
            last_mid: tokio::sync::Mutex::new(None),
            vol_ema: tokio::sync::Mutex::new(Ema::new(default_vol_window())),
        }
    }

    /// Current fair mid: blend between the ticker's mid and the book's
    /// micro-price, weighted by `micro_weight`.
    async fn fair_mid(&self) -> Result<(Decimal, Option<Decimal>)> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        let simple_mid = if let Some(last) = ticker.last.as_option() {
            let p = longtrader_contract::ext::common_to_decimal(last)?;
            if p > Decimal::ZERO { p } else { color_eyre::eyre::bail!("no usable price") }
        } else {
            color_eyre::eyre::bail!("no ticker")
        };

        let book = self
            .market
            .fetch_order_book(crate::proto::market::FetchOrderBookRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: self.config.common.symbol.clone(),
                pagination: buffa::MessageField::some(crate::proto::common::Pagination {
                    limit: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;
        let micro_mid = if let Ok(book) = &book {
            let best_bid = book.bids.first().and_then(|l| l.price.as_option());
            let best_ask = book.asks.first().and_then(|l| l.price.as_option());
            let bid_qty = book.bids.first().and_then(|l| l.amount.as_option());
            let ask_qty = book.asks.first().and_then(|l| l.amount.as_option());
            match (best_bid, best_ask, bid_qty, ask_qty) {
                (Some(b), Some(a), Some(bq), Some(aq)) => {
                    match (
                        longtrader_contract::ext::common_to_decimal(b).ok(),
                        longtrader_contract::ext::common_to_decimal(a).ok(),
                        longtrader_contract::ext::common_to_decimal(bq).ok(),
                        longtrader_contract::ext::common_to_decimal(aq).ok(),
                    ) {
                        (Some(b), Some(a), Some(bq), Some(aq)) => micro_price(b, a, bq, aq),
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        };
        Ok((blended_mid(simple_mid, micro_mid, self.config.micro_weight), micro_mid))
    }

    async fn update_volatility(&self, mid: Decimal) -> Option<Decimal> {
        let mut last = self.last_mid.lock().await;
        let mut ema = self.vol_ema.lock().await;
        if let Some(prev) = *last {
            let ret = (mid - prev).abs();
            ema.push(ret);
        }
        *last = Some(mid);
        ema.value()
    }

    pub async fn tick(&self) -> Result<()> {
        let (mid, _) = self.fair_mid().await?;
        let sigma_est = self.update_volatility(mid).await;
        let sigma = sigma_est.unwrap_or(self.config.sigma);
        let inv = *self.inventory.lock().await;
        let Some(quotes) = plan_simplified_glft_quotes(
            mid,
            inv,
            self.config.gamma,
            sigma,
            self.config.time_horizon,
            self.config.vol_to_spread,
        ) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        cancel_all_orders(self.gateway.as_ref(), &exchange.clone(), &self.config.common.symbol)
            .await?;
        let bid_coid = format!("sglft-{}", ulid::Ulid::generate());
        let ask_coid = format!("sglft-{}", ulid::Ulid::generate());
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                bid_coid,
                true,
                quotes.bid,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "simplified GLFT bid placement failed");
        }
        if let Err(err) = self
            .gateway
            .create_order(limit_order(
                &exchange,
                &self.config.common.symbol,
                ask_coid,
                false,
                quotes.ask,
                self.config.qty,
            ))
            .await
        {
            tracing::error!(error = %err, "simplified GLFT ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for SimplifiedGlftMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            vol_to_spread = %self.config.vol_to_spread,
            gamma = %self.config.gamma,
            "starting simplified GLFT market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "simplified GLFT tick failed");
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
    fn micro_price_tilts_toward_heavy_side() {
        // Bid has more qty than ask → micro-price tilts up (toward ask price,
        // anticipating buy pressure pushing the price higher).
        let mp = micro_price(dec!(99), dec!(101), dec!(10), dec!(1)).expect("mp");
        assert!(mp > dec!(100));
        // Ask heavier → micro-price tilts down.
        let mp2 = micro_price(dec!(99), dec!(101), dec!(1), dec!(10)).expect("mp");
        assert!(mp2 < dec!(100));
    }

    #[test]
    fn micro_price_balanced_is_mid() {
        assert_eq!(micro_price(dec!(99), dec!(101), dec!(5), dec!(5)), Some(dec!(100)));
    }

    #[test]
    fn micro_price_rejects_bad_input() {
        assert!(micro_price(dec!(0), dec!(101), dec!(5), dec!(5)).is_none());
        assert!(micro_price(dec!(99), dec!(0), dec!(5), dec!(5)).is_none());
        assert!(micro_price(dec!(99), dec!(101), dec!(0), dec!(0)).is_none());
    }

    #[test]
    fn blended_mid_with_no_micro_is_simple() {
        assert_eq!(blended_mid(dec!(100), None, dec!(0.5)), dec!(100));
    }

    #[test]
    fn blended_mid_weighted() {
        // 50/50 blend between 100 and 102.
        assert_eq!(blended_mid(dec!(100), Some(dec!(102)), dec!(0.5)), dec!(101));
        // Full weight to micro.
        assert_eq!(blended_mid(dec!(100), Some(dec!(102)), dec!(1)), dec!(102));
    }

    #[test]
    fn simplified_skew_scales_with_inventory() {
        assert_eq!(simplified_skew(dec!(0), dec!(0.1), dec!(1), dec!(1)), dec!(0));
        assert_eq!(simplified_skew(dec!(1), dec!(0.1), dec!(1), dec!(1)), dec!(0.1));
        assert_eq!(simplified_skew(dec!(-1), dec!(0.1), dec!(1), dec!(1)), dec!(-0.1));
    }

    #[test]
    fn plan_simplified_glcenters_on_reservation() {
        let q =
            plan_simplified_glft_quotes(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1), dec!(0.5))
                .expect("quotes");
        // half_spread = 0.5 * 1 = 0.5.
        assert_eq!(q.half_spread, dec!(0.5));
        assert_eq!(q.bid, dec!(99.5));
        assert_eq!(q.ask, dec!(100.5));
    }

    #[test]
    fn plan_simplified_rejects_bad_input() {
        assert!(
            plan_simplified_glft_quotes(dec!(0), dec!(0), dec!(0.1), dec!(1), dec!(1), dec!(0.5))
                .is_none()
        );
        assert!(
            plan_simplified_glft_quotes(
                dec!(100),
                dec!(0),
                dec!(0.1),
                dec!(-1),
                dec!(1),
                dec!(0.5)
            )
            .is_none()
        );
        assert!(
            plan_simplified_glft_quotes(
                dec!(100),
                dec!(0),
                dec!(0.1),
                dec!(1),
                dec!(1),
                dec!(-0.5)
            )
            .is_none()
        );
    }

    #[test]
    fn skew_with_inventory_shifts_reservation() {
        let flat =
            plan_simplified_glft_quotes(dec!(100), dec!(0), dec!(0.1), dec!(1), dec!(1), dec!(0.5))
                .expect("flat");
        let long =
            plan_simplified_glft_quotes(dec!(100), dec!(2), dec!(0.1), dec!(1), dec!(1), dec!(0.5))
                .expect("long");
        // Positive inventory → R drops → both quotes drop.
        assert!(long.bid < flat.bid);
        assert!(long.ask < flat.ask);
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nvol_to_spread = \"0.3\"\ngamma = \"0.2\"")
                .expect("toml");
        let cfg = SimplifiedGlftMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.vol_to_spread, dec!(0.3));
        assert_eq!(cfg.gamma, dec!(0.2));
        assert_eq!(cfg.vol_window, 20);
        assert_eq!(cfg.micro_weight, dec!(0.5));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTCUSDT\"\nvol_to_spread = \"0.7\"\ngamma = \"0.05\"\n\
             sigma = \"2.0\"\ntime_horizon = \"0.5\"\nqty = \"0.1\"\n\
             vol_window = 30\nmicro_weight = \"0.8\"",
        )
        .expect("toml");
        let cfg = SimplifiedGlftMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.vol_to_spread, dec!(0.7));
        assert_eq!(cfg.gamma, dec!(0.05));
        assert_eq!(cfg.sigma, dec!(2));
        assert_eq!(cfg.time_horizon, dec!(0.5));
        assert_eq!(cfg.qty, dec!(0.1));
        assert_eq!(cfg.vol_window, 30);
        assert_eq!(cfg.micro_weight, dec!(0.8));
    }
}
