//! Queue-based market maker for large-tick assets.
//!
//! When the tick size is wide relative to the spread (large-tick regime), the
//! first queue dominates price formation. The strategy tracks (a) book
//! pressure — the imbalance between bid and ask depth across all levels —
//! and (b) trade impulse — the signed volume of recent trades — to forecast
//! short-term price moves. Quotes are sized and distanced accordingly.
//!
//! ```text
//!   pressure   = log(Σ bid_qty / Σ ask_qty)
//!   impulse    = Σ(buy_volume) − Σ(sell_volume)    (signed, rolling window)
//!   signal     = w₁ · pressure + w₂ · normalise(impulse)
//!   fair      += α · signal · mid
//! ```
//!
//! A wide-book (more bid depth) or strong buy impulse tilts the fair value up;
//! the ask tightens to capture the expected move, the bid widens to avoid
//! adverse selection. The strategy does not place orders deeper than
//! `max_depth` ticks from mid; an aggressive quote only rests within the top
//! few levels of the book.

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, cancel_all_orders, limit_order, params_from_table},
};

/// Configuration for the queue market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueMmConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Half-spread baseline as fraction.
    #[serde(default = "default_half_spread")]
    pub half_spread: Decimal,
    /// Quote base quantity.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    /// Look depth for book pressure (number of levels).
    #[serde(default = "default_depth")]
    pub depth: u32,
    /// Trade-impulse rolling window (number of ticks).
    #[serde(default = "default_impulse_window")]
    pub impulse_window: usize,
    /// Signal weight `w₁` on book pressure.
    #[serde(default = "default_w1")]
    pub w_pressure: Decimal,
    /// Signal weight `w₂` on trade impulse.
    #[serde(default = "default_w2")]
    pub w_impulse: Decimal,
    /// Aggressiveness factor `α`.
    #[serde(default = "default_alpha")]
    pub alpha: Decimal,
    /// Half-spread widening per unit of adverse signal (tickle per unit of
    /// `|signal|`).
    #[serde(default = "default_spread_widen")]
    pub spread_widen: Decimal,
}

fn default_half_spread() -> Decimal {
    Decimal::new(5, 4)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}
fn default_depth() -> u32 {
    5
}
fn default_impulse_window() -> usize {
    20
}
fn default_w1() -> Decimal {
    dec!(0.5)
}
fn default_w2() -> Decimal {
    dec!(0.5)
}
fn default_alpha() -> Decimal {
    dec!(0.5)
}
fn default_spread_widen() -> Decimal {
    dec!(0.001)
}

impl QueueMmConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Book pressure: symmetric between −1 and 1 for equal depth, positive when
/// bid depth dominates.
///
/// `None` when either side has zero depth.
#[must_use]
pub fn book_pressure(bid_total: Decimal, ask_total: Decimal) -> Option<Decimal> {
    if bid_total <= Decimal::ZERO || ask_total <= Decimal::ZERO {
        return None;
    }
    let diff = bid_total - ask_total;
    let sum = bid_total + ask_total;
    Some(diff / sum)
}

/// Rolling trade impulse accumulator.
#[derive(Debug, Clone)]
pub struct TradeImpulse {
    window: usize,
    signed_volumes: VecDeque<Decimal>,
}

impl TradeImpulse {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, signed_volumes: VecDeque::with_capacity(window) }
    }

    /// Record a trade by its signed volume: positive = buy, negative = sell.
    pub fn push(&mut self, signed_volume: Decimal) {
        self.signed_volumes.push_back(signed_volume);
        if self.signed_volumes.len() > self.window {
            self.signed_volumes.pop_front();
        }
    }

    /// Sum of signed volumes in the window — the raw impulse.
    #[must_use]
    pub fn raw(&self) -> Decimal {
        self.signed_volumes.iter().copied().sum()
    }

    /// Normalised impulse: divide by the maximum absolute volume in the
    /// window so that the metric is in roughly [−1, 1] across assets of
    /// varying size.
    #[must_use]
    pub fn normalised(&self) -> Option<Decimal> {
        if self.signed_volumes.is_empty() {
            return None;
        }
        let raw = self.raw();
        let max_abs = self.signed_volumes.iter().map(|v| v.abs()).max().unwrap_or(Decimal::ONE);
        if max_abs.is_zero() {
            return None;
        }
        Some(raw / max_abs)
    }
}

/// Composite alpha signal.
#[must_use]
pub fn composite_signal(
    pressure: Option<Decimal>,
    impulse_norm: Option<Decimal>,
    w_pressure: Decimal,
    w_impulse: Decimal,
) -> Decimal {
    let p = pressure.unwrap_or(Decimal::ZERO);
    let i = impulse_norm.unwrap_or(Decimal::ZERO);
    w_pressure * p + w_impulse * i
}

/// Quote plan accounting for signal-driven skew.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueQuotes {
    pub fair: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_half_spread: Decimal,
    pub ask_half_spread: Decimal,
}

#[must_use]
pub fn plan_queue_quotes(
    mid: Decimal,
    signal: Decimal,
    base_half_spread: Decimal,
    spread_widen: Decimal,
) -> Option<QueueQuotes> {
    if mid <= Decimal::ZERO || base_half_spread < Decimal::ZERO {
        return None;
    }
    let fair = mid * (Decimal::ONE + signal);
    let bid_half = base_half_spread + spread_widen * signal.max(Decimal::ZERO);
    let ask_half = base_half_spread + spread_widen * (-signal).max(Decimal::ZERO);
    let bid = fair * (Decimal::ONE - bid_half);
    let ask = fair * (Decimal::ONE + ask_half);
    Some(QueueQuotes { fair, bid, ask, bid_half_spread: bid_half, ask_half_spread: ask_half })
}

pub struct QueueMm {
    config: QueueMmConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    impulse: tokio::sync::Mutex<TradeImpulse>,
}

impl QueueMm {
    pub fn new(
        config: QueueMmConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            impulse: tokio::sync::Mutex::new(TradeImpulse::new(config.impulse_window)),
            config,
            gateway,
            market,
        }
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

    async fn fetch_book_totals(&self) -> Result<(Decimal, Decimal)> {
        let req = crate::proto::market::FetchOrderBookRequest {
            exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
            symbol: self.config.common.symbol.clone(),
            pagination: buffa::MessageField::some(crate::proto::common::Pagination {
                limit: u64::from(self.config.depth),
                ..Default::default()
            }),
            ..Default::default()
        };
        let book = self.market.fetch_order_book(req).await?;
        let bid_total: Decimal = book
            .bids
            .iter()
            .filter_map(|l| l.amount.as_option())
            .filter_map(|d| longtrader_contract::ext::common_to_decimal(d).ok())
            .sum();
        let ask_total: Decimal = book
            .asks
            .iter()
            .filter_map(|l| l.amount.as_option())
            .filter_map(|d| longtrader_contract::ext::common_to_decimal(d).ok())
            .sum();
        Ok((bid_total, ask_total))
    }

    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_mid().await?;
        let (bid_total, ask_total) = self.fetch_book_totals().await?;
        let pressure = book_pressure(bid_total, ask_total);
        let impulse_norm = self.impulse.lock().await.normalised();
        let signal =
            composite_signal(pressure, impulse_norm, self.config.w_pressure, self.config.w_impulse);
        let Some(quotes) = plan_queue_quotes(
            mid,
            signal * self.config.alpha,
            self.config.half_spread,
            self.config.spread_widen,
        ) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        cancel_all_orders(self.gateway.as_ref(), &exchange.clone(), &self.config.common.symbol)
            .await?;
        let bid_coid = format!("queuemm-{}", ulid::Ulid::generate());
        let ask_coid = format!("queuemm-{}", ulid::Ulid::generate());
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
            tracing::error!(error = %err, "queue bid placement failed");
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
            tracing::error!(error = %err, "queue ask placement failed");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for QueueMm {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            depth = %self.config.depth,
            impulse_window = %self.config.impulse_window,
            "starting queue market maker"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "queue tick failed");
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
    fn book_pressure_balanced_is_zero() {
        assert_eq!(book_pressure(dec!(10), dec!(10)), Some(dec!(0)));
    }

    #[test]
    fn book_pressure_bid_heavy_positive() {
        assert_eq!(book_pressure(dec!(20), dec!(10)), Some(dec!(1) / dec!(3)));
    }

    #[test]
    fn book_pressure_empty_side_is_none() {
        assert!(book_pressure(dec!(0), dec!(10)).is_none());
        assert!(book_pressure(dec!(10), dec!(0)).is_none());
    }

    #[test]
    fn impulse_raw_sums_signed_volumes() {
        let mut ti = TradeImpulse::new(5);
        ti.push(dec!(10));
        ti.push(dec!(-5));
        ti.push(dec!(3));
        assert_eq!(ti.raw(), dec!(8));
    }

    #[test]
    fn impulse_normalised_scales() {
        let mut ti = TradeImpulse::new(5);
        ti.push(dec!(10));
        ti.push(dec!(-5));
        // raw = 5, max_abs = 10 → 0.5.
        assert_eq!(ti.normalised(), Some(dec!(0.5)));
    }

    #[test]
    fn impulse_empty_is_none() {
        let ti = TradeImpulse::new(5);
        assert!(ti.normalised().is_none());
    }

    #[test]
    fn impulse_window_slides() {
        let mut ti = TradeImpulse::new(2);
        ti.push(dec!(10));
        ti.push(dec!(20));
        ti.push(dec!(30));
        // Only 20 and 30 remain.
        assert_eq!(ti.raw(), dec!(50));
    }

    #[test]
    fn composite_signal_blend() {
        let s = composite_signal(Some(dec!(0.5)), Some(dec!(-0.3)), dec!(0.5), dec!(0.5));
        assert_eq!(s, dec!(0.1));
    }

    #[test]
    fn composite_signal_handles_none() {
        let s = composite_signal(None, None, dec!(0.5), dec!(0.5));
        assert_eq!(s, dec!(0));
    }

    #[test]
    fn plan_queue_quotes_skews_spread() {
        // Positive signal → bid widens (avoid adverse selection), ask tightens.
        let q = plan_queue_quotes(dec!(100), dec!(0.5), dec!(0.01), dec!(0.001)).expect("quotes");
        assert!(q.bid_half_spread > q.ask_half_spread);
        // Negative signal → ask widens, bid tightens.
        let q = plan_queue_quotes(dec!(100), dec!(-0.5), dec!(0.01), dec!(0.001)).expect("quotes");
        assert!(q.ask_half_spread > q.bid_half_spread);
    }

    #[test]
    fn plan_queue_quotes_rejects_bad_input() {
        assert!(plan_queue_quotes(dec!(0), dec!(0.5), dec!(0.01), dec!(0.001)).is_none());
        assert!(plan_queue_quotes(dec!(100), dec!(0.5), dec!(-0.01), dec!(0.001)).is_none());
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nhalf_spread = \"0.001\"\ndepth = 10")
                .expect("toml");
        let cfg = QueueMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.half_spread, dec!(0.001));
        assert_eq!(cfg.depth, 10);
        assert_eq!(cfg.impulse_window, 20);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"X\"\nhalf_spread = \"0.002\"\nqty = \"0.5\"\ndepth = 3\n\
             impulse_window = 50\nw_pressure = \"0.7\"\nw_impulse = \"0.3\"\n\
             alpha = \"0.8\"\nspread_widen = \"0.005\"",
        )
        .expect("toml");
        let cfg = QueueMmConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.qty, dec!(0.5));
        assert_eq!(cfg.depth, 3);
        assert_eq!(cfg.impulse_window, 50);
        assert_eq!(cfg.w_pressure, dec!(0.7));
        assert_eq!(cfg.w_impulse, dec!(0.3));
        assert_eq!(cfg.alpha, dec!(0.8));
        assert_eq!(cfg.spread_widen, dec!(0.005));
    }
}
