//! Statistical Arbitrage — pairs trading with z-score mean reversion.
//!
//! Identifies a pair of cointegrated assets and trades the spread
//! between them. The spread is standardised into a z-score. When the
//! z-score exceeds a threshold, the spread is opened (long the
//! underperformer, short the outperformer). When it reverts, the
//! spread is closed for a profit.
//!
//! ```text
//!   spread = price_A − hedge_ratio · price_B
//!   z = (spread − mean(spread)) / stdev(spread)
//!   if  z >  threshold → short spread (sell A / buy B)
//!   if  z < −threshold → long spread (buy A / sell B)
//!   if  |z| < exit_threshold → close
//! ```
//!
//! Reference: <https://hummingbot.org/strategies/statistical-arbitrage/>

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use crate::{
    indicators::decimal_sqrt,
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Configuration for the statistical arbitrage strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct StatArbConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Symbol for asset A (the lead).
    pub symbol_a: String,
    /// Symbol for asset B (the lag).
    pub symbol_b: String,
    /// Hedge ratio (units of B per unit of A).
    #[serde(default = "default_hedge_ratio")]
    pub hedge_ratio: Decimal,
    /// Rolling window for the spread mean/stdev.
    #[serde(default = "default_window")]
    pub window: usize,
    /// Z-score entry threshold.
    #[serde(default = "default_entry")]
    pub entry_threshold: Decimal,
    /// Z-score exit threshold.
    #[serde(default = "default_exit")]
    pub exit_threshold: Decimal,
    /// Position size in units of A.
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_hedge_ratio() -> Decimal {
    dec!(1)
}
fn default_window() -> usize {
    50
}
fn default_entry() -> Decimal {
    dec!(2)
}
fn default_exit() -> Decimal {
    dec!(0.5)
}
fn default_qty() -> Decimal {
    dec!(1)
}

impl StatArbConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Spread tracker with rolling statistics.
#[derive(Debug, Clone)]
pub struct SpreadTracker {
    window: usize,
    buf: VecDeque<Decimal>,
}

impl SpreadTracker {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, spread: Decimal) {
        self.buf.push_back(spread);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    #[must_use]
    pub fn mean(&self) -> Option<Decimal> {
        if self.buf.is_empty() {
            return None;
        }
        let count = Decimal::from(self.buf.len());
        self.buf.iter().try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))?.checked_div(count)
    }

    #[must_use]
    pub fn stdev(&self) -> Option<Decimal> {
        let n = self.buf.len();
        if n < 2 {
            return None;
        }
        let mean = self.mean()?;
        let count = Decimal::from(n);
        let sq = self.buf.iter().try_fold(Decimal::ZERO, |acc, &x| {
            let d = x.checked_sub(mean)?;
            acc.checked_add(d.checked_mul(d)?)
        })?;
        Some(decimal_sqrt(sq.checked_div(count)?))
    }

    #[must_use]
    pub fn z_score(&self, spread: Decimal) -> Option<Decimal> {
        let mean = self.mean()?;
        let stdev = self.stdev()?;
        if stdev.is_zero() {
            return None;
        }
        Some((spread - mean) / stdev)
    }
}

/// Stat arb signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatArbSignal {
    /// Long spread: buy A, sell B.
    LongSpread,
    /// Short spread: sell A, buy B.
    ShortSpread,
    /// Close position.
    Close,
    /// No signal.
    NoTrade,
}

/// Classify the spread z-score into a trading signal.
#[must_use]
pub fn classify_stat_arb(
    z: Option<Decimal>,
    entry_threshold: Decimal,
    exit_threshold: Decimal,
    has_position: bool,
) -> StatArbSignal {
    let z = match z {
        None => return StatArbSignal::NoTrade,
        Some(v) => v,
    };
    if has_position {
        if z.abs() < exit_threshold { StatArbSignal::Close } else { StatArbSignal::NoTrade }
    } else if z > entry_threshold {
        StatArbSignal::ShortSpread
    } else if z < -entry_threshold {
        StatArbSignal::LongSpread
    } else {
        StatArbSignal::NoTrade
    }
}

/// Statistical arbitrage strategy.
pub struct StatArb {
    config: StatArbConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    tracker: tokio::sync::Mutex<SpreadTracker>,
    has_position: tokio::sync::Mutex<i8>, // −1 short, 0 flat, +1 long
}

impl StatArb {
    pub fn new(
        config: StatArbConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let window = config.window;
        Self {
            config,
            gateway,
            market,
            tracker: tokio::sync::Mutex::new(SpreadTracker::new(window)),
            has_position: tokio::sync::Mutex::new(0),
        }
    }

    async fn fetch_price(&self, symbol: &str) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: symbol.to_string(),
                ..Default::default()
            })
            .await?;
        if let Some(last) = ticker.last.as_option() {
            let p = longtrader_contract::ext::common_to_decimal(last)?;
            if p > Decimal::ZERO {
                return Ok(p);
            }
        }
        color_eyre::eyre::bail!("no price for {symbol}")
    }

    pub async fn tick(&self) -> Result<()> {
        let price_a = self.fetch_price(&self.config.symbol_a).await?;
        let price_b = self.fetch_price(&self.config.symbol_b).await?;
        let spread = price_a - self.config.hedge_ratio * price_b;
        let mut tracker = self.tracker.lock().await;
        tracker.push(spread);
        let z = tracker.z_score(spread);
        drop(tracker);

        let has_pos = *self.has_position.lock().await;
        let has_pos_bool = has_pos != 0;
        let signal = classify_stat_arb(
            z,
            self.config.entry_threshold,
            self.config.exit_threshold,
            has_pos_bool,
        );

        match signal {
            StatArbSignal::NoTrade => {}
            StatArbSignal::LongSpread => {
                let qty = self.config.qty;
                let coid = format!("staab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_a,
                        coid,
                        true,
                        price_a,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb buy A failed");
                }
                let coid = format!("staab-{}", ulid::Ulid::generate());
                let qty_b = qty * self.config.hedge_ratio;
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_b,
                        coid,
                        false,
                        price_b,
                        qty_b,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb sell B failed");
                }
                *self.has_position.lock().await = 1;
            }
            StatArbSignal::ShortSpread => {
                let qty = self.config.qty;
                let coid = format!("staab-{}", ulid::Ulid::generate());
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_a,
                        coid,
                        false,
                        price_a,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb sell A failed");
                }
                let coid = format!("staab-{}", ulid::Ulid::generate());
                let qty_b = qty * self.config.hedge_ratio;
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_b,
                        coid,
                        true,
                        price_b,
                        qty_b,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb buy B failed");
                }
                *self.has_position.lock().await = -1;
            }
            StatArbSignal::Close => {
                let pos = *self.has_position.lock().await;
                let qty = self.config.qty;
                // Reverse the position to close.
                let coid = format!("staab-{}", ulid::Ulid::generate());
                let side_a = pos < 0; // Was short A → buy to close.
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_a,
                        coid,
                        side_a,
                        price_a,
                        qty,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb close A failed");
                }
                let coid = format!("staab-{}", ulid::Ulid::generate());
                let qty_b = qty * self.config.hedge_ratio;
                let side_b = pos > 0; // Was long B → sell to close.
                if let Err(err) = self
                    .gateway
                    .create_order(limit_order(
                        &self.config.common.proto_exchange_id(),
                        &self.config.symbol_b,
                        coid,
                        side_b,
                        price_b,
                        qty_b,
                    ))
                    .await
                {
                    tracing::error!(error = %err, "stat arb close B failed");
                }
                *self.has_position.lock().await = 0;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for StatArb {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol_a = %self.config.symbol_a,
            symbol_b = %self.config.symbol_b,
            hedge_ratio = %self.config.hedge_ratio,
            "starting statistical arbitrage"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "stat arb tick failed");
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
    fn spread_tracker_mean_and_stdev() {
        let mut t = SpreadTracker::new(5);
        t.push(dec!(1));
        t.push(dec!(2));
        t.push(dec!(3));
        assert_eq!(t.mean(), Some(dec!(2)));
        assert!(t.stdev().is_some());
    }

    #[test]
    fn spread_tracker_z_score() {
        let mut t = SpreadTracker::new(10);
        for _ in 0..5 {
            t.push(dec!(10));
        }
        // All identical → stdev = 0 → no z-score.
        assert!(t.z_score(dec!(10)).is_none());
        t.push(dec!(20));
        assert!(t.z_score(dec!(20)).is_some());
    }

    #[test]
    fn spread_tracker_window_slides() {
        let mut t = SpreadTracker::new(2);
        t.push(dec!(10));
        t.push(dec!(20));
        t.push(dec!(30));
        assert_eq!(t.mean(), Some(dec!(25)));
    }

    #[test]
    fn classify_stat_arb_entry_and_exit() {
        assert_eq!(
            classify_stat_arb(Some(dec!(2.5)), dec!(2), dec!(0.5), false),
            StatArbSignal::ShortSpread
        );
        assert_eq!(
            classify_stat_arb(Some(dec!(-2.5)), dec!(2), dec!(0.5), false),
            StatArbSignal::LongSpread
        );
        assert_eq!(
            classify_stat_arb(Some(dec!(0.2)), dec!(2), dec!(0.5), true),
            StatArbSignal::Close
        );
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nsymbol_a = \"BTC/USDT\"\n\
             symbol_b = \"ETH/USDT\"\nexchange_id = \"mock\"\nentry_threshold = \"1.5\"",
        )
        .expect("toml");
        let cfg = StatArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.symbol_a, "BTC/USDT");
        assert_eq!(cfg.symbol_b, "ETH/USDT");
        assert_eq!(cfg.entry_threshold, dec!(1.5));
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\nsymbol_a = \"BTC/USDT\"\n\
             symbol_b = \"ETH/USDT\"\nhedge_ratio = \"0.5\"\nwindow = 100\nqty = \"0.1\"",
        )
        .expect("toml");
        let cfg = StatArbConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.hedge_ratio, dec!(0.5));
        assert_eq!(cfg.window, 100);
        assert_eq!(cfg.qty, dec!(0.1));
    }
}
