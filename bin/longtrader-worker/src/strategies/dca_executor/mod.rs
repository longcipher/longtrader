//! DCA (Dollar-Cost Averaging) position executor.
//!
//! Opens a position gradually through a series of limit orders at
//! progressively better prices. When one level fills, the strategy may
//! open the next level closer to the market. Configurable with stop-loss,
//! take-profit, and trailing-stop barriers. Each level has its own amount
//! and price.
//!
//! Reference: <https://hummingbot.org/strategies/dca/>

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

/// Configuration for the DCA executor.
#[derive(Debug, Clone, Deserialize)]
pub struct DcaConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Target prices for each DCA level (e.g. `["99", "98", "97"]`).
    pub prices: Vec<Decimal>,
    /// Amount (quote) per level.
    pub amounts_quote: Vec<Decimal>,
    /// True = buy, false = sell.
    #[serde(default = "default_is_buy")]
    pub is_buy: bool,
    /// Maker-mode (limit orders).
    #[serde(default = "default_is_maker")]
    pub is_maker: bool,
    /// Stop-loss as a fraction of average entry (e.g. 0.05 = 5%).
    #[serde(default = "default_stop_loss")]
    pub stop_loss: Decimal,
    /// Take-profit as a fraction of average entry.
    #[serde(default)]
    pub take_profit: Decimal,
    /// Trailing-stop activation threshold.
    #[serde(default)]
    pub trailing_activation: Decimal,
    /// Trailing-stop trailing distance.
    #[serde(default)]
    pub trailing_delta: Decimal,
}

fn default_is_buy() -> bool {
    true
}
fn default_is_maker() -> bool {
    true
}
fn default_stop_loss() -> Decimal {
    dec!(0.05)
}

impl DcaConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// DCA execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcaState {
    Active,
    Complete,
    StopLoss,
    TakeProfit,
    TrailingStop,
}

/// Volume-weighted average entry price for DCA levels.
///
/// `total_quote / Σ(amount_i / price_i)` — quote spent divided by base
/// accumulated across all levels.
#[must_use]
pub fn average_entry_price(amounts_quote: &[Decimal], prices: &[Decimal]) -> Option<Decimal> {
    if amounts_quote.len() != prices.len() || amounts_quote.is_empty() {
        return None;
    }
    let total_quote: Decimal = amounts_quote.iter().sum();
    if total_quote.is_zero() {
        return None;
    }
    let total_base: Decimal = amounts_quote
        .iter()
        .zip(prices.iter())
        .map(|(amt, price)| if price.is_zero() { Decimal::ZERO } else { amt / price })
        .sum();
    if total_base.is_zero() {
        return None;
    }
    Some(total_quote / total_base)
}

/// Net PnL as a fraction, given average entry and current price.
#[must_use]
pub fn dca_pnl_fraction(
    average_entry: Decimal,
    current_price: Decimal,
    is_buy: bool,
) -> Option<Decimal> {
    if average_entry <= Decimal::ZERO || current_price <= Decimal::ZERO {
        return None;
    }
    if is_buy {
        Some((current_price - average_entry) / average_entry)
    } else {
        Some((average_entry - current_price) / average_entry)
    }
}

/// Check whether a level's price is activated given the current market.
#[must_use]
pub fn level_activated(level_price: Decimal, current_price: Decimal, is_buy: bool) -> bool {
    if is_buy { current_price <= level_price } else { current_price >= level_price }
}

pub struct DcaExecutor {
    config: DcaConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    state: tokio::sync::Mutex<DcaState>,
    levels_placed: tokio::sync::Mutex<Vec<bool>>,
    peak_pnl: tokio::sync::Mutex<Decimal>,
}

impl DcaExecutor {
    pub fn new(
        config: DcaConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self {
            config,
            gateway,
            market,
            state: tokio::sync::Mutex::new(DcaState::Active),
            levels_placed: tokio::sync::Mutex::new(Vec::new()),
            peak_pnl: tokio::sync::Mutex::new(Decimal::ZERO),
        }
    }

    async fn current_price(&self) -> Result<Decimal> {
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
        color_eyre::eyre::bail!("no usable price");
    }

    pub async fn tick(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if *state != DcaState::Active {
            return Ok(());
        }
        let mut placed = self.levels_placed.lock().await;
        if placed.len() != self.config.prices.len() {
            *placed = self.config.prices.iter().map(|_| false).collect();
        }

        let current_price = self.current_price().await?;

        // Place next active level.
        for (i, (price, amount)) in
            self.config.prices.iter().zip(self.config.amounts_quote.iter()).enumerate()
        {
            if placed[i] {
                continue;
            }
            if level_activated(*price, current_price, self.config.is_buy) {
                let exchange = self.config.common.proto_exchange_id();
                let coid = format!("dca-{}-{}-{}", i, ulid::Ulid::generate(), 0);
                let order_price = if self.config.is_maker { *price } else { current_price };
                let req = limit_order(
                    &exchange,
                    &self.config.common.symbol,
                    coid,
                    self.config.is_buy,
                    order_price,
                    *amount / *price,
                );
                if let Err(err) = self.gateway.create_order(req).await {
                    tracing::error!(error = %err, "DCA order failed");
                }
                placed[i] = true;
            }
        }

        // Check barriers.
        let Some(avg_entry) = average_entry_price(&self.config.amounts_quote, &self.config.prices)
        else {
            return Ok(());
        };
        let pnl =
            dca_pnl_fraction(avg_entry, current_price, self.config.is_buy).unwrap_or(Decimal::ZERO);
        let mut peak = self.peak_pnl.lock().await;
        if pnl > *peak {
            *peak = pnl;
        }

        if self.config.stop_loss > Decimal::ZERO && pnl <= -self.config.stop_loss {
            *state = DcaState::StopLoss;
        } else if self.config.take_profit > Decimal::ZERO && pnl >= self.config.take_profit {
            *state = DcaState::TakeProfit;
        } else if self.config.trailing_activation > Decimal::ZERO &&
            *peak >= self.config.trailing_activation &&
            pnl < *peak - self.config.trailing_delta
        {
            *state = DcaState::TrailingStop;
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for DcaExecutor {
    async fn run(&self) -> Result<()> {
        tracing::info!(
            symbol = %self.config.common.symbol,
            levels = %self.config.prices.len(),
            is_buy = %self.config.is_buy,
            "starting DCA executor"
        );
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            let state = *self.state.lock().await;
            if state != DcaState::Active {
                return Ok(());
            }
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "DCA tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn average_entry_price_weighted() {
        // total_quote = 300, total_base = 100/10 + 200/20 = 10 + 10 = 20
        // avg = 300 / 20 = 15
        let amounts = vec![dec!(100), dec!(200)];
        let prices = vec![dec!(10), dec!(20)];
        let avg = average_entry_price(&amounts, &prices).expect("avg");
        assert_eq!(avg, dec!(15));
    }

    #[test]
    fn average_entry_price_mismatched_lengths() {
        let a1 = vec![dec!(100)];
        let p2 = vec![dec!(10), dec!(20)];
        assert!(average_entry_price(&a1, &p2).is_none());
        let empty: Vec<Decimal> = vec![];
        assert!(average_entry_price(&empty, &empty).is_none());
    }

    #[test]
    fn dca_pnl_fraction_buy_profit() {
        let pnl = dca_pnl_fraction(dec!(100), dec!(110), true).expect("pnl");
        assert_eq!(pnl, dec!(0.1));
    }

    #[test]
    fn dca_pnl_fraction_sell_profit() {
        let pnl = dca_pnl_fraction(dec!(100), dec!(90), false).expect("pnl");
        assert_eq!(pnl, dec!(0.1));
    }

    #[test]
    fn level_activated_buy() {
        assert!(level_activated(dec!(100), dec!(99), true));
        assert!(!level_activated(dec!(100), dec!(101), true));
    }

    #[test]
    fn level_activated_sell() {
        assert!(level_activated(dec!(100), dec!(101), false));
        assert!(!level_activated(dec!(100), dec!(99), false));
    }

    #[test]
    fn config_parses_from_table() {
        let table: toml::Table = toml::from_str(
            r#"symbol = "BTC/USDT"
prices = ["99", "98", "97"]
amounts_quote = ["100", "200", "300"]
is_buy = true"#,
        )
        .expect("toml");
        let cfg = DcaConfig::from_params(&table).expect("parse");
        let expected_prices: Vec<Decimal> = vec![dec!(99), dec!(98), dec!(97)];
        let expected_amounts: Vec<Decimal> = vec![dec!(100), dec!(200), dec!(300)];
        assert_eq!(cfg.prices, expected_prices);
        assert_eq!(cfg.amounts_quote, expected_amounts);
        assert!(cfg.is_buy);
    }

    #[test]
    fn config_overrides_defaults() {
        let table: toml::Table = toml::from_str(
            r#"symbol = "ETH/USDT"
prices = ["1900", "1800"]
amounts_quote = ["500", "1000"]
is_buy = false
is_maker = false
stop_loss = "0.1"
take_profit = "0.2""#,
        )
        .expect("toml");
        let cfg = DcaConfig::from_params(&table).expect("parse");
        assert!(!cfg.is_buy);
        assert!(!cfg.is_maker);
        assert_eq!(cfg.stop_loss, dec!(0.1));
        assert_eq!(cfg.take_profit, dec!(0.2));
    }
}
