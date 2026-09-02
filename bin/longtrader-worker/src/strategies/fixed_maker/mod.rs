//! Fixed-spread two-sided maker strategy over the worker ticker-poll model.
//!
//! Polls the ticker, re-centres on the mid price, and replaces the ladder
//! with a bid at `mid * (1 - bid_spread)` and an ask at
//! `mid * (1 + ask_spread)`.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, limit_order, params_from_table},
};

/// Config for the worker fixed maker.
#[derive(Debug, Clone, Deserialize)]
pub struct FixedMakerConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_spread")]
    pub bid_spread: Decimal,
    #[serde(default = "default_spread")]
    pub ask_spread: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_spread() -> Decimal {
    Decimal::new(1, 3)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl FixedMakerConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Pure quote planner around a mid price.
#[must_use]
pub fn plan_quotes(
    mid: Decimal,
    bid_spread: Decimal,
    ask_spread: Decimal,
) -> Option<(Decimal, Decimal)> {
    if mid <= Decimal::ZERO || bid_spread < Decimal::ZERO || ask_spread < Decimal::ZERO {
        return None;
    }
    Some((mid * (Decimal::ONE - bid_spread), mid * (Decimal::ONE + ask_spread)))
}

/// Worker fixed maker strategy.
pub struct FixedMaker {
    config: FixedMakerConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl FixedMaker {
    pub fn new(
        config: FixedMakerConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// Latest price from ticker last/close, falling back to bid-ask mid.
    async fn current_price(&self) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
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
        color_eyre::eyre::bail!("no usable ticker price");
    }

    /// One poll cycle: cancel and re-quote both sides.
    pub async fn tick(&self) -> Result<()> {
        let mid = self.current_price().await?;
        let Some((bid, ask)) = plan_quotes(mid, self.config.bid_spread, self.config.ask_spread)
        else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        self.gateway
            .cancel_all_orders(crate::proto::trading::CancelAllOrdersRequest {
                exchange_id: buffa::MessageField::some(exchange.clone()),
                symbol: self.config.common.symbol.clone(),
                ..Default::default()
            })
            .await?;
        for (price, is_buy) in [(bid, true), (ask, false)] {
            let coid = format!("fixedmaker-{}", ulid::Ulid::generate());
            let request = limit_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                price,
                self.config.qty,
            );
            if let Err(err) = self.gateway.create_order(request).await {
                tracing::error!(side = if is_buy { "bid" } else { "ask" }, error = %err, "quote failed");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for FixedMaker {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "fixed-maker tick failed");
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
    fn quotes_spread_around_mid() {
        let (bid, ask) = plan_quotes(dec!(100), dec!(0.01), dec!(0.02)).expect("quotes");
        assert_eq!(bid, dec!(99));
        assert_eq!(ask, dec!(102));
    }

    #[test]
    fn rejects_degenerate_inputs() {
        assert!(plan_quotes(dec!(0), dec!(0.01), dec!(0.01)).is_none());
        assert!(plan_quotes(dec!(100), dec!(-0.01), dec!(0.01)).is_none());
    }

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTCUSDT\"\nbid_spread = \"0.0005\"").expect("toml");
        let cfg = FixedMakerConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.bid_spread.to_string(), "0.0005");
        // ask falls back to its default.
        assert_eq!(cfg.ask_spread.to_string(), "0.001");
    }
}
