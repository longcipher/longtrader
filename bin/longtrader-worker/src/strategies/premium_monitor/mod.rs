//! Cross-venue premium monitor.
//!
//! Polls the same symbol on two venues and reports the premium
//! `primary / hedge - 1`. Breaches beyond `alert_threshold` are logged at
//! warn level (the notification outlet is the control plane's log stream).

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::MarketDataSource,
    strategies::{CrossVenueParams, Strategy},
};

/// Config for the premium monitor.
#[derive(Debug, Clone, Deserialize)]
pub struct PremiumMonitorConfig {
    #[serde(flatten)]
    pub venues: CrossVenueParams,
    /// Alert when |premium| exceeds this fraction.
    pub alert_threshold: Decimal,
}

/// Pure premium computation.
#[must_use]
pub fn premium(primary: Decimal, hedge: Decimal) -> Option<Decimal> {
    if hedge.is_zero() || primary.is_zero() {
        return None;
    }
    Some(primary / hedge - Decimal::ONE)
}

/// Premium monitor strategy.
pub struct PremiumMonitor {
    config: PremiumMonitorConfig,
    market: Arc<dyn MarketDataSource>,
    last_premium: Mutex<Option<Decimal>>,
}

impl PremiumMonitor {
    pub fn new(config: PremiumMonitorConfig, market: Arc<dyn MarketDataSource>) -> Self {
        Self { config, market, last_premium: Mutex::new(None) }
    }

    async fn ticker_last(&self, exchange_id: &crate::proto::common::ExchangeId) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(exchange_id.clone()),
                symbol: self.config.venues.symbol.clone(),
                ..Default::default()
            })
            .await?;
        ticker
            .last
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .ok_or_else(|| color_eyre::eyre::eyre!("ticker missing last price"))
    }

    /// One measurement cycle; returns the observed premium.
    pub async fn tick(&self) -> Result<Decimal> {
        let p = self.ticker_last(&self.config.venues.primary.proto()).await?;
        let h = self.ticker_last(&self.config.venues.hedge.proto()).await?;
        let prem = premium(p, h).unwrap_or_default();
        *self.last_premium.lock().await = Some(prem);
        if prem.abs() > self.config.alert_threshold {
            tracing::warn!(%p, %h, %prem, "premium breach");
        }
        Ok(prem)
    }
}

#[async_trait]
impl Strategy for PremiumMonitor {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.venues.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "premium-monitor tick failed");
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
    fn premium_math() {
        assert_eq!(premium(dec!(101), dec!(100)), Some(dec!(0.01)));
        assert_eq!(premium(Decimal::ZERO, dec!(100)), None);
    }
}
