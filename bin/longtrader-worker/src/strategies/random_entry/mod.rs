//! Random-order smoke-test strategy.
//!
//! Every `interval_secs` the strategy submits one market order with a
//! random side, with probability `entry_probability`. Randomness comes from
//! a deterministic SplitMix64 generator seeded by `seed`, so runs are
//! reproducible. Intended for connectivity / risk-pipeline smoke tests.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::TradingGateway,
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the worker random-entry strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct RandomEntryConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_probability")]
    pub entry_probability: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
    #[serde(default = "default_seed")]
    pub seed: u64,
}

const fn default_interval() -> u64 {
    60
}
fn default_probability() -> Decimal {
    Decimal::new(5, 2)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 4)
}
const fn default_seed() -> u64 {
    0x9E37_79B9_7F4A_7C15
}

impl RandomEntryConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// SplitMix64 deterministic PRNG.
#[derive(Debug)]
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform fraction in `[0, 1)`.
    fn next_fraction(&mut self) -> Decimal {
        const SCALE: u128 = 1_000_000_000_000_000_000;
        let scaled = ((u128::from(self.next_u64()) * SCALE) >> 64) as i64;
        Decimal::new(scaled, 18)
    }
}

/// Decides whether to fire and on which side for one tick.
#[must_use]
pub fn decide(rng: &mut SplitMix64, probability: Decimal) -> Option<bool> {
    if rng.next_fraction() >= probability {
        return None;
    }
    Some(rng.next_u64() & 1 == 0)
}

/// Worker random-entry strategy.
pub struct RandomEntry {
    config: RandomEntryConfig,
    gateway: Arc<dyn TradingGateway>,
}

impl RandomEntry {
    pub fn new(config: RandomEntryConfig, gateway: Arc<dyn TradingGateway>) -> Self {
        Self { config, gateway }
    }

    /// One decision + optional order submission.
    pub async fn tick(&self, rng: &mut SplitMix64) -> Result<()> {
        let Some(is_buy) = decide(rng, self.config.entry_probability) else {
            return Ok(());
        };
        let exchange = self.config.common.proto_exchange_id();
        let coid = format!("random-{}", ulid::Ulid::generate());
        let request =
            market_order(&exchange, &self.config.common.symbol, coid, is_buy, self.config.qty);
        self.gateway.create_order(request).await?;
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, "random order submitted");
        Ok(())
    }
}

#[async_trait]
impl Strategy for RandomEntry {
    async fn run(&self) -> Result<()> {
        let interval = std::time::Duration::from_secs(self.config.interval_secs.max(1));
        let mut rng = SplitMix64(self.config.seed);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.tick(&mut rng).await {
                tracing::error!(error = %err, "random tick failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn probability_one_always_fires() {
        let mut rng = SplitMix64(42);
        for _ in 0..100 {
            assert!(decide(&mut rng, Decimal::ONE).is_some());
        }
    }

    #[test]
    fn probability_zero_never_fires() {
        let mut rng = SplitMix64(42);
        for _ in 0..100 {
            assert!(decide(&mut rng, Decimal::ZERO).is_none());
        }
    }

    #[test]
    fn same_seed_reproduces_sequence() {
        let seq = |seed: u64| {
            let mut rng = SplitMix64(seed);
            (0..50).map(|_| decide(&mut rng, dec!(0.5))).collect::<Vec<_>>()
        };
        assert_eq!(seq(7), seq(7));
        assert_ne!(seq(7), seq(8));
    }
}
