//! Funding-rate carry strategy (single-venue lite version).
//!
//! Polls the funding rate: when it exceeds `enter_threshold` the strategy
//! holds a long (collecting funding paid by longs in positive-rate regimes
//! is venue-dependent — here we treat a *negative* rate as favourable for
//! longs and a *positive* rate as favourable for shorts, matching perp
//! conventions where longs pay when the rate is positive). Exits when the
//! rate crosses back through the exit threshold.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    ports::{FundingRateSource, TradingGateway},
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the funding-carry strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct XfundingLiteConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Enter (short) when rate >= this value.
    #[serde(default = "default_enter")]
    pub enter_threshold: Decimal,
    /// Exit when the rate magnitude falls below this value.
    #[serde(default = "default_exit")]
    pub exit_threshold: Decimal,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

fn default_enter() -> Decimal {
    Decimal::new(1, 4)
}
fn default_exit() -> Decimal {
    Decimal::new(1, 5)
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl XfundingLiteConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Flat,
    Short,
}

/// Funding-carry strategy.
pub struct XfundingLite {
    config: XfundingLiteConfig,
    gateway: Arc<dyn TradingGateway>,
    funding: Arc<dyn FundingRateSource>,
    phase: Mutex<Phase>,
}

impl XfundingLite {
    pub fn new(
        config: XfundingLiteConfig,
        gateway: Arc<dyn TradingGateway>,
        funding: Arc<dyn FundingRateSource>,
    ) -> Self {
        Self { config, gateway, funding, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    ///
    /// The phase is advanced only *after* `create_order` succeeds, and that
    /// ordering is load-bearing: the phase is this strategy's edge trigger, so
    /// switching `Flat -> Short` (or back) before the venue confirms would make a
    /// transient rejection look like an open position and the entry would never
    /// be retried. The transition is therefore written on the far side of the
    /// `create_order` await, which also makes it single-applied: one `tick`
    /// resolves at most one transition, and a rejected order leaves the old phase
    /// in place for the next poll to re-decide.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.funding.fetch_funding_rate(&exchange, &self.config.common.symbol).await?;
        // Decided under the lock, which is released across the RPC.
        let next = {
            let phase = self.phase.lock().await;
            match *phase {
                // Positive rate: shorts collect → enter short.
                Phase::Flat if snap.rate >= self.config.enter_threshold => Phase::Short,
                Phase::Short if snap.rate.abs() < self.config.exit_threshold => Phase::Flat,
                _ => return Ok(()),
            }
        };
        // Closing a short is a buy; opening one is a sell.
        let is_buy = next == Phase::Flat;
        let coid = format!("xfund-{}", ulid::Ulid::generate());
        self.gateway
            .create_order(market_order(
                &exchange,
                &self.config.common.symbol,
                coid,
                is_buy,
                self.config.qty,
            ))
            .await?;
        *self.phase.lock().await = next;
        if is_buy {
            tracing::info!(rate = %snap.rate, "funding carry closed");
        } else {
            tracing::info!(rate = %snap.rate, "funding carry short opened");
        }
        Ok(())
    }
}

#[async_trait]
impl Strategy for XfundingLite {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "xfunding-lite tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use std::sync::Arc as StdArc;

    use buffa::EnumValue;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, proto::trading};

    #[tokio::test]
    async fn positive_rate_opens_short_and_zero_rate_closes() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.0005)).await;
        let config = XfundingLiteConfig::from_params(
            &toml::from_str("symbol = \"BTCUSDT\"\nqty = \"0.1\"").expect("toml"),
        )
        .expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let funding: StdArc<dyn FundingRateSource> = adapter.clone();
        let s = XfundingLite::new(config, gateway, funding);

        s.tick().await.expect("tick");
        assert_eq!(adapter.trigger_orders().await.len(), 0);
        let open = adapter
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(open.len(), 1, "short opened");

        adapter.set_funding_rate(Decimal::ZERO).await;
        s.tick().await.expect("tick");
        let open = adapter
            .fetch_open_orders(crate::proto::trading::FetchOpenOrdersRequest::default())
            .await
            .expect("orders");
        assert_eq!(open.len(), 2, "close buy placed");
    }

    // -----------------------------------------------------------------------
    // The two threshold comparisons
    // -----------------------------------------------------------------------

    async fn open_orders(adapter: &MockAdapter) -> Vec<trading::Order> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("the mock lists open orders")
    }

    /// The configured quantity must reach the venue on both legs.
    fn order_amount(order: &trading::Order) -> Decimal {
        order
            .amount
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()
            .expect("the contract decimal decodes")
            .expect("the amount field is set")
    }

    fn carry(adapter: &StdArc<MockAdapter>, doc: &str) -> XfundingLite {
        let full = format!("symbol = \"BTCUSDT\"\n{doc}");
        let config =
            XfundingLiteConfig::from_params(&toml::from_str(&full).expect("toml")).expect("config");
        let gateway: StdArc<dyn TradingGateway> = adapter.clone();
        let funding: StdArc<dyn FundingRateSource> = adapter.clone();
        XfundingLite::new(config, gateway, funding)
    }

    /// The enter comparison is `>=`, so a rate sitting exactly on the threshold
    /// opens the short. Anything stricter would make the documented threshold
    /// unreachable.
    #[tokio::test]
    async fn a_rate_exactly_at_the_enter_threshold_opens_the_short() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");

        s.tick().await.expect("tick");
        let orders = open_orders(&adapter).await;
        assert_eq!(orders.len(), 1, "`rate >= enter_threshold` includes the threshold itself");
        assert!(matches!(orders[0].side, EnumValue::Known(trading::OrderSide::Sell)));
        assert_eq!(*s.phase.lock().await, Phase::Short);
    }

    #[tokio::test]
    async fn a_rate_just_below_the_enter_threshold_stays_flat() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.000999)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");

        s.tick().await.expect("tick");
        assert!(open_orders(&adapter).await.is_empty());
        assert_eq!(*s.phase.lock().await, Phase::Flat);
    }

    /// Only a *positive* rate is favourable for a short. A deeply negative rate
    /// is the best possible carry for a long, so entering a short on it would
    /// pay the position to hold it.
    #[tokio::test]
    async fn a_negative_rate_never_opens_a_short_from_flat() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(-0.5)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");

        s.tick().await.expect("tick");
        assert!(open_orders(&adapter).await.is_empty());
        assert_eq!(*s.phase.lock().await, Phase::Flat);
    }

    /// The exit comparison is a strict `<` on the rate *magnitude*, so a rate
    /// exactly on the exit threshold keeps the short open — on both signs.
    #[tokio::test]
    async fn a_rate_magnitude_exactly_at_the_exit_threshold_holds_the_short() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");
        s.tick().await.expect("opened the short");

        for rate in [dec!(0.0001), dec!(-0.0001)] {
            adapter.set_funding_rate(rate).await;
            s.tick().await.expect("tick");
            assert_eq!(
                open_orders(&adapter).await.len(),
                1,
                "the exit comparison is a strict `<`, so {rate} still holds the short"
            );
            assert_eq!(*s.phase.lock().await, Phase::Short);
        }
    }

    #[tokio::test]
    async fn a_rate_magnitude_just_below_the_exit_threshold_closes_the_short() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(
            &adapter,
            "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"\nqty = \"0.25\"",
        );
        s.tick().await.expect("opened the short");

        adapter.set_funding_rate(dec!(0.00009)).await;
        s.tick().await.expect("closed the short");
        let orders = open_orders(&adapter).await;
        assert_eq!(orders.len(), 2);
        assert!(matches!(orders[1].side, EnumValue::Known(trading::OrderSide::Buy)));
        assert_eq!(*s.phase.lock().await, Phase::Flat);
    }

    /// The phase guard is what stops the strategy from stacking the same leg on
    /// every poll while the rate stays extreme.
    #[tokio::test]
    async fn a_leg_is_never_repeated_while_the_rate_stays_extreme() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.5)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");

        for _ in 0..3 {
            s.tick().await.expect("tick");
        }
        assert_eq!(open_orders(&adapter).await.len(), 1);
    }

    /// The phase is the edge trigger, so a rejection must not consume it: the
    /// strategy stays flat and the next poll re-sends the same short exactly
    /// once (the retry latches, the poll after that is the no-op arm).
    #[tokio::test]
    async fn a_rejected_entry_stays_flat_so_the_next_poll_retries_once() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.5)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");
        adapter.fail_next_creates(1).await;

        assert!(s.tick().await.is_err(), "the venue rejection must surface");
        assert_eq!(*s.phase.lock().await, Phase::Flat, "the phase must not switch on a rejection");

        s.tick().await.expect("the second cycle does not error");
        assert_eq!(*s.phase.lock().await, Phase::Short);
        let orders = open_orders(&adapter).await;
        assert_eq!(orders.len(), 1, "the retried signal is re-sent");
        assert!(matches!(orders[0].side, EnumValue::Known(trading::OrderSide::Sell)));

        s.tick().await.expect("tick");
        assert_eq!(open_orders(&adapter).await.len(), 1, "the retry latched exactly once");
    }

    /// The mirror: a rejected exit must leave the short open, or the strategy
    /// would forget it is short and never close the position.
    #[tokio::test]
    async fn a_rejected_exit_keeps_the_short_open_so_the_next_poll_retries() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");
        s.tick().await.expect("opened the short");

        adapter.set_funding_rate(Decimal::ZERO).await;
        adapter.fail_next_creates(1).await;
        assert!(s.tick().await.is_err(), "the venue rejection must surface");
        assert_eq!(*s.phase.lock().await, Phase::Short, "a failed exit must not go flat");

        s.tick().await.expect("the second cycle does not error");
        assert_eq!(*s.phase.lock().await, Phase::Flat);
        let orders = open_orders(&adapter).await;
        assert_eq!(orders.len(), 2, "the retried exit is re-sent");
        assert!(matches!(orders[1].side, EnumValue::Known(trading::OrderSide::Buy)));
    }

    /// Both legs carry the configured quantity.
    #[tokio::test]
    async fn both_legs_use_the_configured_quantity() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(
            &adapter,
            "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"\nqty = \"0.25\"",
        );
        s.tick().await.expect("opened the short");
        adapter.set_funding_rate(Decimal::ZERO).await;
        s.tick().await.expect("closed the short");

        let orders = open_orders(&adapter).await;
        assert_eq!(order_amount(&orders[0]), dec!(0.25));
        assert_eq!(order_amount(&orders[1]), dec!(0.25));
    }

    /// A venue that tracks no funding for this contract must not be answered
    /// with someone else's rate, and the failure has to reach the caller.
    #[tokio::test]
    async fn a_rate_read_the_venue_cannot_answer_propagates() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.5)).await;
        adapter.set_funding_symbols(vec!["ETHUSDT".to_string()]).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");

        let err = s.tick().await.expect_err("an unlisted contract must not borrow a rate");
        assert!(err.to_string().contains("no funding for BTCUSDT"), "{err}");
        assert!(open_orders(&adapter).await.is_empty(), "no leg may be opened on a failed read");
        assert_eq!(*s.phase.lock().await, Phase::Flat);
    }

    /// A failed read happens before the phase is consulted, so it must leave the
    /// position phase alone: otherwise the strategy would forget it is short.
    #[tokio::test]
    async fn a_failed_rate_read_leaves_the_position_phase_untouched() {
        let adapter = StdArc::new(MockAdapter::new(dec!(100)));
        adapter.set_funding_rate(dec!(0.001)).await;
        let s = carry(&adapter, "enter_threshold = \"0.001\"\nexit_threshold = \"0.0001\"");
        s.tick().await.expect("opened the short");

        adapter.set_funding_symbols(vec!["ETHUSDT".to_string()]).await;
        assert!(s.tick().await.is_err());

        adapter.set_funding_symbols(vec!["BTCUSDT".to_string()]).await;
        adapter.set_funding_rate(Decimal::ZERO).await;
        s.tick().await.expect("closed the short");
        assert_eq!(open_orders(&adapter).await.len(), 2, "the phase survived the failed read");
    }

    /// Pin the defaults: they decide at which rate the strategy opens and closes
    /// when the config says nothing.
    #[test]
    fn the_default_thresholds_and_quantity_are_pinned() {
        let table: toml::Table = toml::from_str("symbol = \"BTCUSDT\"").expect("toml");
        let cfg = XfundingLiteConfig::from_params(&table).expect("config");
        assert_eq!(cfg.enter_threshold, dec!(0.0001));
        assert_eq!(cfg.exit_threshold, dec!(0.00001));
        assert_eq!(cfg.qty, dec!(0.001));
    }
}
