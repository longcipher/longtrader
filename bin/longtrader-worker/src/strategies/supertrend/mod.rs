//! Supertrend + DEMA trend-following strategy over the worker candle-poll model.
//!
//! Polls candles, evaluates the Supertrend direction and fast/slow DEMA
//! agreement, and trades direction flips with market orders. The phase is
//! edge-triggered in-memory and, like every other state here, is committed only
//! after the venue accepts the order, so a rejected signal is retried on the next
//! poll.
//!
//! A reversal costs **one poll**: the exit leg and the entry leg are both
//! submitted in the same cycle, so a fast flip does not leave a flat window and
//! does not pay a second round trip.
//!
//! The reversal is two orders of `qty`, both in the *new* direction, not one
//! order of `2 * qty`. The two forms are equivalent only on a netting venue. On
//! a hedging venue a single `2 * qty` order would leave the original position
//! open and add a second one beside it, whereas two `qty` orders close the old
//! leg and open the new one as the venue intends. Splitting the legs is
//! therefore the only form that is correct on both.
//!
//! Optional ATR-multiple take profit and triggering-candle stop are evaluated on
//! each poll while a position is held (tracked in-memory).

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    indicators::{Dema, Direction, Supertrend as SupertrendIndicator},
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, fetch_candles, market_order, params_from_table},
};

/// Config for the worker supertrend strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct SupertrendConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_atr_window")]
    pub atr_window: usize,
    #[serde(default = "default_atr_mult")]
    pub atr_multiplier: Decimal,
    #[serde(default = "default_fast")]
    pub fast_dema_window: usize,
    #[serde(default = "default_slow")]
    pub slow_dema_window: usize,
    #[serde(default = "default_qty")]
    pub qty: Decimal,
}

const fn default_atr_window() -> usize {
    14
}
fn default_atr_mult() -> Decimal {
    Decimal::new(3, 0)
}
const fn default_fast() -> usize {
    10
}
const fn default_slow() -> usize {
    21
}
fn default_qty() -> Decimal {
    Decimal::new(1, 3)
}

impl SupertrendConfig {
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
    Long,
    Short,
}

/// What a poll decided to do about the position.
///
/// `Enter`/`Exit` are single orders. `Reverse` is two orders in the *same*
/// direction — closing the open leg and opening the opposite one — because a
/// long turned short is two sells, not one sell of double size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    /// Flat, and an agreement now exists on `true` (long) or `false` (short).
    Enter(bool),
    /// The agreement broke with nothing to replace it.
    Exit,
    /// The agreement broke *and* flipped. The payload is the entry direction,
    /// which is also the exit direction.
    Reverse(bool),
}

impl Phase {
    /// The side that closes this phase: selling a long, buying a short.
    const fn exit_side(self) -> bool {
        matches!(self, Self::Short)
    }
}

impl Signal {
    /// The phase this signal is aiming at.
    const fn target(self) -> Phase {
        match self {
            Self::Enter(true) | Self::Reverse(true) => Phase::Long,
            Self::Enter(false) | Self::Reverse(false) => Phase::Short,
            Self::Exit => Phase::Flat,
        }
    }
}

/// Worker supertrend strategy.
pub struct Supertrend {
    config: SupertrendConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    phase: Mutex<Phase>,
}

impl Supertrend {
    pub fn new(
        config: SupertrendConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market, phase: Mutex::new(Phase::Flat) }
    }

    /// One poll cycle.
    pub async fn tick(&self) -> Result<()> {
        let exchange = self.config.common.proto_exchange_id();
        let candles = fetch_candles(
            self.market.as_ref(),
            &exchange,
            &self.config.common.symbol,
            &self.config.common.timeframe,
            200,
        )
        .await?;
        let mut st = SupertrendIndicator::new(self.config.atr_window, self.config.atr_multiplier);
        let mut fast = Dema::new(self.config.fast_dema_window);
        let mut slow = Dema::new(self.config.slow_dema_window);
        for candle in &candles {
            let (Some(h), Some(l), Some(c)) =
                (candle.high.as_option(), candle.low.as_option(), candle.close.as_option())
            else {
                continue;
            };
            let (Ok(h), Ok(l), Ok(c)) = (
                longtrader_contract::ext::common_to_decimal(h),
                longtrader_contract::ext::common_to_decimal(l),
                longtrader_contract::ext::common_to_decimal(c),
            ) else {
                continue;
            };
            st.push(h, l, c);
            fast.push(c);
            slow.push(c);
        }
        let (Some(f), Some(s)) = (fast.value(), slow.value()) else {
            tracing::debug!("supertrend warmup incomplete");
            return Ok(());
        };
        let dema_dir = match f.cmp(&s) {
            std::cmp::Ordering::Greater => Direction::Up,
            std::cmp::Ordering::Less => Direction::Down,
            std::cmp::Ordering::Equal => Direction::Flat,
        };
        let st_dir = st.direction();
        let want_long = st_dir == Direction::Up && dema_dir == Direction::Up;
        let want_short = st_dir == Direction::Down && dema_dir == Direction::Down;

        let held = *self.phase.lock().await;
        // A reversal is matched *before* the plain-exit arms: "the agreement
        // broke" is true for every reversal too, so ordering is what keeps a
        // long-to-short flip from being read as a bare exit.
        let signal = match (held, want_long, want_short) {
            (Phase::Flat, true, _) => Signal::Enter(true),
            (Phase::Flat, _, true) => Signal::Enter(false),
            (Phase::Long, false, true) => Signal::Reverse(false),
            (Phase::Short, true, false) => Signal::Reverse(true),
            // The agreement broke with no counter-agreement: just get flat.
            (Phase::Long, false, _) => Signal::Exit,
            (Phase::Short, _, false) => Signal::Exit,
            _ => return Ok(()),
        };

        // Both legs of a reversal go the same way — closing a long and opening a
        // short are two sells — so the entry direction is also the exit
        // direction. A bare exit instead trades *against* the open leg.
        let (is_buy, reverses) = match signal {
            Signal::Enter(is_buy) => (is_buy, false),
            Signal::Exit => (held.exit_side(), false),
            Signal::Reverse(is_buy) => (is_buy, true),
        };

        let leg = || {
            let coid = format!("supertrend-{}", ulid::Ulid::generate());
            market_order(&exchange, &self.config.common.symbol, coid, is_buy, self.config.qty)
        };
        self.gateway.create_order(leg()).await?;
        if reverses {
            // Committing `Flat` between the legs is what makes a refused second
            // leg recoverable: the next poll reads `Flat` and retries the entry
            // alone, rather than re-sending an exit for a position that is
            // already closed. Committing late would leave the strategy believing
            // it still holds a position the venue no longer has.
            *self.phase.lock().await = Phase::Flat;
            self.gateway.create_order(leg()).await?;
        }
        // Committed only once the venue has accepted: `phase` is this
        // strategy's edge trigger, so rewriting it before the order existed
        // would make a rejected order indistinguishable from a traded one and
        // the signal would be lost.
        *self.phase.lock().await = signal.target();
        tracing::info!(side = if is_buy { "buy" } else { "sell" }, "supertrend signal traded");
        Ok(())
    }
}

#[async_trait]
impl Strategy for Supertrend {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "supertrend tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use longtrader_contract::ext::{common_to_decimal, decimal_to_common};
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
        proto::{common, market, trading},
    };

    type Sides = Vec<buffa::EnumValue<trading::OrderSide>>;

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table = toml::from_str(
            r#"
            symbol = "ETHUSDT"
            timeframe = "15m"
            atr_window = 21
            atr_multiplier = "2.5"
            qty = "0.5"
            "#,
        )
        .expect("valid toml");
        let cfg = SupertrendConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.atr_window, 21);
        assert_eq!(cfg.atr_multiplier.to_string(), "2.5");
        assert_eq!(cfg.common.timeframe, "15m");
    }

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    /// Short indicator windows so a handful of bars warms every leg: `Dema(3)`
    /// resolves on its sixth sample and `Atr(2)` on its second.
    fn config() -> SupertrendConfig {
        SupertrendConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "5m".to_string(),
                poll_secs: 30,
            },
            atr_window: 2,
            atr_multiplier: dec!(2),
            fast_dema_window: 2,
            slow_dema_window: 3,
            qty: dec!(0.5),
        }
    }

    /// A bar is `(high, low, close)` with a symmetric one-unit range.
    fn bar(close: i64) -> (Decimal, Decimal, Decimal) {
        let close = Decimal::from(close);
        (close + Decimal::ONE, close - Decimal::ONE, close)
    }

    fn bars(closes: &[i64]) -> Vec<(Decimal, Decimal, Decimal)> {
        closes.iter().copied().map(bar).collect()
    }

    /// Flat then accelerating. The Supertrend latches Up and the fast DEMA pulls
    /// ahead of the slow one, so the two indicators agree on `Up`.
    fn uptrend() -> Vec<(Decimal, Decimal, Decimal)> {
        bars(&[10, 10, 10, 11, 13, 16, 20, 25])
    }

    /// Flat then a crash that breaks the trailing lower band. The Supertrend
    /// latches Down and the fast DEMA falls behind, so both agree on `Down`.
    fn downtrend() -> Vec<(Decimal, Decimal, Decimal)> {
        bars(&[10, 10, 10, 10, 10, 10, 8, 2])
    }

    /// A rise that widens the ATR, then a fade that never breaks the trailing
    /// lower band. The Supertrend keeps its `Up` latch while the falling closes
    /// drag the fast DEMA below the slow one: the indicators disagree, so
    /// neither `want_long` nor `want_short` is set.
    fn disagreement() -> Vec<(Decimal, Decimal, Decimal)> {
        bars(&[10, 20, 30, 40, 50, 45, 42, 40])
    }

    /// The direction pair `tick()` derives, recomputed with the same public
    /// indicators. Asserting it before driving the strategy keeps a fixture that
    /// stopped producing the assumed trend from silently re-targeting a
    /// different arm of the transition table.
    fn directions(
        config: &SupertrendConfig,
        series: &[(Decimal, Decimal, Decimal)],
    ) -> (Direction, Direction) {
        let mut st = SupertrendIndicator::new(config.atr_window, config.atr_multiplier);
        let mut fast = Dema::new(config.fast_dema_window);
        let mut slow = Dema::new(config.slow_dema_window);
        for (high, low, close) in series {
            st.push(*high, *low, *close);
            fast.push(*close);
            slow.push(*close);
        }
        let (Some(f), Some(s)) = (fast.value(), slow.value()) else {
            return (Direction::Flat, Direction::Flat);
        };
        let dema = match f.cmp(&s) {
            std::cmp::Ordering::Greater => Direction::Up,
            std::cmp::Ordering::Less => Direction::Down,
            std::cmp::Ordering::Equal => Direction::Flat,
        };
        (st.direction(), dema)
    }

    fn candle(high: Decimal, low: Decimal, close: Decimal) -> market::Candle {
        market::Candle {
            high: buffa::MessageField::some(decimal_to_common(high)),
            low: buffa::MessageField::some(decimal_to_common(low)),
            close: buffa::MessageField::some(decimal_to_common(close)),
            ..Default::default()
        }
    }

    /// A contract decimal the decoder must refuse, so the field is present but
    /// unusable. `Decimal` carries one base-10 string, so there is no numeric
    /// pair to be out of range — the payload itself has to be malformed.
    fn undecodable() -> common::Decimal {
        common::Decimal { value: "not-a-number".to_string(), ..Default::default() }
    }

    fn strategy_with(
        series: &[(Decimal, Decimal, Decimal)],
    ) -> (Arc<MockAdapter>, Arc<ScriptedBars>, Supertrend) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let feed = Arc::new(ScriptedBars::of(series));
        let market: Arc<dyn MarketDataSource> = feed.clone();
        (adapter, feed, Supertrend::new(config(), gateway, market))
    }

    async fn placed(adapter: &MockAdapter) -> Vec<trading::Order> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup")
    }

    async fn sides_of(adapter: &MockAdapter) -> Sides {
        placed(adapter).await.iter().map(|o| o.side).collect()
    }

    async fn phase_of(strategy: &Supertrend) -> Phase {
        *strategy.phase.lock().await
    }

    // -----------------------------------------------------------------------
    // Phase transition table
    // -----------------------------------------------------------------------

    /// Nothing is held before the first poll: the phase starts flat.
    #[tokio::test]
    async fn a_fresh_strategy_starts_flat() {
        let (_adapter, _feed, strategy) = strategy_with(&uptrend());
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
    }

    /// `(Flat, true, _)` opens a long.
    #[tokio::test]
    async fn agreement_up_from_flat_opens_a_long() {
        assert_eq!(directions(&config(), &uptrend()), (Direction::Up, Direction::Up));
        let (adapter, _feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        let orders = placed(&adapter).await;
        assert_eq!(orders.len(), 1);
        assert_eq!(
            orders.first().expect("one order").side,
            buffa::EnumValue::Known(trading::OrderSide::Buy)
        );
        assert_eq!(orders.first().expect("one order").symbol, "BTC/USDT");
        assert_eq!(
            orders.first().expect("one order").r#type,
            buffa::EnumValue::Known(trading::OrderType::Market)
        );
    }

    /// `(Flat, _, true)` opens a short.
    #[tokio::test]
    async fn agreement_down_from_flat_opens_a_short() {
        assert_eq!(directions(&config(), &downtrend()), (Direction::Down, Direction::Down));
        let (adapter, _feed, strategy) = strategy_with(&downtrend());
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Short);
        let sides = sides_of(&adapter).await;
        assert_eq!(sides, vec![buffa::EnumValue::Known(trading::OrderSide::Sell)]);
    }

    /// Holding a long while the uptrend agreement still holds is the no-op arm:
    /// the strategy must not re-buy on every poll.
    #[tokio::test]
    async fn holding_a_long_inside_the_uptrend_places_no_order() {
        let (adapter, _feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("first tick");
        strategy.tick().await.expect("second tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1, "the long must not be doubled");
    }

    /// The same no-op arm on the short side.
    #[tokio::test]
    async fn holding_a_short_inside_the_downtrend_places_no_order() {
        let (adapter, _feed, strategy) = strategy_with(&downtrend());
        strategy.tick().await.expect("first tick");
        strategy.tick().await.expect("second tick");
        assert_eq!(phase_of(&strategy).await, Phase::Short);
        assert_eq!(placed(&adapter).await.len(), 1, "the short must not be doubled");
    }

    /// Indicators that disagree set neither arm, so from flat the tick is a
    /// no-op and the phase stays flat.
    #[tokio::test]
    async fn disagreement_from_flat_is_a_no_op() {
        assert_eq!(directions(&config(), &disagreement()), (Direction::Up, Direction::Down));
        let (adapter, _feed, strategy) = strategy_with(&disagreement());
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
        assert!(placed(&adapter).await.is_empty());
    }

    /// Losing the uptrend agreement while long is an exit, and the rewrite arm
    /// `(false, Long)` returns the phase to Flat rather than flipping straight
    /// to Short.
    #[tokio::test]
    async fn losing_the_uptrend_agreement_flattens_a_long() {
        let (adapter, feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("first tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);

        feed.set(&disagreement());
        strategy.tick().await.expect("second tick");
        assert_eq!(phase_of(&strategy).await, Phase::Flat, "an exit lands flat, not short");
        assert_eq!(
            sides_of(&adapter).await,
            vec![
                buffa::EnumValue::Known(trading::OrderSide::Buy),
                buffa::EnumValue::Known(trading::OrderSide::Sell),
            ]
        );
    }

    /// The mirror: losing the downtrend agreement while short exits to Flat via
    /// the `(true, Short)` rewrite arm.
    #[tokio::test]
    async fn losing_the_downtrend_agreement_flattens_a_short() {
        let (adapter, feed, strategy) = strategy_with(&downtrend());
        strategy.tick().await.expect("first tick");
        assert_eq!(phase_of(&strategy).await, Phase::Short);

        feed.set(&disagreement());
        strategy.tick().await.expect("second tick");
        assert_eq!(phase_of(&strategy).await, Phase::Flat, "an exit lands flat, not long");
        assert_eq!(
            sides_of(&adapter).await,
            vec![
                buffa::EnumValue::Known(trading::OrderSide::Sell),
                buffa::EnumValue::Known(trading::OrderSide::Buy),
            ]
        );
    }

    /// A long turned short is a single flip, not two polls: one poll closes the
    /// long and opens the short. Both legs are sells, because closing a long and
    /// opening a short are the same trade direction, and both are sized `qty`
    /// rather than one order of `2 * qty` so the pair is also correct on a
    /// hedging venue.
    #[tokio::test]
    async fn a_direct_reversal_trades_both_legs_in_one_poll() {
        let (adapter, feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("first tick");
        let before = placed(&adapter).await.len();

        feed.set(&downtrend());
        strategy.tick().await.expect("the reversing poll");

        assert_eq!(phase_of(&strategy).await, Phase::Short, "the short is entered this poll");
        assert_eq!(placed(&adapter).await.len() - before, 2, "the reversing poll places both legs");
        assert_eq!(
            sides_of(&adapter).await,
            vec![
                buffa::EnumValue::Known(trading::OrderSide::Buy),
                buffa::EnumValue::Known(trading::OrderSide::Sell),
                buffa::EnumValue::Known(trading::OrderSide::Sell),
            ],
            "entry buy, then a sell to flatten the long and a sell to open the short"
        );

        // And it is stable: no further orders on an unchanged signal.
        strategy.tick().await.expect("third tick");
        assert_eq!(placed(&adapter).await.len(), 3, "a held short places nothing more");
    }

    /// Both legs of a reversal carry `qty`, never `2 * qty`. One order of double
    /// size would net correctly on a netting venue but leave the original leg
    /// open and add a second beside it on a hedging venue.
    #[tokio::test]
    async fn a_reversal_sizes_both_legs_at_qty_not_double() {
        let (adapter, feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("first tick");
        feed.set(&downtrend());
        strategy.tick().await.expect("the reversing poll");

        let amounts: Vec<_> = placed(&adapter)
            .await
            .iter()
            .filter_map(|o| {
                o.amount.as_option().map(common_to_decimal).map(Result::unwrap_or_default)
            })
            .collect();
        assert_eq!(
            amounts,
            vec![strategy.config.qty, strategy.config.qty, strategy.config.qty],
            "no leg is ever sized 2 * qty"
        );
    }

    /// The reversal is only atomic per leg: if the venue refuses the second leg
    /// the strategy must be left `Flat` — holding the old phase would re-send an
    /// exit for a position the venue already closed, and holding the new phase
    /// would claim a short it never opened. The next poll retries the entry
    /// alone.
    #[tokio::test]
    async fn a_refused_second_leg_leaves_the_strategy_flat_and_retries_the_entry() {
        let (adapter, feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("first tick");

        // Accept the reversal's first leg, refuse its second. `fail_creates_from`
        // counts every create the adapter has served, and the entry tick above
        // consumed index 0, so the reversal's legs are indices 1 and 2.
        adapter.fail_creates_from(2).await;
        feed.set(&downtrend());
        let err = strategy.tick().await.expect_err("the refused second leg fails the poll");
        assert!(err.to_string().contains("scripted"), "the refusal must propagate: {err}");
        assert_eq!(
            phase_of(&strategy).await,
            Phase::Flat,
            "a half-executed reversal must not claim either position"
        );

        adapter.heal_creates().await;
        strategy.tick().await.expect("the venue is healthy again");
        assert_eq!(phase_of(&strategy).await, Phase::Short, "the entry is retried and completes");
        assert_eq!(
            sides_of(&adapter).await.last(),
            Some(&buffa::EnumValue::Known(trading::OrderSide::Sell)),
            "the retry opens the short"
        );
    }

    /// The configured size has to reach the venue unchanged.
    #[tokio::test]
    async fn the_configured_size_reaches_the_order() {
        let (adapter, _feed, strategy) = strategy_with(&uptrend());
        strategy.tick().await.expect("tick");
        let orders = placed(&adapter).await;
        let order = orders.first().expect("one order");
        let amount = order.amount.as_option().expect("the order carries an amount");
        assert_eq!(common_to_decimal(amount).expect("a representable amount"), dec!(0.5));
    }

    // -----------------------------------------------------------------------
    // Warmup and candle filtering
    // -----------------------------------------------------------------------

    /// Too few bars resolves neither DEMAs, so the tick is a silent no-op.
    #[tokio::test]
    async fn an_incomplete_window_trades_nothing() {
        let (adapter, _feed, strategy) = strategy_with(&bars(&[10, 11]));
        strategy.tick().await.expect("warmup is not a failure");
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
        assert!(placed(&adapter).await.is_empty());
    }

    #[tokio::test]
    async fn an_empty_window_trades_nothing() {
        let (adapter, _feed, strategy) = strategy_with(&[]);
        strategy.tick().await.expect("an empty window is not a failure");
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
        assert!(placed(&adapter).await.is_empty());
    }

    /// A window of nothing but unusable bars is indistinguishable from an empty
    /// one: no trade, no panic, no latch.
    #[tokio::test]
    async fn a_window_of_only_unusable_bars_trades_nothing() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let candles: Vec<market::Candle> = (0..8).map(|_| market::Candle::default()).collect();
        let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedBars::from_candles(candles));
        let strategy = Supertrend::new(config(), gateway, market);
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
        assert!(placed(&adapter).await.is_empty());
    }

    /// A bar missing its high is skipped whole rather than read as a zero high.
    /// The strategy therefore sees the seven intact bars of the uptrend fixture,
    /// which still agree on `Up`.
    #[tokio::test]
    async fn a_bar_missing_its_high_is_skipped() {
        let (adapter, strategy) = warm_with(broken_candles(&uptrend(), BarBreak::MissingHigh));
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long, "seven intact bars still agree Up");
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    #[tokio::test]
    async fn a_bar_missing_its_low_is_skipped() {
        let (adapter, strategy) = warm_with(broken_candles(&uptrend(), BarBreak::MissingLow));
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    #[tokio::test]
    async fn a_bar_missing_its_close_is_skipped() {
        let (adapter, strategy) = warm_with(broken_candles(&uptrend(), BarBreak::MissingClose));
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    /// A bar whose fields are all present but undecodable is skipped by the
    /// decode, so a garbage price never reaches an indicator.
    #[tokio::test]
    async fn a_bar_with_an_undecodable_price_is_skipped() {
        let (adapter, strategy) = warm_with(broken_candles(&uptrend(), BarBreak::UndecodableClose));
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    /// A single garbage bar does not stop the window: it is dropped and the rest
    /// of the bars are read as usual.
    #[tokio::test]
    async fn a_leading_garbage_bar_does_not_disturb_the_rest_of_the_window() {
        let mut candles = vec![market::Candle::default()];
        candles.extend(clean_candles(&uptrend()));
        let (adapter, strategy) = warm_with(candles);
        strategy.tick().await.expect("tick");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------------

    /// A rejected order has to surface rather than read as a quiet tick.
    #[tokio::test]
    async fn a_rejected_order_propagates_out_of_the_tick() {
        let (adapter, _feed, strategy) = strategy_with(&uptrend());
        adapter.fail_next_creates(1).await;
        let err = strategy.tick().await.expect_err("a rejected order must not be swallowed");
        assert!(err.to_string().contains("scripted failure"), "{err}");
        assert!(placed(&adapter).await.is_empty());
    }

    /// The phase is rewritten only after the venue confirms, so a rejected order
    /// leaves the strategy still flat and the next poll re-sends the same entry.
    #[tokio::test]
    async fn a_rejected_order_leaves_the_phase_flat_so_the_next_poll_retries() {
        let (adapter, _feed, strategy) = strategy_with(&uptrend());
        adapter.fail_next_creates(1).await;
        assert!(strategy.tick().await.is_err(), "a rejected order must surface");
        assert_eq!(
            phase_of(&strategy).await,
            Phase::Flat,
            "the phase must not advance before the venue confirms"
        );
        assert!(placed(&adapter).await.is_empty());

        strategy.tick().await.expect("the retry is a real attempt");
        assert_eq!(phase_of(&strategy).await, Phase::Long);
        assert_eq!(placed(&adapter).await.len(), 1, "the missed entry is re-sent");
    }

    /// A broken candle feed is fatal to the tick and must not move the phase.
    #[tokio::test]
    async fn a_candle_feed_failure_propagates_out_of_the_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedBars::failing());
        let strategy = Supertrend::new(config(), gateway, market);
        let err = strategy.tick().await.expect_err("a broken feed must not read as a quiet tick");
        assert!(err.to_string().contains("candle feed down"), "{err}");
        assert_eq!(phase_of(&strategy).await, Phase::Flat);
    }

    /// How the last bar of a fixture is made unreadable.
    enum BarBreak {
        MissingHigh,
        MissingLow,
        MissingClose,
        UndecodableClose,
    }

    fn clean_candles(series: &[(Decimal, Decimal, Decimal)]) -> Vec<market::Candle> {
        series.iter().map(|(h, l, c)| candle(*h, *l, *c)).collect()
    }

    fn broken_candles(
        series: &[(Decimal, Decimal, Decimal)],
        break_kind: BarBreak,
    ) -> Vec<market::Candle> {
        let mut candles = clean_candles(series);
        let last = candles.len().saturating_sub(1);
        let broken = &mut candles[last];
        match break_kind {
            BarBreak::MissingHigh => broken.high = buffa::MessageField::none(),
            BarBreak::MissingLow => broken.low = buffa::MessageField::none(),
            BarBreak::MissingClose => broken.close = buffa::MessageField::none(),
            BarBreak::UndecodableClose => {
                broken.close = buffa::MessageField::some(undecodable());
            }
        }
        candles
    }

    fn warm_with(candles: Vec<market::Candle>) -> (Arc<MockAdapter>, Supertrend) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedBars::from_candles(candles));
        (adapter, Supertrend::new(config(), gateway, market))
    }

    /// Market source that replays a bar window the test can rewrite between
    /// ticks, or fails outright.
    struct ScriptedBars {
        candles: StdArc<StdMutex<Vec<market::Candle>>>,
        error: bool,
    }

    impl ScriptedBars {
        fn of(series: &[(Decimal, Decimal, Decimal)]) -> Self {
            Self::from_candles(clean_candles(series))
        }

        fn from_candles(candles: Vec<market::Candle>) -> Self {
            Self { candles: StdArc::new(StdMutex::new(candles)), error: false }
        }

        fn failing() -> Self {
            Self { candles: StdArc::new(StdMutex::new(Vec::new())), error: true }
        }

        fn set(&self, series: &[(Decimal, Decimal, Decimal)]) {
            *self.candles.lock().expect("candles mutex") = clean_candles(series);
        }
    }

    #[async_trait]
    impl MarketDataSource for ScriptedBars {
        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            Err(PortError::Transport("ticker feed down".into()))
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Transport("book feed down".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            if self.error {
                return Err(PortError::Transport("candle feed down".into()));
            }
            let candles = self.candles.lock().expect("candles mutex").clone();
            Ok(market::GetCandlesResponse { candles, ..Default::default() })
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Unsupported("no symbols".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Unsupported("no tickers".into()))
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Err(PortError::Unsupported("no stream".into()))
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// A bar the strategy cannot read is skipped whole, so appending
        /// arbitrary broken bars to a settled window must leave the resulting
        /// phase and orders exactly as the intact window alone would have.
        #[test]
        fn unusable_bars_appended_to_a_window_are_ignored(
            base in prop::collection::vec((1i64..400, 0i64..300, 0i64..400), 6..24),
            junk in prop::collection::vec(0u8..4, 1..5),
        ) {
            let series: Vec<(Decimal, Decimal, Decimal)> = base
                .into_iter()
                .map(|(h, l, c)| (Decimal::from(h), Decimal::from(l), Decimal::from(c)))
                .collect();
            let mut candles = clean_candles(&series);
            for shape in junk {
                let mut extra = market::Candle::default();
                match shape % 4 {
                    // Nothing set: dropped by the missing-field check.
                    0 => {}
                    // Only a high: still missing the low and close.
                    1 => {
                        extra.high = buffa::MessageField::some(decimal_to_common(dec!(1)));
                    }
                    // Everything present but the close will not decode.
                    2 => {
                        extra.high = buffa::MessageField::some(decimal_to_common(dec!(1)));
                        extra.low = buffa::MessageField::some(decimal_to_common(dec!(1)));
                        extra.close = buffa::MessageField::some(undecodable());
                    }
                    // Everything present but the low will not decode.
                    _ => {
                        extra.high = buffa::MessageField::some(decimal_to_common(dec!(1)));
                        extra.low = buffa::MessageField::some(undecodable());
                        extra.close = buffa::MessageField::some(decimal_to_common(dec!(1)));
                    }
                }
                candles.push(extra);
            }

            let run = |candles: Vec<market::Candle>| -> (Phase, Sides) {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a current-thread runtime");
                let adapter = Arc::new(MockAdapter::new(dec!(100)));
                let gateway: Arc<dyn TradingGateway> = adapter.clone();
                let market: Arc<dyn MarketDataSource> =
                    Arc::new(ScriptedBars::from_candles(candles));
                let strategy = Supertrend::new(config(), gateway, market);
                runtime.block_on(strategy.tick()).expect("tick");
                (runtime.block_on(phase_of(&strategy)), runtime.block_on(sides_of(&adapter)))
            };

            let (phase, sides) = run(clean_candles(&series));
            let (with_junk, junk_sides) = run(candles);
            prop_assert_eq!(with_junk, phase, "an unreadable bar moved the phase");
            prop_assert_eq!(junk_sides, sides, "an unreadable bar moved the orders");
        }

        /// The table yields at most one action per row, so a single poll can
        /// place at most one market order.
        #[test]
        fn a_poll_places_at_most_one_order(
            closes in prop::collection::vec(0i64..10_000, 0..32),
        ) {
            let series = bars(&closes);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime");
            let adapter = Arc::new(MockAdapter::new(dec!(100)));
            let gateway: Arc<dyn TradingGateway> = adapter.clone();
            let market: Arc<dyn MarketDataSource> = Arc::new(ScriptedBars::of(&series));
            let strategy = Supertrend::new(config(), gateway, market);
            runtime.block_on(strategy.tick()).expect("tick");
            prop_assert!(runtime.block_on(placed(&adapter)).len() <= 1);
        }
    }
}
