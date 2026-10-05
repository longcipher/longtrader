//! Portfolio rebalance strategy (also the engine behind `market_cap`).
//!
//! Values each configured symbol's balance at its ticker price into the
//! quote currency, compares actual weights against target weights, and
//! rebalances with market orders when a symbol drifts beyond `band_pct`.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, market_order, params_from_table},
};

/// Config for the rebalance strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct RebalanceConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Quote currency used for valuation.
    #[serde(default = "default_quote")]
    pub quote_asset: String,
    /// Target weights per base asset, e.g. `BTC = "0.6"`.
    pub targets: BTreeMap<String, Decimal>,
    /// Rebalance when weight drift exceeds this fraction.
    #[serde(default = "default_band")]
    pub band_pct: Decimal,
}

fn default_quote() -> String {
    String::from("USDT")
}
fn default_band() -> Decimal {
    Decimal::new(5, 2)
}

/// Tolerance on the target-weight sum.
///
/// Hand-maintained weight tables rarely add up to exactly one, and a rounding
/// error of a few parts per million must not stop startup; a drift of any real
/// size still has to be an error.
fn weight_sum_tolerance() -> Decimal {
    Decimal::new(1, 6)
}

impl RebalanceConfig {
    /// Parses config from the `[strategy.params]` table and rejects a weight
    /// table that could not steer a portfolio to its targets.
    ///
    /// # Errors
    /// Propagates deserialization errors, and returns an error naming the
    /// offending weight — or the actual sum — when `targets` is empty, holds a
    /// negative weight, or does not sum to one.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        let config: Self = params_from_table(table)?;
        config.validate_targets()?;
        Ok(config)
    }

    /// Checks that `targets` is a usable weight table.
    ///
    /// Each planned delta is `total * target - held`, so the deltas net out to
    /// `total * (Σtargets - 1)`. Weights that do not add up to one therefore plan
    /// a net buy or a net sell that the strategy has no cash leg for, and a
    /// negative weight inverts a leg's direction outright. Neither shows up at
    /// runtime — the plan just looks plausible — so it belongs here, where it
    /// turns silent accounting drift into a startup failure.
    fn validate_targets(&self) -> Result<()> {
        if self.targets.is_empty() {
            return Err(color_eyre::eyre::eyre!("targets must not be empty"));
        }
        if let Some((asset, weight)) =
            self.targets.iter().find(|(_, weight)| weight.is_sign_negative())
        {
            return Err(color_eyre::eyre::eyre!(
                "target weight for {asset} must not be negative, got {weight}"
            ));
        }
        let total: Decimal = self.targets.values().copied().sum();
        if (total - Decimal::ONE).abs() > weight_sum_tolerance() {
            return Err(color_eyre::eyre::eyre!("targets must sum to 1, got {total}"));
        }
        Ok(())
    }
}

/// Computes per-asset trade deltas (in quote value) to move actual weights
/// toward targets. Positive delta = buy.
///
/// An empty plan is returned for a total that is not strictly positive. A zero
/// total has no weight to scale, and a negative one — a portfolio whose holdings
/// net short — would flip the sign of every target slice and make the plan buy
/// exactly the assets it should sell.
#[must_use]
pub fn plan_trades(
    values: &BTreeMap<String, Decimal>,
    targets: &BTreeMap<String, Decimal>,
) -> BTreeMap<String, Decimal> {
    let total: Decimal = values.values().copied().sum();
    let mut plan = BTreeMap::new();
    if total.is_zero() || total.is_sign_negative() {
        return plan;
    }
    for (asset, target) in targets {
        let actual_value = values.get(asset).copied().unwrap_or_default();
        let want = total * *target;
        let delta = want - actual_value;
        if !delta.is_zero() {
            plan.insert(asset.clone(), delta);
        }
    }
    plan
}

/// Rebalance strategy.
pub struct Rebalance {
    config: RebalanceConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl Rebalance {
    pub fn new(
        config: RebalanceConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    /// Current price of `asset` in quote units.
    ///
    /// A venue that reports a present-but-zero `last` yields `Decimal::ZERO`,
    /// which would make every later `delta / price` a divide-by-zero panic
    /// inside `rust_decimal`. Treat it as "no usable price" instead.
    async fn price_of(&self, asset: &str) -> Result<Decimal> {
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol: format!("{asset}{}", self.config.quote_asset),
                ..Default::default()
            })
            .await?;
        let price = ticker
            .last
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .ok_or_else(|| color_eyre::eyre::eyre!("no price for {asset}"))?;
        if price <= Decimal::ZERO {
            return Err(color_eyre::eyre::eyre!("no usable price for {asset}: {price}"));
        }
        Ok(price)
    }

    /// One valuation + rebalance cycle; returns executed trades.
    pub async fn tick(&self) -> Result<BTreeMap<String, Decimal>> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.gateway.sync_state(&exchange).await?;
        let mut values = BTreeMap::new();
        let mut prices = BTreeMap::new();
        for asset in self.config.targets.keys() {
            let balance = snap
                .balances
                .iter()
                .find(|b| b.currency == *asset)
                .and_then(|b| b.total.as_option())
                .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
                .unwrap_or_default();
            let price = if *asset == self.config.quote_asset {
                Decimal::ONE
            } else {
                self.price_of(asset).await?
            };
            values.insert(asset.clone(), balance * price);
            prices.insert(asset.clone(), price);
        }
        let plan = plan_trades(&values, &self.config.targets);
        let total_value: Decimal = values.values().copied().sum();
        // `plan_trades` is where the non-zero divisor is established: it returns
        // an empty plan unless the total is strictly positive, so a non-empty
        // plan is proof that `total_value` is non-zero and the weights below are
        // safe to divide by.
        for (asset, price) in &prices {
            // Walk the priced targets rather than the plan's keys: `plan` is keyed
            // by target and omits a zero gap, so an absent entry means there is
            // nothing to close. Reading the price from the map the valuation pass
            // already filled keeps this to one ticker read per asset per tick.
            let delta = plan.get(asset).copied().unwrap_or_default();
            if delta.is_zero() {
                continue;
            }
            let actual_weight = values.get(asset).copied().unwrap_or_default() / total_value;
            let target_weight = self.config.targets.get(asset).copied().unwrap_or_default();
            if (actual_weight - target_weight).abs() <= self.config.band_pct {
                tracing::debug!(asset = %asset, "within rebalance band; skipping");
                continue;
            }
            // Convert the quote-value delta into a quantity at the price the
            // valuation used. `price` is strictly positive (the quote leg is one,
            // every other asset goes through `price_of`'s guard) and `delta` is
            // non-zero here, so the quantity cannot be zero — and a zero-quantity
            // market order is refused by every venue.
            let qty = (delta / price).abs();
            let coid = format!("rebal-{}", ulid::Ulid::generate());
            let request = market_order(
                &exchange,
                &format!("{asset}{}", self.config.quote_asset),
                coid,
                delta > Decimal::ZERO,
                qty,
            );
            self.gateway.create_order(request).await?;
            tracing::info!(asset = %asset, %delta, "rebalance trade");
        }
        Ok(plan)
    }
}

#[async_trait]
impl Strategy for Rebalance {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "rebalance tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap as Map,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use longtrader_contract::ext::{common_to_decimal, decimal_to_common};
    use proptest::prelude::*;
    use rust_decimal_macros as rm;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketEventStream, OverflowPolicy, PortError},
        proto::{market, trading},
    };

    #[test]
    fn plan_moves_toward_targets() {
        let mut values = Map::new();
        values.insert(String::from("BTC"), rm::dec!(700));
        values.insert(String::from("USDT"), rm::dec!(300));
        let mut targets = Map::new();
        targets.insert(String::from("BTC"), rm::dec!(0.5));
        targets.insert(String::from("USDT"), rm::dec!(0.5));
        let plan = plan_trades(&values, &targets);
        assert_eq!(plan["BTC"], rm::dec!(-200));
        assert_eq!(plan["USDT"], rm::dec!(200));
    }

    #[test]
    fn empty_portfolio_yields_no_plan() {
        let values: Map<String, Decimal> = Map::new();
        let mut targets = Map::new();
        targets.insert(String::from("BTC"), Decimal::ONE);
        assert!(plan_trades(&values, &targets).is_empty());
    }

    // -----------------------------------------------------------------------
    // plan_trades
    // -----------------------------------------------------------------------

    fn map_of(pairs: &[(&str, Decimal)]) -> Map<String, Decimal> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
    }

    /// A portfolio whose legs cancel out is worth zero, and dividing by a zero
    /// total is exactly what the guard is for: no plan, no panic.
    #[test]
    fn a_portfolio_that_nets_to_zero_yields_no_plan() {
        let values = map_of(&[("BTC", dec!(5)), ("USDT", dec!(-5))]);
        let targets = map_of(&[("BTC", dec!(0.5)), ("USDT", dec!(0.5))]);
        assert!(plan_trades(&values, &targets).is_empty());
    }

    /// A total below zero means the holdings net short, and the plan scales every
    /// target slice by it: `total * target` flips sign, so a target that should
    /// be bought becomes a sale and vice versa. The valuation is nonsense, so the
    /// plan is empty rather than reversed.
    #[test]
    fn a_net_short_portfolio_yields_no_plan() {
        let values = map_of(&[("BTC", dec!(-200)), ("USDT", dec!(100))]);
        let targets = map_of(&[("BTC", dec!(0.5)), ("USDT", dec!(0.5))]);
        assert!(plan_trades(&values, &targets).is_empty());
    }

    /// An asset the portfolio does not hold is valued at zero, so its plan entry
    /// is the full target slice — a buy that opens the position.
    #[test]
    fn an_unheld_target_is_bought_to_its_full_slice() {
        let values = map_of(&[("BTC", dec!(100))]);
        let targets = map_of(&[("BTC", dec!(0.5)), ("ETH", dec!(0.5))]);
        let plan = plan_trades(&values, &targets);
        assert_eq!(plan["BTC"], dec!(-50));
        assert_eq!(plan["ETH"], dec!(50));
    }

    /// A zero target on a held asset is a full exit, not a "no change".
    #[test]
    fn a_zero_target_liquidates_the_holding() {
        let values = map_of(&[("BTC", dec!(100))]);
        let targets = map_of(&[("BTC", dec!(0))]);
        assert_eq!(plan_trades(&values, &targets)["BTC"], dec!(-100));
    }

    /// An asset already exactly on target produces no entry at all, so the plan
    /// never carries a zero-sized order.
    #[test]
    fn an_asset_already_on_target_is_omitted() {
        let values = map_of(&[("BTC", dec!(100))]);
        let targets = map_of(&[("BTC", dec!(1))]);
        assert!(plan_trades(&values, &targets).is_empty());
    }

    /// Short positions carry negative value, and the plan treats them like any
    /// other signed holding.
    #[test]
    fn a_negative_holding_is_planned_as_a_buy_to_its_target() {
        let values = map_of(&[("BTC", dec!(-100)), ("USDT", dec!(200))]);
        let targets = map_of(&[("BTC", dec!(0.5)), ("USDT", dec!(0.5))]);
        let plan = plan_trades(&values, &targets);
        assert_eq!(plan["BTC"], dec!(150), "covering a short needs a buy");
        assert_eq!(plan["USDT"], dec!(-150));
    }

    /// The plan is keyed by target, so a holding nobody targeted is never
    /// scheduled for sale — it is simply invisible to `plan_trades`.
    #[test]
    fn an_untargeted_holding_is_not_planned_at_all() {
        let values = map_of(&[("BTC", dec!(100)), ("DOGE", dec!(500))]);
        let targets = map_of(&[("BTC", dec!(1))]);
        let plan = plan_trades(&values, &targets);
        assert_eq!(plan.len(), 1);
        assert!(!plan.contains_key("DOGE"), "the stray holding must be left alone");
    }

    // -----------------------------------------------------------------------
    // Rebalance::tick
    // -----------------------------------------------------------------------

    /// `MockAdapter` reports a single 10 000 USD balance; the scripted prices
    /// decide what that is worth in the quote currency.
    #[derive(Clone)]
    struct StubPrices {
        prices: Map<String, Decimal>,
        error: bool,
        reads: Arc<AtomicUsize>,
    }

    impl StubPrices {
        fn with(prices: &[(&str, Decimal)]) -> Arc<Self> {
            Arc::new(Self {
                prices: map_of(prices),
                error: false,
                reads: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self { prices: Map::new(), error: true, reads: Arc::new(AtomicUsize::new(0)) })
        }

        /// How many ticker reads this stub has served.
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MarketDataSource for StubPrices {
        async fn fetch_ticker(
            &self,
            req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.error {
                return Err(PortError::Transport("ticker feed down".into()));
            }
            let last = match self.prices.get(&req.symbol) {
                Some(price) => buffa::MessageField::some(decimal_to_common(*price)),
                None => buffa::MessageField::none(),
            };
            Ok(market::Ticker { symbol: req.symbol, last, ..Default::default() })
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Err(PortError::Unsupported("no book".into()))
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            Err(PortError::Unsupported("no candles".into()))
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

    fn rebalance_config(
        quote: &str,
        targets: &[(&str, Decimal)],
        band: Decimal,
    ) -> RebalanceConfig {
        RebalanceConfig {
            common: CommonParams {
                exchange_id: "mock".to_string(),
                label: String::new(),
                symbol: "BTC/USDT".to_string(),
                timeframe: "5m".to_string(),
                poll_secs: 30,
            },
            quote_asset: quote.to_string(),
            targets: map_of(targets),
            band_pct: band,
        }
    }

    async fn open_orders(adapter: &MockAdapter) -> Vec<trading::Order> {
        adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup")
    }

    fn order_for<'a>(orders: &'a [trading::Order], symbol: &str) -> Option<&'a trading::Order> {
        orders.iter().find(|o| o.symbol == symbol)
    }

    fn amount_of(order: &trading::Order) -> Decimal {
        let raw = order.amount.as_option().expect("the mock echoes the requested amount");
        common_to_decimal(raw).expect("the mock echoes a representable amount")
    }

    /// A foreign holding is sold down to its target weight while the quote leg
    /// is bought back up. The quote leg is priced at 1 by contract, so its size
    /// is the quote-value delta itself.
    #[tokio::test]
    async fn a_drifted_holding_is_sold_and_the_quote_leg_bought_back() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(0.5)), ("USDT", dec!(0.5))], dec!(0.05)),
            adapter.clone(),
            StubPrices::with(&[("USDUSDT", dec!(200))]),
        );
        let plan = rebal.tick().await.expect("tick");
        assert_eq!(plan["USD"], dec!(-1_000_000));
        assert_eq!(plan["USDT"], dec!(1_000_000));

        let open = open_orders(&adapter).await;
        assert_eq!(open.len(), 2);
        let sell = order_for(&open, "USDUSDT").expect("a USDUSDT leg");
        assert_eq!(sell.side, buffa::EnumValue::Known(trading::OrderSide::Sell));
        // 1 000 000 of quote value at 200 per unit.
        assert_eq!(amount_of(sell), dec!(5000));
        let buy = order_for(&open, "USDTUSDT").expect("a USDTUSDT leg");
        assert_eq!(buy.side, buffa::EnumValue::Known(trading::OrderSide::Buy));
        assert_eq!(amount_of(buy), dec!(1_000_000), "the quote leg is priced at one");
    }

    /// One ticker read per asset per tick, whatever the tick decides to trade.
    ///
    /// Sizing an order from a second price read would both double the venue calls
    /// and mix two prices inside one trade: the delta comes from the valuation
    /// price while the quantity would come from whatever the venue reported after
    /// the first read.
    #[tokio::test]
    async fn each_asset_is_priced_exactly_once_per_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let prices = StubPrices::with(&[("USDUSDT", dec!(200))]);
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(0.5)), ("USDT", dec!(0.5))], dec!(0.05)),
            adapter.clone(),
            prices.clone(),
        );
        rebal.tick().await.expect("tick");
        assert_eq!(prices.reads(), 1, "only the USD leg needs a ticker; USDT is its own numéraire");

        rebal.tick().await.expect("second tick");
        assert_eq!(prices.reads(), 2, "the count is per tick, not cached across ticks");
    }

    /// The band filter is inclusive: a drift of exactly `band_pct` is left
    /// alone, so a portfolio sitting on the edge of the band does not churn.
    #[tokio::test]
    async fn a_drift_exactly_on_the_band_is_not_traded() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(0.95))], dec!(0.05)),
            adapter.clone(),
            StubPrices::with(&[("USDUSDT", dec!(200))]),
        );
        let plan = rebal.tick().await.expect("tick");
        assert_eq!(plan["USD"], dec!(-100_000), "the plan is still computed");
        assert!(open_orders(&adapter).await.is_empty(), "|1 - 0.95| == band_pct must not trade");
    }

    /// One step past the band the trade goes through, so the inclusive filter
    /// is not an accidental "never trade".
    #[tokio::test]
    async fn a_drift_past_the_band_is_traded() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(0.9499))], dec!(0.05)),
            adapter.clone(),
            StubPrices::with(&[("USDUSDT", dec!(200))]),
        );
        rebal.tick().await.expect("tick");
        let open = open_orders(&adapter).await;
        assert_eq!(open.len(), 1);
        assert_eq!(amount_of(&open[0]), dec!(501), "100 200 of quote value at 200 per unit");
    }

    /// A ticker with no `last` cannot be valued, and the strategy says which
    /// asset it was rather than trading on a zero price.
    #[tokio::test]
    async fn an_unpriced_asset_is_reported_by_name() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(1))], dec!(0.05)),
            adapter.clone(),
            StubPrices::with(&[]),
        );
        let err = rebal.tick().await.expect_err("no price must not be treated as zero");
        assert!(err.to_string().contains("no price for USD"), "{err}");
    }

    /// A transport failure has to surface; swallowing it would let the strategy
    /// report a plan it could not act on.
    #[tokio::test]
    async fn a_ticker_failure_propagates_out_of_the_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(1))], dec!(0.05)),
            adapter.clone(),
            StubPrices::failing(),
        );
        let err = rebal.tick().await.expect_err("a broken price feed must not read as flat");
        assert!(err.to_string().contains("ticker feed down"), "{err}");
    }

    /// A rejected order is fatal to the tick: the remaining legs are not traded
    /// on a portfolio the strategy has already missized.
    #[tokio::test]
    async fn a_rejected_order_propagates_out_of_the_tick() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        adapter.fail_next_creates(1).await;
        let rebal = Rebalance::new(
            rebalance_config("USDT", &[("USD", dec!(0.5))], dec!(0.05)),
            adapter.clone(),
            StubPrices::with(&[("USDUSDT", dec!(200))]),
        );
        let err = rebal.tick().await.expect_err("a rejected order must not be swallowed");
        assert!(err.to_string().contains("scripted failure"), "{err}");
        assert!(open_orders(&adapter).await.is_empty());
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ntargets = { BTC = \"0.6\", ETH = \"0.4\" }\n\
             band_pct = \"0.02\"\n",
        )
        .expect("valid toml");
        let cfg = RebalanceConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.quote_asset, "USDT");
        assert_eq!(cfg.band_pct, dec!(0.02));
        assert_eq!(cfg.targets["BTC"], dec!(0.6));
    }

    #[test]
    fn the_band_defaults_to_five_percent() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntargets = { BTC = \"1\" }").expect("valid toml");
        let cfg = RebalanceConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.band_pct, dec!(0.05));
    }

    /// Without targets there is no portfolio to steer, so startup must fail
    /// rather than run a strategy that never trades.
    #[test]
    fn missing_targets_fails_config() {
        let table: toml::Table = toml::from_str("symbol = \"BTC/USDT\"").expect("valid toml");
        let err = RebalanceConfig::from_params(&table).expect_err("targets is required");
        assert!(err.to_string().contains("targets"), "{err}");
    }

    // -----------------------------------------------------------------------
    // Weight-table validation
    //
    // The planned deltas net out to `total * (Σtargets - 1)`, so a table that
    // does not add up to one plans a net buy or net sell the strategy has no
    // cash leg for. The config parser is the only place that can catch it.
    // -----------------------------------------------------------------------

    /// The error has to name the actual sum: "targets are wrong" is not
    /// actionable, "targets must sum to 1, got 1.2" is.
    #[test]
    fn a_weight_table_that_does_not_sum_to_one_fails_config() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntargets = { BTC = \"0.6\", ETH = \"0.6\" }")
                .expect("valid toml");
        let err = RebalanceConfig::from_params(&table).expect_err("1.2 is not a weight table");
        let message = err.to_string();
        assert!(message.contains("targets"), "{message}");
        assert!(message.contains("1.2"), "the error must name the actual sum: {message}");
    }

    /// A shortfall is as unsteerable as an overshoot: the missing slice has no
    /// leg, so the plan would buy or sell it out of nothing.
    #[test]
    fn a_weight_table_that_under_sums_fails_config() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntargets = { BTC = \"0.6\" }")
                .expect("valid toml");
        let err = RebalanceConfig::from_params(&table).expect_err("0.6 alone is not a portfolio");
        assert!(err.to_string().contains("0.6"), "{err}");
    }

    /// A negative weight inverts one leg's direction, which reads like a
    /// deliberate short but is really a typo.
    #[test]
    fn a_negative_target_weight_fails_config() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntargets = { BTC = \"1.5\", ETH = \"-0.5\" }")
                .expect("valid toml");
        let err =
            RebalanceConfig::from_params(&table).expect_err("a negative weight is not a target");
        let message = err.to_string();
        assert!(message.contains("ETH"), "the error must name the offending asset: {message}");
        assert!(message.contains("-0.5"), "{message}");
    }

    /// An explicitly empty table is not a portfolio of zero weight — it is no
    /// portfolio at all, and would otherwise start up and never trade.
    #[test]
    fn an_explicitly_empty_target_table_fails_config() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntargets = {}").expect("valid toml");
        let err = RebalanceConfig::from_params(&table).expect_err("no targets is not a portfolio");
        assert!(err.to_string().contains("targets"), "{err}");
    }

    /// A hand-maintained table rarely lands on exactly one, so the tolerance is
    /// what keeps rounding from stopping startup. One part per million is far
    /// below any drift worth trading.
    #[test]
    fn a_weight_table_within_the_tolerance_is_accepted() {
        let table: toml::Table = toml::from_str(
            "symbol = \"BTC/USDT\"\ntargets = { BTC = \"0.5\", ETH = \"0.4999999\" }",
        )
        .expect("valid toml");
        let cfg = RebalanceConfig::from_params(&table).expect("a 1e-7 shortfall is rounding");
        assert_eq!(cfg.targets["ETH"], dec!(0.4999999));
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Targets that cover the portfolio and add up to one are cash
        /// neutral: whatever one leg buys, the others sell.
        #[test]
        fn targets_summing_to_one_conserve_the_portfolio(
            held in 1i64..1_000_000,
            weight_pct in 1u32..100,
        ) {
            let weight = Decimal::from(weight_pct) / dec!(100);
            let rest = Decimal::ONE - weight;
            let values = map_of(&[("BTC", Decimal::from(held))]);
            let targets = map_of(&[("BTC", weight), ("USDT", rest)]);
            let plan = plan_trades(&values, &targets);
            let net: Decimal = plan.values().copied().sum();
            prop_assert_eq!(net, Decimal::ZERO, "the plan must net to zero: {:?}", plan );
        }

        /// Every entry is the exact gap to the target, so applying the plan
        /// lands each leg on its weight — never past it.
        #[test]
        fn each_entry_closes_the_gap_to_its_target_exactly(
            held in 0i64..1_000_000,
            other in 0i64..1_000_000,
            weight_pct in 0u32..=100,
        ) {
            let weight = Decimal::from(weight_pct) / dec!(100);
            let values = map_of(&[("BTC", Decimal::from(held)), ("ETH", Decimal::from(other))]);
            let targets = map_of(&[("BTC", weight), ("ETH", Decimal::ONE - weight)]);
            let total: Decimal = values.values().copied().sum();
            let plan = plan_trades(&values, &targets);
            for (asset, delta) in &plan {
                let landed = values[asset] + delta;
                prop_assert_eq!(landed, total * targets[asset], "{} overshot its target", asset );
            }
        }

        /// `plan_trades` is linear in the portfolio, so a uniformly scaled book
        /// produces a uniformly scaled trade list.
        #[test]
        fn the_plan_scales_with_the_portfolio(
            held in 1i64..100_000,
            other in 1i64..100_000,
            scale in 1i64..100,
            weight_pct in 1u32..100,
        ) {
            let weight = Decimal::from(weight_pct) / dec!(100);
            let targets = map_of(&[("BTC", weight), ("ETH", Decimal::ONE - weight)]);
            let base = map_of(&[("BTC", Decimal::from(held)), ("ETH", Decimal::from(other))]);
            let scaled: Map<String, Decimal> = base
                .iter()
                .map(|(asset, value)| (asset.clone(), *value * Decimal::from(scale)))
                .collect();
            let factor = Decimal::from(scale);
            let plan = plan_trades(&base, &targets);
            let scaled_plan = plan_trades(&scaled, &targets);
            prop_assert_eq!(
                scaled_plan.keys().collect::<Vec<_>>(),
                plan.keys().collect::<Vec<_>>()
            );
            for (asset, delta) in &plan {
                prop_assert_eq!(scaled_plan[asset], *delta * factor, "{} did not scale", asset );
            }
        }

        /// No entry is ever zero, and every key is a configured target, so the
        /// caller never has to filter the plan before sizing orders.
        #[test]
        fn the_plan_holds_only_nonzero_gaps_towards_configured_targets(
            held in -100_000i64..100_000,
            other in -100_000i64..100_000,
            weight_pct in 0u32..=100,
        ) {
            let weight = Decimal::from(weight_pct) / dec!(100);
            let values = map_of(&[("BTC", Decimal::from(held)), ("ETH", Decimal::from(other))]);
            let targets = map_of(&[("BTC", weight), ("ETH", Decimal::ONE - weight)]);
            let plan = plan_trades(&values, &targets);
            for (asset, delta) in &plan {
                prop_assert!(targets.contains_key(asset), "{asset} is not a target");
                prop_assert!(!delta.is_zero(), "{asset} planned a zero-sized trade");
            }
        }

        /// A non-positive total yields nothing at all, whatever the targets: the
        /// valuation it scales is either nothing or a short book.
        #[test]
        fn a_non_positive_portfolio_total_plans_nothing(
            held in -1_000_000i64..1_000_000,
            other in -1_000_000i64..1_000_000,
            weight_pct in 0u32..=100,
        ) {
            let weight = Decimal::from(weight_pct) / dec!(100);
            let values = map_of(&[("BTC", Decimal::from(held)), ("ETH", Decimal::from(other))]);
            let total: Decimal = values.values().copied().sum();
            if total.is_sign_negative() || total.is_zero() {
                let targets = map_of(&[("BTC", weight), ("ETH", Decimal::ONE - weight)]);
                prop_assert!(
                    plan_trades(&values, &targets).is_empty(),
                    "a total of {total} must plan nothing"
                );
            }
        }

        /// The tolerance on the weight sum is the only slack: inside it the
        /// config parses, outside it startup fails with the actual sum.
        #[test]
        fn the_weight_sum_tolerance_is_the_only_slack(
            weight_micro in 0i64..2_000_001,
            offset in 0i64..8,
        ) {
            let total_micro = 1_000_000 + offset;
            let weight = Decimal::from(weight_micro) / dec!(1_000_000);
            let table: toml::Table = toml::from_str(&format!(
                "symbol = \"BTC/USDT\"\ntargets = {{ BTC = \"{weight}\" }}"
            ))
            .expect("the generated table parses");
            // Tolerance is 1e-6, i.e. one micro, so a single-weight table is legal
            // from one micro below one up to and including one micro above it.
            // `total_micro` is 1e6 (one) plus `offset`, all in micros.
            let within = (weight_micro - total_micro).abs() <= 1;
            let outcome = RebalanceConfig::from_params(&table);
            if within {
                prop_assert!(
                    outcome.is_ok(),
                    "a weight of {weight} is within tolerance: {outcome:?}"
                );
            } else {
                let message =
                    outcome.expect_err("a weight this far from one must not parse").to_string();
                prop_assert!(message.contains("targets"), "{message}");
                prop_assert!(message.contains("sum to 1"), "{message}");
            }
        }
    }
}
