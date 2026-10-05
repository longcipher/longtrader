//! Market-cap weighted rebalance.
//!
//! A thin preset over the rebalance engine: target weights are supplied as
//! a static table (manually maintained from market-cap rankings) instead of
//! a live data feed. The preset fixes the rebalance band at 5% and exposes no
//! other knobs.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the market-cap rebalance.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketCapConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    #[serde(default = "default_quote")]
    pub quote_asset: String,
    /// Static weights per asset derived from market-cap ranking.
    ///
    /// Unlike [`RebalanceConfig`](crate::strategies::rebalance::RebalanceConfig),
    /// these are *not* checked against the sum-to-one rule: the preset builds its
    /// rebalance config directly, so an unbalanced cap table is accepted here and
    /// the arithmetic is the operator's.
    pub weights: std::collections::BTreeMap<String, Decimal>,
}

fn default_quote() -> String {
    String::from("USDT")
}

impl MarketCapConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Market-cap rebalance strategy.
pub struct MarketCap {
    inner: crate::strategies::rebalance::Rebalance,
}

impl MarketCap {
    pub fn new(
        config: MarketCapConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        let rc = crate::strategies::rebalance::RebalanceConfig {
            common: config.common,
            quote_asset: config.quote_asset,
            targets: config.weights,
            band_pct: Decimal::new(5, 2),
        };
        Self { inner: crate::strategies::rebalance::Rebalance::new(rc, gateway, market) }
    }

    /// One valuation + rebalance cycle; returns the planned quote-value deltas.
    ///
    /// Delegates to [`Rebalance::tick`](crate::strategies::rebalance::Rebalance::tick).
    /// The preset adds no behaviour of its own: it maps `weights` onto targets and
    /// fixes the band at 5%, and everything observable about it comes through here.
    pub async fn tick(&self) -> Result<std::collections::BTreeMap<String, Decimal>> {
        self.inner.tick().await
    }
}

#[async_trait]
impl Strategy for MarketCap {
    async fn run(&self) -> Result<()> {
        self.inner.run().await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use async_trait::async_trait;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
        proto::{market, trading},
        strategies::Strategy,
    };

    fn cap(config: MarketCapConfig, market: Arc<dyn MarketDataSource>) -> MarketCap {
        let gateway: Arc<dyn TradingGateway> = Arc::new(MockAdapter::new(dec!(100)));
        MarketCap::new(config, gateway, market)
    }

    fn config_of(table: &str) -> MarketCapConfig {
        let parsed: toml::Table = toml::from_str(table).expect("valid toml");
        MarketCapConfig::from_params(&parsed).expect("parse")
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    /// The preset supplies no band of its own, so the whole preset surface is
    /// `symbol`, the optional quote, and the static weight table.
    #[test]
    fn config_parses_from_params_table() {
        let cfg = config_of("symbol = \"BTC/USDT\"\nweights = { BTC = \"0.6\", ETH = \"0.4\" }");
        assert_eq!(cfg.common.symbol, "BTC/USDT");
        assert_eq!(cfg.quote_asset, "USDT", "USDT is the documented default valuation currency");
        assert_eq!(cfg.weights["BTC"], dec!(0.6));
        assert_eq!(cfg.weights["ETH"], dec!(0.4));
    }

    #[test]
    fn the_quote_asset_is_overridable() {
        let cfg =
            config_of("symbol = \"BTC/USDT\"\nquote_asset = \"BTC\"\nweights = { ETH = \"1\" }");
        assert_eq!(cfg.quote_asset, "BTC");
    }

    /// The weight table *is* the strategy: without it the preset has nothing to
    /// steer towards, so startup must fail rather than run a no-op.
    #[test]
    fn missing_weights_fails_config() {
        let parsed: toml::Table = toml::from_str("symbol = \"BTC/USDT\"").expect("valid toml");
        let err = MarketCapConfig::from_params(&parsed).expect_err("weights is required");
        assert!(err.to_string().contains("weights"), "{err}");
    }

    #[test]
    fn missing_symbol_fails_config() {
        let parsed: toml::Table = toml::from_str("weights = { BTC = \"1\" }").expect("valid toml");
        let err = MarketCapConfig::from_params(&parsed).expect_err("symbol is required");
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    /// An empty table is accepted — a preset with nothing to hold is inert, not
    /// broken — so the constructor has to survive it.
    #[test]
    fn an_empty_weight_table_is_accepted_and_builds() {
        let cfg = config_of("symbol = \"BTC/USDT\"\nweights = {}");
        assert!(cfg.weights.is_empty());
        let market: Arc<dyn MarketDataSource> = Arc::new(MockAdapter::new(dec!(100)));
        let _preset = cap(cfg, market);
    }

    /// A weight table that does not add up to one is not rejected at startup.
    /// `plan_trades` will then plan a net buy worth the shortfall, which the
    /// strategy has no cash leg for unless the quote is listed explicitly. The
    /// preset builds its rebalance config directly, so it does not inherit the
    /// sum-to-one check `RebalanceConfig::from_params` applies.
    #[test]
    fn a_weight_table_that_does_not_sum_to_one_is_accepted() {
        let cfg = config_of("symbol = \"BTC/USDT\"\nweights = { BTC = \"0.6\", ETH = \"0.6\" }");
        let total: Decimal = cfg.weights.values().copied().sum();
        assert_eq!(total, dec!(1.2));
    }

    /// Weights are a pure projection: the configured decimal reaches the preset
    /// with its precision intact, so a hand-maintained cap table is not rounded
    /// on the way in.
    #[test]
    fn weights_keep_their_configured_precision() {
        let cfg =
            config_of("symbol = \"BTC/USDT\"\nweights = { BTC = \"0.33333333333333333333\" }");
        assert_eq!(cfg.weights["BTC"].to_string(), "0.33333333333333333333");
    }

    // -----------------------------------------------------------------------
    // Delegated rebalance
    // -----------------------------------------------------------------------

    /// `tick` is the preset's whole surface over the engine: one cycle, and the
    /// plan it computed. `MockAdapter` reports a single 10 000 USD balance, so a
    /// 50% cap weight on USD plans a 5 000-unit sale.
    #[tokio::test]
    async fn a_tick_delegates_and_returns_the_planned_deltas() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = adapter.clone();
        let preset = MarketCap::new(
            config_of("symbol = \"USD/USDT\"\nweights = { USD = \"0.5\" }"),
            gateway,
            market,
        );

        let plan = preset.tick().await.expect("tick");
        assert_eq!(plan["USD"], dec!(-500_000), "half the portfolio is planned away");
        let open = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup");
        assert_eq!(open.len(), 1, "the delegated tick traded once");
        assert_eq!(open[0].symbol, "USDUSDT");
    }

    /// The preset exposes no band of its own, so the engine's default is the one
    /// in force — and it is 5%. A weight of 0.95 sits exactly on that band and is
    /// left alone; anything past it trades.
    #[tokio::test]
    async fn the_preset_trades_on_a_five_percent_band() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = adapter.clone();
        let preset = MarketCap::new(
            config_of("symbol = \"USD/USDT\"\nweights = { USD = \"0.95\" }"),
            gateway.clone(),
            market.clone(),
        );
        preset.tick().await.expect("tick");
        let resting = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup");
        assert!(resting.is_empty(), "a drift of exactly 5% is inside the band");

        let past = MarketCap::new(
            config_of("symbol = \"USD/USDT\"\nweights = { USD = \"0.94\" }"),
            gateway,
            market,
        );
        past.tick().await.expect("tick");
        let resting = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup");
        assert_eq!(resting.len(), 1, "6% of drift is outside a 5% band");
    }

    /// The preset is a thin wrapper, so the way to observe it is through the
    /// orders the delegated rebalance places. `MockAdapter` reports a single
    /// 10 000 USD balance, so a 50% cap weight on USD forces a 5 000-unit sell
    /// — the preset reached the engine, mapped `weights` onto `targets`, and
    /// traded.
    #[tokio::test]
    async fn the_delegated_rebalance_trades_against_the_weight_table() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let gateway: Arc<dyn TradingGateway> = adapter.clone();
        let market: Arc<dyn MarketDataSource> = adapter.clone();
        let preset = MarketCap::new(
            config_of("symbol = \"USD/USDT\"\nweights = { USD = \"0.5\" }"),
            gateway,
            market,
        );
        let runner = tokio::spawn(async move { preset.run().await });
        // One poll cycle runs immediately and then sleeps for `poll_secs`.
        tokio::time::sleep(Duration::from_millis(50)).await;
        runner.abort();

        let open = adapter
            .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
            .await
            .expect("test setup");
        assert_eq!(open.len(), 1, "the delegated tick must have traded once");
        assert_eq!(open[0].symbol, "USDUSDT");
    }

    /// The delegated loop treats a port failure the same way `Rebalance::run`
    /// does — log and keep polling — so one bad price read cannot end the
    /// session.
    #[tokio::test]
    async fn a_port_failure_does_not_end_the_delegated_run_loop() {
        let cfg = config_of("symbol = \"USD/USDT\"\nweights = { USD = \"0.5\" }");
        let preset = cap(cfg, Arc::new(FailingPrices));
        let outcome = tokio::time::timeout(Duration::from_millis(50), preset.run()).await;
        assert!(outcome.is_err(), "the run loop must keep polling after a price failure");
    }

    /// Market source whose every read is a transport error.
    struct FailingPrices;

    #[async_trait]
    impl MarketDataSource for FailingPrices {
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
            Err(PortError::Transport("candle feed down".into()))
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Err(PortError::Transport("symbol feed down".into()))
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Err(PortError::Transport("symbol feed down".into()))
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Err(PortError::Transport("ticker feed down".into()))
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
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// The cap table is a pure projection through the config parser, so no
        /// weight is reordered, rescaled, or dropped on the way in.
        #[test]
        fn the_weight_table_round_trips_unchanged(
            entries in prop::collection::vec(("[A-Z]{2,5}", 0i64..10_000), 0..8),
        ) {
            let mut expected: BTreeMap<String, i64> = BTreeMap::new();
            for (asset, weight) in entries {
                let _ = expected.insert(asset, weight);
            }
            let body: Vec<String> = expected
                .iter()
                .map(|(asset, weight)| {
                    format!("{asset} = \"{}.{:02}\"", weight / 100, weight % 100)
                })
                .collect();
            let doc = format!("symbol = \"BTC/USDT\"\nweights = {{ {} }}", body.join(", "));
            let cfg = MarketCapConfig::from_params(
                &toml::from_str::<toml::Table>(&doc).expect("the generated table parses"),
            )
            .expect("the generated weights parse");
            prop_assert_eq!(cfg.weights.len(), expected.len());
            for (asset, weight) in &expected {
                prop_assert_eq!(cfg.weights[asset.as_str()], Decimal::from(*weight) / dec!(100));
            }
        }
    }
}
