//! NAV (net asset value) recorder.
//!
//! Periodically snapshots the account, values every balance at the venue
//! ticker price into the quote currency, and logs the total. The log
//! stream doubles as the control-plane record.

use std::sync::Arc;

use async_trait::async_trait;
use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, TradingGateway},
    strategies::{CommonParams, Strategy, params_from_table},
};

/// Config for the NAV recorder.
#[derive(Debug, Clone, Deserialize)]
pub struct NavRecorderConfig {
    #[serde(flatten)]
    pub common: CommonParams,
    /// Quote currency used for valuation.
    #[serde(default = "default_quote")]
    pub quote_asset: String,
}

fn default_quote() -> String {
    String::from("USDT")
}

impl NavRecorderConfig {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

/// Values one balance row into the quote currency.
pub fn value_balance(
    currency: &str,
    total: Decimal,
    price: Option<Decimal>,
    quote: &str,
) -> Decimal {
    if currency == quote {
        return total;
    }
    match price {
        Some(p) if p > Decimal::ZERO => total * p,
        _ => Decimal::ZERO,
    }
}

/// NAV recorder strategy.
pub struct NavRecorder {
    config: NavRecorderConfig,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
}

impl NavRecorder {
    pub fn new(
        config: NavRecorderConfig,
        gateway: Arc<dyn TradingGateway>,
        market: Arc<dyn MarketDataSource>,
    ) -> Self {
        Self { config, gateway, market }
    }

    async fn price_of(&self, currency: &str) -> Option<Decimal> {
        let symbol = format!("{currency}{}", self.config.quote_asset);
        let ticker = self
            .market
            .fetch_ticker(crate::proto::market::FetchTickerRequest {
                exchange_id: buffa::MessageField::some(self.config.common.proto_exchange_id()),
                symbol,
                ..Default::default()
            })
            .await
            .ok()?;
        ticker.last.as_option().and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
    }

    /// One snapshot cycle; returns the NAV in the quote currency.
    pub async fn tick(&self) -> Result<Decimal> {
        let exchange = self.config.common.proto_exchange_id();
        let snap = self.gateway.sync_state(&exchange).await?;
        let mut nav = Decimal::ZERO;
        for balance in &snap.balances {
            let total = balance
                .total
                .as_option()
                .and_then(|d| longtrader_contract::ext::common_to_decimal(d).ok())
                .unwrap_or_default();
            let price = if balance.currency == self.config.quote_asset {
                None
            } else {
                self.price_of(&balance.currency).await
            };
            nav += value_balance(&balance.currency, total, price, &self.config.quote_asset);
        }
        tracing::info!(%nav, quote = %self.config.quote_asset, "nav snapshot");
        Ok(nav)
    }
}

#[async_trait]
impl Strategy for NavRecorder {
    async fn run(&self) -> Result<()> {
        let poll = std::time::Duration::from_secs(self.config.common.poll_secs.max(1));
        loop {
            if let Err(err) = self.tick().await {
                tracing::error!(error = %err, "nav-recorder tick failed");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use longtrader_contract::ext::decimal_to_common;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketEventStream, OverflowPolicy, PortError},
        proto::{common, market},
    };

    // -----------------------------------------------------------------------
    // value_balance
    // -----------------------------------------------------------------------

    /// The quote leg is already denominated in the valuation currency, so it is
    /// counted at face value. The price argument is deliberately ignored on this
    /// branch: a caller that resolves a price for the quote currency must not be
    /// able to double-count it.
    #[test]
    fn the_quote_leg_is_counted_at_face_value_and_ignores_the_price() {
        for price in [None, Some(dec!(0)), Some(dec!(-7)), Some(dec!(999999))] {
            assert_eq!(value_balance("USDT", dec!(1234.5), price, "USDT"), dec!(1234.5));
        }
    }

    #[test]
    fn a_non_quote_balance_is_multiplied_by_its_price() {
        assert_eq!(value_balance("BTC", dec!(2), Some(dec!(50000)), "USDT"), dec!(100000));
    }

    /// A missing price cannot be substituted with 1: that would value an unpriced
    /// holding at its raw quantity and inflate the NAV. Zero is the only honest
    /// contribution for "we could not price it".
    #[test]
    fn a_missing_price_values_the_balance_at_zero() {
        assert_eq!(value_balance("BTC", dec!(2), None, "USDT"), Decimal::ZERO);
    }

    /// Zero and negative prices are both "unpriced". A negative contribution
    /// would understate the portfolio instead of flagging the missing price.
    #[test]
    fn a_non_positive_price_values_the_balance_at_zero() {
        assert_eq!(value_balance("BTC", dec!(2), Some(Decimal::ZERO), "USDT"), Decimal::ZERO);
        assert_eq!(value_balance("BTC", dec!(2), Some(dec!(-1)), "USDT"), Decimal::ZERO);
    }

    /// The currency comparison is exact. A differently-cased ticker is a
    /// different asset, so it must take the priced branch and not short-circuit
    /// to face value.
    #[test]
    fn the_currency_match_is_exact_not_case_insensitive() {
        assert_eq!(value_balance("usdt", dec!(5), None, "USDT"), Decimal::ZERO);
    }

    /// With an empty quote asset the face-value branch only matches the empty
    /// currency. That is a config error upstream, but it must not silently
    /// value every holding at face value.
    #[test]
    fn an_empty_quote_only_matches_the_empty_currency() {
        assert_eq!(value_balance("", dec!(5), None, ""), dec!(5));
        assert_eq!(value_balance("BTC", dec!(5), None, ""), Decimal::ZERO);
    }

    /// The balance may be negative (a borrowed or shorted position); valuation
    /// stays linear so the sign carries through the NAV.
    #[test]
    fn a_negative_balance_keeps_its_sign_through_the_valuation() {
        assert_eq!(value_balance("BTC", dec!(-2), Some(dec!(50)), "USDT"), dec!(-100));
        assert_eq!(value_balance("USDT", dec!(-2), Some(dec!(50)), "USDT"), dec!(-2));
    }

    // -----------------------------------------------------------------------
    // NavRecorder::tick
    // -----------------------------------------------------------------------

    fn recorder(quote: &str, market: Arc<dyn MarketDataSource>) -> NavRecorder {
        NavRecorder::new(
            NavRecorderConfig {
                common: CommonParams {
                    exchange_id: "mock".to_string(),
                    label: String::new(),
                    symbol: "BTC/USDT".to_string(),
                    timeframe: "5m".to_string(),
                    poll_secs: 30,
                },
                quote_asset: quote.to_string(),
            },
            Arc::new(MockAdapter::new(dec!(100))),
            market,
        )
    }

    /// A holding in another currency is valued at the ticker price. The mock
    /// reports a single 10 000 USD balance and prices everything at 100.
    #[tokio::test]
    async fn a_non_quote_balance_is_valued_at_the_ticker_price() {
        let market: Arc<dyn MarketDataSource> = Arc::new(MockAdapter::new(dec!(100)));
        let nav = recorder("USDT", market).tick().await.expect("nav");
        assert_eq!(nav, dec!(1_000_000));
    }

    /// When the quote currency *is* the held currency the price branch is never
    /// taken, so the balance counts at face value.
    #[tokio::test]
    async fn the_quote_currency_is_counted_at_face_value() {
        let market: Arc<dyn MarketDataSource> = Arc::new(MockAdapter::new(dec!(100)));
        let nav = recorder("USD", market).tick().await.expect("nav");
        assert_eq!(nav, dec!(10_000));
    }

    /// A ticker transport failure is swallowed by `price_of` and turns into
    /// "no price", so the holding silently drops out of the NAV. The snapshot
    /// still succeeds; only the log line records the loss.
    #[tokio::test]
    async fn a_ticker_failure_values_the_balance_at_zero_instead_of_erroring() {
        let nav = recorder("USDT", StubTicker::failing())
            .tick()
            .await
            .expect("a price failure must not abort the snapshot");
        assert_eq!(nav, Decimal::ZERO);
    }

    /// A ticker with no `last` field is a successful read that carries no price.
    #[tokio::test]
    async fn a_ticker_without_a_last_price_values_the_balance_at_zero() {
        let nav = recorder("USDT", StubTicker::without_last())
            .tick()
            .await
            .expect("an unpriced ticker is not an error");
        assert_eq!(nav, Decimal::ZERO);
    }

    /// A `last` the decoder must refuse is dropped by the decode, which also lands
    /// on zero rather than propagating.
    #[tokio::test]
    async fn an_undecodable_ticker_price_values_the_balance_at_zero() {
        let nav = recorder("USDT", StubTicker::undecodable())
            .tick()
            .await
            .expect("a bad price is not an error");
        assert_eq!(nav, Decimal::ZERO);
    }

    /// The reported NAV is exactly the balance valued at the price the venue
    /// published, with no rounding or scaling in between.
    #[tokio::test]
    async fn a_scripted_ticker_price_drives_the_nav() {
        let market: Arc<dyn MarketDataSource> = StubTicker::with_last(dec!(250));
        let nav = recorder("USDT", market).tick().await.expect("nav");
        assert_eq!(nav, dec!(2_500_000), "10 000 units at 250 each");
    }

    /// Ticker source with exactly one scripted answer, so each way `price_of`
    /// can come back empty is reachable from a test.
    #[derive(Clone)]
    struct StubTicker {
        last: Option<common::Decimal>,
        error: bool,
    }

    impl StubTicker {
        fn with_last(value: Decimal) -> Arc<Self> {
            Arc::new(Self { last: Some(decimal_to_common(value)), error: false })
        }

        fn without_last() -> Arc<Self> {
            Arc::new(Self { last: None, error: false })
        }

        /// A `last` the decoder must refuse, so `price_of` finds no price.
        ///
        /// `Decimal` carries one base-10 string, so there is no numeric pair to be
        /// out of range — the payload itself is outside the grammar.
        fn undecodable() -> Arc<Self> {
            Arc::new(Self {
                last: Some(common::Decimal {
                    value: "not-a-number".to_string(),
                    ..Default::default()
                }),
                error: false,
            })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self { last: None, error: true })
        }
    }

    #[async_trait]
    impl MarketDataSource for StubTicker {
        async fn fetch_ticker(
            &self,
            _req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            if self.error {
                return Err(PortError::Transport("ticker feed down".into()));
            }
            let last = match &self.last {
                Some(value) => buffa::MessageField::some(value.clone()),
                None => buffa::MessageField::none(),
            };
            Ok(market::Ticker { symbol: _req.symbol, last, ..Default::default() })
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

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    #[test]
    fn config_parses_from_params_table() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\ntimeframe = \"1h\"").expect("valid toml");
        let cfg = NavRecorderConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.common.symbol, "BTC/USDT");
        assert_eq!(cfg.quote_asset, "USDT", "USDT is the documented default valuation currency");
        assert_eq!(cfg.common.timeframe, "1h");
    }

    #[test]
    fn the_quote_asset_is_overridable() {
        let table: toml::Table =
            toml::from_str("symbol = \"BTC/USDT\"\nquote_asset = \"BTC\"").expect("valid toml");
        let cfg = NavRecorderConfig::from_params(&table).expect("parse");
        assert_eq!(cfg.quote_asset, "BTC");
    }

    /// Without a symbol the recorder would value whatever the snapshot happens
    /// to list against an unpriced market.
    #[test]
    fn missing_symbol_fails_config() {
        let table: toml::Table = toml::from_str("quote_asset = \"BTC\"").expect("valid toml");
        let err = NavRecorderConfig::from_params(&table).expect_err("symbol is required");
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Valuing a holding in its own currency is the identity, whatever price
        /// the caller happened to resolve for it.
        #[test]
        fn valuing_the_quote_currency_is_the_identity(
            mantissa in -1_000_000i64..1_000_000,
            scale in 0u32..12,
        ) {
            let Ok(total) = Decimal::try_from_i128_with_scale(i128::from(mantissa), scale) else {
                return Ok(());
            };
            prop_assert_eq!(value_balance("USDT", total, None, "USDT"), total);
            prop_assert_eq!(value_balance("USDT", total, Some(Decimal::ZERO), "USDT"), total);
            prop_assert_eq!(value_balance("USDT", total, Some(dec!(42)), "USDT"), total);
        }

        /// Valuation is linear in the balance, so splitting one holding into two
        /// rows cannot move the recorded NAV.
        #[test]
        fn valuing_is_additive_over_the_balance(
            a in -1_000_000i64..1_000_000,
            b in -1_000_000i64..1_000_000,
            price in 1u32..1_000_000,
        ) {
            let (a, b) = (Decimal::from(a), Decimal::from(b));
            let price = Decimal::new(i64::from(price), 4);
            let split = value_balance("BTC", a, Some(price), "USDT")
                + value_balance("BTC", b, Some(price), "USDT");
            prop_assert_eq!(value_balance("BTC", a + b, Some(price), "USDT"), split);
            // The unpriced branch is additive too: two zeroes, or two face values.
            prop_assert_eq!(
                value_balance("BTC", a + b, None, "USDT"),
                value_balance("BTC", a, None, "USDT") + value_balance("BTC", b, None, "USDT")
            );
        }

        /// For a non-negative balance the contribution never goes negative and
        /// never shrinks as the price rises, so a rally cannot lower the NAV.
        #[test]
        fn valuation_is_monotone_in_the_price(
            total in 0u64..1_000_000,
            low_micro in 1u32..1_000_000,
            bump_micro in 0u32..1_000_000,
        ) {
            let total = Decimal::from(total);
            let low = Decimal::new(i64::from(low_micro), 6);
            let high = low + Decimal::new(i64::from(bump_micro), 6);
            let at_low = value_balance("BTC", total, Some(low), "USDT");
            let at_high = value_balance("BTC", total, Some(high), "USDT");
            prop_assert!(at_low >= Decimal::ZERO, "price {low} produced {at_low}");
            prop_assert!(at_low <= at_high, "price {low} valued above price {high}");
        }

        /// A non-positive price is never a signed contribution, for any sign of
        /// balance: the unpriced branch is a flat zero.
        #[test]
        fn a_non_positive_price_never_produces_a_signed_value(
            total in -1_000_000i64..1_000_000,
            price_micro in -1_000_000i64..=0,
        ) {
            let price = Decimal::new(price_micro, 6);
            prop_assert_eq!(
                value_balance("BTC", Decimal::from(total), Some(price), "USDT"),
                Decimal::ZERO
            );
        }
    }
}
