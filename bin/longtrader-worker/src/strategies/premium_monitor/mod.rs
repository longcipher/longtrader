//! Cross-venue premium monitor.
//!
//! Polls the same symbol on two venues and reports the premium
//! `primary / hedge - 1`. Breaches beyond `alert_threshold` are logged at
//! warn level (the notification outlet is the control plane's log stream). A
//! cycle whose legs cannot form a ratio (a zero quote on either side) is
//! reported as *unknown* rather than as a flat `0`, which is the reading for a
//! genuinely par market.

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
///
/// Returns `None` when either side is unusable: a zero or absent price means
/// there is no ratio to report. The division itself goes through
/// `checked_div`/`checked_sub` because `rust_decimal`'s `/` and `-` operators
/// **panic** on overflow, and two individually representable prices (e.g.
/// `1e10` against `1e-25`) overflow the quotient.
#[must_use]
pub fn premium(primary: Decimal, hedge: Decimal) -> Option<Decimal> {
    if hedge.is_zero() || primary.is_zero() {
        return None;
    }
    primary.checked_div(hedge)?.checked_sub(Decimal::ONE)
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
    ///
    /// `Ok(None)` means "measured, but unquotable": `premium` is `None` when a leg
    /// quoted zero, so there is no ratio to report. That is deliberately *not*
    /// collapsed into a zero premium — `0` is the reading for a genuinely flat
    /// market, and reporting an unquotable cycle as `0` would both hide a broken
    /// feed and never breach `alert_threshold`. `last_premium` keeps the same
    /// distinction (`None` = unknown) and no alert is raised for a measurement that
    /// never happened.
    pub async fn tick(&self) -> Result<Option<Decimal>> {
        let p = self.ticker_last(&self.config.venues.primary.proto()).await?;
        let h = self.ticker_last(&self.config.venues.hedge.proto()).await?;
        let prem = premium(p, h);
        *self.last_premium.lock().await = prem;
        if let Some(prem) = prem {
            if prem.abs() > self.config.alert_threshold {
                tracing::warn!(%p, %h, %prem, "premium breach");
            }
        } else {
            tracing::warn!(%p, %h, "premium unquotable: a leg quoted zero");
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
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use async_trait::async_trait;
    use buffa::MessageField;
    // Imported item by item rather than as `prelude::*`: the prelude also exports
    // a `Strategy` trait, which collides with `crate::strategies::Strategy` here.
    use proptest::prelude::{
        ProptestConfig, prop, prop_assert, prop_assert_eq, prop_assume, proptest,
    };
    use proptest::strategy::Strategy as ProptestStrategy;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        ports::{MarketEventStream, OverflowPolicy, PortError},
        proto::market,
    };

    #[test]
    fn premium_math() {
        assert_eq!(premium(dec!(101), dec!(100)), Some(dec!(0.01)));
        assert_eq!(premium(Decimal::ZERO, dec!(100)), None);
    }

    // -----------------------------------------------------------------------
    // premium()
    // -----------------------------------------------------------------------

    /// The hedge is the denominator: a zero hedge has no ratio to report, and
    /// inventing one would either divide by zero or read as a flat market.
    #[test]
    fn a_hedge_of_zero_has_no_premium() {
        assert_eq!(premium(dec!(100), Decimal::ZERO), None);
        assert_eq!(premium(dec!(100), dec!(-0.5)), Some(dec!(-201)));
    }

    /// The guard is `is_zero`, not a scale check: a zero written with decimals
    /// is still a zero hedge.
    #[test]
    fn a_scaled_zero_hedge_has_no_premium() {
        assert_eq!(premium(dec!(100), dec!(0.00000)), None);
        assert_eq!(premium(dec!(0.00000), dec!(100)), None);
    }

    #[test]
    fn both_venues_quoting_zero_have_no_premium() {
        assert_eq!(premium(Decimal::ZERO, Decimal::ZERO), None);
    }

    /// Parity is exactly zero, not `Some(0.0000000000000000000000000001)`: the
    /// subtraction is what makes the alert threshold comparable.
    #[test]
    fn matching_venues_are_exactly_zero() {
        assert_eq!(premium(dec!(100), dec!(100)), Some(Decimal::ZERO));
        assert_eq!(premium(dec!(0.0001), dec!(0.0001)), Some(Decimal::ZERO));
        assert_eq!(premium(dec!(-42), dec!(-42)), Some(Decimal::ZERO));
    }

    #[test]
    fn a_discount_against_the_hedge_is_a_negative_premium() {
        assert_eq!(premium(dec!(99), dec!(100)), Some(dec!(-0.01)));
        assert_eq!(premium(dec!(100), dec!(200)), Some(dec!(-0.5)));
    }

    // -----------------------------------------------------------------------
    // tick()
    //
    // `MockAdapter` answers every exchange with the same price and never fails,
    // so a source that quotes per venue is needed to exercise the ratio at all.
    // -----------------------------------------------------------------------

    /// A market-data source that quotes one scripted last price per venue id and
    /// can be made to fail for one of them.
    /// `(venue id, last price)`; `None` means a ticker with no last price.
    type Quotes = StdArc<StdMutex<Vec<(String, Option<Decimal>)>>>;

    #[derive(Clone, Default)]
    struct StubMarket {
        quotes: Quotes,
        /// Venues whose `fetch_ticker` must fail.
        failing: StdArc<StdMutex<Vec<String>>>,
        /// Every venue id a ticker was requested for, in order.
        seen: StdArc<StdMutex<Vec<String>>>,
    }

    impl StubMarket {
        fn quoting(quotes: &[(&str, Option<Decimal>)]) -> Self {
            let owned: Vec<(String, Option<Decimal>)> =
                quotes.iter().map(|(venue, last)| ((*venue).to_string(), *last)).collect();
            Self { quotes: StdArc::new(StdMutex::new(owned)), ..Self::default() }
        }

        fn failing_on(self, venue: &str) -> Self {
            self.failing.lock().expect("quotes").push(venue.to_string());
            self
        }

        fn seen_venues(&self) -> Vec<String> {
            self.seen.lock().expect("quotes").clone()
        }

        /// Every port method a two-venue premium measurement never calls.
        fn unused<T>(op: &str) -> Result<T, PortError> {
            Err(PortError::Unsupported(op.to_string()))
        }
    }

    #[async_trait]
    impl MarketDataSource for StubMarket {
        async fn fetch_ticker(
            &self,
            req: market::FetchTickerRequest,
        ) -> Result<market::Ticker, PortError> {
            let venue = match req.exchange_id.as_option() {
                Some(id) => id.id.clone(),
                None => String::new(),
            };
            self.seen.lock().expect("quotes").push(venue.clone());
            if self.failing.lock().expect("quotes").contains(&venue) {
                return Err(PortError::Transport(format!("venue {venue} is unreachable")));
            }
            let quotes = self.quotes.lock().expect("quotes");
            let quote =
                quotes.iter().find(|(id, _)| *id == venue).map(|(_, last)| *last).ok_or_else(
                    || PortError::NotFound(format!("no quote configured for venue {venue}")),
                )?;
            let last = match quote {
                Some(price) => {
                    let wire = longtrader_contract::ext::decimal_to_common(price);
                    MessageField::some(wire)
                }
                None => MessageField::none(),
            };
            Ok(market::Ticker { symbol: req.symbol, last, ..Default::default() })
        }

        async fn fetch_order_book(
            &self,
            _req: market::FetchOrderBookRequest,
        ) -> Result<market::OrderBook, PortError> {
            Self::unused("fetch_order_book")
        }

        async fn get_candles(
            &self,
            _req: market::GetCandlesRequest,
        ) -> Result<market::GetCandlesResponse, PortError> {
            Self::unused("get_candles")
        }

        async fn list_symbols(
            &self,
            _req: market::ListSymbolsRequest,
        ) -> Result<market::ListSymbolsResponse, PortError> {
            Self::unused("list_symbols")
        }

        async fn search_symbols(
            &self,
            _req: market::SearchSymbolsRequest,
        ) -> Result<market::SearchSymbolsResponse, PortError> {
            Self::unused("search_symbols")
        }

        async fn list_tickers(
            &self,
            _req: market::ListTickersRequest,
        ) -> Result<market::ListTickersResponse, PortError> {
            Self::unused("list_tickers")
        }

        async fn subscribe_market_data(
            &self,
            _req: market::StreamMarketDataRequest,
            _policy: OverflowPolicy,
        ) -> Result<MarketEventStream, PortError> {
            Self::unused("subscribe_market_data")
        }
    }

    /// A two-venue config; both legs watch the same symbol.
    fn monitor(
        market: StubMarket,
        primary: &str,
        hedge: &str,
        alert_threshold: &str,
    ) -> PremiumMonitor {
        let doc = format!(
            "symbol = \"BTCUSDT\"\nalert_threshold = \"{alert_threshold}\"\n\
             [primary]\nexchange_id = \"{primary}\"\n\
             [hedge]\nexchange_id = \"{hedge}\""
        );
        let table: toml::Table = toml::from_str(&doc).expect("toml");
        let config: PremiumMonitorConfig =
            crate::strategies::params::params_from_table(&table).expect("config");
        PremiumMonitor::new(config, StdArc::new(market))
    }

    #[tokio::test]
    async fn a_richer_primary_venue_reports_a_positive_premium() {
        let market = StubMarket::quoting(&[("a", Some(dec!(110))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("tick"), Some(dec!(0.1)));
        assert_eq!(*s.last_premium.lock().await, Some(dec!(0.1)));
    }

    #[tokio::test]
    async fn a_cheaper_primary_venue_reports_a_discount() {
        let market = StubMarket::quoting(&[("a", Some(dec!(90))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("tick"), Some(dec!(-0.1)));
    }

    #[tokio::test]
    async fn matching_venues_report_exactly_zero_and_record_it() {
        let market = StubMarket::quoting(&[("a", Some(dec!(100))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("tick"), Some(Decimal::ZERO));
        assert_eq!(*s.last_premium.lock().await, Some(Decimal::ZERO));
    }

    /// Both legs must actually be polled: a config that named the primary twice
    /// would report a permanent zero premium.
    #[tokio::test]
    async fn both_configured_venues_are_polled() {
        let market = StubMarket::quoting(&[("a", Some(dec!(100))), ("b", Some(dec!(100)))]);
        let s = monitor(market.clone(), "a", "b", "0.2");

        s.tick().await.expect("tick");
        assert_eq!(market.seen_venues(), vec!["a".to_string(), "b".to_string()]);
    }

    /// A zero hedge makes the ratio undefined. Reporting that as a flat premium
    /// would be indistinguishable from a par market, and `0` can never breach
    /// `alert_threshold`, so the cycle is reported as unknown.
    #[tokio::test]
    async fn a_zero_hedge_venue_reports_an_unknown_premium() {
        let market = StubMarket::quoting(&[("a", Some(dec!(100))), ("b", Some(Decimal::ZERO))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("a zero hedge is not an error"), None);
        assert_eq!(*s.last_premium.lock().await, None, "unknown, not Some(0)");
    }

    /// The same on the numerator side: a zero primary is a broken quote, not a
    /// discount of a hundred percent.
    #[tokio::test]
    async fn a_zero_primary_venue_reports_an_unknown_premium() {
        let market = StubMarket::quoting(&[("a", Some(Decimal::ZERO)), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("a zero primary is not an error"), None);
        assert_eq!(*s.last_premium.lock().await, None, "unknown, not Some(0)");
    }

    /// A cycle that cannot be quoted must not leave the previous reading behind:
    /// the record describes the latest state, and `None` is how it says
    /// "unknown" rather than "par".
    #[tokio::test]
    async fn an_unquotable_cycle_clears_the_previously_recorded_premium() {
        let market = StubMarket::quoting(&[("a", Some(dec!(110))), ("b", Some(dec!(100)))]);
        let s = monitor(market.clone(), "a", "b", "0.2");
        assert_eq!(s.tick().await.expect("tick"), Some(dec!(0.1)));
        assert_eq!(*s.last_premium.lock().await, Some(dec!(0.1)));

        // The hedge venue starts quoting zero: unquotable, not par.
        *market.quotes.lock().expect("quotes") =
            vec![("a".to_string(), Some(dec!(110))), ("b".to_string(), Some(Decimal::ZERO))];
        assert_eq!(s.tick().await.expect("a zero hedge is not an error"), None);
        assert_eq!(*s.last_premium.lock().await, None);
    }

    /// A ticker with no last price is a malformed answer, not a flat market.
    #[tokio::test]
    async fn a_ticker_without_a_last_price_is_an_error() {
        let market = StubMarket::quoting(&[("a", Some(dec!(100))), ("b", None)]);
        let s = monitor(market, "a", "b", "0.2");

        let err = s.tick().await.expect_err("a ticker with no last price must surface");
        assert!(err.to_string().contains("missing last price"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_primary_venue_propagates() {
        let market =
            StubMarket::quoting(&[("a", Some(dec!(100))), ("b", Some(dec!(100)))]).failing_on("a");
        let s = monitor(market, "a", "b", "0.2");

        let err = s.tick().await.expect_err("an unreachable venue must surface");
        assert!(err.to_string().contains("venue a is unreachable"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_hedge_venue_propagates() {
        let market =
            StubMarket::quoting(&[("a", Some(dec!(100))), ("b", Some(dec!(100)))]).failing_on("b");
        let s = monitor(market, "a", "b", "0.2");

        let err = s.tick().await.expect_err("an unreachable venue must surface");
        assert!(err.to_string().contains("venue b is unreachable"), "{err}");
    }

    /// A failed cycle must not leave a stale premium behind: the last value has
    /// to describe the last successful measurement or nothing at all.
    #[tokio::test]
    async fn a_failed_cycle_records_no_premium() {
        let market = StubMarket::quoting(&[("a", Some(dec!(100))), ("b", None)]);
        let s = monitor(market, "a", "b", "0.2");

        assert!(s.tick().await.is_err());
        assert_eq!(*s.last_premium.lock().await, None);
    }

    /// The breach alert is a log side effect, so the observable contract is that
    /// a breach is still returned and recorded in full rather than clamped.
    #[tokio::test]
    async fn a_premium_beyond_the_alert_threshold_is_reported_in_full() {
        let market = StubMarket::quoting(&[("a", Some(dec!(150))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("tick"), Some(dec!(0.5)));
        assert_eq!(*s.last_premium.lock().await, Some(dec!(0.5)));
    }

    /// A discount breaches on magnitude, not on sign.
    #[tokio::test]
    async fn a_discount_beyond_the_alert_threshold_is_reported_in_full() {
        let market = StubMarket::quoting(&[("a", Some(dec!(50))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.2");

        assert_eq!(s.tick().await.expect("tick"), Some(dec!(-0.5)));
    }

    #[tokio::test]
    async fn a_premium_exactly_at_the_alert_threshold_is_reported_unchanged() {
        let market = StubMarket::quoting(&[("a", Some(dec!(110))), ("b", Some(dec!(100)))]);
        let s = monitor(market, "a", "b", "0.1");

        assert_eq!(s.tick().await.expect("tick"), Some(dec!(0.1)));
    }

    /// `alert_threshold` is required: with no default a typo would silently
    /// leave the monitor alerting on everything.
    #[test]
    fn an_alert_threshold_is_required() {
        let doc =
            "symbol = \"BTCUSDT\"\n[primary]\nexchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"";
        let table: toml::Table = toml::from_str(doc).expect("toml");
        let parsed: Result<PremiumMonitorConfig, _> =
            crate::strategies::params::params_from_table(&table);
        assert!(parsed.is_err(), "alert_threshold is a required field");
    }

    /// Prices whose magnitude and scale keep `primary / hedge` inside the
    /// representable `Decimal` range, so these properties cannot trip the
    /// division overflow an unbounded strategy would hit.
    ///
    /// The bound is spelled `ProptestStrategy` because the prelude's `Strategy`
    /// collides with `crate::strategies::Strategy` in this scope.
    fn arb_price() -> impl ProptestStrategy<Value = Decimal> {
        (prop::num::i64::ANY, 0u32..=4).prop_map(|(mantissa, scale)| Decimal::new(mantissa, scale))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Two venues quoting the same price are at parity: the reported premium
        /// is exactly zero, which is what makes a threshold comparison honest.
        #[test]
        fn two_venues_at_the_same_price_are_exactly_at_parity(price in arb_price()) {
            prop_assume!(!price.is_zero());
            prop_assert!(premium(price, price).is_some_and(|prem| prem.is_zero()));
        }

        /// `is_zero` is a value test, so any zero representation has no premium
        /// and any non-zero price on both legs always has one.
        #[test]
        fn a_premium_exists_exactly_when_neither_price_is_zero(
            primary in arb_price(),
            hedge in arb_price(),
        ) {
            prop_assert_eq!(
                premium(primary, hedge).is_some(),
                !primary.is_zero() && !hedge.is_zero(), "primary={} hedge={}", primary, hedge );
        }

        /// Doubling the primary while holding the hedge fixed scales the premium
        /// by the same factor minus one.
        #[test]
        fn scaling_the_primary_scales_the_premium(
            hedge in arb_price(),
            factor in 1i64..1_000,
        ) {
            prop_assume!(!hedge.is_zero());
            let factor = Decimal::from(factor);
            let primary = hedge * factor;
            let expected = factor - Decimal::ONE;
            prop_assert_eq!(premium(primary, hedge), Some(expected));
        }
    }
}
