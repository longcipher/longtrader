//! Shared strategy parameter types (single owner for `CommonParams` family).
//!
//! Extracted from `strategies::mod` so param parsing has one home.

use color_eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, PortError},
    proto::{common, market},
};

/// Parameter fields shared by every candle-driven strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct CommonParams {
    /// Venue identifier string (e.g. `"binance"`).
    #[serde(default = "default_exchange")]
    pub exchange_id: String,
    /// Optional venue sub-account label.
    #[serde(default)]
    pub label: String,
    /// Target symbol.
    pub symbol: String,
    /// Candle timeframe (e.g. `"5m"`).
    #[serde(default = "default_timeframe")]
    pub timeframe: String,
    /// Poll cadence in seconds.
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
}

/// One venue endpoint reference for cross-venue strategies.
#[derive(Debug, Clone, Deserialize)]
pub struct VenueRef {
    /// Venue identifier string.
    #[serde(default = "default_exchange")]
    pub exchange_id: String,
    /// Optional sub-account label.
    #[serde(default)]
    pub label: String,
}

impl VenueRef {
    /// Builds the proto `ExchangeId`.
    #[must_use]
    pub fn proto(&self) -> common::ExchangeId {
        crate::adapters::exchange_id(&self.exchange_id, &self.label)
    }
}

/// Two-venue addressing shared by the cross-venue family.
#[derive(Debug, Clone, Deserialize)]
pub struct CrossVenueParams {
    /// Quoting venue (orders rest here).
    pub primary: VenueRef,
    /// Reference / hedging venue (fair value and hedges).
    pub hedge: VenueRef,
    /// Target symbol (same on both venues).
    pub symbol: String,
    /// Poll cadence in seconds.
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
}

impl CrossVenueParams {
    /// Parses config from the `[strategy.params]` table.
    ///
    /// # Errors
    /// Propagates deserialization errors.
    pub fn from_params(table: &toml::Table) -> Result<Self> {
        params_from_table(table)
    }
}

pub(crate) const fn default_poll_secs() -> u64 {
    30
}
fn default_timeframe() -> String {
    String::from("5m")
}
fn default_exchange() -> String {
    String::from("mock")
}

impl CommonParams {
    /// Builds the proto `ExchangeId` from the configured venue + label.
    #[must_use]
    pub fn proto_exchange_id(&self) -> common::ExchangeId {
        crate::adapters::exchange_id(&self.exchange_id, &self.label)
    }
}

/// Deserializes a strategy-specific config from the `[strategy.params]` table.
///
/// # Errors
///
/// Returns an error when required fields are missing or typed wrongly.
pub fn params_from_table<T: serde::de::DeserializeOwned>(table: &toml::Table) -> Result<T> {
    toml::Value::Table(table.clone())
        .try_into()
        .map_err(|err| color_eyre::eyre::eyre!("invalid strategy params: {err}"))
}

/// Map a configuration timeframe string onto the contract enum.
///
/// Both the short config spellings (`"5m"`) and the enum-suffixed ones
/// (`"M5"`) are accepted, case- and space-insensitively. An unknown value is
/// an error, never a silent `M1`: a strategy that asked for a period the venue
/// does not serve would otherwise trade on the wrong bars.
pub(crate) fn timeframe_from_str(tf: &str) -> Result<market::Timeframe, PortError> {
    let normalized = tf.trim().to_ascii_uppercase();
    Ok(match normalized.as_str() {
        "" | "1M" | "M1" => market::Timeframe::M1,
        "5M" | "M5" => market::Timeframe::M5,
        "15M" | "M15" => market::Timeframe::M15,
        "30M" | "M30" => market::Timeframe::M30,
        "1H" | "H1" => market::Timeframe::H1,
        "4H" | "H4" => market::Timeframe::H4,
        "1D" | "D1" => market::Timeframe::D1,
        "1W" | "W1" => market::Timeframe::W1,
        "1S" | "S1" => market::Timeframe::S1,
        "100S" | "S100" => market::Timeframe::S100,
        other => {
            return Err(PortError::InvalidArgument(format!(
                "unsupported timeframe {other:?} (supported: S1/S100/M1/M5/M15/M30/H1/H4/D1/W1)"
            )));
        }
    })
}

/// Fetches the most recent candles for a symbol.
pub(crate) async fn fetch_candles(
    market: &dyn MarketDataSource,
    exchange_id: &common::ExchangeId,
    symbol: &str,
    timeframe: &str,
    limit: u32,
) -> Result<Vec<market::Candle>, PortError> {
    let response = market
        .get_candles(market::GetCandlesRequest {
            exchange_id: buffa::MessageField::some(exchange_id.clone()),
            symbol: symbol.to_string(),
            timeframe: buffa::EnumValue::Known(timeframe_from_str(timeframe)?),
            pagination: crate::proto::common::Pagination {
                limit: u64::from(limit),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await?;
    Ok(response.candles)
}

/// Extracts close prices from candles (skipping unset decimals).
pub(crate) fn closes_of(candles: &[market::Candle]) -> Vec<Decimal> {
    candles
        .iter()
        .filter_map(|c| c.close.as_option())
        .filter_map(|d| longtrader_contract::ext::common_to_decimal(d).ok())
        .collect()
}

/// Fetches the close prices of the most recent candle window.
pub(crate) async fn fetch_closes_of(
    market: &dyn MarketDataSource,
    exchange_id: &common::ExchangeId,
    symbol: &str,
    timeframe: &str,
) -> Result<Vec<Decimal>, PortError> {
    let candles = fetch_candles(market, exchange_id, symbol, timeframe, 200).await?;
    Ok(closes_of(&candles))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    // -----------------------------------------------------------------------
    // timeframe_from_str
    // -----------------------------------------------------------------------

    /// Every spelling in the config alphabet must map to its contract enum, in
    /// both the short (`"5m"`) and the enum-suffixed (`"M5"`) form.
    #[test]
    fn every_documented_timeframe_spelling_is_accepted() {
        let cases = [
            ("", market::Timeframe::M1),
            ("1m", market::Timeframe::M1),
            ("M1", market::Timeframe::M1),
            ("5m", market::Timeframe::M5),
            ("M5", market::Timeframe::M5),
            ("15m", market::Timeframe::M15),
            ("M15", market::Timeframe::M15),
            ("30m", market::Timeframe::M30),
            ("M30", market::Timeframe::M30),
            ("1h", market::Timeframe::H1),
            ("H1", market::Timeframe::H1),
            ("4h", market::Timeframe::H4),
            ("H4", market::Timeframe::H4),
            ("1d", market::Timeframe::D1),
            ("D1", market::Timeframe::D1),
            ("1w", market::Timeframe::W1),
            ("W1", market::Timeframe::W1),
            ("1s", market::Timeframe::S1),
            ("S1", market::Timeframe::S1),
            ("100s", market::Timeframe::S100),
            ("S100", market::Timeframe::S100),
        ];
        for (input, expected) in cases {
            assert_eq!(
                timeframe_from_str(input).expect("documented spelling"),
                expected,
                "timeframe {input:?}"
            );
        }
    }

    /// The parser is case- and whitespace-insensitive so a config written by
    /// hand in any style still resolves.
    #[test]
    fn the_timeframe_parser_ignores_case_and_surrounding_space() {
        for input in ["m5", "M5", "m5 ", " m5", "  m5\t"] {
            assert_eq!(
                timeframe_from_str(input).expect("case/space insensitive"),
                market::Timeframe::M5,
                "{input:?}"
            );
        }
    }

    /// An unknown period must be an error, never a silent `M1`: a strategy that
    /// asked for bars the venue does not serve would otherwise trade the wrong
    /// timeframe.
    #[test]
    fn an_unsupported_timeframe_is_an_error_not_a_default() {
        for input in ["2m", "M2", "1x", "hourly", "5", "m", "-5m", "M0", "S2", "999m"] {
            let err = timeframe_from_str(input).expect_err("must not silently default");
            assert!(matches!(err, PortError::InvalidArgument(_)), "{input:?} produced {err:?}");
            let message = err.to_string();
            assert!(
                message.contains("unsupported timeframe"),
                "the message must say what went wrong: {message}"
            );
        }
    }

    /// The error enumerates the supported set so an operator can fix the config
    /// without reading the source.
    #[test]
    fn the_timeframe_error_lists_the_supported_periods() {
        let err = timeframe_from_str("2m").expect_err("unsupported");
        let message = err.to_string();
        for supported in ["S1", "S100", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1"] {
            assert!(message.contains(supported), "the error must mention {supported}: {message}");
        }
    }

    /// The error echoes the normalized (trimmed, upper-cased) input, so a
    /// typo is recognisable in a log line.
    #[test]
    fn the_timeframe_error_echoes_the_normalized_input() {
        let err = timeframe_from_str("  bogus  ").expect_err("unsupported");
        assert!(err.to_string().contains("BOGUS"), "{err}");
    }

    // -----------------------------------------------------------------------
    // params_from_table
    // -----------------------------------------------------------------------

    fn table(doc: &str) -> toml::Table {
        let value: toml::Value = toml::from_str(doc).expect("the fixture parses");
        match value {
            toml::Value::Table(table) => table,
            _ => unreachable!("the fixture is always a table"),
        }
    }

    /// A strategy config must materialize with every documented default applied
    /// when the key is absent.
    #[test]
    fn common_params_apply_every_documented_default() {
        let params: CommonParams = params_from_table(&table("symbol = \"BTC/USDT\""))
            .expect("only the symbol is required");
        assert_eq!(params.symbol, "BTC/USDT");
        assert_eq!(params.exchange_id, "mock", "the default venue is the mock backend");
        assert_eq!(params.label, "");
        assert_eq!(params.timeframe, "5m");
        assert_eq!(params.poll_secs, 30);
    }

    /// `symbol` is the one required field; omitting it must be a startup error,
    /// not a strategy that polls an empty symbol forever.
    #[test]
    fn common_params_require_a_symbol() {
        let err = params_from_table::<CommonParams>(&table("exchange_id = \"binance\""))
            .expect_err("symbol is required");
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    #[test]
    fn every_common_param_can_be_overridden() {
        let params: CommonParams = params_from_table(&table(
            "symbol = \"ETH/USDT\"\nexchange_id = \"htx\"\nlabel = \"sub\"\ntimeframe = \"1h\"\npoll_secs = 7\n",
        ))
        .expect("all fields supplied");
        assert_eq!(params.symbol, "ETH/USDT");
        assert_eq!(params.exchange_id, "htx");
        assert_eq!(params.label, "sub");
        assert_eq!(params.timeframe, "1h");
        assert_eq!(params.poll_secs, 7);
    }

    /// `proto_exchange_id` is how a strategy addresses a venue; it must carry
    /// both halves through to the contract.
    #[test]
    fn common_params_build_the_proto_exchange_id() {
        let params: CommonParams = params_from_table(&table(
            "symbol = \"X\"\nexchange_id = \"binance\"\nlabel = \"sub-2\"\n",
        ))
        .expect("parses");
        let id = params.proto_exchange_id();
        assert_eq!(id.id, "binance");
        assert_eq!(id.label, "sub-2");
    }

    #[test]
    fn venue_ref_applies_its_defaults_and_builds_its_proto() {
        let params: VenueRef =
            params_from_table(&table("exchange_id = \"okx\"\nlabel = \"acct\"")).expect("parses");
        assert_eq!(params.exchange_id, "okx");
        let id = params.proto();
        assert_eq!(id.id, "okx");
        assert_eq!(id.label, "acct");

        let bare: VenueRef = params_from_table(&table("")).expect("all fields optional");
        assert_eq!(bare.exchange_id, "mock");
        assert_eq!(bare.label, "");
    }

    /// A cross-venue strategy must be able to name two distinct venues; an empty
    /// `hedge` defaulting to the primary would silently collapse the hedge leg.
    #[test]
    fn cross_venue_params_require_both_legs() {
        let err = params_from_table::<CrossVenueParams>(&table(
            "symbol = \"X\"\n[primary]\nexchange_id = \"a\"\n",
        ))
        .expect_err("hedge is required");
        assert!(err.to_string().contains("hedge"), "{err}");

        let params: CrossVenueParams = params_from_table(&table(
            "symbol = \"X\"\n[primary]\nexchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"\n",
        ))
        .expect("both legs present");
        assert_eq!(params.primary.exchange_id, "a");
        assert_eq!(params.hedge.exchange_id, "b");
        assert_eq!(params.poll_secs, 30);
    }

    #[test]
    fn cross_venue_params_round_trip_through_from_params() {
        let table =
            table("symbol = \"X\"\n[primary]\nexchange_id = \"a\"\n[hedge]\nexchange_id = \"b\"\n");
        let params = CrossVenueParams::from_params(&table).expect("parses");
        assert_eq!(params.symbol, "X");
    }

    /// A typed mismatch is reported with the offending field, not swallowed.
    #[test]
    fn a_wrongly_typed_param_is_rejected() {
        let err = params_from_table::<CommonParams>(&table("symbol = 42")).expect_err("type error");
        assert!(err.to_string().contains("symbol"), "{err}");

        let err = params_from_table::<CommonParams>(&table("symbol = \"X\"\npoll_secs = \"soon\""))
            .expect_err("type error");
        assert!(err.to_string().contains("poll_secs"), "{err}");
    }

    /// Every error from the shared helper is namespaced so an operator can tell
    /// which strategy failed to start.
    #[test]
    fn param_errors_are_namespaced() {
        let err = params_from_table::<CommonParams>(&table("")).expect_err("missing symbol");
        assert!(err.to_string().contains("invalid strategy params"), "{err}");
    }

    // -----------------------------------------------------------------------
    // closes_of
    // -----------------------------------------------------------------------

    fn candle_with_close(close: Option<Decimal>) -> market::Candle {
        market::Candle {
            close: close
                .map(longtrader_contract::ext::decimal_to_common)
                .map_or_default(buffa::MessageField::some),
            ..Default::default()
        }
    }

    /// A candle whose close is present on the wire but carries `value` verbatim,
    /// so a test can hand the decoder a payload this crate would never write.
    fn candle_with_wire_close(value: &str) -> market::Candle {
        market::Candle {
            close: buffa::MessageField::some(common::Decimal {
                value: value.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn closes_of_extracts_every_decodable_close() {
        let candles = vec![
            candle_with_close(Some(dec!(1.5))),
            candle_with_close(Some(dec!(2.5))),
            candle_with_close(Some(dec!(3.5))),
        ];
        assert_eq!(closes_of(&candles), vec![dec!(1.5), dec!(2.5), dec!(3.5)]);
    }

    /// A candle with no close is skipped rather than treated as a zero close,
    /// which would drag every indicator down.
    #[test]
    fn closes_of_skips_candles_without_a_close() {
        let candles = vec![
            candle_with_close(Some(dec!(1))),
            candle_with_close(None),
            candle_with_close(Some(dec!(3))),
        ];
        assert_eq!(closes_of(&candles), vec![dec!(1), dec!(3)]);
    }

    /// Every way a present close can fail to decode drops the candle rather than
    /// substituting a zero. `Decimal` carries one base-10 string, so these are
    /// three properties of that payload: blank, outside the grammar, and
    /// well-formed but wider than a `rust_decimal` can hold.
    #[test]
    fn closes_of_skips_undecodable_decimals() {
        let candles = vec![
            candle_with_close(Some(dec!(7))),
            candle_with_wire_close(""),
            candle_with_wire_close("1e3"),
            candle_with_wire_close(" 1"),
            candle_with_wire_close(&"9".repeat(29)),
        ];
        assert_eq!(closes_of(&candles), vec![dec!(7)]);
    }

    /// There is no second representation to fall back on. A payload the grammar
    /// rejects is simply a close the strategy cannot see — it is not recovered
    /// from any other field, because there is no other field.
    #[test]
    fn closes_of_drops_a_payload_outside_the_contract_grammar() {
        for garbage in ["not-a-number", "1_000", "+1", ".5", "1."] {
            assert!(
                closes_of(&[candle_with_wire_close(garbage)]).is_empty(),
                "{garbage:?} is not a close price"
            );
        }
    }

    #[test]
    fn closes_of_an_empty_window_is_empty() {
        assert!(closes_of(&[]).is_empty());
    }

    #[test]
    fn closes_of_an_all_unusable_window_is_empty() {
        let candles = vec![candle_with_close(None), candle_with_close(None)];
        assert!(closes_of(&candles).is_empty());
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Case and surrounding whitespace never change which period resolves.
        #[test]
        fn the_timeframe_parser_normalizes_case_and_space(
            base in prop::sample::select(&["1m", "M1", "5m", "M5", "15m", "30m", "1h", "4h", "1d", "1w", "1s", "100s"]),
            pad_left in 0usize..4,
            pad_right in 0usize..4,
        ) {
            let expected = timeframe_from_str(base).expect("documented spelling");
            let padded = format!("{}{base}{}", " ".repeat(pad_left), " ".repeat(pad_right));
            prop_assert_eq!(timeframe_from_str(&padded).expect("normalizes"), expected);
            prop_assert_eq!(timeframe_from_str(&base.to_uppercase()).expect("normalizes"), expected);
            prop_assert_eq!(timeframe_from_str(&base.to_lowercase()).expect("normalizes"), expected);
        }

        /// A string that is not in the documented alphabet always errors rather
        /// than defaulting to `M1`.
        #[test]
        fn an_undocumented_timeframe_always_errors(input in "[a-zA-Z0-9]{1,6}") {
            // Compare in the parser's own normalized form (trimmed, upper-cased).
            let documented = [
                "1M", "M1", "5M", "M5", "15M", "M15", "30M", "M30", "1H", "H1", "4H", "H4",
                "1D", "D1", "1W", "W1", "1S", "S1", "100S", "S100",
            ];
            let normalized = input.trim().to_ascii_uppercase();
            if documented.contains(&normalized.as_str()) {
                return Ok(());
            }
            prop_assert!(
                matches!(timeframe_from_str(&input), Err(PortError::InvalidArgument(_))),
                "{input:?} must not resolve"
            );
        }

        /// Every close that encodes successfully decodes back, so `closes_of`
        /// never loses a value it was handed.
        #[test]
        fn closes_of_never_drops_a_well_formed_close(
            values in prop::collection::vec(-1_000_000i64..1_000_000, 0..20),
        ) {
            let candles: Vec<market::Candle> = values
                .into_iter()
                .map(|v| candle_with_close(Some(Decimal::from(v))))
                .collect();
            let closes = closes_of(&candles);
            prop_assert_eq!(closes.len(), candles.len());
        }

        /// `exchange_id` is a pure projection: the venue string reaches the
        /// contract untouched.
        #[test]
        fn the_proto_exchange_id_never_transforms_the_venue(
            venue in "[a-z0-9_-]{1,12}",
            label in "[a-z0-9_-]{0,12}",
        ) {
            let params: CommonParams = params_from_table(&table(&format!(
                "symbol = \"X\"\nexchange_id = \"{venue}\"\nlabel = \"{label}\"\n"
            )))
            .expect("the generated table parses");
            let id = params.proto_exchange_id();
            prop_assert_eq!(id.id.as_str(), venue.as_str());
            prop_assert_eq!(id.label.as_str(), label.as_str());
        }
    }
}
