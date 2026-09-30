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
