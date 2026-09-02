//! Portable strategies. A strategy depends only on the ports in
//! [`crate::ports`]; backend selection is config-driven.
//!
//! The candle-driven family shares one pattern: each strategy polls candles
//! via [`MarketDataSource::get_candles`](crate::ports::MarketDataSource) and
//! trades through [`TradingGateway`](crate::ports::TradingGateway).
//! Every strategy ships its own `README.md` alongside the module.

pub mod autoborrow;
pub mod balance_align;
pub mod boll_grid;
pub mod convert;
pub mod cross_depth_maker;
pub mod cross_fixed_maker;
pub mod cross_maker;
pub mod dca_scheduler;
pub mod deposit_transfer;
pub mod ema_cross;
pub mod fixed_maker;
pub mod hedge_grid;
pub mod irr;
pub mod market_cap;
pub mod nav_recorder;
pub mod premium_monitor;
pub mod random_entry;
pub mod rebalance;
pub mod sentinel;
pub mod simple_grid;
pub mod supertrend;
pub mod xfunding_lite;

use async_trait::async_trait;
use color_eyre::Result;
use longtrader_contract::ext::decimal_to_common;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ports::{MarketDataSource, PortError},
    proto::{common, market, trading},
};

#[async_trait]
pub trait Strategy: Send + Sync {
    async fn run(&self) -> Result<()>;
}

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

const fn default_poll_secs() -> u64 {
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
    pub fn proto_exchange_id(&self) -> common::ExchangeId {
        crate::adapters::exchange_id(&self.exchange_id, &self.label)
    }
}

/// Deserializes a strategy-specific config from the `[strategy.params]`
/// table.
///
/// # Errors
///
/// Returns an error when required fields are missing or typed wrongly.
pub fn params_from_table<T: serde::de::DeserializeOwned>(table: &toml::Table) -> Result<T> {
    toml::Value::Table(table.clone())
        .try_into()
        .map_err(|err| color_eyre::eyre::eyre!("invalid strategy params: {err}"))
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
            timeframe: timeframe.to_string(),
            limit,
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

/// Builds a limit-order create request.
pub(crate) fn limit_order(
    exchange_id: &common::ExchangeId,
    symbol: &str,
    client_order_id: String,
    is_buy: bool,
    price: Decimal,
    qty: Decimal,
) -> trading::CreateOrderRequest {
    order_request(exchange_id, symbol, client_order_id, is_buy, Some(price), qty, false)
}

/// Builds a market-order create request.
pub(crate) fn market_order(
    exchange_id: &common::ExchangeId,
    symbol: &str,
    client_order_id: String,
    is_buy: bool,
    qty: Decimal,
) -> trading::CreateOrderRequest {
    order_request(exchange_id, symbol, client_order_id, is_buy, None, qty, true)
}

#[allow(clippy::too_many_arguments)]
fn order_request(
    exchange_id: &common::ExchangeId,
    symbol: &str,
    client_order_id: String,
    is_buy: bool,
    price: Option<Decimal>,
    qty: Decimal,
    is_market: bool,
) -> trading::CreateOrderRequest {
    trading::CreateOrderRequest {
        exchange_id: buffa::MessageField::some(exchange_id.clone()),
        order: buffa::MessageField::some(trading::OrderRequest {
            client_order_id,
            symbol: symbol.to_string(),
            r#type: buffa::EnumValue::Known(if is_market {
                trading::OrderType::Market
            } else {
                trading::OrderType::Limit
            }),
            side: buffa::EnumValue::Known(if is_buy {
                trading::OrderSide::Buy
            } else {
                trading::OrderSide::Sell
            }),
            amount: buffa::MessageField::some(decimal_to_common(qty)),
            price: price.map(decimal_to_common).map(buffa::MessageField::some).unwrap_or_default(),
            trigger_price: buffa::MessageField::none(),
            time_in_force: buffa::EnumValue::Known(trading::TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            params: Default::default(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
