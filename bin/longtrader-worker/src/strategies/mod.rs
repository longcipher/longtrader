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
pub mod params;
pub mod premium_monitor;
pub mod random_entry;
pub mod rebalance;
pub mod registry;
pub mod sentinel;
pub mod simple_grid;
pub mod supertrend;
pub mod xfunding_lite;

use async_trait::async_trait;
use color_eyre::Result;
use longtrader_contract::ext::decimal_to_common;
pub use params::{CommonParams, CrossVenueParams, VenueRef, params_from_table};
pub(crate) use params::{closes_of, fetch_candles, fetch_closes_of};
pub use registry::{
    StrategyContext, StrategyDescriptor, StrategyFactory, build_strategy, registry,
};
use rust_decimal::Decimal;

use crate::proto::{common, trading};

#[async_trait]
pub trait Strategy: Send + Sync {
    async fn run(&self) -> Result<()>;
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
            price: price.map(decimal_to_common).map_or_default(buffa::MessageField::some),
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
