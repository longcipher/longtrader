use color_eyre::{Result, eyre::eyre};
use longtrader_contract::ext::decimal_to_common;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, trading::v1 as utrading},
};
use rust_decimal::Decimal;

use crate::output::Renderer;

fn parse_dec(s: &str, field: &'static str) -> Result<Decimal> {
    s.parse::<Decimal>().map_err(|_| eyre!("bad decimal for {field}: {s:?}"))
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn place_order(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    side: utrading::OrderSide,
    quantity: &str,
    price: Option<&str>,
    take_profit: Option<&str>,
    stop_loss: Option<&str>,
    output: &Renderer,
) -> Result<()> {
    // A price turns the order into a limit; without one it is a market order.
    let order_type =
        if price.is_some() { utrading::OrderType::Limit } else { utrading::OrderType::Market };

    let bracket = |v: Option<&str>, field: &'static str| -> Result<_> {
        Ok(v.map(|p| parse_dec(p, field).map(|d| decimal_to_common(d).into()))
            .transpose()?
            .unwrap_or_default())
    };

    let order = utrading::OrderRequest {
        client_order_id: format!("cli-{}", ulid::Ulid::generate()),
        symbol: symbol.to_string(),
        r#type: buffa::EnumValue::Known(order_type),
        side: buffa::EnumValue::Known(side),
        amount: decimal_to_common(parse_dec(quantity, "quantity")?).into(),
        price: bracket(price, "price")?,
        take_profit: bracket(take_profit, "take_profit")?,
        stop_loss: bracket(stop_loss, "stop_loss")?,
        ..Default::default()
    };

    let req = utrading::CreateOrderRequest {
        exchange_id: exchange.clone().into(),
        order: order.into(),
        ..Default::default()
    };

    match client.create_order(req).await {
        Ok(order) => {
            output.render_orders(&[order]);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
