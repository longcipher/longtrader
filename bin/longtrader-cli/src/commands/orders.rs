use longtrader_proto::{client::TerminalClient, proto::longtrader::terminal::v1::Side};

use crate::output::Renderer;

pub(crate) async fn place_order(
    client: &TerminalClient,
    venue: &str,
    symbol: &str,
    side: Side,
    quantity: &str,
    price: Option<&str>,
    take_profit: Option<&str>,
    stop_loss: Option<&str>,
    output: &Renderer,
) {
    let order_type = if price.is_some() {
        longtrader_proto::proto::longtrader::terminal::v1::OrderType::Limit
    } else {
        longtrader_proto::proto::longtrader::terminal::v1::OrderType::Market
    };

    let client_order_id = format!("cli-{}", ulid::Ulid::generate());

    match client
        .place_order(
            venue,
            symbol,
            side,
            order_type,
            quantity,
            price,
            take_profit,
            stop_loss,
            &client_order_id,
        )
        .await
    {
        Ok(order) => output.render_orders(&[order]),
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
