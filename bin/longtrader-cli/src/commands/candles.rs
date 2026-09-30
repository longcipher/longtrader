use color_eyre::Result;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, market::v1 as umarket},
};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    timeframe: &str,
    limit: u32,
    output: &Renderer,
) -> Result<()> {
    let tf = match timeframe.to_uppercase().as_str() {
        "S100" => umarket::Timeframe::S100,
        "S1" => umarket::Timeframe::S1,
        "M1" => umarket::Timeframe::M1,
        "M5" => umarket::Timeframe::M5,
        "M15" => umarket::Timeframe::M15,
        "M30" => umarket::Timeframe::M30,
        "H1" => umarket::Timeframe::H1,
        "H4" => umarket::Timeframe::H4,
        "D1" => umarket::Timeframe::D1,
        "W1" => umarket::Timeframe::W1,
        _ => return Err(color_eyre::Report::msg(format!("Unsupported timeframe: {timeframe}"))),
    };

    match client.get_candles(exchange, symbol, tf, limit).await {
        Ok(candles) => {
            output.render_candles(&candles);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
