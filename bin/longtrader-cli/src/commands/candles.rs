use color_eyre::Result;
use longtrader_proto::{client::TerminalClient, proto::longtrader::terminal::v1::Timeframe};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    venue: &str,
    symbol: &str,
    timeframe: &str,
    limit: u32,
    output: &Renderer,
) -> Result<()> {
    let tf = match timeframe.to_uppercase().as_str() {
        "S100" => Timeframe::S100,
        "S1" => Timeframe::S1,
        "M1" => Timeframe::M1,
        "M5" => Timeframe::M5,
        "M15" => Timeframe::M15,
        "M30" => Timeframe::M30,
        "H1" => Timeframe::H1,
        "H4" => Timeframe::H4,
        "D1" => Timeframe::D1,
        "W1" => Timeframe::W1,
        _ => return Err(color_eyre::Report::msg(format!("Unsupported timeframe: {timeframe}"))),
    };

    match client.get_candles(venue, symbol, tf, limit).await {
        Ok(candles) => {
            output.render_candles(&candles);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
