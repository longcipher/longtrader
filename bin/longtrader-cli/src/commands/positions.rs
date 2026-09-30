use color_eyre::Result;
use longtrader_proto::{client::TerminalClient, proto::longtrader::common::v1 as common};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    output: &Renderer,
) -> Result<()> {
    match client.get_positions(exchange, &[]).await {
        Ok(positions) => {
            output.render_positions(&positions);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
