use color_eyre::Result;
use longtrader_proto::{client::TerminalClient, proto::longtrader::common::v1 as common};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    query: &str,
    limit: u32,
    output: &Renderer,
) -> Result<()> {
    match client.search_symbols(exchange, query, limit).await {
        Ok(symbols) => {
            output.render_symbols(&symbols);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
