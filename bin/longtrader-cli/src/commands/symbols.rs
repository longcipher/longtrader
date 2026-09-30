use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::{commands::exchange_id, output::Renderer};

pub(crate) async fn run(client: &TerminalClient, venue: &str, output: &Renderer) -> Result<()> {
    match client.list_symbols(&exchange_id(venue)).await {
        Ok(symbols) => {
            output.render_symbols(&symbols);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
