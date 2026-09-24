use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, venue: &str, output: &Renderer) -> Result<()> {
    match client.get_symbols(venue).await {
        Ok(symbols) => {
            output.render_symbols(&symbols);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
