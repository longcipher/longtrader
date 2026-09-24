use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    venue: &str,
    query: &str,
    limit: u32,
    output: &Renderer,
) -> Result<()> {
    match client.search_symbols(venue, query, limit).await {
        Ok(symbols) => {
            output.render_symbols(&symbols);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
