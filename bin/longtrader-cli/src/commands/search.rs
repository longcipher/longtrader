use tradingcharts_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    venue: &str,
    query: &str,
    limit: u32,
    output: &Renderer,
) {
    match client.search_symbols(venue, query, limit).await {
        Ok(symbols) => output.render_symbols(&symbols),
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
