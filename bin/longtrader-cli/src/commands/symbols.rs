use tradingcharts_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, venue: &str, output: &Renderer) {
    match client.get_symbols(venue).await {
        Ok(symbols) => output.render_symbols(&symbols),
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
