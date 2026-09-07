use tradingcharts_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, output: &Renderer) {
    match client.list_venues().await {
        Ok(venues) => output.render_venues(&venues),
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
