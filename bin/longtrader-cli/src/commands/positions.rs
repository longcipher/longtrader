use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, venue: &str, output: &Renderer) {
    match client.get_positions(venue).await {
        Ok(positions) => output.render_positions(&positions),
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
