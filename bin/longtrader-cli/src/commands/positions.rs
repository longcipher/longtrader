use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, venue: &str, output: &Renderer) -> Result<()> {
    match client.get_positions(venue).await {
        Ok(positions) => {
            output.render_positions(&positions);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
