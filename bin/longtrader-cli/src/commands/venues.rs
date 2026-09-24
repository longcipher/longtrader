use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, output: &Renderer) -> Result<()> {
    match client.list_venues().await {
        Ok(venues) => {
            output.render_venues(&venues);
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
