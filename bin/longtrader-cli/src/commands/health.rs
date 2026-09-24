use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(client: &TerminalClient, output: &Renderer) -> Result<()> {
    match client.health().await {
        Ok(resp) => {
            if let Renderer { format: crate::output::OutputFormat::Json, .. } = output {
                println!("{}", serde_json::to_string_pretty(&resp).unwrap_or_default());
            } else {
                println!("Status: {}", resp.status);
                println!("Uptime: {}s", resp.uptime_secs);
                if !resp.venues.is_empty() {
                    println!("Venues:");
                    for v in &resp.venues {
                        println!("  - {} (connected: {})", v.name, v.connected);
                    }
                }
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
