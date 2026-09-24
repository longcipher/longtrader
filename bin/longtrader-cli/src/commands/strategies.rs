use color_eyre::Result;
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn list(client: &TerminalClient, output: &Renderer) -> Result<()> {
    match client.list_strategies().await {
        Ok(items) => {
            if items.is_empty() {
                output.render_msg("no strategies");
            } else {
                for s in &items {
                    output.render_msg(&format!(
                        "{} {} [{}] {}",
                        s.strategy_id, s.name, s.status, s.error
                    ));
                }
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn start(
    client: &TerminalClient,
    strategy_id: &str,
    name: Option<&str>,
    output: &Renderer,
) -> Result<()> {
    match client.start_strategy(strategy_id, name.unwrap_or(strategy_id), "{}").await {
        Ok(s) => {
            output.render_msg(&format!("started {} [{}]", s.strategy_id, s.status));
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn stop(
    client: &TerminalClient,
    strategy_id: &str,
    cancel_all: bool,
    output: &Renderer,
) -> Result<()> {
    match client.stop_strategy(strategy_id, cancel_all).await {
        Ok(s) => {
            output.render_msg(&format!("stopped {} [{}]", s.strategy_id, s.status));
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
