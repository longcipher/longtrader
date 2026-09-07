use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn list(_client: &TerminalClient, output: &Renderer) {
    output.render_msg(
        "Strategy listing requires direct RPC call (not yet implemented in TerminalClient)",
    );
}

pub(crate) async fn start(
    _client: &TerminalClient,
    _strategy_id: &str,
    _name: Option<&str>,
    output: &Renderer,
) {
    output.render_msg(
        "Strategy start requires direct RPC call (not yet implemented in TerminalClient)",
    );
}

pub(crate) async fn stop(
    _client: &TerminalClient,
    _strategy_id: &str,
    _cancel_all: bool,
    output: &Renderer,
) {
    output.render_msg(
        "Strategy stop requires direct RPC call (not yet implemented in TerminalClient)",
    );
}
