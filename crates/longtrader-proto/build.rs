fn main() {
    let workspace_proto = format!("{}/../../proto", env!("CARGO_MANIFEST_DIR"));
    connectrpc_build::Config::new()
        .files(&[
            &format!("{workspace_proto}/longtrader/terminal/v1/messages.proto"),
            &format!("{workspace_proto}/longtrader/terminal/v1/market.proto"),
            &format!("{workspace_proto}/longtrader/terminal/v1/trading.proto"),
            &format!("{workspace_proto}/longtrader/terminal/v1/runtime.proto"),
            &format!("{workspace_proto}/longtrader/terminal/v1/strategy.proto"),
        ])
        .includes(&[&workspace_proto])
        .include_file("_connectrpc.rs")
        .compile()
        .expect("failed to compile longtrader terminal proto");
}
