#![allow(missing_docs, missing_debug_implementations, clippy::print_stdout, clippy::print_stderr)]

mod commands;
mod output;

use clap::Parser;
use commands::Commands;
use output::OutputFormat;

#[derive(Parser, Debug)]
#[command(
    name = "longtrader",
    version,
    about = "LongTrader CLI — market data, trading, and streaming over Connect-RPC"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8810", global = true)]
    endpoint: String,

    #[arg(long, global = true)]
    token: Option<String>,

    #[arg(long, default_value = "mock", global = true)]
    venue: String,

    #[arg(long, value_enum, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let client = if let Some(token) = &cli.token {
        tradingcharts_proto::client::TerminalClient::new_with_token(&cli.endpoint, token)
    } else {
        tradingcharts_proto::client::TerminalClient::new(&cli.endpoint)
    };

    let output = output::Renderer::new(cli.format);

    commands::dispatch(&client, &cli.venue, &output, cli.command).await;
    Ok(())
}
