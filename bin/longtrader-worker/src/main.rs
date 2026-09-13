#![allow(missing_docs, missing_debug_implementations)]

use std::sync::Arc;

use clap::Parser;
use color_eyre::{Result, eyre::bail};
use longtrader_worker::{
    adapters::RemoteAdapter,
    config::Config,
    ports::{FundingRateSource, MarketDataSource, TradingGateway, VenueOpInvoker, WalletGateway},
    session::SessionManager,
    strategies::{self, StrategyContext},
};

/// Default control-plane / terminal API endpoint when none is configured.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7888";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
#[hotpath::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    let backend = config.resolved_backend();
    tracing::info!(
        backend = %backend,
        strategy = %config.strategy.strategy_type,
        "starting worker"
    );

    // A single `RemoteAdapter` implements every strategy-facing port, so the
    // worker keeps exactly one backend connection per session (no per-port
    // adapter sprawl, no split session state).
    let endpoint = config.api_endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    let adapter = Arc::new(RemoteAdapter::new(&endpoint, &config.api_token()));
    tracing::info!(endpoint = %endpoint, "connected to terminal API");

    match backend.as_str() {
        "api" | "terminal" => start_with(adapter, &config).await,
        // Historically documented but never implemented; map to the unified
        // endpoint with a warning rather than crashing on a valid-looking config.
        "daemon" => {
            tracing::warn!("backend 'daemon' is not implemented; using the unified 'api' endpoint");
            start_with(adapter, &config).await
        }
        other => bail!("unknown backend: {other} (expected 'api' or 'terminal')"),
    }
}

/// Start the optional control plane and then run the strategy loop.
async fn start_with(adapter: Arc<RemoteAdapter>, config: &Config) -> Result<()> {
    let gateway: Arc<dyn TradingGateway> = adapter.clone();
    let market: Arc<dyn MarketDataSource> = adapter.clone();
    let funding: Arc<dyn FundingRateSource> = adapter.clone();
    let ops: Arc<dyn VenueOpInvoker> = adapter.clone();
    let wallet: Arc<dyn WalletGateway> = adapter.clone();

    let ctx = StrategyContext {
        config: config.clone(),
        gateway: gateway.clone(),
        market: market.clone(),
        funding,
        ops,
        wallet,
    };
    let strategy = strategies::build_strategy(&ctx)?;

    if let Some(bind) = &config.listen_endpoint {
        let token = config.api_token();
        let exchange_id = longtrader_worker::adapters::exchange_id(
            config.strategy.params.venue(),
            config.strategy.params.venue_label(),
        );
        let manager = SessionManager::new(
            if token.is_empty() { None } else { Some(token) },
            exchange_id,
            gateway,
            market,
        );
        let manager = Arc::new(manager);
        manager.install_self();
        let bind = bind.clone();
        tokio::spawn(async move {
            if let Err(err) = longtrader_worker::session::server::run(bind, manager).await {
                tracing::error!(error = %err, "control plane server exited");
            }
        });
    }

    strategy.run().await
}
