#![allow(missing_docs, missing_debug_implementations)]

use std::sync::Arc;

use clap::Parser;
use color_eyre::{Result, eyre::bail};
use longtrader_worker::{
    adapters::{MockAdapter, RemoteAdapter, TerminalAdapter},
    config::Config,
    ports::{
        FundingRateSource, MarketDataSource, TradingGateway, TriggerOrderGateway, VenueOpInvoker,
        WalletGateway,
    },
    session::{Capabilities, SessionManager},
    strategies::{self, StrategyContext},
};

/// Default terminal API endpoint (embedded `longtrader-terminal`, see H04).
/// Standalone `longtrader-api serve` listens on `0.0.0.0:8080` — set
/// `api_endpoint = "http://127.0.0.1:8080"` explicitly for that layout.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8810";

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

    // Fail-fast on badly mounted secrets: a configured token file that yields
    // an empty token would otherwise run unauthenticated.
    if config.api_token_file.is_some() && config.api_token().is_empty() {
        bail!(
            "api_token_file {:?} unreadable or empty; refusing to start unauthenticated",
            config.api_token_file
        );
    }

    match backend.as_str() {
        "mock" => {
            // Offline dry-run: deterministic in-process venue, no network.
            let mock = Arc::new(MockAdapter::new(rust_decimal::Decimal::from(95_000)));
            tracing::info!("using MockAdapter (offline dry-run)");
            start_with_ports(
                mock.clone(),
                mock.clone(),
                mock.clone(),
                mock.clone(),
                mock.clone(),
                mock.clone(),
                &config,
            )
            .await
        }
        "terminal" => {
            // Terminal backends serve `longtrader.terminal.v1` (venue-string
            // surface: tradingcharts-server, longtrader-api terminal router).
            let endpoint =
                config.api_endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
            let adapter = Arc::new(TerminalAdapter::new(&endpoint, &config.api_token()));
            tracing::info!(endpoint = %endpoint, backend = "terminal", "connected to terminal API");
            start_with_terminal(adapter, &config).await
        }
        "api" => {
            // Unified backends serve `longtrader.{trading,market}.v1` natively.
            let endpoint =
                config.api_endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
            let adapter = Arc::new(RemoteAdapter::new(&endpoint, &config.api_token()));
            tracing::info!(endpoint = %endpoint, backend = "api", "connected to unified API");
            start_with(adapter, &config).await
        }
        // Historically documented but never implemented; map to the unified
        // endpoint with a warning rather than crashing on a valid-looking config.
        "daemon" => {
            tracing::warn!("backend 'daemon' is not implemented; using the unified 'api' endpoint");
            let endpoint =
                config.api_endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
            let adapter = Arc::new(RemoteAdapter::new(&endpoint, &config.api_token()));
            start_with(adapter, &config).await
        }
        other => bail!("unknown backend: {other} (expected 'mock', 'api' or 'terminal')"),
    }
}

/// Start the optional control plane and then run the strategy loop.
async fn start_with(adapter: Arc<RemoteAdapter>, config: &Config) -> Result<()> {
    let gateway: Arc<dyn TradingGateway> = adapter.clone();
    let market: Arc<dyn MarketDataSource> = adapter.clone();
    let funding: Arc<dyn FundingRateSource> = adapter.clone();
    let triggers: Arc<dyn TriggerOrderGateway> = adapter.clone();
    let ops: Arc<dyn VenueOpInvoker> = adapter.clone();
    let wallet: Arc<dyn WalletGateway> = adapter.clone();
    start_with_ports(gateway, market, funding, triggers, ops, wallet, config).await
}

/// Terminal variant of [`start_with`] (same control plane, terminal ports).
async fn start_with_terminal(adapter: Arc<TerminalAdapter>, config: &Config) -> Result<()> {
    let gateway: Arc<dyn TradingGateway> = adapter.clone();
    let market: Arc<dyn MarketDataSource> = adapter.clone();
    let funding: Arc<dyn FundingRateSource> = adapter.clone();
    let triggers: Arc<dyn TriggerOrderGateway> = adapter.clone();
    let ops: Arc<dyn VenueOpInvoker> = adapter.clone();
    let wallet: Arc<dyn WalletGateway> = adapter.clone();
    start_with_ports(gateway, market, funding, triggers, ops, wallet, config).await
}

/// Report the capabilities the selected backend will actually serve.
///
/// A strategy needing a capability the backend cannot serve is worth flagging
/// at startup rather than at the first tick. Every in-tree backend implements
/// all four ports, so this is normally informational; it stays as a guard for a
/// future backend that does not.
fn report_capabilities(strategy: &str, caps: &Capabilities) {
    const NEEDS: &[(&str, &str)] = &[
        ("xfunding_lite", "funding rates"),
        ("premium_monitor", "funding rates"),
        ("autoborrow", "venue ops"),
        ("convert", "venue ops"),
        ("deposit_transfer", "wallet"),
        ("balance_align", "wallet"),
    ];
    for (name, capability) in NEEDS {
        if *name != strategy {
            continue;
        }
        let available = match *capability {
            "funding rates" => caps.funding.is_some(),
            "venue ops" => caps.ops.is_some(),
            "wallet" => caps.wallet.is_some(),
            other => {
                tracing::error!(
                    capability = other,
                    "unknown capability name in report_capabilities"
                );
                false
            }
        };
        if available {
            tracing::info!(strategy = %strategy, capability = %capability, "capability available");
        } else {
            tracing::warn!(
                strategy = %strategy,
                capability = %capability,
                "backend does not serve this capability; affected RPCs will answer unimplemented"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_with_ports(
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
    funding: Arc<dyn FundingRateSource>,
    triggers: Arc<dyn TriggerOrderGateway>,
    ops: Arc<dyn VenueOpInvoker>,
    wallet: Arc<dyn WalletGateway>,
    config: &Config,
) -> Result<()> {
    let ctx = StrategyContext {
        config: config.clone(),
        gateway: gateway.clone(),
        market: market.clone(),
        funding: funding.clone(),
        ops: ops.clone(),
        wallet: wallet.clone(),
    };
    let strategy = strategies::build_strategy(&ctx)?;

    // Reported before the control plane branch: a capability problem should be
    // visible at startup whether or not a remote control plane is configured.
    let capabilities = Capabilities {
        funding: Some(funding.clone()),
        triggers: Some(triggers.clone()),
        wallet: Some(wallet.clone()),
        ops: Some(ops.clone()),
    };
    report_capabilities(&config.strategy.strategy_type, &capabilities);

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
        )
        .with_capabilities(capabilities);
        let manager = Arc::new(manager);
        manager.install_self().await;
        let bind = bind.clone();
        tokio::spawn(async move {
            if let Err(err) = longtrader_worker::session::server::run(bind, manager).await {
                tracing::error!(error = %err, "control plane server exited");
            }
        });
    }

    // Call start() lifecycle hook before running the strategy.
    strategy.start().await?;
    // Run the strategy and ensure stop() is called when it exits.
    let result = strategy.run().await;
    // Call stop() lifecycle hook after the strategy exits.
    strategy.stop().await?;
    result
}
