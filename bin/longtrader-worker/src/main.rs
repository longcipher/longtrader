#![allow(missing_docs, missing_debug_implementations)]

use std::sync::Arc;

use clap::Parser;
use color_eyre::{Result, eyre::bail};
use longtrader_worker::{
    adapters::{MockAdapter, RemoteAdapter},
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
/// `endpoint = "http://127.0.0.1:8080"` explicitly for that layout.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8810";

/// How long in-flight control-plane RPCs get to finish after the strategy
/// stops (or Ctrl-C arrives) before the server task is aborted. Bounded so a
/// wedged venue cancel cannot keep the process alive forever.
const CONTROL_PLANE_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

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
    // `Config::load` already whitelisted the backend; re-validate here so a
    // programmatically constructed `Config` gets the same fail-fast.
    let backend = config.validated_backend()?.to_string();
    tracing::info!(
        backend = %backend,
        strategy = %config.strategy.strategy_type,
        "starting worker"
    );

    // Fail-fast on badly mounted secrets: `Config::api_token` refuses a
    // configured-but-unreadable/empty token file rather than degrading to an
    // unauthenticated worker. Read once and reuse (see `start_with_ports`):
    // two reads could observe two files.
    let token = config.api_token()?;

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
                token.as_str(),
            )
            .await
        }
        // The remote backend speaks the canonical `longtrader.{market,trading}.v1`
        // services over Connect, whether they are served by an embedded
        // `longtrader-terminal` or a standalone `longtrader-api`.
        "api" => {
            let endpoint = config.endpoint_or_default(DEFAULT_ENDPOINT);
            let adapter = Arc::new(RemoteAdapter::new(&endpoint, token.as_str()));
            tracing::info!(endpoint = %endpoint, "connected to backend");
            start_with(adapter, &config, token.as_str()).await
        }
        // Unreachable via `Config::load` (whitelisted), kept as defense for
        // programmatically constructed configs.
        other => bail!("unknown backend: {other} (expected 'mock' or 'api')"),
    }
}

/// Start the optional control plane and then run the strategy loop.
async fn start_with(adapter: Arc<RemoteAdapter>, config: &Config, token: &str) -> Result<()> {
    let gateway: Arc<dyn TradingGateway> = adapter.clone();
    let market: Arc<dyn MarketDataSource> = adapter.clone();
    let funding: Arc<dyn FundingRateSource> = adapter.clone();
    let triggers: Arc<dyn TriggerOrderGateway> = adapter.clone();
    let ops: Arc<dyn VenueOpInvoker> = adapter.clone();
    let wallet: Arc<dyn WalletGateway> = adapter.clone();
    start_with_ports(gateway, market, funding, triggers, ops, wallet, config, token).await
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
    token: &str,
) -> Result<()> {
    let ctx = StrategyContext {
        config: config.clone(),
        gateway: gateway.clone(),
        market: market.clone(),
        funding: funding.clone(),
        triggers: triggers.clone(),
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
        // `token` was read once in `main`: re-reading the file here could
        // observe a rotated secret (TOCTOU) and leave the adapter and the
        // control plane holding different credentials.
        let token = token.to_string();
        let exchange_id = longtrader_worker::ports::exchange_id(
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
        // Graceful shutdown: the strategy finishing (or Ctrl-C) signals the
        // server, in-flight RPCs get `CONTROL_PLANE_SHUTDOWN_GRACE` to finish,
        // and only then is the task aborted. `abort()` alone would drop live
        // kill-switch cancels mid-round-trip.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let mut server_handle = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            if let Err(err) = longtrader_worker::session::server::run(bind, manager, shutdown).await
            {
                tracing::error!(error = %err, "control plane server exited");
            }
        });

        // Control plane is scoped to this run: stop it on every exit path, not
        // just the happy one, so a failed `stop()` hook cannot leak it.
        let outcome = tokio::select! {
            result = drive_strategy(strategy.as_ref()) => result,
            signal = tokio::signal::ctrl_c() => {
                match signal {
                    Ok(()) => tracing::info!("shutdown signal received; stopping strategy"),
                    Err(err) => tracing::warn!(error = %err, "shutdown signal handler failed"),
                }
                strategy.stop().await
            }
        };
        let _ = shutdown_tx.send(());
        if tokio::time::timeout(CONTROL_PLANE_SHUTDOWN_GRACE, &mut server_handle).await.is_err() {
            server_handle.abort();
        }
        outcome
    } else {
        drive_strategy(strategy.as_ref()).await
    }
}

/// Run one strategy through its lifecycle: `start()`, then `run()` to
/// completion, then `stop()` exactly once. A `stop()` failure takes precedence
/// over the `run()` outcome, matching the inline order it replaces.
async fn drive_strategy(strategy: &dyn strategies::Strategy) -> Result<()> {
    // Call start() lifecycle hook before running the strategy.
    strategy.start().await?;
    // Run the strategy and ensure stop() is called when it exits.
    let result = strategy.run().await;
    // Call stop() lifecycle hook after the strategy exits.
    strategy.stop().await?;
    result
}
