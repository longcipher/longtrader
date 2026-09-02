#![allow(missing_docs, missing_debug_implementations)]

use std::sync::Arc;

use clap::Parser;
use color_eyre::{Result, eyre::bail};
use longtrader_worker::{
    adapters::RemoteAdapter,
    config::Config,
    ports::{MarketDataSource, TradingGateway},
    session::SessionManager,
    strategies::{
        Strategy,
        autoborrow::{Autoborrow, AutoborrowConfig},
        balance_align::BalanceAlign,
        boll_grid::{BollGrid, BollGridConfig},
        convert::{Convert, ConvertConfig},
        cross_depth_maker::CrossDepthMaker,
        cross_fixed_maker::CrossFixedMaker,
        cross_maker::CrossMaker,
        dca_scheduler::{DcaScheduler, DcaSchedulerConfig},
        deposit_transfer::{DepositTransfer, DepositTransferConfig},
        ema_cross::{EmaCross, EmaCrossConfig},
        fixed_maker::{FixedMaker, FixedMakerConfig},
        hedge_grid::HedgeGrid,
        irr::{Irr, IrrConfig},
        market_cap::{MarketCap, MarketCapConfig},
        random_entry::{RandomEntry, RandomEntryConfig},
        rebalance::{Rebalance, RebalanceConfig},
        sentinel::{Sentinel, SentinelConfig},
        simple_grid::{SimpleGrid, SimpleGridConfig},
        supertrend::{Supertrend, SupertrendConfig},
        xfunding_lite::{XfundingLite, XfundingLiteConfig},
    },
};

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

    match backend.as_str() {
        "api" | "terminal" => {
            let endpoint =
                config.api_endpoint.clone().unwrap_or_else(|| "http://127.0.0.1:7888".to_string());
            let adapter = Arc::new(RemoteAdapter::new(&endpoint, &config.api_token()));
            tracing::info!(endpoint = %endpoint, "connected to terminal API");
            start_with(adapter, &config).await
        }
        other => bail!("unknown backend: {other}"),
    }
}

/// Builds a fresh remote adapter for extended-port strategies.
fn adapter_for(config: &Config) -> Result<Arc<RemoteAdapter>> {
    let endpoint =
        config.api_endpoint.clone().unwrap_or_else(|| "http://127.0.0.1:7888".to_string());
    Ok(Arc::new(RemoteAdapter::new(&endpoint, &config.api_token())))
}

/// Builds the configured strategy over the shared adapter.
fn build_strategy(
    config: &Config,
    gateway: Arc<dyn TradingGateway>,
    market: Arc<dyn MarketDataSource>,
) -> Result<Box<dyn Strategy>> {
    let kind = config.strategy.strategy_type.as_str();
    let params = config.strategy.params.table();
    // Backwards compatibility: an empty type selects the original grid.
    let kind = if kind.is_empty() { "simple_grid" } else { kind };
    match kind {
        "simple_grid" => {
            let grid_config: SimpleGridConfig = config.grid_config()?;
            Ok(Box::new(SimpleGrid::new(grid_config, gateway, market)))
        }
        "ema_cross" => {
            let cfg = EmaCrossConfig::from_params(params)?;
            Ok(Box::new(EmaCross::new(cfg, gateway, market)))
        }
        "supertrend" => {
            let cfg = SupertrendConfig::from_params(params)?;
            Ok(Box::new(Supertrend::new(cfg, gateway, market)))
        }
        "boll_grid" => {
            let cfg = BollGridConfig::from_params(params)?;
            Ok(Box::new(BollGrid::new(cfg, gateway, market)))
        }
        "dca_scheduler" | "dca" => {
            let cfg = DcaSchedulerConfig::from_params(params)?;
            Ok(Box::new(DcaScheduler::new(cfg, gateway)))
        }
        "fixed_maker" => {
            let cfg = FixedMakerConfig::from_params(params)?;
            Ok(Box::new(FixedMaker::new(cfg, gateway, market)))
        }
        "random_entry" => {
            let cfg = RandomEntryConfig::from_params(params)?;
            Ok(Box::new(RandomEntry::new(cfg, gateway)))
        }
        "xfunding_lite" => {
            let cfg = XfundingLiteConfig::from_params(params)?;
            let funding: Arc<dyn longtrader_worker::ports::FundingRateSource> =
                adapter_for(config)?;
            Ok(Box::new(XfundingLite::new(cfg, gateway, funding)))
        }
        "sentinel" => {
            let cfg = SentinelConfig::from_params(params)?;
            Ok(Box::new(Sentinel::new(cfg, gateway, market)))
        }
        "autoborrow" => {
            let cfg = AutoborrowConfig::from_params(params)?;
            let ops: Arc<dyn longtrader_worker::ports::VenueOpInvoker> = adapter_for(config)?;
            Ok(Box::new(Autoborrow::new(cfg, gateway, ops)))
        }
        "convert" => {
            let cfg = ConvertConfig::from_params(params)?;
            let ops: Arc<dyn longtrader_worker::ports::VenueOpInvoker> = adapter_for(config)?;
            Ok(Box::new(Convert::new(cfg, ops)))
        }
        "deposit_transfer" => {
            let cfg = DepositTransferConfig::from_params(params)?;
            let wallet: Arc<dyn longtrader_worker::ports::WalletGateway> = adapter_for(config)?;
            Ok(Box::new(DepositTransfer::new(cfg, wallet)))
        }
        "cross_fixed_maker" => {
            let cfg = longtrader_worker::strategies::params_from_table(params)?;
            Ok(Box::new(CrossFixedMaker::new(cfg, gateway, market)))
        }
        "cross_depth_maker" => {
            let cfg = longtrader_worker::strategies::params_from_table(params)?;
            Ok(Box::new(CrossDepthMaker::new(cfg, gateway, market)))
        }
        "cross_maker" => {
            let cfg = longtrader_worker::strategies::params_from_table(params)?;
            Ok(Box::new(CrossMaker::new(cfg, gateway, market)))
        }
        "hedge_grid" => {
            let cfg = longtrader_worker::strategies::params_from_table(params)?;
            Ok(Box::new(HedgeGrid::new(cfg, gateway, market)))
        }
        "balance_align" => {
            let cfg = longtrader_worker::strategies::params_from_table(params)?;
            let wallet: Arc<dyn longtrader_worker::ports::WalletGateway> = adapter_for(config)?;
            Ok(Box::new(BalanceAlign::new(cfg, gateway, wallet)))
        }
        "premium_monitor" => {
            let cfg: longtrader_worker::strategies::premium_monitor::PremiumMonitorConfig =
                longtrader_worker::strategies::params_from_table(params)?;
            Ok(Box::new(longtrader_worker::strategies::premium_monitor::PremiumMonitor::new(
                cfg, market,
            )))
        }
        "nav_recorder" => {
            let cfg = longtrader_worker::strategies::nav_recorder::NavRecorderConfig::from_params(
                params,
            )?;
            Ok(Box::new(longtrader_worker::strategies::nav_recorder::NavRecorder::new(
                cfg, gateway, market,
            )))
        }
        "rebalance" => {
            let cfg = RebalanceConfig::from_params(params)?;
            Ok(Box::new(Rebalance::new(cfg, gateway, market)))
        }
        "market_cap" => {
            let cfg = MarketCapConfig::from_params(params)?;
            Ok(Box::new(MarketCap::new(cfg, gateway, market)))
        }
        "irr" => {
            let cfg = IrrConfig::from_params(params)?;
            Ok(Box::new(Irr::new(cfg, gateway)))
        }
        other => bail!("unknown strategy type: {other}"),
    }
}

/// Start the optional control plane and then run the strategy loop.
async fn start_with<A>(adapter: Arc<A>, config: &Config) -> Result<()>
where
    A: TradingGateway + MarketDataSource + 'static,
{
    let gateway: Arc<dyn TradingGateway> = adapter.clone();
    let market: Arc<dyn MarketDataSource> = adapter;
    let strategy = build_strategy(config, gateway.clone(), market.clone())?;

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
