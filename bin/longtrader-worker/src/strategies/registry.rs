//! Strategy registry (single owner for construction dispatch).
//!
//! Extracted from `strategies::mod`: adding a strategy means appending one
//! entry here — no string dispatch elsewhere (open/closed).

use std::sync::Arc;

use color_eyre::Result;

use super::{
    Strategy, autoborrow, balance_align, boll_grid, convert, cross_depth_maker, cross_fixed_maker,
    cross_maker, dca_scheduler, deposit_transfer, ema_cross, fixed_maker, hedge_grid, irr,
    market_cap, nav_recorder, params::params_from_table, premium_monitor, random_entry, rebalance,
    sentinel, simple_grid, supertrend, xfunding_lite,
};
use crate::{
    config::Config,
    ports::{FundingRateSource, MarketDataSource, TradingGateway, VenueOpInvoker, WalletGateway},
};

/// Ports a strategy may consume. All are sourced from one backend adapter,
/// keeping a single backend connection per session.
pub struct StrategyContext {
    pub config: Config,
    pub gateway: Arc<dyn TradingGateway>,
    pub market: Arc<dyn MarketDataSource>,
    pub funding: Arc<dyn FundingRateSource>,
    pub ops: Arc<dyn VenueOpInvoker>,
    pub wallet: Arc<dyn WalletGateway>,
}

/// Builds a strategy from its [`StrategyContext`]. Pure function of the config
/// plus ports; never touches global state.
pub type StrategyFactory = fn(&StrategyContext) -> Result<Box<dyn Strategy>>;

/// Self-describing registration record for one strategy.
pub struct StrategyDescriptor {
    pub name: &'static str,
    pub factory: StrategyFactory,
}

/// All known strategies. Order is irrelevant; lookup is by `name`.
pub fn registry() -> &'static [StrategyDescriptor] {
    &[
        StrategyDescriptor {
            name: "simple_grid",
            factory: |ctx| {
                let cfg = ctx.config.grid_config()?;
                Ok(Box::new(simple_grid::SimpleGrid::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "ema_cross",
            factory: |ctx| {
                let cfg =
                    ema_cross::EmaCrossConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(ema_cross::EmaCross::new(cfg, ctx.gateway.clone(), ctx.market.clone())))
            },
        },
        StrategyDescriptor {
            name: "supertrend",
            factory: |ctx| {
                let cfg =
                    supertrend::SupertrendConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(supertrend::Supertrend::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "boll_grid",
            factory: |ctx| {
                let cfg =
                    boll_grid::BollGridConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(boll_grid::BollGrid::new(cfg, ctx.gateway.clone(), ctx.market.clone())))
            },
        },
        StrategyDescriptor {
            name: "dca_scheduler",
            factory: |ctx| {
                let cfg = dca_scheduler::DcaSchedulerConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(dca_scheduler::DcaScheduler::new(cfg, ctx.gateway.clone())))
            },
        },
        StrategyDescriptor {
            name: "dca",
            factory: |ctx| {
                let cfg = dca_scheduler::DcaSchedulerConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(dca_scheduler::DcaScheduler::new(cfg, ctx.gateway.clone())))
            },
        },
        StrategyDescriptor {
            name: "fixed_maker",
            factory: |ctx| {
                let cfg =
                    fixed_maker::FixedMakerConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(fixed_maker::FixedMaker::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "random_entry",
            factory: |ctx| {
                let cfg = random_entry::RandomEntryConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(random_entry::RandomEntry::new(cfg, ctx.gateway.clone())))
            },
        },
        StrategyDescriptor {
            name: "xfunding_lite",
            factory: |ctx| {
                let cfg = xfunding_lite::XfundingLiteConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(xfunding_lite::XfundingLite::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.funding.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "sentinel",
            factory: |ctx| {
                let cfg =
                    sentinel::SentinelConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(sentinel::Sentinel::new(cfg, ctx.gateway.clone(), ctx.market.clone())))
            },
        },
        StrategyDescriptor {
            name: "autoborrow",
            factory: |ctx| {
                let cfg =
                    autoborrow::AutoborrowConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(autoborrow::Autoborrow::new(cfg, ctx.gateway.clone(), ctx.ops.clone())))
            },
        },
        StrategyDescriptor {
            name: "convert",
            factory: |ctx| {
                let cfg = convert::ConvertConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(convert::Convert::new(cfg, ctx.ops.clone())))
            },
        },
        StrategyDescriptor {
            name: "deposit_transfer",
            factory: |ctx| {
                let cfg = deposit_transfer::DepositTransferConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(deposit_transfer::DepositTransfer::new(cfg, ctx.wallet.clone())))
            },
        },
        StrategyDescriptor {
            name: "cross_fixed_maker",
            factory: |ctx| {
                let cfg = params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(cross_fixed_maker::CrossFixedMaker::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "cross_depth_maker",
            factory: |ctx| {
                let cfg = params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(cross_depth_maker::CrossDepthMaker::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "cross_maker",
            factory: |ctx| {
                let cfg = params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(cross_maker::CrossMaker::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "hedge_grid",
            factory: |ctx| {
                let cfg = params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(hedge_grid::HedgeGrid::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "balance_align",
            factory: |ctx| {
                let cfg = params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(balance_align::BalanceAlign::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.wallet.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "premium_monitor",
            factory: |ctx| {
                let cfg: premium_monitor::PremiumMonitorConfig =
                    params_from_table(ctx.config.strategy.params.table())?;
                Ok(Box::new(premium_monitor::PremiumMonitor::new(cfg, ctx.market.clone())))
            },
        },
        StrategyDescriptor {
            name: "nav_recorder",
            factory: |ctx| {
                let cfg = nav_recorder::NavRecorderConfig::from_params(
                    ctx.config.strategy.params.table(),
                )?;
                Ok(Box::new(nav_recorder::NavRecorder::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "rebalance",
            factory: |ctx| {
                let cfg =
                    rebalance::RebalanceConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(rebalance::Rebalance::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "market_cap",
            factory: |ctx| {
                let cfg =
                    market_cap::MarketCapConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(market_cap::MarketCap::new(
                    cfg,
                    ctx.gateway.clone(),
                    ctx.market.clone(),
                )))
            },
        },
        StrategyDescriptor {
            name: "irr",
            factory: |ctx| {
                let cfg = irr::IrrConfig::from_params(ctx.config.strategy.params.table())?;
                Ok(Box::new(irr::Irr::new(cfg, ctx.gateway.clone())))
            },
        },
    ]
}

/// Resolve and build the configured strategy by name.
///
/// # Errors
///
/// Returns an error when `strategy.type` is empty of unknown.
pub fn build_strategy(ctx: &StrategyContext) -> Result<Box<dyn Strategy>> {
    let kind = if ctx.config.strategy.strategy_type.is_empty() {
        "simple_grid"
    } else {
        ctx.config.strategy.strategy_type.as_str()
    };
    let descriptor = registry()
        .iter()
        .find(|d| d.name == kind)
        .ok_or_else(|| color_eyre::eyre::eyre!("unknown strategy type: {kind}"))?;
    (descriptor.factory)(ctx)
}
