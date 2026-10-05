//! Strategy registry (single owner for construction dispatch).
//!
//! Extracted from `strategies::mod`: adding a strategy means appending one
//! entry here — no string dispatch elsewhere (open/closed).
//!
//! Uses `OnceLock` for lazy initialization, allowing strategies to be
//! registered from other crates or modules without modifying this file.

use std::sync::{Arc, OnceLock};

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

static REGISTRY: OnceLock<Vec<StrategyDescriptor>> = OnceLock::new();

/// All known strategies. Order is irrelevant; lookup is by `name`.
///
/// Uses `OnceLock` for lazy initialization. The registry is initialized once
/// on first access and reused thereafter.
pub fn registry() -> &'static [StrategyDescriptor] {
    REGISTRY.get_or_init(|| {
        vec![
            StrategyDescriptor {
                name: "simple_grid",
                factory: |ctx| {
                    let cfg = simple_grid::SimpleGridConfig::from_config(&ctx.config)?;
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
                    Ok(Box::new(ema_cross::EmaCross::new(
                        cfg,
                        ctx.gateway.clone(),
                        ctx.market.clone(),
                    )))
                },
            },
            StrategyDescriptor {
                name: "supertrend",
                factory: |ctx| {
                    let cfg = supertrend::SupertrendConfig::from_params(
                        ctx.config.strategy.params.table(),
                    )?;
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
                    Ok(Box::new(boll_grid::BollGrid::new(
                        cfg,
                        ctx.gateway.clone(),
                        ctx.market.clone(),
                    )))
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
                    let cfg = fixed_maker::FixedMakerConfig::from_params(
                        ctx.config.strategy.params.table(),
                    )?;
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
                    Ok(Box::new(sentinel::Sentinel::new(
                        cfg,
                        ctx.gateway.clone(),
                        ctx.market.clone(),
                    )))
                },
            },
            StrategyDescriptor {
                name: "autoborrow",
                factory: |ctx| {
                    let cfg = autoborrow::AutoborrowConfig::from_params(
                        ctx.config.strategy.params.table(),
                    )?;
                    Ok(Box::new(autoborrow::Autoborrow::new(cfg, ctx.ops.clone())))
                },
            },
            StrategyDescriptor {
                name: "convert",
                factory: |ctx| {
                    let cfg =
                        convert::ConvertConfig::from_params(ctx.config.strategy.params.table())?;
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
                    let cfg = rebalance::RebalanceConfig::from_params(
                        ctx.config.strategy.params.table(),
                    )?;
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
                    let cfg = market_cap::MarketCapConfig::from_params(
                        ctx.config.strategy.params.table(),
                    )?;
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
    })
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

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use buffa::EnumValue;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{adapters::MockAdapter, proto::trading, strategies::Strategy};

    /// Every parameter any registered strategy declares, in one table. Deserializers
    /// ignore keys they do not model, so one superset builds all of them and a
    /// missing key is the only way a factory can fail.
    const ALL_PARAMS: &str = "\
symbol = \"BTC/USDT\"
exchange_id = \"mock\"
lower_price = \"90\"
upper_price = \"110\"
num_levels = 4
qty_per_level = \"0.1\"
primary = { exchange_id = \"mock\", label = \"\" }
hedge = { exchange_id = \"mock\", label = \"\" }
asset = \"USDT\"
threshold = \"1\"
min_balance = \"100\"
repay_above = \"1000\"
from_asset = \"USDT\"
to_asset = \"USDC\"
min_amount = \"10\"
dest_label = \"futures\"
inventory_cap = \"1\"
alert_threshold = \"0.01\"
targets = { BTC = \"1\" }
weights = { BTC = \"0.6\" }
signal_file = \"/tmp/longtrader-registry-irr-signal\"
";

    /// Only `simple_grid` quotes this table immediately: every other registered
    /// strategy either needs a key the table lacks or waits for a signal before
    /// trading. Combined with the `grid-` client-order namespace — which only
    /// `simple_grid` emits — this turns "the default was applied" into an
    /// assertion rather than a restatement of the source.
    const GRID_ONLY_PARAMS: &str = "\
symbol = \"BTC/USDT\"
lower_price = \"90\"
upper_price = \"110\"
num_levels = 4
qty_per_level = \"0.1\"
";

    /// A `Config` with `strategy.type = kind` carrying `params`.
    fn config_of(kind: &str, params: &str) -> Config {
        let doc = format!(
            "daemon_endpoint = \"http://127.0.0.1:1\"\n\
             [strategy]\ntype = \"{kind}\"\n\
             [strategy.params]\n{params}"
        );
        toml::from_str(&doc).expect("the fixture config must parse")
    }

    /// A `StrategyContext` over a caller-supplied backend, so a test can observe
    /// what the built strategy did to the venue.
    fn context_over(adapter: Arc<MockAdapter>, config: Config) -> StrategyContext {
        StrategyContext {
            config,
            gateway: Arc::clone(&adapter) as Arc<dyn TradingGateway>,
            market: Arc::clone(&adapter) as Arc<dyn MarketDataSource>,
            funding: Arc::clone(&adapter) as Arc<dyn FundingRateSource>,
            ops: Arc::clone(&adapter) as Arc<dyn VenueOpInvoker>,
            wallet: Arc::clone(&adapter) as Arc<dyn WalletGateway>,
        }
    }

    /// `kind`'s context, with every one of the five ports served by one mock.
    fn context(kind: &str) -> StrategyContext {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        context_over(adapter, config_of(kind, ALL_PARAMS))
    }

    /// `kind`'s context plus a handle to the mock behind it.
    fn context_with_adapter(kind: &str) -> (Arc<MockAdapter>, StrategyContext) {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let ctx = context_over(Arc::clone(&adapter), config_of(kind, ALL_PARAMS));
        (adapter, ctx)
    }

    /// The failure text for `kind`, asserted to exist and to name the kind.
    fn build_error(kind: &str) {
        let outcome = build_strategy(&context(kind)).err();
        let rendered = match outcome {
            Some(err) => format!("{err:#}"),
            None => String::new(),
        };
        assert!(!rendered.is_empty(), "{kind} must not resolve to a strategy");
        assert!(
            rendered.contains(kind),
            "the error must name the kind it could not build: {rendered}"
        );
    }

    /// Every registered name must build. A factory that cannot parse the
    /// documented parameters is dead weight the operator only discovers at
    /// startup, which is the worst possible moment.
    #[test]
    fn every_registered_strategy_builds() {
        assert!(!registry().is_empty(), "the registry must not be empty");
        for descriptor in registry() {
            let outcome = build_strategy(&context(descriptor.name)).err();
            assert!(outcome.is_none(), "{} must build, got {outcome:?}", descriptor.name);
        }
    }

    /// An omitted `strategy.type` must resolve to the documented default rather
    /// than failing: a config file that omits the key is the simplest thing a user
    /// can write, and it should not be a startup failure.
    #[tokio::test]
    async fn an_omitted_strategy_type_defaults_to_simple_grid() {
        let adapter = Arc::new(MockAdapter::new(dec!(100)));
        let ctx = context_over(Arc::clone(&adapter), config_of("", GRID_ONLY_PARAMS));
        let strategy =
            build_strategy(&ctx).expect("an omitted strategy.type must not fail startup");

        let orders = orders_after_first_tick(strategy, &adapter).await;
        assert!(!orders.is_empty(), "the default strategy must start quoting");
        assert!(
            orders.iter().all(|o| o.client_order_id.starts_with("grid-")),
            "the default must be the grid, not another registered name: {:?}",
            orders.iter().map(|o| &o.client_order_id).collect::<Vec<_>>()
        );
        assert_eq!(orders[0].r#type, EnumValue::Known(trading::OrderType::Limit));
    }

    /// An unknown kind is the operator's typo, and the error has to say which word
    /// was wrong — otherwise the only clue is which strategies do exist.
    #[test]
    fn an_unknown_strategy_type_is_refused_and_names_the_kind() {
        build_error("not-a-strategy");
    }

    /// A duplicate name is unreachable through the `find` lookup, so one of the two
    /// entries is silently dead and which one wins is decided by array order.
    #[test]
    fn the_registry_has_no_duplicate_names() {
        let mut seen = HashSet::new();
        for descriptor in registry() {
            assert!(seen.insert(descriptor.name), "duplicate registry name: {}", descriptor.name);
        }
        assert_eq!(seen.len(), registry().len(), "every entry must have a distinct name");
        assert!(
            seen.contains("dca") && seen.contains("dca_scheduler"),
            "both DCA spellings must stay registered"
        );
    }

    /// Drive a built strategy's `run()` until the mock has an open order, then stop
    /// it. The DCA cadence is configured far in the future, so the initial tick is
    /// the only one that fires.
    async fn orders_after_first_tick(
        strategy: Box<dyn Strategy>,
        adapter: &Arc<MockAdapter>,
    ) -> Vec<trading::Order> {
        let task = tokio::spawn(async move { strategy.run().await });
        let mut placed = Vec::new();
        for _ in 0..100 {
            let open = adapter
                .fetch_open_orders(trading::FetchOpenOrdersRequest::default())
                .await
                .expect("the mock must answer an open-orders read");
            if !open.is_empty() {
                placed = open;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        task.abort();
        placed
    }

    /// `dca` is an alias for `dca_scheduler`. The two factories are separate
    /// entries, so only behaviour can prove they are the same strategy — and
    /// behaviour is what a divergence would actually cost a client: two DCA
    /// schedules quoting the same account.
    #[tokio::test]
    async fn the_dca_alias_behaves_identically_to_dca_scheduler() {
        let (adapter_alias, ctx_alias) = context_with_adapter("dca");
        let (adapter_canonical, ctx_canonical) = context_with_adapter("dca_scheduler");
        let alias = build_strategy(&ctx_alias).expect("the dca alias must build");
        let canonical = build_strategy(&ctx_canonical).expect("dca_scheduler must build");

        let from_alias = orders_after_first_tick(alias, &adapter_alias).await;
        let from_canonical = orders_after_first_tick(canonical, &adapter_canonical).await;
        assert_eq!(from_alias.len(), 1, "the alias must place exactly one order");
        assert_eq!(from_canonical.len(), 1, "the canonical name must place exactly one order");

        for orders in [&from_alias, &from_canonical] {
            assert!(
                orders[0].client_order_id.starts_with("dca-"),
                "both spellings must use the DCA order namespace: {}",
                orders[0].client_order_id
            );
            assert_eq!(orders[0].side, EnumValue::Known(trading::OrderSide::Buy));
            assert_eq!(orders[0].r#type, EnumValue::Known(trading::OrderType::Market));
            assert_eq!(orders[0].symbol, "BTC/USDT");
        }
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(
                from_alias[0].amount.as_option().expect("amount")
            )
            .expect("decodes"),
            longtrader_contract::ext::common_to_decimal(
                from_canonical[0].amount.as_option().expect("amount")
            )
            .expect("decodes"),
            "the alias must size the same order as the canonical name"
        );
    }
}
