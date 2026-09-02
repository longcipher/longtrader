//! Axum wiring for the worker control plane: Connect services mounted as the
//! fallback service, mirroring `bin/longtrader-api` house style.
//!
//! Mounted services: `WorkerSessionService` (AttachSession/KeepAlive/ReconcileState/
//! SetKillSwitchPolicy/RegisterStrategy/StrategyStatus/StopStrategy/ReportLog/
//! StreamStrategyEvents) plus proxied `TradingService` / `MarketDataService`.
//!
//! State machine & watchdog context: session lifecycle ATTACHED → SYNCING →
//! ACTIVE → KILL_SWITCH_TRIPPED is enforced in `SessionManager`; this router
//! simply exposes it. ReconcileState returns an atomic snapshot stamped with
//! `snapshot_sequence`. Lease is 3x heartbeat (`LEASE_HEARTBEAT_BUDGET`) and
//! `KillSwitchPolicy` Scope routes SESSION_ORDERS / ALL_ORDERS / NONE.

use std::sync::Arc;

use longtrader_contract::proto::longtrader::{
    market::v1::MarketDataServiceExt, trading::v1::TradingServiceExt,
    worker::v1::WorkerSessionServiceExt,
};

use super::{SessionManager, proxy, service};

/// Register all control-plane services onto one Connect router.
pub fn build_router(manager: Arc<SessionManager>) -> connectrpc::Router {
    let session_svc = Arc::new(service::WorkerSessionServiceImpl { manager: Arc::clone(&manager) });
    let trading_svc = Arc::new(proxy::TradingProxy {
        gateway: manager.gateway(),
        default_exchange: manager.default_exchange().clone(),
    });
    let market_svc = Arc::new(proxy::MarketDataProxy { market: manager.market() });

    let router = session_svc.register(connectrpc::Router::new());
    let router = trading_svc.register(router);
    market_svc.register(router)
}

/// Bind and serve until the process exits.
pub async fn run(bind: String, manager: Arc<SessionManager>) -> color_eyre::Result<()> {
    let router = build_router(manager);
    let app = axum::Router::new().fallback_service(router.into_axum_service());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "worker control plane listening");
    axum::serve(listener, app).await?;
    Ok(())
}
