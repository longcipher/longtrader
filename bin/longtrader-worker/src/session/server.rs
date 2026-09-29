//! Axum wiring for the worker control plane: Connect services mounted as the
//! fallback service, mirroring `bin/longtrader-api` house style.
//!
//! Mounted services: `WorkerSessionService` (AttachSession/KeepAlive/ReconcileState/
//! SetKillSwitchPolicy/RegisterStrategy/StrategyStatus/StopStrategy/ReportLog/
//! StreamStrategyEvents) plus proxied `TradingService` / `MarketDataService` /
//! `VenueOpService`.
//!
//! State machine & watchdog context: session lifecycle ATTACHED → SYNCING →
//! ACTIVE → KILL_SWITCH_TRIPPED is enforced in `SessionManager`; this router
//! simply exposes it. ReconcileState returns an atomic snapshot stamped with
//! `snapshot_sequence`. Lease is 3x heartbeat (`LEASE_HEARTBEAT_BUDGET`) and
//! `KillSwitchPolicy` Scope routes SESSION_ORDERS / ALL_ORDERS / NONE.

use std::sync::Arc;

use longtrader_contract::proto::longtrader::{
    market::v1::MarketDataServiceExt, ops::v1::VenueOpServiceExt, trading::v1::TradingServiceExt,
    worker::v1::WorkerSessionServiceExt,
};

use super::{SessionManager, proxy, service};

/// Register all control-plane services onto one Connect router.
pub fn build_router(manager: Arc<SessionManager>) -> connectrpc::Router {
    let caps = manager.capabilities();
    let session_svc = Arc::new(service::WorkerSessionServiceImpl { manager: Arc::clone(&manager) });
    let trading_svc = Arc::new(proxy::TradingProxy {
        gateway: manager.gateway(),
        default_exchange: manager.default_exchange().clone(),
        // The manager is threaded in so order-submitting RPCs are gated on the
        // session lifecycle and attribute orders for the kill-switch.
        manager: Some(Arc::clone(&manager)),
        // Optional per-capability ports: a backend that never declared one
        // answers `unimplemented` rather than a misleading empty success.
        triggers: caps.triggers.clone(),
        wallet: caps.wallet.clone(),
    });
    let market_svc = Arc::new(proxy::MarketDataProxy {
        market: manager.market(),
        default_exchange: manager.default_exchange().clone(),
        funding: caps.funding.clone(),
    });
    let ops_svc = Arc::new(proxy::VenueOpProxy {
        ops: caps.ops.clone(),
        default_exchange: manager.default_exchange().clone(),
    });

    let router = session_svc.register(connectrpc::Router::new());
    let router = trading_svc.register(router);
    let router = market_svc.register(router);
    // Disambiguate: the generated `Ext` traits each have a `register`, and a
    // blanket impl covers every service, so the call must name the ops one.
    VenueOpServiceExt::register(ops_svc, router)
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

#[cfg(test)]
mod tests {
    use connectrpc::client::ClientConfig;

    use super::*;
    use crate::{
        adapters::MockAdapter,
        ports::{MarketDataSource, TradingGateway},
        proto::{common, market, ops, trading, worker},
    };

    /// Serve `manager`'s router on an ephemeral port and return its address.
    async fn spawn(manager: Arc<SessionManager>) -> std::net::SocketAddr {
        let app = axum::Router::new().fallback_service(build_router(manager).into_axum_service());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    /// Boot the real router on an ephemeral port and return generated clients
    /// pointed at it. Exercises the whole wire path — routing, Connect
    /// encoding, the proxy gate, and session attribution — so the assertions
    /// below cover `build_router` too, not just the manager.
    async fn boot() -> (
        worker::WorkerSessionServiceClient<connectrpc::client::HttpClient>,
        trading::TradingServiceClient<connectrpc::client::HttpClient>,
    ) {
        let adapter = Arc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let manager = Arc::new(SessionManager::new(
            None,
            common::ExchangeId::default(),
            Arc::clone(&adapter) as Arc<dyn TradingGateway>,
            adapter as Arc<dyn MarketDataSource>,
        ));
        // The watchdog needs a weak self-reference; without it attach() would
        // skip lease enforcement, which is irrelevant for these RPC tests.
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        let config = ClientConfig::new(uri);
        (
            worker::WorkerSessionServiceClient::new(transport.clone(), config.clone()),
            trading::TradingServiceClient::new(transport, config),
        )
    }

    /// Boot the router with a backend that serves every venue capability, the
    /// way `main.rs` wires an in-tree adapter.
    async fn boot_with_capabilities() -> (
        Arc<MockAdapter>,
        trading::TradingServiceClient<connectrpc::client::HttpClient>,
        market::MarketDataServiceClient<connectrpc::client::HttpClient>,
        ops::VenueOpServiceClient<connectrpc::client::HttpClient>,
    ) {
        let adapter = Arc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let manager = Arc::new(
            SessionManager::new(
                None,
                common::ExchangeId::default(),
                Arc::clone(&adapter) as Arc<dyn TradingGateway>,
                Arc::clone(&adapter) as Arc<dyn MarketDataSource>,
            )
            .with_capabilities(crate::session::Capabilities::all(Arc::clone(&adapter))),
        );
        // The watchdog needs a weak self-reference; without it attach() would
        // skip lease enforcement, which is irrelevant for these RPC tests.
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        let config = ClientConfig::new(uri);
        (
            adapter,
            trading::TradingServiceClient::new(transport.clone(), config.clone()),
            market::MarketDataServiceClient::new(transport.clone(), config.clone()),
            ops::VenueOpServiceClient::new(transport, config),
        )
    }

    /// Boot the router with a backend that declares *no* capabilities, so the
    /// "unimplemented" path is covered end to end rather than only in a unit
    /// test.
    async fn boot_without_capabilities()
    -> trading::TradingServiceClient<connectrpc::client::HttpClient> {
        let adapter = Arc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let manager = Arc::new(SessionManager::new(
            None,
            common::ExchangeId::default(),
            Arc::clone(&adapter) as Arc<dyn TradingGateway>,
            adapter as Arc<dyn MarketDataSource>,
        ));
        // The watchdog needs a weak self-reference; without it attach() would
        // skip lease enforcement, which is irrelevant for these RPC tests.
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        trading::TradingServiceClient::new(transport, ClientConfig::new(uri))
    }

    /// Boot the capability-enabled router, also returning the session client so
    /// a test can drive a session through its lifecycle.
    async fn boot_session_with_capabilities() -> (
        worker::WorkerSessionServiceClient<connectrpc::client::HttpClient>,
        trading::TradingServiceClient<connectrpc::client::HttpClient>,
    ) {
        let adapter = Arc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let manager = Arc::new(
            SessionManager::new(
                None,
                common::ExchangeId::default(),
                Arc::clone(&adapter) as Arc<dyn TradingGateway>,
                Arc::clone(&adapter) as Arc<dyn MarketDataSource>,
            )
            .with_capabilities(crate::session::Capabilities::all(adapter)),
        );
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        let config = ClientConfig::new(uri);
        (
            worker::WorkerSessionServiceClient::new(transport.clone(), config.clone()),
            trading::TradingServiceClient::new(transport, config),
        )
    }

    /// A `common.v1.Decimal` for `value`, fully populated.
    fn dec(
        value: rust_decimal::Decimal,
    ) -> buffa::MessageField<common::Decimal, buffa::Inline<common::Decimal>> {
        longtrader_contract::ext::decimal_to_common(value).into()
    }

    fn order_req(session_id: &str) -> trading::CreateOrderRequest {
        trading::CreateOrderRequest {
            exchange_id: buffa::MessageField::none(),
            order: buffa::MessageField::some(trading::OrderRequest {
                client_order_id: "grid-e2e".to_string(),
                symbol: "BTC/USDT".to_string(),
                r#type: buffa::EnumValue::Known(trading::OrderType::Limit),
                side: buffa::EnumValue::Known(trading::OrderSide::Buy),
                amount: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(
                    rust_decimal_macros::dec!(1),
                )),
                price: buffa::MessageField::some(longtrader_contract::ext::decimal_to_common(
                    rust_decimal_macros::dec!(99),
                )),
                time_in_force: buffa::EnumValue::Known(trading::TimeInForce::Gtc),
                ..Default::default()
            }),
            session_id: session_id.to_string(),
            ..Default::default()
        }
    }

    /// Attach a session and drive it to `ACTIVE`, returning its id.
    async fn active_session(
        session_client: &worker::WorkerSessionServiceClient<connectrpc::client::HttpClient>,
    ) -> String {
        let attached = session_client
            .attach_session(worker::AttachSessionRequest::default())
            .await
            .expect("attach")
            .into_owned();
        let session_id = attached.session_id.clone();
        session_client
            .reconcile_state(worker::ReconcileStateRequest {
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("reconcile");
        session_id
    }

    // ---- Session gating (pre-existing behaviour, kept under the new router)
    // ---- wiring. ------------------------------------------------------

    /// A session-scoped order submitted before `ReconcileState` must be
    /// rejected, not silently executed: the strategy has not yet seen the
    /// account it is trading.
    #[tokio::test]
    async fn pre_active_submission_is_rejected() {
        let (session_client, trading_client) = boot().await;
        let attached = session_client
            .attach_session(worker::AttachSessionRequest::default())
            .await
            .expect("attach")
            .into_owned();

        let err = trading_client
            .create_order(order_req(&attached.session_id))
            .await
            .expect_err("pre-ACTIVE submission must be rejected");
        assert_eq!(err.code, connectrpc::ErrorCode::FailedPrecondition, "got {err:?}");
        assert!(
            err.message.as_deref().unwrap_or_default().contains("SYNC_IN_PROGRESS"),
            "the stable reason token must survive to the wire: {err:?}"
        );
    }

    /// After reconciling, the same order is admitted — so the gate is a
    /// lifecycle check, not a blanket refusal.
    #[tokio::test]
    async fn active_submission_is_admitted_and_attributed() {
        let (session_client, trading_client) = boot().await;
        let session_id = active_session(&session_client).await;

        let created = trading_client
            .create_order(order_req(&session_id))
            .await
            .expect("ACTIVE submission must be admitted")
            .into_owned();
        let order = created.order.as_option().expect("order in response");
        assert_eq!(order.symbol, "BTC/USDT");

        let status = session_client
            .strategy_status(worker::StrategyStatusRequest {
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("status")
            .into_owned();
        assert_eq!(
            status.orders_submitted, 1,
            "the submitted order must be attributed to the session that placed it"
        );
    }

    /// An unscoped (operator / CLI) call carries no `session_id` and must not
    /// be gated, or the CLI could not trade at all.
    #[tokio::test]
    async fn an_unscoped_submission_is_not_gated() {
        let (_session_client, trading_client) = boot().await;
        let created = trading_client
            .create_order(order_req(""))
            .await
            .expect("an operator call has no session lifecycle to gate on")
            .into_owned();
        assert!(created.order.as_option().is_some(), "the order must be returned");
    }

    // ---- Venue capabilities over the wire -------------------------------
    //
    // These cover the funding / conditional-order / wallet / venue-op RPCs end
    // to end. The adapter unit tests cover the conversions; these prove the
    // proxy wiring, the Connect encoding, and the error codes survive a real
    // round trip through `build_router`.

    #[tokio::test]
    async fn funding_rate_reaches_a_strategy() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        adapter.set_funding_rate(dec!(0.0001)).await;

        let resp = market_client
            .fetch_funding_rate(market::FetchFundingRateRequest {
                exchange_id: buffa::MessageField::some(common::ExchangeId {
                    id: "mock".to_string(),
                    ..Default::default()
                }),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("funding rate")
            .into_owned();
        let rate = resp.funding_rate.as_option().expect("rate present");
        // Assert the wire value, not the local one: a decimal that encoded
        // wrong would arrive as 0 and look like free funding.
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(rate.rate.as_option().expect("rate"))
                .expect("decodes"),
            dec!(0.0001)
        );
    }

    #[tokio::test]
    async fn trigger_order_lifecycle_reaches_a_strategy() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        let exchange = || {
            buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            })
        };

        let created = trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: exchange(),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    client_order_id: "e2e-stop".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(0.001)),
                    reduce_only: true,
                    trigger_type: buffa::EnumValue::Known(trading::TriggerPriceType::Last),
                    order_type: buffa::EnumValue::Known(trading::OrderType::Market),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("create trigger order")
            .into_owned();
        let order = created.order.as_option().expect("order in response");
        assert!(!order.id.is_empty(), "the venue must assign an id");
        assert_eq!(order.symbol, "BTC/USDT");
        assert!(order.reduce_only, "reduce_only must survive the round trip");
        assert_eq!(order.status, buffa::EnumValue::Known(trading::TriggerOrderStatus::Open));
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(order.qty.as_option().expect("qty"))
                .expect("decodes"),
            dec!(0.001)
        );

        let listed = trading_client
            .list_trigger_orders(trading::ListTriggerOrdersRequest {
                exchange_id: exchange(),
                symbols: vec![],
                ..Default::default()
            })
            .await
            .expect("list trigger orders")
            .into_owned();
        assert_eq!(listed.orders.len(), 1, "the backstop must be visible");
        assert_eq!(listed.orders[0].id, order.id);

        let canceled = trading_client
            .cancel_trigger_order(trading::CancelTriggerOrderRequest {
                exchange_id: exchange(),
                order_id: order.id.clone(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("cancel trigger order")
            .into_owned();
        assert_eq!(
            canceled.order.as_option().expect("order").status,
            buffa::EnumValue::Known(trading::TriggerOrderStatus::Canceled)
        );

        // Cancelling twice must fail rather than report a second success: the
        // second call would otherwise claim a backstop is gone that is not.
        let err = trading_client
            .cancel_trigger_order(trading::CancelTriggerOrderRequest {
                exchange_id: exchange(),
                order_id: order.id.clone(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("already canceled");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");
    }

    /// Every way a conditional order is silently useless must be rejected
    /// before it reaches the venue: a zero/negative quantity, and a
    /// zero/negative trigger price (which would fire immediately and degenerate
    /// into a market order — the exact opposite of a protective stop).
    #[tokio::test]
    async fn a_degenerate_trigger_order_is_rejected() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        let exchange = || {
            buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            })
        };
        let order = |trigger_price: rust_decimal::Decimal, qty: rust_decimal::Decimal| {
            trading::CreateTriggerOrderRequest {
                exchange_id: exchange(),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(trigger_price),
                    qty: dec(qty),
                    ..Default::default()
                }),
                ..Default::default()
            }
        };

        for (label, trigger_price, qty) in [
            ("zero qty", dec!(95000), dec!(0)),
            ("negative qty", dec!(95000), dec!(-1)),
            ("zero trigger", dec!(0), dec!(1)),
            ("negative trigger", dec!(-95000), dec!(1)),
        ] {
            let outcome = trading_client.create_trigger_order(order(trigger_price, qty)).await;
            let err =
                outcome.expect_err(&format!("{label} must be rejected, but the venue accepted it"));
            assert_eq!(
                err.code,
                connectrpc::ErrorCode::InvalidArgument,
                "{label} must not reach the venue: {err:?}"
            );
        }

        // Nothing was placed by any of the rejected attempts.
        let listed = trading_client
            .list_trigger_orders(trading::ListTriggerOrdersRequest {
                exchange_id: exchange(),
                ..Default::default()
            })
            .await
            .expect("list")
            .into_owned();
        assert!(
            listed.orders.is_empty(),
            "a rejected request must not leave an order behind: {listed:?}"
        );
    }

    /// An unspecified side must be rejected, never defaulted to Buy — which
    /// would silently invert a stop.
    #[tokio::test]
    async fn a_trigger_order_with_an_unspecified_side_is_rejected() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;

        let outcome = trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: buffa::MessageField::some(common::ExchangeId {
                    id: "mock".to_string(),
                    ..Default::default()
                }),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Unspecified),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(1)),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;
        let err = outcome.expect_err("a side-less conditional order must be rejected");
        assert!(err.message.as_deref().unwrap_or_default().contains("side"), "{err:?}");
    }

    #[tokio::test]
    async fn transfer_is_idempotent_and_lands_on_the_ledger() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        let exchange = buffa::MessageField::some(common::ExchangeId {
            id: "mock".to_string(),
            ..Default::default()
        });

        let mk = || trading::TransferRequest {
            exchange_id: exchange.clone(),
            asset: "USDT".to_string(),
            amount: dec(dec!(100)),
            dest_label: "futures".to_string(),
            client_transfer_id: "e2e-transfer".to_string(),
            ..Default::default()
        };
        let first = trading_client.transfer(mk()).await.expect("transfer").into_owned();
        let second = trading_client.transfer(mk()).await.expect("retry").into_owned();
        assert_eq!(
            first.transfer_id, second.transfer_id,
            "a retried transfer must not debit twice"
        );

        let ledger = trading_client
            .fetch_ledger_entries(trading::FetchLedgerEntriesRequest {
                exchange_id: exchange.clone(),
                currency: "USDT".to_string(),
                r#type: "transfer".to_string(),
                ..Default::default()
            })
            .await
            .expect("ledger")
            .into_owned();
        assert_eq!(ledger.entries.len(), 1, "one row, not two");
        assert_eq!(ledger.entries[0].direction, "out");
        assert_eq!(ledger.entries[0].status, "completed");
        // The sign must be carried, not re-derived from the amount.
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(
                ledger.entries[0].amount.as_option().expect("amount")
            )
            .expect("decodes"),
            dec!(-100)
        );

        let deposits = trading_client
            .fetch_ledger_entries(trading::FetchLedgerEntriesRequest {
                exchange_id: exchange,
                currency: "USDT".to_string(),
                r#type: "deposit".to_string(),
                ..Default::default()
            })
            .await
            .expect("deposit filter")
            .into_owned();
        assert!(deposits.entries.is_empty(), "the type filter must exclude transfers");
    }

    /// A non-positive transfer is the caller's mistake and must say so.
    #[tokio::test]
    async fn a_bad_transfer_answers_invalid_argument() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;

        for (label, amount) in [("zero", dec!(0)), ("negative", dec!(-5))] {
            let outcome = trading_client
                .transfer(trading::TransferRequest {
                    exchange_id: buffa::MessageField::some(common::ExchangeId {
                        id: "mock".to_string(),
                        ..Default::default()
                    }),
                    asset: "USDT".to_string(),
                    amount: dec(amount),
                    dest_label: "futures".to_string(),
                    ..Default::default()
                })
                .await;
            let err = outcome.expect_err(&format!("a {label} transfer must be rejected"));
            assert_eq!(
                err.code,
                connectrpc::ErrorCode::InvalidArgument,
                "a bad argument must not look like a broken worker: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn venue_ops_discovery_and_invocation_work_together() {
        let (_adapter, _trading, _market, ops_client) = boot_with_capabilities().await;

        let listed = ops_client
            .list_venue_ops(ops::ListVenueOpsRequest {
                exchange_id: "mock".to_string(),
                ..Default::default()
            })
            .await
            .expect("list venue ops")
            .into_owned();
        let names: Vec<&str> = listed.ops.iter().map(|o| o.name.as_str()).collect();
        assert!(
            names.contains(&"account.balance"),
            "the mock venue must advertise its ops: {names:?}"
        );

        let described = ops_client
            .describe_venue_op(ops::DescribeVenueOpRequest {
                exchange_id: "mock".to_string(),
                op: "account.balance".to_string(),
                ..Default::default()
            })
            .await
            .expect("describe")
            .into_owned();
        assert_eq!(described.op.as_option().expect("op").name, "account.balance");

        // An op the venue does not expose must be `not_found`, not an empty
        // descriptor: a caller has to be able to tell the two apart.
        let err = ops_client
            .describe_venue_op(ops::DescribeVenueOpRequest {
                exchange_id: "mock".to_string(),
                op: "wallet.withdraw".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("op the mock venue does not expose");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    #[tokio::test]
    async fn invoke_venue_op_carries_params_and_returns_a_result() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, _market, ops_client) = boot_with_capabilities().await;
        adapter.set_op_balance(dec!(1234)).await;

        let result = ops_client
            .invoke_venue_op(ops::InvokeVenueOpRequest {
                exchange_id: "mock".to_string(),
                op: "account.balance".to_string(),
                params: params_struct(serde_json::json!({"currency": "USDT"})),
                ..Default::default()
            })
            .await
            .expect("invoke")
            .into_owned();
        let json: serde_json::Value =
            serde_json::to_value(result.result.as_option().expect("result")).expect("json");
        assert_eq!(json["currency"], "USDT", "params must reach the venue: {json}");
    }

    /// An unknown op must stay a "not found" style answer through the gateway,
    /// not become an `internal` the caller retries forever.
    #[tokio::test]
    async fn an_unknown_venue_op_is_not_an_internal_error() {
        let (_adapter, _trading, _market, ops_client) = boot_with_capabilities().await;

        let err = ops_client
            .invoke_venue_op(ops::InvokeVenueOpRequest {
                exchange_id: "mock".to_string(),
                op: "wallet.withdraw".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("op the mock does not expose");
        assert!(
            matches!(
                err.code,
                connectrpc::ErrorCode::NotFound | connectrpc::ErrorCode::Unimplemented
            ),
            "a permanent 'no such op' must not look like a server fault: {:?} {err:?}",
            err.code
        );
    }

    /// An object that does not exist must be `not_found`, so a caller can tell
    /// "no funding for this contract" from "this backend has no funding".
    #[tokio::test]
    async fn a_missing_object_answers_not_found() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        adapter.set_funding_rate(dec!(0.0001)).await;
        adapter.set_funding_symbols(vec!["BTC/USDT".to_string()]).await;

        let req = |symbol: &str| market::FetchFundingRateRequest {
            exchange_id: buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            }),
            symbol: symbol.to_string(),
            ..Default::default()
        };

        // The listed contract still answers.
        market_client.fetch_funding_rate(req("BTC/USDT")).await.expect("listed contract");

        // A different contract must not inherit BTC's rate: a carry strategy
        // would size a real position off the wrong number.
        let err =
            market_client.fetch_funding_rate(req("ETH/USDT")).await.expect_err("unlisted contract");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    /// The mock's funding history must be deterministic and on the settlement
    /// grid, so a backtest against it reproduces run to run.
    #[tokio::test]
    async fn funding_history_is_deterministic_and_evenly_spaced() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        adapter.set_funding_rate(dec!(0.0001)).await;

        let req = || market::FetchFundingRateHistoryRequest {
            exchange_id: buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            }),
            symbol: "BTC/USDT".to_string(),
            limit: 5,
            ..Default::default()
        };

        let first =
            market_client.fetch_funding_rate_history(req()).await.expect("history").into_owned();
        let second =
            market_client.fetch_funding_rate_history(req()).await.expect("history").into_owned();
        assert_eq!(first.points.len(), 5);
        let first_times: Vec<i64> = first.points.iter().map(|p| p.funding_time_ms).collect();
        let second_times: Vec<i64> = second.points.iter().map(|p| p.funding_time_ms).collect();
        assert_eq!(first_times, second_times, "the same request must not drift with wall time");
        // Newest first, evenly spaced by the settlement interval.
        assert!(first_times.windows(2).all(|w| w[0] > w[1]), "{first_times:?}");
        let gaps: Vec<i64> = first_times.windows(2).map(|w| w[0] - w[1]).collect();
        assert!(
            gaps.windows(2).all(|w| w[0] == w[1]),
            "settlements must be evenly spaced: {gaps:?}"
        );
    }

    /// A backend that never declared a capability must answer `unimplemented`,
    /// not an empty success. A caller that sees `internal` retries forever
    /// against a permanent answer.
    #[tokio::test]
    async fn a_backend_without_capabilities_answers_unimplemented() {
        use rust_decimal_macros::dec;
        let trading_client = boot_without_capabilities().await;
        let exchange = || {
            buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            })
        };

        let err = trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: exchange(),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(1)),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect_err("no trigger capability");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented, "{err:?}");

        let err = trading_client
            .transfer(trading::TransferRequest {
                exchange_id: exchange(),
                asset: "USDT".to_string(),
                amount: dec(dec!(1)),
                dest_label: "futures".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("no wallet capability");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented, "{err:?}");

        let err = trading_client
            .fetch_ledger_entries(trading::FetchLedgerEntriesRequest {
                exchange_id: exchange(),
                ..Default::default()
            })
            .await
            .expect_err("no wallet capability");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented, "{err:?}");
    }

    /// Build a `google.protobuf.Struct` message field from JSON.
    fn params_struct(
        v: serde_json::Value,
    ) -> buffa::MessageField<
        buffa_types::google::protobuf::Struct,
        buffa::Inline<buffa_types::google::protobuf::Struct>,
    > {
        let wire: buffa_types::google::protobuf::Struct =
            serde_json::from_value(v).expect("params are a Struct");
        wire.into()
    }

    /// `mutating` is what a client reads to decide whether a call needs a
    /// confirmation prompt. Reporting `account.transfer` as read-only would be
    /// a safety regression, so the descriptor must survive the proxy intact.
    #[tokio::test]
    async fn venue_op_descriptors_carry_mutating_and_the_param_schema() {
        let (_adapter, _trading, _market, ops_client) = boot_with_capabilities().await;

        let listed = ops_client
            .list_venue_ops(ops::ListVenueOpsRequest {
                exchange_id: "mock".to_string(),
                ..Default::default()
            })
            .await
            .expect("list")
            .into_owned();

        let transfer = listed
            .ops
            .iter()
            .find(|o| o.name == "account.transfer")
            .expect("account.transfer is advertised");
        assert!(
            transfer.mutating,
            "a fund-moving op must be flagged mutating, or a client will not prompt"
        );
        assert!(!transfer.category.is_empty(), "category must survive: {transfer:?}");
        let param_names: Vec<&str> = transfer.params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            param_names,
            vec!["asset", "amount", "dest"],
            "the parameter schema is what makes the list usable"
        );

        let balance = listed
            .ops
            .iter()
            .find(|o| o.name == "account.balance")
            .expect("account.balance is advertised");
        assert!(!balance.mutating, "a read must not be flagged mutating");
    }

    /// `DescribeVenueOp` exists to hand back a full schema. Returning a bare
    /// name makes it useless for building an `InvokeVenueOp` request.
    #[tokio::test]
    async fn describe_venue_op_returns_a_usable_schema() {
        let (_adapter, _trading, _market, ops_client) = boot_with_capabilities().await;

        let described = ops_client
            .describe_venue_op(ops::DescribeVenueOpRequest {
                exchange_id: "mock".to_string(),
                op: "margin.borrow".to_string(),
                ..Default::default()
            })
            .await
            .expect("describe")
            .into_owned();
        let op = described.op.as_option().expect("op");
        assert_eq!(op.name, "margin.borrow");
        assert!(op.mutating, "borrowing changes account state");
        assert_eq!(op.params.len(), 2, "the schema must be present, not empty");
        assert!(op.params.iter().all(|p| p.required));
    }

    /// A cancel must return the venue's record, not a value rebuilt from the
    /// request. A synthesised response is byte-identical whether the right
    /// order was cancelled or not, and its zeroed price/qty/side are
    /// indistinguishable from real values.
    #[tokio::test]
    async fn cancel_returns_the_orders_real_geometry() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        let exchange = || {
            buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            })
        };

        let created = trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: exchange(),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    client_order_id: "cancel-geometry".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(0.25)),
                    reduce_only: true,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("create")
            .into_owned();
        let order = created.order.as_option().expect("order");

        let canceled = trading_client
            .cancel_trigger_order(trading::CancelTriggerOrderRequest {
                exchange_id: exchange(),
                order_id: order.id.clone(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("cancel")
            .into_owned();
        let back = canceled.order.as_option().expect("order");

        // The geometry must be the venue's, not zeros from `..Default::default()`.
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(
                back.trigger_price.as_option().expect("trigger_price")
            )
            .expect("decodes"),
            dec!(95000),
            "a cancel response must not zero the trigger price"
        );
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(back.qty.as_option().expect("qty"))
                .expect("decodes"),
            dec!(0.25)
        );
        assert_eq!(back.side, buffa::EnumValue::Known(trading::OrderSide::Sell));
        assert!(back.reduce_only, "reduce_only must survive the cancel");
        assert_eq!(back.client_order_id, "cancel-geometry");
    }

    /// The funding RPCs must behave like the rest of `market.v1`: an
    /// unqualified request resolves to the configured default rather than
    /// being the only method that rejects it.
    #[tokio::test]
    async fn funding_resolves_an_omitted_exchange_id() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        adapter.set_funding_rate(dec!(0.0001)).await;

        let resp = market_client
            .fetch_funding_rate(market::FetchFundingRateRequest {
                // No `exchange_id`, exactly as `fetch_ticker` tolerates.
                exchange_id: buffa::MessageField::none(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("an unqualified funding request must resolve to the default exchange")
            .into_owned();
        assert!(resp.funding_rate.as_option().is_some());
    }

    /// A conditional order and a transfer are both financially meaningful, so a
    /// session that has not reconciled must not be able to place one — the same
    /// `SYNC_IN_PROGRESS` rule that already applies to `CreateOrder`. Without
    /// it the gate is trivially bypassed by using the other order path.
    #[tokio::test]
    async fn pre_active_capability_submissions_are_rejected() {
        use rust_decimal_macros::dec;
        let (session_client, trading_client) = boot_session_with_capabilities().await;
        let attached = session_client
            .attach_session(worker::AttachSessionRequest::default())
            .await
            .expect("attach")
            .into_owned();
        let session_id = attached.session_id.clone();
        let exchange = || {
            buffa::MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                ..Default::default()
            })
        };

        let err = trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: exchange(),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(1)),
                    ..Default::default()
                }),
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect_err("a pre-ACTIVE session must not place a conditional order");
        assert_eq!(err.code, connectrpc::ErrorCode::FailedPrecondition, "{err:?}");
        assert!(
            err.message.as_deref().unwrap_or_default().contains("SYNC_IN_PROGRESS"),
            "the stable reason token must survive to the wire: {err:?}"
        );

        let err = trading_client
            .transfer(trading::TransferRequest {
                exchange_id: exchange(),
                asset: "USDT".to_string(),
                amount: dec(dec!(10)),
                dest_label: "futures".to_string(),
                session_id,
                ..Default::default()
            })
            .await
            .expect_err("a pre-ACTIVE session must not move funds");
        assert_eq!(err.code, connectrpc::ErrorCode::FailedPrecondition, "{err:?}");

        // Nothing was placed or moved by either rejected attempt.
        let listed = trading_client
            .list_trigger_orders(trading::ListTriggerOrdersRequest {
                exchange_id: exchange(),
                ..Default::default()
            })
            .await
            .expect("list")
            .into_owned();
        assert!(listed.orders.is_empty(), "a rejected request must not leave an order");
    }

    /// The gate must open once the session reconciles, and an unscoped operator
    /// call must keep working — otherwise the CLI could not trade.
    #[tokio::test]
    async fn an_active_session_may_place_a_conditional_order() {
        use rust_decimal_macros::dec;
        let (session_client, trading_client) = boot_session_with_capabilities().await;
        let session_id = active_session(&session_client).await;

        trading_client
            .create_trigger_order(trading::CreateTriggerOrderRequest {
                exchange_id: buffa::MessageField::some(common::ExchangeId {
                    id: "mock".to_string(),
                    ..Default::default()
                }),
                order: buffa::MessageField::some(trading::TriggerOrderRequest {
                    symbol: "BTC/USDT".to_string(),
                    side: buffa::EnumValue::Known(trading::OrderSide::Sell),
                    trigger_price: dec(dec!(95000)),
                    qty: dec(dec!(1)),
                    ..Default::default()
                }),
                session_id,
                ..Default::default()
            })
            .await
            .expect("an ACTIVE session must be admitted");
    }

    /// An unscoped call carries no `session_id` and must not be gated.
    #[tokio::test]
    async fn an_unscoped_transfer_is_not_gated() {
        use rust_decimal_macros::dec;
        let (_session_client, trading_client, _market, _ops) = boot_with_capabilities().await;

        trading_client
            .transfer(trading::TransferRequest {
                exchange_id: buffa::MessageField::some(common::ExchangeId {
                    id: "mock".to_string(),
                    ..Default::default()
                }),
                asset: "USDT".to_string(),
                amount: dec(dec!(10)),
                dest_label: "futures".to_string(),
                ..Default::default()
            })
            .await
            .expect("an operator call has no session lifecycle to gate on");
    }
}
