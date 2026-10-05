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
            err.details.iter().any(|d| d.type_url == "longtrader.common.v1.ErrorDetail"),
            "the structured ErrorDetail must survive to the wire: {err:?}"
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
            err.details.iter().any(|d| d.type_url == "longtrader.common.v1.ErrorDetail"),
            "the structured ErrorDetail must survive to the wire: {err:?}"
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

    // ---- Shared fixtures for the remaining surface ------------------------
    //
    // The helpers below are the same shape as the ones above (a real Connect
    // round trip through `build_router`); they exist because these tests need
    // the *other* side of the fixtures — the adapter handle, the session client,
    // or the market client — in the same process.

    /// The mock venue's `exchange_id` request field.
    fn mock_exchange() -> buffa::MessageField<common::ExchangeId, buffa::Inline<common::ExchangeId>>
    {
        common::ExchangeId { id: "mock".to_string(), ..Default::default() }.into()
    }

    /// A `google.protobuf.Duration` of `millis`, for lease negotiation.
    fn lease_duration(millis: u64) -> buffa_types::google::protobuf::Duration {
        buffa_types::google::protobuf::Duration {
            seconds: i64::try_from(millis / 1000).expect("whole seconds fit i64"),
            nanos: i32::try_from((millis % 1000) * 1_000_000).expect("millis fit i32 nanos"),
            ..Default::default()
        }
    }

    /// The `lease_timeout` policy field. `None` means "leave the negotiated lease
    /// alone", which is why an absent field is meaningful rather than zero.
    fn lease_field(
        millis: Option<u64>,
    ) -> buffa::MessageField<
        buffa_types::google::protobuf::Duration,
        buffa::Inline<buffa_types::google::protobuf::Duration>,
    > {
        millis.map_or_else(buffa::MessageField::none, |ms| {
            buffa::MessageField::some(lease_duration(ms))
        })
    }

    /// A limit-order payload for `symbol`. The session id lives on the enclosing
    /// request, so both the batch RPC and an operator-style unscoped placement
    /// need the inner message built separately.
    fn order_payload(symbol: &str, coid: &str) -> trading::OrderRequest {
        let mut order = order_req("").order.as_option().expect("order request").clone();
        order.symbol = symbol.to_string();
        order.client_order_id = coid.to_string();
        order
    }

    /// One limit-order leg for the batch `CreateOrders` RPC.
    fn batch_leg(coid: &str) -> trading::OrderRequest {
        order_payload("BTC/USDT", coid)
    }

    /// An *unscoped* create-order request, the way the CLI places one. A non-empty
    /// `session_id` would be gated against a session lifecycle that a plain
    /// operator call has none of.
    fn operator_order(symbol: &str, coid: &str) -> trading::CreateOrderRequest {
        trading::CreateOrderRequest {
            order: buffa::MessageField::some(order_payload(symbol, coid)),
            ..order_req("")
        }
    }

    /// One `worker.v1.LogEvent` for the client-streaming `ReportLog` RPC.
    fn log_event(session_id: &str, level: worker::LogLevel, message: &str) -> worker::LogEvent {
        worker::LogEvent {
            session_id: session_id.to_string(),
            level: buffa::EnumValue::Known(level),
            message: message.to_string(),
            ..Default::default()
        }
    }

    /// Boot the capability-enabled router with the session client *and* the
    /// backend handle, so a test can script an adapter-side failure and still
    /// observe what the session was credited with.
    async fn boot_scripted() -> (
        Arc<MockAdapter>,
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
            .with_capabilities(crate::session::Capabilities::all(Arc::clone(&adapter))),
        );
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        let config = ClientConfig::new(uri);
        (
            adapter,
            worker::WorkerSessionServiceClient::new(transport.clone(), config.clone()),
            trading::TradingServiceClient::new(transport, config),
        )
    }

    /// Boot a router whose backend declares no capabilities, returning the
    /// market client, so `market.v1`'s funding RPCs get the same `unimplemented`
    /// coverage the trading ones already have.
    async fn boot_market_without_capabilities()
    -> market::MarketDataServiceClient<connectrpc::client::HttpClient> {
        let adapter = Arc::new(MockAdapter::new(rust_decimal_macros::dec!(100)));
        let manager = Arc::new(SessionManager::new(
            None,
            common::ExchangeId::default(),
            Arc::clone(&adapter) as Arc<dyn TradingGateway>,
            adapter as Arc<dyn MarketDataSource>,
        ));
        manager.install_self().await;
        let addr = spawn(manager).await;

        let transport = connectrpc::client::HttpClient::plaintext();
        let uri: axum::http::Uri = format!("http://{addr}").parse().expect("valid uri");
        market::MarketDataServiceClient::new(transport, ClientConfig::new(uri))
    }

    /// How long to wait for one more streamed event. A `StreamStrategyEvents`
    /// subscription has no natural end — the session outlives the test — so a
    /// drain has to stop on an idle gap rather than on a clean `None`.
    const STREAM_IDLE: std::time::Duration = std::time::Duration::from_millis(500);

    // ---- Market data passthrough -------------------------------------------
    //
    // The worker is a thin proxy here; these RPCs exist so an
    // external-language strategy sees the same surface a native one does. The
    // adapter's own unit tests cover the conversions, so what is asserted here
    // is that the proxy wiring and the Connect encoding survive the round trip.

    #[tokio::test]
    async fn instrument_discovery_lists_and_searches_the_mocks_symbols() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;

        let listed = market_client
            .list_symbols(market::ListSymbolsRequest {
                exchange_id: mock_exchange(),
                ..Default::default()
            })
            .await
            .expect("list symbols")
            .into_owned();
        assert_eq!(listed.symbols.len(), 1, "the mock advertises one instrument");
        assert_eq!(listed.symbols[0].name, "MOCK-USDT");
        assert_eq!(listed.symbols[0].quote_asset, "USDT", "instrument metadata must survive");

        // Search is a case-insensitive substring match over name / display name.
        let hit = market_client
            .search_symbols(market::SearchSymbolsRequest {
                exchange_id: mock_exchange(),
                query: "mock".to_string(),
                ..Default::default()
            })
            .await
            .expect("search")
            .into_owned();
        assert_eq!(hit.symbols.len(), 1, "a substring match must be case-insensitive");

        let miss = market_client
            .search_symbols(market::SearchSymbolsRequest {
                exchange_id: mock_exchange(),
                query: "no-such-instrument".to_string(),
                ..Default::default()
            })
            .await
            .expect("search")
            .into_owned();
        assert!(miss.symbols.is_empty(), "an unmatched query yields no rows, not an error");
    }

    /// A ticker read is the price a strategy sizes on, so the assertion is on
    /// the *wire* decimal: a value that encoded wrong would arrive as 0 and look
    /// like a free fill.
    #[tokio::test]
    async fn ticker_reads_carry_the_venue_value_across_the_wire() {
        use rust_decimal_macros::dec;
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;

        let one = market_client
            .fetch_ticker(market::FetchTickerRequest {
                exchange_id: mock_exchange(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("fetch ticker")
            .into_owned();
        let last = one.ticker.as_option().expect("ticker present");
        assert_eq!(last.symbol, "BTC/USDT");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(last.last.as_option().expect("last"))
                .expect("decodes"),
            dec!(100),
        );

        // An empty symbol list means "every symbol the venue reports".
        let all = market_client
            .list_tickers(market::ListTickersRequest {
                exchange_id: mock_exchange(),
                symbols: vec![],
                ..Default::default()
            })
            .await
            .expect("list tickers")
            .into_owned();
        assert_eq!(all.tickers.len(), 1);

        // An explicit filter must be honoured rather than returning everything:
        // a batch of one symbol must not silently become a batch of all.
        let none = market_client
            .list_tickers(market::ListTickersRequest {
                exchange_id: mock_exchange(),
                symbols: vec!["NOPE/USDT".to_string()],
                ..Default::default()
            })
            .await
            .expect("list tickers")
            .into_owned();
        assert!(none.tickers.is_empty(), "an unknown symbol must yield no row, not a price");
    }

    #[tokio::test]
    async fn order_book_depth_follows_the_requested_page_size() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;

        let deep = market_client
            .fetch_order_book(market::FetchOrderBookRequest {
                exchange_id: mock_exchange(),
                symbol: "BTC/USDT".to_string(),
                pagination: buffa::MessageField::some(common::Pagination {
                    limit: 5,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("order book")
            .into_owned();
        let book = deep.orderbook.as_option().expect("orderbook present");
        assert_eq!(book.symbol, "BTC/USDT");
        assert_eq!(book.bids.len(), 5, "the requested depth must reach the adapter intact");
        assert_eq!(book.asks.len(), 5);

        // Bids must sit below the mid and asks above it: a crossed book would
        // make the very first maker fill lose money.
        let best_bid = longtrader_contract::ext::common_to_decimal(
            book.bids[0].price.as_option().expect("bid price"),
        )
        .expect("decodes");
        let best_ask = longtrader_contract::ext::common_to_decimal(
            book.asks[0].price.as_option().expect("ask price"),
        )
        .expect("decodes");
        assert!(best_bid < best_ask, "the book must not be crossed: {best_bid} / {best_ask}");

        // No pagination: the proxy still resolves a usable one-level book rather
        // than erroring, like every other unqualified market call.
        let shallow = market_client
            .fetch_order_book(market::FetchOrderBookRequest {
                exchange_id: mock_exchange(),
                symbol: "BTC/USDT".to_string(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("order book")
            .into_owned();
        assert_eq!(shallow.orderbook.as_option().expect("orderbook").bids.len(), 1);
    }

    #[tokio::test]
    async fn candles_are_returned_ascending_and_honour_the_limit() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;

        let resp = market_client
            .get_candles(market::GetCandlesRequest {
                exchange_id: mock_exchange(),
                symbol: "BTC/USDT".to_string(),
                timeframe: buffa::EnumValue::Known(market::Timeframe::M5),
                pagination: buffa::MessageField::some(common::Pagination {
                    limit: 7,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("candles")
            .into_owned();
        assert_eq!(resp.candles.len(), 7, "the requested limit must be honoured");
        let times: Vec<i64> = resp.candles.iter().map(|c| c.timestamp_ms).collect();
        assert!(times.windows(2).all(|w| w[0] < w[1]), "candles must ascend: {times:?}");
    }

    /// A batch funding read must carry the *requested* symbol on every row: a
    /// carry strategy sizes a position off this row, so an unattributed rate is
    /// worse than no rate at all.
    #[tokio::test]
    async fn batch_funding_rates_report_the_symbols_that_were_asked_for() {
        use rust_decimal_macros::dec;
        let (adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        adapter.set_funding_rate(dec!(0.0001)).await;

        let listed = market_client
            .list_funding_rates(market::ListFundingRatesRequest {
                exchange_id: mock_exchange(),
                symbols: vec!["BTC/USDT".to_string(), "ETH/USDT".to_string()],
                ..Default::default()
            })
            .await
            .expect("batch funding")
            .into_owned();
        let symbols: Vec<&str> = listed.funding_rates.iter().map(|r| r.symbol.as_str()).collect();
        assert_eq!(symbols, vec!["BTC/USDT", "ETH/USDT"]);
        for rate in &listed.funding_rates {
            assert_eq!(
                longtrader_contract::ext::common_to_decimal(rate.rate.as_option().expect("rate"))
                    .expect("decodes"),
                dec!(0.0001),
            );
            assert!(
                rate.next_funding_time_ms > 0,
                "the settlement time must reach the client or a carry leg cannot time its exit"
            );
        }

        // An unpriced contract is absent from the batch, never given another
        // contract's rate.
        adapter.set_funding_symbols(vec!["BTC/USDT".to_string()]).await;
        let narrowed = market_client
            .list_funding_rates(market::ListFundingRatesRequest {
                exchange_id: mock_exchange(),
                symbols: vec!["BTC/USDT".to_string(), "ETH/USDT".to_string()],
                ..Default::default()
            })
            .await
            .expect("batch funding")
            .into_owned();
        let symbols: Vec<&str> = narrowed.funding_rates.iter().map(|r| r.symbol.as_str()).collect();
        assert_eq!(symbols, vec!["BTC/USDT"], "an unpriced contract must be omitted");
    }

    /// The funding RPCs are capability-gated like their trading counterparts: a
    /// backend that cannot price funding must say `unimplemented`, because an
    /// empty success reads as "this venue has no basis at all".
    #[tokio::test]
    async fn batch_funding_rates_without_a_backend_capability_is_unimplemented() {
        let market_client = boot_market_without_capabilities().await;
        let err = market_client
            .list_funding_rates(market::ListFundingRatesRequest {
                exchange_id: mock_exchange(),
                symbols: vec!["BTC/USDT".to_string()],
                ..Default::default()
            })
            .await
            .expect_err("no funding capability");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented, "{err:?}");
    }

    /// A ticker subscription takes the `DropOldest` arm of the proxy's policy
    /// selection; the events must reach the client as a live stream.
    #[tokio::test]
    async fn a_ticker_subscription_streams_events_to_the_client() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        let mut stream = market_client
            .stream_market_data(market::StreamMarketDataRequest {
                exchange_id: mock_exchange(),
                subscriptions: vec![market::StreamSubscription {
                    channel: buffa::EnumValue::Known(market::StreamChannel::Ticker),
                    symbol: "BTC/USDT".to_string(),
                    ..Default::default()
                }],
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("ticker stream");
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.message::<market::MarketDataEvent>(),
        )
        .await
        .expect("the mock polls every symbol it was subscribed to")
        .expect("stream read")
        .expect("a subscribed symbol must produce events")
        .to_owned_message();
        assert!(
            matches!(event.event, Some(market::market_data_event::Event::Ticker(_))),
            "got a non-ticker event: {event:?}"
        );
        assert!(event.header.sequence > 0, "every streamed event must carry a watermark");
        assert!(!event.resume_token.is_empty(), "a streamed event must be resumable");
    }

    /// An orderbook subscription is the one that selects `Coalesce` — the most
    /// conservative of the two stream policies — and it must survive being
    /// mixed with a ticker subscription on the same request. Abandoning the
    /// stream mid-flight is the normal case (a strategy crash), so the router
    /// has to keep serving afterwards.
    #[tokio::test]
    async fn an_orderbook_subscription_selects_the_coalesce_policy() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        let subscription = |channel: market::StreamChannel| market::StreamSubscription {
            channel: buffa::EnumValue::Known(channel),
            symbol: "BTC/USDT".to_string(),
            ..Default::default()
        };
        let mut stream = market_client
            .stream_market_data(market::StreamMarketDataRequest {
                exchange_id: mock_exchange(),
                subscriptions: vec![
                    subscription(market::StreamChannel::Orderbook),
                    subscription(market::StreamChannel::Ticker),
                ],
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("orderbook stream");

        // The mock answers each channel with a synthetic event of the matching
        // variant, so the two subscriptions here deliver an `Orderbook` and a
        // `Ticker`. What is asserted is that both are pumped under the
        // coalescing policy without wedging.
        let mut received = 0usize;
        for _ in 0..4 {
            let next =
                tokio::time::timeout(STREAM_IDLE, stream.message::<market::MarketDataEvent>())
                    .await;
            let Ok(Ok(Some(_))) = next else { break };
            received += 1;
        }
        drop(stream);
        assert!(received > 0, "an orderbook subscription must still deliver events");

        let health = market_client
            .list_symbols(market::ListSymbolsRequest {
                exchange_id: mock_exchange(),
                ..Default::default()
            })
            .await
            .expect("the router must keep serving after an abandoned stream")
            .into_owned();
        assert_eq!(health.symbols.len(), 1);
    }

    /// A stream whose venue cannot be resolved has nothing to poll, so it must
    /// never fabricate an event — and it must *end* rather than hang. With no
    /// producer task left holding the pipe open, the poller closes the channel
    /// and the client observes a clean end-of-stream. A client that waited for
    /// an event here would previously have waited forever.
    #[tokio::test]
    async fn a_stream_without_an_exchange_id_ends_without_producing_an_event() {
        let (_adapter, _trading, market_client, _ops) = boot_with_capabilities().await;
        let mut stream = market_client
            .stream_market_data(market::StreamMarketDataRequest {
                exchange_id: buffa::MessageField::none(),
                subscriptions: vec![market::StreamSubscription {
                    channel: buffa::EnumValue::Known(market::StreamChannel::Ticker),
                    symbol: "BTC/USDT".to_string(),
                    ..Default::default()
                }],
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let first =
            tokio::time::timeout(STREAM_IDLE, stream.message::<market::MarketDataEvent>()).await;
        assert!(
            matches!(first, Ok(Ok(None))),
            "an unresolvable venue must end the stream with no event: {first:?}"
        );
    }

    // ---- Trading passthrough ----------------------------------------------
    //
    // Everything below is a straight port forward, so what matters is that the
    // response carries the venue's own record (a response rebuilt from the
    // request is byte-identical whether the right thing happened or not) and
    // that a rejection is typed.

    #[tokio::test]
    async fn cancelling_names_the_order_it_cancelled_and_refuses_an_unknown_id() {
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        let created = trading_client.create_order(order_req("")).await.expect("place").into_owned();
        let order = created.order.as_option().expect("order").clone();

        let canceled = trading_client
            .cancel_order(trading::CancelOrderRequest {
                exchange_id: mock_exchange(),
                order_id: order.id.clone(),
                symbol: order.symbol.clone(),
                ..Default::default()
            })
            .await
            .expect("cancel")
            .into_owned();
        let back = canceled.order.as_option().expect("order");
        assert_eq!(back.id, order.id, "the cancel must name the order it cancelled");
        assert_eq!(back.status, buffa::EnumValue::Known(trading::OrderStatus::Canceled));
        assert_eq!(back.client_order_id, "grid-e2e", "the geometry must survive the round trip");

        // An id the venue never issued must not read as a successful cancel.
        let err = trading_client
            .cancel_order(trading::CancelOrderRequest {
                exchange_id: mock_exchange(),
                order_id: "mock-does-not-exist".to_string(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("an id the venue never issued");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");
    }

    #[tokio::test]
    async fn cancel_all_clears_the_open_set_and_the_list_reflects_it() {
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        for coid in ["grid-a", "grid-b"] {
            trading_client.create_order(operator_order("BTC/USDT", coid)).await.expect("place");
        }
        let open = trading_client
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: String::new(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("open orders")
            .into_owned();
        assert_eq!(open.orders.len(), 2);

        let canceled = trading_client
            .cancel_all_orders(trading::CancelAllOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: String::new(),
                ..Default::default()
            })
            .await
            .expect("cancel all")
            .into_owned();
        assert_eq!(canceled.orders.len(), 2, "every cancelled order must be reported back");
        assert!(
            canceled
                .orders
                .iter()
                .all(|o| o.status == buffa::EnumValue::Known(trading::OrderStatus::Canceled)),
            "a cancel-all must not report an order as still open"
        );

        let after = trading_client
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: String::new(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("open orders")
            .into_owned();
        assert!(after.orders.is_empty(), "the open set must actually be empty afterwards");
    }

    /// A symbol filter must be honoured: cancelling one symbol must leave the
    /// other symbol's order resting, or a strategy scoping its own cancel would
    /// flatten a position it does not own.
    #[tokio::test]
    async fn cancel_all_honours_the_symbol_filter() {
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;
        trading_client.create_order(operator_order("BTC/USDT", "grid-btc")).await.expect("btc");
        trading_client.create_order(operator_order("ETH/USDT", "grid-eth")).await.expect("eth");

        let canceled = trading_client
            .cancel_all_orders(trading::CancelAllOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: "BTC/USDT".to_string(),
                ..Default::default()
            })
            .await
            .expect("cancel all")
            .into_owned();
        assert_eq!(canceled.orders.len(), 1);
        assert_eq!(canceled.orders[0].symbol, "BTC/USDT");

        let survivors = trading_client
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: "ETH/USDT".to_string(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("open orders")
            .into_owned();
        assert_eq!(survivors.orders.len(), 1, "an unrelated symbol must be left alone");
        assert_eq!(survivors.orders[0].symbol, "ETH/USDT");
    }

    /// The account balance must be the venue's number, not the worker's opinion
    /// of it — an SMM strategy sizes every order off this field.
    #[tokio::test]
    async fn account_reads_come_from_the_venue() {
        use rust_decimal_macros::dec;
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;

        let resp = trading_client
            .get_account(trading::GetAccountRequest {
                exchange_id: mock_exchange(),
                ..Default::default()
            })
            .await
            .expect("account")
            .into_owned();
        let account = resp.account.as_option().expect("account present");
        // The mock prices at 100 and reports balance = price * 1000.
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(
                account.balance.as_option().expect("balance")
            )
            .expect("decodes"),
            dec!(100000),
        );
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(
                account.free_margin.as_option().expect("free margin")
            )
            .expect("decodes"),
            dec!(100000),
        );
    }

    /// A backend that simply has no history must answer with an empty result, not
    /// a server fault: an `internal` here would be retried forever against a
    /// permanent answer.
    #[tokio::test]
    async fn history_and_position_reads_return_empty_rather_than_failing() {
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;

        let positions = trading_client
            .get_positions(trading::GetPositionsRequest {
                exchange_id: mock_exchange(),
                symbols: vec![],
                ..Default::default()
            })
            .await
            .expect("positions")
            .into_owned();
        assert!(positions.positions.is_empty());

        let history = trading_client
            .get_order_history(trading::GetOrderHistoryRequest {
                exchange_id: mock_exchange(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("order history")
            .into_owned();
        assert!(history.orders.is_empty());

        let closed = trading_client
            .get_closed_positions(trading::GetClosedPositionsRequest {
                exchange_id: mock_exchange(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("closed positions")
            .into_owned();
        assert!(closed.positions.is_empty());

        trading_client
            .close_all_positions(trading::CloseAllPositionsRequest {
                exchange_id: mock_exchange(),
                ..Default::default()
            })
            .await
            .expect("closing zero positions is a success, not a fault");
    }

    /// A position the venue does not hold is the caller's mistake, and the answer
    /// has to say so: an empty success would read as "position closed" and the
    /// strategy would move on believing it is flat.
    #[tokio::test]
    async fn closing_or_modifying_an_unknown_position_is_an_invalid_argument() {
        let (_adapter, trading_client, _market, _ops) = boot_with_capabilities().await;

        let err = trading_client
            .close_position(trading::ClosePositionRequest {
                exchange_id: mock_exchange(),
                position_id: "no-such-position".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("an unknown position id");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");

        let err = trading_client
            .modify_position(trading::ModifyPositionRequest {
                exchange_id: mock_exchange(),
                position_id: "no-such-position".to_string(),
                take_profit: buffa::MessageField::some(
                    longtrader_contract::ext::decimal_to_common(rust_decimal_macros::dec!(120)),
                ),
                stop_loss: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect_err("an unknown position id");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");
    }

    /// A batch is attributed exactly like a single order, and every leg must
    /// carry the venue's own id — that id is what the kill-switch cancels by.
    #[tokio::test]
    async fn a_batch_submission_is_attributed_to_its_session() {
        let (session_client, trading_client) = boot_session_with_capabilities().await;
        let session_id = active_session(&session_client).await;

        let created = trading_client
            .create_orders(trading::CreateOrdersRequest {
                exchange_id: mock_exchange(),
                orders: vec![batch_leg("grid-1"), batch_leg("grid-2")],
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("batch")
            .into_owned();
        assert_eq!(created.orders.len(), 2);
        assert!(
            created.orders.iter().all(|o| !o.id.is_empty()),
            "every leg must carry the venue's id or the kill-switch cannot cancel it"
        );

        let status = session_client
            .strategy_status(worker::StrategyStatusRequest { session_id, ..Default::default() })
            .await
            .expect("status")
            .into_owned();
        assert_eq!(status.orders_submitted, 2, "the whole batch must be attributed");
    }

    /// The gate is all-or-nothing for a batch and runs before the first leg
    /// reaches the venue — otherwise a pre-ACTIVE session could place half a
    /// batch and the remaining half would be silently unattributed.
    #[tokio::test]
    async fn a_pre_active_batch_is_rejected_whole() {
        let (session_client, trading_client) = boot_session_with_capabilities().await;
        let attached = session_client
            .attach_session(worker::AttachSessionRequest::default())
            .await
            .expect("attach")
            .into_owned();

        let err = trading_client
            .create_orders(trading::CreateOrdersRequest {
                exchange_id: mock_exchange(),
                orders: vec![batch_leg("grid-1"), batch_leg("grid-2")],
                session_id: attached.session_id.clone(),
                ..Default::default()
            })
            .await
            .expect_err("a pre-ACTIVE session must not place a batch");
        assert_eq!(err.code, connectrpc::ErrorCode::FailedPrecondition, "{err:?}");
        assert!(
            err.details.iter().any(|d| d.type_url == "longtrader.common.v1.ErrorDetail"),
            "the typed reason must survive to the wire: {err:?}"
        );

        let open = trading_client
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: String::new(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("open orders")
            .into_owned();
        assert!(open.orders.is_empty(), "a rejected batch must not leave an order behind");
    }

    /// When the venue rejects a batch the RPC fails and the session is credited
    /// with nothing: a session that believes it submitted two orders it never
    /// got would size its next cancel wrongly.
    #[tokio::test]
    async fn a_failed_batch_attributes_nothing_to_the_session() {
        let (adapter, session_client, trading_client) = boot_scripted().await;
        let session_id = active_session(&session_client).await;
        adapter.fail_next_creates(1).await;

        let err = trading_client
            .create_orders(trading::CreateOrdersRequest {
                exchange_id: mock_exchange(),
                orders: vec![batch_leg("grid-1"), batch_leg("grid-2")],
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect_err("the scripted venue failure must surface");
        // The mock scripts an HTTP-flavoured `500`, which is not a gRPC status
        // code, so the mapper degrades it to `internal` rather than guessing.
        assert_eq!(err.code, connectrpc::ErrorCode::Internal, "{err:?}");

        let status = session_client
            .strategy_status(worker::StrategyStatusRequest { session_id, ..Default::default() })
            .await
            .expect("status")
            .into_owned();
        assert_eq!(status.orders_submitted, 0, "a failed batch must not be attributed");
    }

    // ---- worker.v1 RPCs ----------------------------------------------------

    #[tokio::test]
    async fn keep_alive_reports_the_negotiated_interval_and_rejects_an_unknown_session() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;

        let resp = session_client
            .keep_alive(worker::KeepAliveRequest {
                session_id: session_id.clone(),
                client_time_ns: 0,
                ..Default::default()
            })
            .await
            .expect("keep alive")
            .into_owned();
        assert_eq!(resp.heartbeat_interval_ms, crate::session::DEFAULT_HEARTBEAT_MS);
        assert!(resp.server_time_ns > 0, "the host must stamp its own clock");

        let err = session_client
            .keep_alive(worker::KeepAliveRequest {
                session_id: "no-such-session".to_string(),
                client_time_ns: 0,
                ..Default::default()
            })
            .await
            .expect_err("a heartbeat for a session that does not exist");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    /// `SetKillSwitchPolicy` must refuse what it cannot route or honour: an
    /// unroutable scope discriminant and an out-of-bounds lease are both the
    /// caller's mistake, so both are `invalid_argument` rather than a silent
    /// no-op that leaves the session on a policy its owner never chose.
    #[tokio::test]
    async fn set_kill_switch_policy_rejects_an_unknown_scope_and_a_bad_lease() {
        /// Send one policy for `session_id`; `lease_ms = None` leaves the lease
        /// untouched, `None` scope means "leave the scope untouched".
        async fn set_policy(
            client: &worker::WorkerSessionServiceClient<connectrpc::client::HttpClient>,
            session_id: &str,
            lease_ms: Option<u64>,
            scope: buffa::EnumValue<worker::kill_switch_policy::Scope>,
        ) -> Result<(), connectrpc::ConnectError> {
            client
                .set_kill_switch_policy(worker::SetKillSwitchPolicyRequest {
                    session_id: session_id.to_string(),
                    policy: buffa::MessageField::some(worker::KillSwitchPolicy {
                        lease_timeout: lease_field(lease_ms),
                        scope,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .map(|_| ())
        }

        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;

        set_policy(
            &session_client,
            &session_id,
            Some(600),
            buffa::EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders),
        )
        .await
        .expect("a well-formed policy must be accepted");

        // An explicitly unspecified scope means "leave unchanged"; treating it as
        // a reset would silently downgrade a chosen kill-switch scope.
        set_policy(
            &session_client,
            &session_id,
            None,
            buffa::EnumValue::Known(worker::kill_switch_policy::Scope::Unspecified),
        )
        .await
        .expect("Unspecified must mean leave-unchanged, not a reset");

        let err = set_policy(&session_client, &session_id, None, buffa::EnumValue::Unknown(99))
            .await
            .expect_err("an unroutable scope discriminant");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");

        // 400ms is below the documented 500ms DoS floor.
        let err = set_policy(
            &session_client,
            &session_id,
            Some(400),
            buffa::EnumValue::Known(worker::kill_switch_policy::Scope::SessionOrders),
        )
        .await
        .expect_err("a lease below the documented floor");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");

        let err = session_client
            .set_kill_switch_policy(worker::SetKillSwitchPolicyRequest {
                session_id: "no-such-session".to_string(),
                policy: buffa::MessageField::some(worker::KillSwitchPolicy {
                    lease_timeout: lease_field(None),
                    scope: buffa::EnumValue::Known(worker::kill_switch_policy::Scope::None),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect_err("a policy for a session that does not exist");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    #[tokio::test]
    async fn register_strategy_publishes_the_identity_status_reports() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;

        let before = session_client
            .strategy_status(worker::StrategyStatusRequest {
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("status")
            .into_owned();
        assert!(before.strategy_id.is_empty(), "no strategy has registered yet");
        assert_eq!(before.state, buffa::EnumValue::Known(worker::SessionState::Active));

        let registered = session_client
            .register_strategy(worker::RegisterStrategyRequest {
                session_id: session_id.clone(),
                name: "grid-e2e".to_string(),
                params: std::iter::once(("symbol".to_string(), "BTC/USDT".to_string())).collect(),
                ..Default::default()
            })
            .await
            .expect("register")
            .into_owned();
        assert!(!registered.strategy_id.is_empty(), "the host must issue a strategy id");

        let after = session_client
            .strategy_status(worker::StrategyStatusRequest { session_id, ..Default::default() })
            .await
            .expect("status")
            .into_owned();
        assert_eq!(after.strategy_id, registered.strategy_id);
        assert_eq!(after.name, "grid-e2e");
        assert!(
            after.started_at.as_option().is_some(),
            "a registered strategy must carry a start time"
        );
    }

    /// The RPC-level stop must cancel the session's orders, report the state it
    /// actually reached, and be safe to repeat — a supervisor that retries must
    /// not be able to knock a stopped session out of its terminal state.
    #[tokio::test]
    async fn stop_strategy_cancels_the_sessions_orders_and_is_repeatable() {
        let (session_client, trading_client) = boot_session_with_capabilities().await;
        let session_id = active_session(&session_client).await;
        let created =
            trading_client.create_order(order_req(&session_id)).await.expect("place").into_owned();
        let order = created.order.as_option().expect("order").clone();

        let stopped = session_client
            .stop_strategy(worker::StopStrategyRequest {
                session_id: session_id.clone(),
                cancel_open_orders: true,
                ..Default::default()
            })
            .await
            .expect("stop")
            .into_owned();
        assert_eq!(
            stopped.final_state,
            buffa::EnumValue::Known(worker::SessionState::GracefulShutdown),
        );

        let open = trading_client
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: mock_exchange(),
                symbol: String::new(),
                pagination: buffa::MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("open orders")
            .into_owned();
        assert!(
            !open.orders.iter().any(|o| o.id == order.id),
            "StopStrategy(cancel_open_orders) must reach the venue"
        );

        let again = session_client
            .stop_strategy(worker::StopStrategyRequest {
                session_id: session_id.clone(),
                cancel_open_orders: false,
                ..Default::default()
            })
            .await
            .expect("a repeated stop must not fail")
            .into_owned();
        assert_eq!(
            again.final_state,
            buffa::EnumValue::Known(worker::SessionState::GracefulShutdown),
            "the final state must stay terminal across a repeated stop"
        );

        let err = session_client
            .stop_strategy(worker::StopStrategyRequest {
                session_id: "no-such-session".to_string(),
                cancel_open_orders: false,
                ..Default::default()
            })
            .await
            .expect_err("stopping a session that does not exist");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    /// `ReportLog` is the strategy's only log channel, so the accepted counter
    /// has to be honest, and a log addressed to a session that does not exist
    /// must not be swallowed into a success.
    #[tokio::test]
    async fn report_log_counts_what_it_accepted_and_refuses_an_unknown_session() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;
        session_client
            .register_strategy(worker::RegisterStrategyRequest {
                session_id: session_id.clone(),
                name: "grid-e2e".to_string(),
                params: Default::default(),
                ..Default::default()
            })
            .await
            .expect("register");

        let resp = session_client
            .report_log(connectrpc::stream_iter(vec![
                log_event(&session_id, worker::LogLevel::Info, "first"),
                log_event(&session_id, worker::LogLevel::Error, "second"),
                // An enum discriminant the host does not know. The wire value
                // survives, so the host — not the codec — has to decide.
                worker::LogEvent {
                    session_id: session_id.clone(),
                    level: buffa::EnumValue::Unknown(99),
                    message: "third".to_string(),
                    ..Default::default()
                },
            ]))
            .await
            .expect("report log")
            .into_owned();
        assert_eq!(resp.accepted, 3, "every event in the stream must be counted");

        let status = session_client
            .strategy_status(worker::StrategyStatusRequest {
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("status")
            .into_owned();
        assert_eq!(status.log_events, 3, "every accepted event must be counted on the session");

        // An unroutable level becomes `Unspecified` rather than being dropped or
        // recorded as a level the strategy never sent.
        let mut stream = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id: session_id.clone(),
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let mut logs = Vec::new();
        for _ in 0..8 {
            let next =
                tokio::time::timeout(STREAM_IDLE, stream.message::<worker::StrategyEvent>()).await;
            let Ok(Ok(Some(message))) = next else { break };
            let event = message.to_owned_message();
            if let Some(worker::strategy_event::Event::Log(log)) = event.event {
                logs.push(log);
            }
        }
        drop(stream);
        let levels: Vec<buffa::EnumValue<worker::LogLevel>> =
            logs.iter().map(|l| l.level).collect();
        assert_eq!(
            levels,
            vec![
                buffa::EnumValue::Known(worker::LogLevel::Info),
                buffa::EnumValue::Known(worker::LogLevel::Error),
                buffa::EnumValue::Known(worker::LogLevel::Unspecified),
            ]
        );
        let messages: Vec<&str> = logs.iter().map(|l| l.message.as_str()).collect();
        assert_eq!(messages, vec!["first", "second", "third"], "the stream must stay ordered");

        let err = session_client
            .report_log(connectrpc::stream_iter(vec![log_event(
                "no-such-session",
                worker::LogLevel::Info,
                "orphan",
            )]))
            .await
            .expect_err("a log for a session that does not exist");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err:?}");
    }

    /// A resume token is the cursor a reconnecting strategy hands back, so one
    /// the host cannot parse must be rejected. Treating it as "start from
    /// scratch" would silently replay every event the client already applied.
    #[tokio::test]
    async fn a_non_numeric_resume_token_is_rejected() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;

        // A server-streaming RPC opens successfully and surfaces the failure on
        // the first read, so the rejection has to be asserted there.
        let mut stream = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id,
                resume_token: "not-a-sequence".to_string(),
                ..Default::default()
            })
            .await
            .expect("the stream opens");
        let err = stream.message::<worker::StrategyEvent>().await.expect_err("a bad token");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument, "{err:?}");
        assert!(
            err.message.as_deref().unwrap_or_default().contains("resume_token"),
            "the message must name the offending field: {err:?}"
        );
    }

    /// Replay from a valid token resumes *after* it and stays gap-free, so the
    /// watermark the client is handed can be trusted for a full `ReconcileState`.
    #[tokio::test]
    async fn a_valid_resume_token_replays_only_what_the_client_missed() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;
        for i in 0..4 {
            session_client
                .report_log(connectrpc::stream_iter(vec![log_event(
                    &session_id,
                    worker::LogLevel::Info,
                    &format!("m{i}"),
                )]))
                .await
                .expect("report log");
        }

        // From the beginning: the two reconcile transitions plus four logs.
        let mut all = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id: session_id.clone(),
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let mut sequences = Vec::new();
        for _ in 0..6 {
            let next =
                tokio::time::timeout(STREAM_IDLE, all.message::<worker::StrategyEvent>()).await;
            let Ok(Ok(Some(message))) = next else { break };
            sequences.push(message.to_owned_message().header.sequence);
        }
        drop(all);
        assert_eq!(sequences, vec![1, 2, 3, 4, 5, 6], "the ring must replay in order");

        let mut tail = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id: session_id.clone(),
                resume_token: "4".to_string(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let mut tail_sequences = Vec::new();
        for _ in 0..4 {
            let next =
                tokio::time::timeout(STREAM_IDLE, tail.message::<worker::StrategyEvent>()).await;
            let Ok(Ok(Some(message))) = next else { break };
            tail_sequences.push(message.to_owned_message().header.sequence);
        }
        drop(tail);
        assert_eq!(tail_sequences, vec![5, 6], "a resume must start strictly after the token");
        assert!(
            !crate::session::SessionHandle::is_gap(4, tail_sequences[0]),
            "a replay from a live ring must be gap-free"
        );

        // A token past the end of the ring replays nothing rather than wrapping
        // or fabricating an event the client never missed.
        let mut beyond = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id,
                resume_token: "99".to_string(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let read =
            tokio::time::timeout(STREAM_IDLE, beyond.message::<worker::StrategyEvent>()).await;
        drop(beyond);
        assert!(read.is_err(), "a token past the ring's end must replay nothing: {read:?}");
    }

    /// Abandoning a stream is the normal case, not an exception: a strategy that
    /// crashes mid-stream must not take the session or the router with it, and a
    /// later subscriber must still see the backlog plus everything after it.
    #[tokio::test]
    async fn abandoning_an_event_stream_leaves_the_session_usable() {
        let (session_client, _trading) = boot().await;
        let session_id = active_session(&session_client).await;

        let mut stream = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id: session_id.clone(),
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("stream");
        // Drain the replay so the stream is parked on the broadcast receiver,
        // which is the state a crashed strategy would leave behind.
        for _ in 0..2 {
            let _ =
                tokio::time::timeout(STREAM_IDLE, stream.message::<worker::StrategyEvent>()).await;
        }
        drop(stream);

        let status = session_client
            .strategy_status(worker::StrategyStatusRequest {
                session_id: session_id.clone(),
                ..Default::default()
            })
            .await
            .expect("the session must still answer after a dropped stream")
            .into_owned();
        assert_eq!(status.state, buffa::EnumValue::Known(worker::SessionState::Active));

        session_client
            .report_log(connectrpc::stream_iter(vec![log_event(
                &session_id,
                worker::LogLevel::Warn,
                "after-disconnect",
            )]))
            .await
            .expect("report log");

        let mut again = session_client
            .stream_strategy_events(worker::StreamStrategyEventsRequest {
                session_id,
                resume_token: String::new(),
                ..Default::default()
            })
            .await
            .expect("stream");
        let mut received = 0usize;
        for _ in 0..4 {
            let next =
                tokio::time::timeout(STREAM_IDLE, again.message::<worker::StrategyEvent>()).await;
            let Ok(Ok(Some(_))) = next else { break };
            received += 1;
        }
        drop(again);
        assert_eq!(received, 3, "two reconcile transitions plus the post-disconnect log");
    }
}
