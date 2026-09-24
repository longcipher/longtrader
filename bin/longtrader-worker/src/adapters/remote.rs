//! Open-source `RemoteAdapter`: speaks the public contract (`longtrader.*.v1`)
//! directly via Connect `application/proto` over `hpx`.
//!
//! The legacy `tradingcharts.terminal.v1` translation layer has been removed
//! for the open source repo; every method forwards the contract DTO without
//! string-decimal parsing or `terminal_core` timeframe mapping. Timeframes
//! are already strings in `market::GetCandlesRequest`, so no conversion is
//! needed. Unsupported extended capabilities surface as
//! [`PortError::Unsupported`] (see `docs/full-catalog-migration-plan.md` §P2).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::{Message, MessageField};
use rust_decimal::Decimal;

use crate::{
    overflow,
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, trading, worker},
};

const SERVICE_TRADING: &str = "longtrader.trading.v1.TradingService";
const SERVICE_MARKET: &str = "longtrader.market.v1.MarketDataService";

fn ts_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or_default(),
        ..Default::default()
    }
}

fn ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_default(|d| i64::try_from(d.as_millis()).unwrap_or_default())
}

/// Minimal Connect client over `hpx` – mirrors the former `TerminalClient`
/// but targets the open `longtrader.*.v1` services. One `RemoteAdapter`
/// per config, cloned as `Arc<dyn TradingGateway + MarketDataSource + ...>`.
///
/// A single adapter implements every strategy-facing port, so the worker keeps
/// exactly one backend connection per session.
#[derive(Clone)]
pub struct RemoteAdapter {
    base_url: String,
    token: String,
    http: hpx::Client,
    /// Monotonic snapshot watermark source for deterministic recovery
    /// (`ReconcileStateResponse.snapshot_sequence`, design doc §6.3).
    snapshot_seq: Arc<AtomicU64>,
}

impl RemoteAdapter {
    #[must_use]
    pub fn new(base_url: &str, token: &str) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        // HTTP/2 is enabled so the same client can carry server-streaming
        // (`StreamMarketData`/`StreamUpdates`) once the backend exposes it;
        // unary calls work over either version.
        let http = match hpx::Client::builder().build() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("hpx build failed, using default client: {e}");
                hpx::Client::new()
            }
        };
        Self { base_url, token: token.to_string(), http, snapshot_seq: Arc::new(AtomicU64::new(0)) }
    }

    async fn unary<Q: Message, R: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: Q,
    ) -> Result<R, PortError> {
        longtrader_proto::transport::unary(
            &self.http,
            &self.base_url,
            service,
            method,
            &self.token,
            req,
        )
        .await
        .map_err(|e| match e {
            longtrader_proto::transport::TransportError::Http(m) => PortError::Transport(m),
            longtrader_proto::transport::TransportError::Rpc { code, message } => {
                PortError::Rpc { code, message }
            }
            longtrader_proto::transport::TransportError::Decode(m) => {
                PortError::Transport(format!("decode {method}: {m}"))
            }
        })
    }
}

#[async_trait]
impl TradingGateway for RemoteAdapter {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let resp: trading::CreateOrderResponse =
            self.unary(SERVICE_TRADING, "CreateOrder", req).await?;
        resp.order.into_option().ok_or_else(|| PortError::MissingField("order".to_string()))
    }

    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::CreateOrdersResponse =
            self.unary(SERVICE_TRADING, "CreateOrders", req).await?;
        Ok(resp.orders)
    }

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let resp: trading::CancelOrderResponse =
            self.unary(SERVICE_TRADING, "CancelOrder", req).await?;
        resp.order.into_option().ok_or_else(|| PortError::MissingField("order".to_string()))
    }

    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::CancelAllOrdersResponse =
            self.unary(SERVICE_TRADING, "CancelAllOrders", req).await?;
        Ok(resp.orders)
    }

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let resp: trading::FetchOpenOrdersResponse =
            self.unary(SERVICE_TRADING, "FetchOpenOrders", req).await?;
        Ok(resp.orders)
    }

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError> {
        let resp: trading::GetAccountResponse =
            self.unary(SERVICE_TRADING, "GetAccount", req).await?;
        Ok(resp)
    }

    async fn get_positions(
        &self,
        req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError> {
        let resp: trading::GetPositionsResponse =
            self.unary(SERVICE_TRADING, "GetPositions", req).await?;
        Ok(resp)
    }

    async fn get_order_history(
        &self,
        req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError> {
        let resp: trading::GetOrderHistoryResponse =
            self.unary(SERVICE_TRADING, "GetOrderHistory", req).await?;
        Ok(resp)
    }

    async fn get_closed_positions(
        &self,
        req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError> {
        let resp: trading::GetClosedPositionsResponse =
            self.unary(SERVICE_TRADING, "GetClosedPositions", req).await?;
        Ok(resp)
    }

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError> {
        let resp: trading::ClosePositionResponse =
            self.unary(SERVICE_TRADING, "ClosePosition", req).await?;
        Ok(resp)
    }

    async fn close_all_positions(
        &self,
        req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError> {
        let resp: trading::CloseAllPositionsResponse =
            self.unary(SERVICE_TRADING, "CloseAllPositions", req).await?;
        Ok(resp)
    }

    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        // Aggregate three contract calls into one atomic-ish snapshot.
        // The snapshot watermark is `ms_now`; deltas after that will be replayed.
        let account_resp: trading::GetAccountResponse = self
            .unary(
                SERVICE_TRADING,
                "GetAccount",
                trading::GetAccountRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;
        let account = account_resp.account.into_option().unwrap_or_default();
        // Project aggregate `trading::Account` onto a single `account::Balance` (USD) – matches
        // the old terminal adapter's projection, preserving strategy expectations until a
        // proper `AccountService` is exposed.
        let balances = vec![account::Balance {
            currency: "USD".to_string(),
            free: account.free_margin.clone(),
            used: account.margin_used.clone(),
            total: account.equity.clone(),
            ..Default::default()
        }];

        let positions_resp: trading::GetPositionsResponse = self
            .unary(
                SERVICE_TRADING,
                "GetPositions",
                trading::GetPositionsRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;

        let open_orders_resp: trading::FetchOpenOrdersResponse = self
            .unary(
                SERVICE_TRADING,
                "FetchOpenOrders",
                trading::FetchOpenOrdersRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    ..Default::default()
                },
            )
            .await?;

        Ok(worker::ReconcileStateResponse {
            // Monotonic watermark so the session can discard already-applied
            // deltas after the snapshot point (design doc §6.3).
            snapshot_sequence: self.snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1,
            snapshot_time: MessageField::some(ts_from_ms(ms_now())),
            balances,
            positions: positions_resp.positions,
            open_orders: open_orders_resp.orders,
            ..Default::default()
        })
    }
}

#[async_trait]
impl MarketDataSource for RemoteAdapter {
    async fn fetch_ticker(
        &self,
        req: market::FetchTickerRequest,
    ) -> Result<market::Ticker, PortError> {
        let resp: market::FetchTickerResponse =
            self.unary(SERVICE_MARKET, "FetchTicker", req).await?;
        resp.ticker.into_option().ok_or_else(|| PortError::MissingField("ticker".to_string()))
    }

    async fn fetch_order_book(
        &self,
        req: market::FetchOrderBookRequest,
    ) -> Result<market::OrderBook, PortError> {
        let resp: market::FetchOrderBookResponse =
            self.unary(SERVICE_MARKET, "FetchOrderBook", req).await?;
        resp.orderbook.into_option().ok_or_else(|| PortError::MissingField("orderbook".to_string()))
    }

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError> {
        let resp: market::GetCandlesResponse =
            self.unary(SERVICE_MARKET, "GetCandles", req).await?;
        Ok(resp)
    }

    async fn list_symbols(
        &self,
        req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError> {
        let resp: market::ListSymbolsResponse =
            self.unary(SERVICE_MARKET, "ListSymbols", req).await?;
        Ok(resp)
    }

    async fn subscribe_market_data(
        &self,
        req: market::StreamMarketDataRequest,
        policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError> {
        // Backed by periodic unary fetches through a backpressure-aware policy
        // channel, realising the design-doc per-session isolation until the
        // backend exposes server-streaming. Each subscription polls its snapshot
        // on a fixed cadence; the channel applies `policy` (DropOldest / Coalesce
        // / Block) so a slow consumer never stalls the worker or other sessions.
        const POLL_INTERVAL_MS: u64 = 1_000;
        const BUFFER_CAP: usize = 16;

        let seq = Arc::new(AtomicU64::new(0));
        let (tx, rx) =
            overflow::policy_channel(BUFFER_CAP, policy, |event: &market::MarketDataEvent| {
                event.header.sequence
            });

        for sub in req.subscriptions {
            let channel = match sub.channel {
                buffa::EnumValue::Known(c) => c,
                buffa::EnumValue::Unknown(_) => market::StreamChannel::Unspecified,
            };
            let symbol = sub.symbol;
            if symbol.is_empty() {
                continue;
            }
            let exchange_id = req.exchange_id.clone();
            let adapter = self.clone();
            let tx = tx.clone();
            let seq = Arc::clone(&seq);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(POLL_INTERVAL_MS));
                loop {
                    ticker.tick().await;
                    match adapter.poll_snapshot(channel, &exchange_id, &symbol, &seq).await {
                        Ok(Some(event)) => {
                            tx.send(event).await;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, symbol = %symbol, "market poll failed");
                        }
                    }
                }
            });
        }
        Ok(rx)
    }
}

impl RemoteAdapter {
    /// Fetch one snapshot for a subscription channel and wrap it as a
    /// `MarketDataEvent` with a monotonically increasing `header.sequence`.
    async fn poll_snapshot(
        &self,
        channel: market::StreamChannel,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        seq: &AtomicU64,
    ) -> Result<Option<market::MarketDataEvent>, PortError> {
        let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
        let header = common::EventHeader { sequence: next, ..Default::default() };
        // Ticker is the sensible default for any non-orderbook channel;
        // trades/ohlcv are not independently fetchable here, so they reuse
        // the ticker feed.
        let event = if channel == market::StreamChannel::Orderbook {
            let book = self
                .fetch_order_book(market::FetchOrderBookRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.to_string(),
                    limit: 100,
                    ..Default::default()
                })
                .await?;
            market::market_data_event::Event::Orderbook(Box::new(book))
        } else {
            let ticker = self
                .fetch_ticker(market::FetchTickerRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.to_string(),
                    ..Default::default()
                })
                .await?;
            market::market_data_event::Event::Ticker(Box::new(ticker))
        };
        Ok(Some(market::MarketDataEvent {
            header: MessageField::some(header),
            event: Some(event),
            resume_token: String::new(),
            ..Default::default()
        }))
    }
}

// ---------------------------------------------------------------------------
// Extended capability ports – still unsupported on the open backend; each
// returns `Unsupported` so strategies degrade gracefully.
// ---------------------------------------------------------------------------

use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TriggerOrderGateway,
    TriggerOrderRequest, VenueOpInvoker, WalletGateway,
};

fn unsupported(capability: &str) -> PortError {
    PortError::Unsupported(format!(
        "open backend does not expose '{capability}' yet; wire a venue-native daemon or mock"
    ))
}

#[async_trait]
impl FundingRateSource for RemoteAdapter {
    async fn fetch_funding_rate(
        &self,
        _exchange_id: &common::ExchangeId,
        _symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError> {
        Err(unsupported("fetch_funding_rate"))
    }
    async fn fetch_funding_rate_history(
        &self,
        _exchange_id: &common::ExchangeId,
        _symbol: &str,
        _limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError> {
        Err(unsupported("fetch_funding_rate_history"))
    }
}

#[async_trait]
impl TriggerOrderGateway for RemoteAdapter {
    async fn create_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        _req: TriggerOrderRequest,
    ) -> Result<String, PortError> {
        Err(unsupported("create_trigger_order"))
    }
    async fn cancel_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        _order_id: &str,
        _symbol: &str,
    ) -> Result<(), PortError> {
        Err(unsupported("cancel_trigger_order"))
    }
}

#[async_trait]
impl VenueOpInvoker for RemoteAdapter {
    async fn invoke_venue_op(
        &self,
        _exchange_id: &common::ExchangeId,
        op: &str,
        _params: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, PortError> {
        Err(unsupported(&format!("invoke_venue_op({op})")))
    }
    async fn list_venue_ops(
        &self,
        _exchange_id: &common::ExchangeId,
    ) -> Result<Vec<String>, PortError> {
        Err(unsupported("list_venue_ops"))
    }
}

#[async_trait]
impl WalletGateway for RemoteAdapter {
    async fn fetch_deposits(
        &self,
        _exchange_id: &common::ExchangeId,
        _limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        Err(unsupported("fetch_deposits"))
    }
    async fn transfer(
        &self,
        _exchange_id: &common::ExchangeId,
        _asset: &str,
        _amount: Decimal,
        _dest_label: &str,
    ) -> Result<(), PortError> {
        Err(unsupported("transfer"))
    }
}
