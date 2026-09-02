//! Open-source `RemoteAdapter`: speaks the public contract (`longtrader.*.v1`)
//! directly via Connect `application/proto` over `hpx`.
//!
//! The legacy `tradingcharts.terminal.v1` translation layer has been removed
//! for the open source repo; every method forwards the contract DTO without
//! string-decimal parsing or `terminal_core` timeframe mapping. Timeframes
//! are already strings in `market::GetCandlesRequest`, so no conversion is
//! needed. Unsupported extended capabilities surface as
//! [`PortError::Unsupported`] (see `docs/full-catalog-migration-plan.md` §P2).

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use buffa::{Message, MessageField};
use rust_decimal::Decimal;

use crate::{
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
        .map(|d| i64::try_from(d.as_millis()).unwrap_or_default())
        .unwrap_or_default()
}

/// Minimal Connect client over `hpx` – mirrors the former `TerminalClient`
/// but targets the open `longtrader.*.v1` services. One `RemoteAdapter`
/// per config, cloned as `Arc<dyn TradingGateway + MarketDataSource>`.
pub struct RemoteAdapter {
    base_url: String,
    token: String,
    http: hpx::Client,
}

impl RemoteAdapter {
    #[must_use]
    pub fn new(base_url: &str, token: &str) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        let http = match hpx::Client::builder().http1_only().build() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("hpx build failed, using default client: {e}");
                hpx::Client::new()
            }
        };
        Self { base_url, token: token.to_string(), http }
    }

    fn url(&self, service: &str, method: &str) -> String {
        format!("{}/{}/{}", self.base_url, service, method)
    }

    async fn unary<Q: Message, R: Message + Default>(
        &self,
        service: &str,
        method: &str,
        req: Q,
    ) -> Result<R, PortError> {
        let url = self.url(service, method);
        let body = req.encode_to_vec();
        let mut builder =
            self.http.post(&url).header("content-type", "application/proto").body(body);
        if !self.token.is_empty() {
            builder = builder.header("authorization", format!("Bearer {}", self.token));
        }
        let resp = builder.send().await.map_err(|e| PortError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let bytes =
                resp.bytes().await.map_err(|e| PortError::Transport(format!("read error: {e}")))?;
            let text = String::from_utf8_lossy(&bytes).to_string();
            return Err(PortError::Rpc { code: u32::from(status.as_u16()), message: text });
        }
        let bytes =
            resp.bytes().await.map_err(|e| PortError::Transport(format!("read error: {e}")))?;
        R::decode_from_slice(&bytes)
            .map_err(|e| PortError::Transport(format!("decode {method}: {e}")))
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
            snapshot_sequence: 0,
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
        _req: market::StreamMarketDataRequest,
        _policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError> {
        Err(PortError::Unsupported(
            "open RemoteAdapter does not yet proxy StreamMarketData; poll fetch_ticker/get_candles instead".to_string(),
        ))
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
