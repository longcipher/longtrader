//! Deterministic in-process fake for tests and dry-run mode.
//!
//! Orders are echoed back with `Open` status and tracked locally; market
//! data emits synthetic tickers around a fixed price so overflow policies
//! can be exercised end-to-end without any network.

use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::{EnumValue, MessageField};
use longtrader_contract::ext::decimal_to_common;
use rust_decimal::Decimal;
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::{
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, trading, worker},
};

#[derive(Debug, Default)]
struct State {
    price: Decimal,
    orders: Vec<trading::Order>,
    fail_next_creates: u32,
    funding_rate: Decimal,
    op_balance: Decimal,
    deposits: Vec<LedgerEntry>,
    transfers: Vec<(String, Decimal, String)>,
    trigger_orders: Vec<(String, TriggerOrderRequest)>,
}

fn now_ts() -> buffa_types::google::protobuf::Timestamp {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    buffa_types::google::protobuf::Timestamp {
        seconds: i64::try_from(dur.as_secs()).unwrap_or_default(),
        nanos: i32::try_from(dur.subsec_nanos()).unwrap_or_default(),
        ..Default::default()
    }
}

/// Deterministic fake implementing both ports.
pub struct MockAdapter {
    state: Mutex<State>,
    ids: AtomicU64,
}

impl MockAdapter {
    /// Create a mock whose ticker price starts at `initial_price`.
    pub fn new(initial_price: Decimal) -> Self {
        Self {
            state: Mutex::new(State { price: initial_price, ..State::default() }),
            ids: AtomicU64::new(1),
        }
    }

    /// Script the next `n` order creations to fail (for error-path tests).
    pub fn fail_next_creates(&self, n: u32) {
        self.state.lock().expect("mock state").fail_next_creates = n;
    }
}

#[async_trait]
impl TradingGateway for MockAdapter {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let mut state = self.state.lock().expect("mock state");
        if state.fail_next_creates > 0 {
            state.fail_next_creates -= 1;
            return Err(PortError::Rpc { code: 500, message: "scripted failure".to_string() });
        }
        let order_req =
            req.order.as_option().ok_or_else(|| PortError::MissingField("order".to_string()))?;
        let id = format!("mock-{}", self.ids.fetch_add(1, Ordering::Relaxed));
        let order = trading::Order {
            id,
            client_order_id: order_req.client_order_id.clone(),
            symbol: order_req.symbol.clone(),
            r#type: order_req.r#type,
            side: order_req.side,
            status: EnumValue::Known(trading::OrderStatus::Open),
            amount: order_req.amount.clone(),
            price: order_req.price.clone(),
            filled: MessageField::some(decimal_to_common(Decimal::ZERO)),
            remaining: order_req.amount.clone(),
            cost: MessageField::some(decimal_to_common(Decimal::ZERO)),
            average: MessageField::none(),
            fee: MessageField::none(),
            fee_currency: String::new(),
            time_in_force: order_req.time_in_force,
            timestamp: MessageField::some(now_ts()),
            last_trade_timestamp: MessageField::some(now_ts()),
            post_only: order_req.post_only,
            reduce_only: order_req.reduce_only,
            info: Default::default(),
            ..Default::default()
        };
        state.orders.push(order.clone());
        Ok(order)
    }

    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let mut placed = Vec::with_capacity(req.orders.len());
        for order in &req.orders {
            placed.push(
                self.create_order(trading::CreateOrderRequest {
                    exchange_id: req.exchange_id.clone(),
                    order: MessageField::some(order.clone()),
                    ..Default::default()
                })
                .await?,
            );
        }
        Ok(placed)
    }

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let mut state = self.state.lock().expect("mock state");
        let order =
            state.orders.iter_mut().find(|o| o.id == req.order_id).ok_or_else(|| {
                PortError::InvalidArgument(format!("unknown order {}", req.order_id))
            })?;
        order.status = EnumValue::Known(trading::OrderStatus::Canceled);
        Ok(order.clone())
    }

    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let mut state = self.state.lock().expect("mock state");
        let mut canceled = Vec::new();
        for order in &mut state.orders {
            let is_open = order.status == EnumValue::Known(trading::OrderStatus::Open);
            if is_open && (req.symbol.is_empty() || order.symbol == req.symbol) {
                order.status = EnumValue::Known(trading::OrderStatus::Canceled);
                canceled.push(order.clone());
            }
        }
        Ok(canceled)
    }

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok(state
            .orders
            .iter()
            .filter(|o| o.status == EnumValue::Known(trading::OrderStatus::Open))
            .filter(|o| req.symbol.is_empty() || o.symbol == req.symbol)
            .cloned()
            .collect())
    }

    async fn sync_state(
        &self,
        _exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok(worker::ReconcileStateResponse {
            snapshot_sequence: 0,
            snapshot_time: MessageField::some(now_ts()),
            balances: vec![account::Balance {
                currency: "USD".to_string(),
                free: MessageField::some(decimal_to_common(Decimal::from(10_000))),
                used: MessageField::some(decimal_to_common(Decimal::ZERO)),
                total: MessageField::some(decimal_to_common(Decimal::from(10_000))),
                ..Default::default()
            }],
            positions: Vec::new(),
            open_orders: state
                .orders
                .iter()
                .filter(|o| o.status == EnumValue::Known(trading::OrderStatus::Open))
                .cloned()
                .collect(),
            ..Default::default()
        })
    }

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError> {
        let _ = &req;
        let price = self.state.lock().expect("mock state").price;
        let d = |v: Decimal| longtrader_contract::ext::decimal_to_common(v);
        Ok(trading::GetAccountResponse {
            account: Some(trading::Account {
                balance: d(price * Decimal::from(1000)).into(),
                equity: d(price * Decimal::from(1000)).into(),
                margin_used: longtrader_contract::ext::decimal_to_common(Decimal::ZERO).into(),
                free_margin: d(price * Decimal::from(1000)).into(),
                margin_frozen: longtrader_contract::ext::decimal_to_common(Decimal::ZERO).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        })
    }

    async fn get_positions(
        &self,
        _req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError> {
        Ok(trading::GetPositionsResponse::default())
    }

    async fn get_order_history(
        &self,
        _req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError> {
        Ok(trading::GetOrderHistoryResponse::default())
    }

    async fn get_closed_positions(
        &self,
        _req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError> {
        Ok(trading::GetClosedPositionsResponse::default())
    }

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError> {
        Err(PortError::MissingField(format!("no open position {}", req.position_id)))
    }

    async fn close_all_positions(
        &self,
        _req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError> {
        Ok(trading::CloseAllPositionsResponse::default())
    }
}

#[async_trait]
impl MarketDataSource for MockAdapter {
    async fn list_symbols(
        &self,
        _req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError> {
        let price = self.state.lock().expect("mock state").price;
        let d = longtrader_contract::ext::decimal_to_common(price);
        let info = market::SymbolInfo {
            name: "MOCK-USDT".to_string(),
            display_name: "Mock / USDT".to_string(),
            base_asset: "MOCK".to_string(),
            quote_asset: "USDT".to_string(),
            contract_size: d.into(),
            tick_size: longtrader_contract::ext::decimal_to_common(rust_decimal_macros::dec!(0.01))
                .into(),
            ..Default::default()
        };
        Ok(market::ListSymbolsResponse { symbols: vec![info], ..Default::default() })
    }

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError> {
        let price = self.state.lock().expect("mock state").price;
        let d = longtrader_contract::ext::decimal_to_common(price);
        let now: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let n = if req.limit == 0 { 10 } else { i64::from(req.limit.min(1000)) };
        let candles = (0..n)
            .map(|i| market::Candle {
                timestamp_ms: (now - (n - i) * 60) * 1000,
                open: d.clone().into(),
                high: d.clone().into(),
                low: d.clone().into(),
                close: d.clone().into(),
                volume: longtrader_contract::ext::decimal_to_common(Decimal::ZERO).into(),
                ..Default::default()
            })
            .collect();
        Ok(market::GetCandlesResponse { candles, ..Default::default() })
    }

    async fn fetch_ticker(
        &self,
        req: market::FetchTickerRequest,
    ) -> Result<market::Ticker, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok(market::Ticker {
            header: MessageField::some(common::EventHeader::default()),
            symbol: req.symbol,
            timestamp: MessageField::some(now_ts()),
            last: MessageField::some(decimal_to_common(state.price)),
            bid: MessageField::some(decimal_to_common(state.price)),
            ask: MessageField::some(decimal_to_common(state.price)),
            ..Default::default()
        })
    }

    async fn fetch_order_book(
        &self,
        req: market::FetchOrderBookRequest,
    ) -> Result<market::OrderBook, PortError> {
        let state = self.state.lock().expect("mock state");
        let step = Decimal::new(1, 2); // 0.01
        let depth = usize::try_from(req.limit.max(1)).unwrap_or(10);
        let (mut bids, mut asks) = (Vec::with_capacity(depth), Vec::with_capacity(depth));
        for i in 1..=depth {
            bids.push(market::PriceLevel {
                price: MessageField::some(decimal_to_common(state.price - step * Decimal::from(i))),
                amount: MessageField::some(decimal_to_common(Decimal::ONE)),
                ..Default::default()
            });
            asks.push(market::PriceLevel {
                price: MessageField::some(decimal_to_common(state.price + step * Decimal::from(i))),
                amount: MessageField::some(decimal_to_common(Decimal::ONE)),
                ..Default::default()
            });
        }
        Ok(market::OrderBook {
            header: MessageField::some(common::EventHeader::default()),
            symbol: req.symbol,
            timestamp: MessageField::some(now_ts()),
            bids,
            asks,
            ..Default::default()
        })
    }

    async fn subscribe_market_data(
        &self,
        req: market::StreamMarketDataRequest,
        policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError> {
        let symbols: Vec<String> = req
            .subscriptions
            .iter()
            .filter(|s| s.channel == EnumValue::Known(market::StreamChannel::Ticker))
            .map(|s| s.symbol.clone())
            .collect();
        let (tx, rx) = crate::overflow::policy_channel::<market::MarketDataEvent, String>(
            16, policy, event_key,
        );
        let price = self.state.lock().expect("mock state").price;
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(20));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut seq: u64 = 0;
            loop {
                if tx.is_closed() {
                    break;
                }
                tick.tick().await;
                seq += 1;
                for symbol in &symbols {
                    let ticker = market::Ticker {
                        header: MessageField::some(common::EventHeader {
                            trace_id: "mock".to_string(),
                            sequence: seq,
                            ..common::EventHeader::default()
                        }),
                        symbol: symbol.clone(),
                        timestamp: MessageField::some(now_ts()),
                        last: MessageField::some(decimal_to_common(price)),
                        ..Default::default()
                    };
                    tx.send(market::MarketDataEvent {
                        header: ticker.header.clone(),
                        event: Some(market::market_data_event::Event::Ticker(Box::new(ticker))),
                        resume_token: format!("{seq}"),
                        ..Default::default()
                    })
                    .await;
                }
            }
        });
        Ok(rx)
    }
}

fn event_key(event: &market::MarketDataEvent) -> String {
    let channel = match event.event.as_ref() {
        Some(market::market_data_event::Event::Ticker(_)) => "ticker",
        Some(market::market_data_event::Event::Orderbook(_)) => "book",
        Some(market::market_data_event::Event::Trade(_)) => "trade",
        Some(market::market_data_event::Event::Ohlcv(_)) => "ohlcv",
        None => "none",
    };
    let symbol = match event.event.as_ref() {
        Some(market::market_data_event::Event::Ticker(t)) => t.symbol.clone(),
        Some(market::market_data_event::Event::Orderbook(b)) => b.symbol.clone(),
        Some(market::market_data_event::Event::Trade(t)) => t.symbol.clone(),
        _ => String::new(),
    };
    format!("{channel}:{symbol}")
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn adapter() -> MockAdapter {
        MockAdapter::new(dec!(100))
    }

    fn order_req(coid: &str, price: Decimal) -> trading::OrderRequest {
        trading::OrderRequest {
            client_order_id: coid.to_string(),
            symbol: "BTC/USDT".to_string(),
            r#type: EnumValue::Known(trading::OrderType::Limit),
            side: EnumValue::Known(trading::OrderSide::Buy),
            amount: MessageField::some(decimal_to_common(dec!(1))),
            price: MessageField::some(decimal_to_common(price)),
            trigger_price: MessageField::none(),
            time_in_force: EnumValue::Known(trading::TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            params: Default::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_then_cancel_round_trip() {
        let a = adapter();
        let placed = a
            .create_order(trading::CreateOrderRequest {
                exchange_id: MessageField::none(),
                order: MessageField::some(order_req("c1", dec!(99))),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert_eq!(placed.client_order_id, "c1");

        let open = a
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: MessageField::none(),
                symbol: String::new(),
                pagination: MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert_eq!(open.len(), 1);

        a.cancel_order(trading::CancelOrderRequest {
            exchange_id: MessageField::none(),
            order_id: placed.id.clone(),
            symbol: String::new(),
            ..Default::default()
        })
        .await
        .expect("test setup");

        let open = a
            .fetch_open_orders(trading::FetchOpenOrdersRequest {
                exchange_id: MessageField::none(),
                symbol: String::new(),
                pagination: MessageField::none(),
                ..Default::default()
            })
            .await
            .expect("test setup");
        assert!(open.is_empty(), "canceled order must leave the open set");
    }

    #[tokio::test]
    async fn scripted_failures_surface_as_rpc_errors() {
        let a = adapter();
        a.fail_next_creates(1);
        let err = a
            .create_order(trading::CreateOrderRequest {
                exchange_id: MessageField::none(),
                order: MessageField::some(order_req("c2", dec!(99))),
                ..Default::default()
            })
            .await
            .expect_err("must fail");
        assert!(matches!(err, PortError::Rpc { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn drop_oldest_stream_keeps_newest_tickers() {
        let a = adapter();
        let req = market::StreamMarketDataRequest {
            exchange_id: MessageField::none(),
            subscriptions: vec![market::StreamSubscription {
                channel: EnumValue::Known(market::StreamChannel::Ticker),
                symbol: "BTC/USDT".to_string(),
                params: Default::default(),
                ..Default::default()
            }],
            resume_token: String::new(),
            ..Default::default()
        };
        let mut rx =
            a.subscribe_market_data(req, OverflowPolicy::DropOldest).await.expect("test setup");
        // Advance enough virtual time for the 20ms emitter to produce many
        // events; the 16-slot DropOldest buffer keeps only the newest.
        tokio::time::advance(Duration::from_millis(500)).await;
        // Yield a few times so the overflow pipe's forwarder task can move
        // buffered events into the channel before we assert on them.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let mut last_seq = 0u64;
        while let Ok(event) = rx.try_recv() {
            if let Some(market::market_data_event::Event::Ticker(t)) = event.event.as_ref() {
                assert!(t.header.sequence > last_seq, "sequences must be monotonically increasing");
                last_seq = t.header.sequence;
            }
        }
        assert!(last_seq > 0, "must have received events");
    }
}

// ---------------------------------------------------------------------------
// Extended capability ports (P2): fully scripted implementations so every
// strategy is testable offline.
// ---------------------------------------------------------------------------

use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TriggerOrderGateway,
    TriggerOrderRequest, VenueOpInvoker, WalletGateway,
};

#[async_trait]
impl FundingRateSource for MockAdapter {
    async fn fetch_funding_rate(
        &self,
        _exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok(FundingRateSnapshot {
            symbol: symbol.to_string(),
            rate: state.funding_rate,
            next_funding_time_ms: now_ms(),
            mark_price: Some(state.price),
        })
    }

    async fn fetch_funding_rate_history(
        &self,
        _exchange_id: &common::ExchangeId,
        _symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok((0..limit.min(64))
            .map(|i| FundingRatePoint {
                rate: state.funding_rate,
                time_ms: now_ms() - i64::from(i * 8),
            })
            .collect())
    }
}

#[async_trait]
impl TriggerOrderGateway for MockAdapter {
    async fn create_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<String, PortError> {
        let mut state = self.state.lock().expect("mock state");
        let id = format!("trigger-{}", self.ids.fetch_add(1, Ordering::Relaxed));
        state.trigger_orders.push((id.clone(), req));
        Ok(id)
    }

    async fn cancel_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        order_id: &str,
        _symbol: &str,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock().expect("mock state");
        state.trigger_orders.retain(|(id, _)| id != order_id);
        Ok(())
    }
}

#[async_trait]
impl VenueOpInvoker for MockAdapter {
    async fn invoke_venue_op(
        &self,
        _exchange_id: &common::ExchangeId,
        op: &str,
        _params: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, PortError> {
        let state = self.state.lock().expect("mock state");
        match op {
            "account.balance" => Ok(serde_json::json!({
                "currency": "USDT",
                "free": state.op_balance.to_string(),
            })),
            "margin.borrow" | "wallet.convert" | "account.transfer" => {
                Ok(serde_json::json!({ "status": "ok", "op": op }))
            }
            other => Err(PortError::Unsupported(format!("op '{other}' not mocked"))),
        }
    }

    async fn list_venue_ops(
        &self,
        _exchange_id: &common::ExchangeId,
    ) -> Result<Vec<String>, PortError> {
        Ok(vec![
            String::from("account.balance"),
            String::from("margin.borrow"),
            String::from("wallet.convert"),
            String::from("account.transfer"),
        ])
    }
}

#[async_trait]
impl WalletGateway for MockAdapter {
    async fn fetch_deposits(
        &self,
        _exchange_id: &common::ExchangeId,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        let state = self.state.lock().expect("mock state");
        Ok(state.deposits.iter().take(limit as usize).cloned().collect())
    }

    async fn transfer(
        &self,
        _exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock().expect("mock state");
        state.transfers.push((asset.to_string(), amount, dest_label.to_string()));
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or_default())
        .unwrap_or_default()
}

impl MockAdapter {
    /// Sets the funding rate returned by [`FundingRateSource`].
    pub fn set_funding_rate(&self, rate: Decimal) {
        self.state.lock().expect("mock state").funding_rate = rate;
    }

    /// Sets the balance returned by the `account.balance` venue op.
    pub fn set_op_balance(&self, balance: Decimal) {
        self.state.lock().expect("mock state").op_balance = balance;
    }

    /// Pushes a completed deposit into the fake ledger.
    pub fn push_deposit(&self, entry: LedgerEntry) {
        self.state.lock().expect("mock state").deposits.push(entry);
    }

    /// Snapshot of transfers performed through [`WalletGateway::transfer`].
    pub fn transfers(&self) -> Vec<(String, Decimal, String)> {
        self.state.lock().expect("mock state").transfers.clone()
    }

    /// Snapshot of outstanding trigger orders.
    pub fn trigger_orders(&self) -> Vec<(String, TriggerOrderRequest)> {
        self.state.lock().expect("mock state").trigger_orders.clone()
    }
}
