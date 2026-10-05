//! Deterministic in-process fake for tests and dry-run mode.
//!
//! Orders are echoed back with `Open` status and tracked locally; market
//! data emits synthetic tickers around a fixed price so overflow policies
//! can be exercised end-to-end without any network.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::{EnumValue, MessageField};
use longtrader_contract::ext::decimal_to_common;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

use crate::{
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, trading, worker},
};

#[derive(Debug, Default)]
struct State {
    price: Decimal,
    orders: Vec<trading::Order>,
    fail_next_creates: u32,
    /// Every `create_order` seen so far, so a script can target one by position.
    creates_seen: u32,
    /// Fail every `create_order` at or after this 0-based index. `None` disables
    /// it; it cannot default to `Some(0)` because that would fail every create.
    fail_creates_from: Option<u32>,
    funding_rate: Decimal,
    op_balance: Decimal,
    deposits: Vec<LedgerEntry>,
    transfers: Vec<(String, Decimal, String)>,
    /// id -> (request, status, created_at_ms).
    trigger_orders: Vec<(String, TriggerOrderRequest, TriggerOrderStatus, i64)>,
    /// client_transfer_id -> receipt, for idempotent retries.
    transfer_receipts: Vec<(String, TransferReceipt)>,
    /// Symbols the mock reports funding for. Empty means "every symbol", which
    /// is the pre-existing permissive default; a test that cares can narrow it.
    funding_symbols: Vec<String>,
}

/// The mock venue's self-describing operation table.
///
/// Built per call rather than held in a `static`: the descriptor owns `String`s
/// and cloning a small table is cheaper than the `LazyLock` this would need.
fn mock_ops() -> Vec<VenueOpDescriptor> {
    vec![
        VenueOpDescriptor {
            name: "account.balance".to_string(),
            category: "account".to_string(),
            summary: "Free balance for one currency.".to_string(),
            mutating: false,
            params: vec![VenueOpParam {
                name: "currency".to_string(),
                r#type: VenueOpParamType::String,
                required: true,
                doc: "Currency to report, e.g. \"USDT\".".to_string(),
                enum_values: Vec::new(),
            }],
        },
        VenueOpDescriptor {
            name: "margin.borrow".to_string(),
            category: "margin".to_string(),
            summary: "Borrow an asset against margin.".to_string(),
            mutating: true,
            params: vec![
                VenueOpParam {
                    name: "asset".to_string(),
                    r#type: VenueOpParamType::String,
                    required: true,
                    doc: "Asset to borrow.".to_string(),
                    enum_values: Vec::new(),
                },
                VenueOpParam {
                    name: "amount".to_string(),
                    r#type: VenueOpParamType::Decimal,
                    required: true,
                    doc: "Amount to borrow; must be positive.".to_string(),
                    enum_values: Vec::new(),
                },
            ],
        },
        VenueOpDescriptor {
            name: "wallet.convert".to_string(),
            category: "wallet".to_string(),
            summary: "Convert between currencies at the venue's rate.".to_string(),
            mutating: true,
            params: vec![
                VenueOpParam {
                    name: "asset".to_string(),
                    r#type: VenueOpParamType::String,
                    required: true,
                    doc: "Asset to convert from.".to_string(),
                    enum_values: Vec::new(),
                },
                VenueOpParam {
                    name: "dest".to_string(),
                    r#type: VenueOpParamType::String,
                    required: true,
                    doc: "Currency to convert to.".to_string(),
                    enum_values: Vec::new(),
                },
            ],
        },
        VenueOpDescriptor {
            name: "account.transfer".to_string(),
            category: "account".to_string(),
            summary: "Move funds between accounts on the same venue.".to_string(),
            mutating: true,
            params: vec![
                VenueOpParam {
                    name: "asset".to_string(),
                    r#type: VenueOpParamType::String,
                    required: true,
                    doc: "Asset to move, e.g. \"USDT\".".to_string(),
                    enum_values: Vec::new(),
                },
                VenueOpParam {
                    name: "amount".to_string(),
                    r#type: VenueOpParamType::Decimal,
                    required: true,
                    doc: "Amount to move; must be positive.".to_string(),
                    enum_values: Vec::new(),
                },
                VenueOpParam {
                    name: "dest".to_string(),
                    r#type: VenueOpParamType::String,
                    required: true,
                    doc: "Destination account label, e.g. \"futures\".".to_string(),
                    enum_values: Vec::new(),
                },
            ],
        },
    ]
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
#[derive(Clone)]
pub struct MockAdapter {
    state: Arc<Mutex<State>>,
    ids: Arc<AtomicU64>,
}

impl MockAdapter {
    /// Create a mock whose ticker price starts at `initial_price`.
    pub fn new(initial_price: Decimal) -> Self {
        Self {
            state: Arc::new(Mutex::new(State { price: initial_price, ..State::default() })),
            ids: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Script the next `n` order creations to fail (for error-path tests).
    pub async fn fail_next_creates(&self, n: u32) {
        let mut state = self.state.lock().await;
        state.fail_next_creates = n;
        state.fail_creates_from = None;
    }

    /// Script a failure *in the middle* of a batch: every creation from the
    /// 0-based `index` onward fails, and the ones before it are accepted.
    ///
    /// `fail_next_creates` always fails from the very first create, so a batch
    /// scripted with it leaves nothing on the venue. That is the wrong shape for
    /// the partial-success case a batch has to recover from, which needs at least
    /// one leg to land before the failure.
    pub async fn fail_creates_from(&self, index: u32) {
        let mut state = self.state.lock().await;
        state.fail_next_creates = 0;
        state.fail_creates_from = Some(index);
    }

    /// Let every creation succeed again, whatever was scripted before.
    pub async fn heal_creates(&self) {
        let mut state = self.state.lock().await;
        state.fail_next_creates = 0;
        state.fail_creates_from = None;
    }
}

#[async_trait]
impl TradingGateway for MockAdapter {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let mut state = self.state.lock().await;
        let seen = state.creates_seen;
        state.creates_seen += 1;
        if state.fail_next_creates > 0 {
            state.fail_next_creates -= 1;
            return Err(PortError::Rpc { code: 500, message: "scripted failure".to_string() });
        }
        if state.fail_creates_from.is_some_and(|from| seen >= from) {
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
        let mut state = self.state.lock().await;
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
        let mut state = self.state.lock().await;
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
        let state = self.state.lock().await;
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
        let state = self.state.lock().await;
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
        let price = self.state.lock().await.price;
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

    async fn modify_position(
        &self,
        _req: trading::ModifyPositionRequest,
    ) -> Result<trading::ModifyPositionResponse, PortError> {
        Err(PortError::MissingField(format!("no open position {}", _req.position_id)))
    }
}

#[async_trait]
impl MarketDataSource for MockAdapter {
    async fn list_symbols(
        &self,
        _req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError> {
        let price = self.state.lock().await.price;
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

    async fn search_symbols(
        &self,
        req: market::SearchSymbolsRequest,
    ) -> Result<market::SearchSymbolsResponse, PortError> {
        let listed = self.list_symbols(market::ListSymbolsRequest::default()).await?;
        let needle = req.query.to_lowercase();
        let symbols: Vec<market::SymbolInfo> = listed
            .symbols
            .into_iter()
            .filter(|s| {
                needle.is_empty() ||
                    s.name.to_lowercase().contains(&needle) ||
                    s.display_name.to_lowercase().contains(&needle)
            })
            .collect();
        Ok(market::SearchSymbolsResponse { symbols, ..Default::default() })
    }

    async fn list_tickers(
        &self,
        req: market::ListTickersRequest,
    ) -> Result<market::ListTickersResponse, PortError> {
        let listed = self.list_symbols(market::ListSymbolsRequest::default()).await?;
        let mut tickers = Vec::new();
        for info in &listed.symbols {
            if !req.symbols.is_empty() && !req.symbols.contains(&info.name) {
                continue;
            }
            tickers.push(
                self.fetch_ticker(market::FetchTickerRequest {
                    symbol: info.name.clone(),
                    ..Default::default()
                })
                .await?,
            );
        }
        Ok(market::ListTickersResponse { tickers, ..Default::default() })
    }

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError> {
        let price = self.state.lock().await.price;
        let d = longtrader_contract::ext::decimal_to_common(price);
        let now: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let raw_limit = req.pagination.as_option().map_or(0, |p| p.limit);
        let n = if raw_limit == 0 { 10 } else { (raw_limit.min(1000)) as i64 };
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
        let state = self.state.lock().await;
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
        let state = self.state.lock().await;
        let step = Decimal::new(1, 2); // 0.01
        let depth =
            usize::try_from(req.pagination.as_option().map_or(1, |p| p.limit).max(1)).unwrap_or(10);
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
        let price = self.state.lock().await.price;
        Ok(crate::adapters::poll_market_data(
            req,
            policy,
            move |channel, _exchange_id, symbol, seq| {
                let price = price;
                async move {
                    let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
                    let header = common::EventHeader {
                        trace_id: "mock".to_string(),
                        sequence: next,
                        ..common::EventHeader::default()
                    };
                    let event = synthetic_event(channel, symbol, price, header.clone());
                    Ok(Some(market::MarketDataEvent {
                        header: MessageField::some(header),
                        event: Some(event),
                        resume_token: format!("{next}"),
                        ..Default::default()
                    }))
                }
            },
        ))
    }
}

/// One synthetic market-data event, in the variant the caller subscribed to.
///
/// The variant must match the channel: a strategy that subscribed for depth
/// cannot be exercised against a feed that answers with tickers, because the
/// event it decodes is not the one it asked for. `STREAM_CHANNEL_UNSPECIFIED`
/// (and any unknown discriminant, which `poll_market_data` normalises to it)
/// falls back to a ticker, the mock's default feed.
fn synthetic_event(
    channel: market::StreamChannel,
    symbol: String,
    price: Decimal,
    header: common::EventHeader,
) -> market::market_data_event::Event {
    use market::market_data_event::Event as Variant;
    let d = |v: Decimal| MessageField::some(decimal_to_common(v));
    let stamp = || MessageField::some(now_ts());
    match channel {
        market::StreamChannel::Orderbook => {
            // One level per side, symmetric around the synthetic price.
            let step = Decimal::new(1, 2); // 0.01
            let level = |offset: Decimal| market::PriceLevel {
                price: d(price + offset),
                amount: d(Decimal::ONE),
                ..Default::default()
            };
            Variant::Orderbook(Box::new(market::OrderBook {
                header: MessageField::some(header),
                symbol,
                timestamp: stamp(),
                bids: vec![level(-step)],
                asks: vec![level(step)],
                ..Default::default()
            }))
        }
        market::StreamChannel::Trades => {
            // Built before the literal: the header moves into its field.
            let id = format!("mock-trade-{}", header.sequence);
            Variant::Trade(Box::new(market::PublicTrade {
                header: MessageField::some(header),
                id,
                symbol,
                timestamp: stamp(),
                price: d(price),
                amount: d(Decimal::ONE),
                // A synthetic print has no taker; `UNSPECIFIED` says so, where a
                // guessed side would read as a real aggressor fill.
                side: EnumValue::Known(market::TradeSide::Unspecified),
                ..Default::default()
            }))
        }
        market::StreamChannel::Ohlcv => Variant::Ohlcv(Box::new(market::OHLCV {
            header: MessageField::some(header),
            timestamp: stamp(),
            open: d(price),
            high: d(price),
            low: d(price),
            close: d(price),
            volume: d(Decimal::ONE),
            ..Default::default()
        })),
        market::StreamChannel::Ticker | market::StreamChannel::Unspecified => {
            Variant::Ticker(Box::new(market::Ticker {
                header: MessageField::some(header),
                symbol,
                timestamp: stamp(),
                last: d(price),
                ..Default::default()
            }))
        }
    }
}

// ponytail: key fn lives in `crate::adapters::market_event_key` (single owner).

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
        a.fail_next_creates(1).await;
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
            exchange_id: MessageField::some(common::ExchangeId {
                id: "mock".to_string(),
                label: String::new(),
                ..Default::default()
            }),
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
        tokio::time::advance(std::time::Duration::from_millis(500)).await;
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

    /// The ledger is documented as "newest first". `transfer` prepends its row
    /// while `push_deposit` appends, so relying on insertion order would report
    /// a transfer as newer than a deposit that happened after it. Timestamps
    /// below are chosen so the two orderings disagree.
    #[tokio::test]
    async fn ledger_is_newest_first_regardless_of_insertion_site() {
        let adapter = MockAdapter::new(dec!(1000));
        let t0 = now_ms();
        adapter
            .push_deposit(LedgerEntry {
                id: "old".into(),
                currency: "USDT".into(),
                amount: dec!(1),
                entry_type: "deposit".into(),
                completed: true,
                time_ms: t0 - 2_000,
            })
            .await;
        // Stamped `t0`, and written to the FRONT of the vec.
        adapter
            .transfer(&common::ExchangeId::default(), "USDT", dec!(2), "futures", "ordering-1")
            .await
            .expect("transfer");
        adapter
            .push_deposit(LedgerEntry {
                id: "new".into(),
                currency: "USDT".into(),
                amount: dec!(3),
                entry_type: "deposit".into(),
                completed: true,
                time_ms: t0 + 2_000,
            })
            .await;

        let rows = adapter
            .list_ledger_entries(&common::ExchangeId::default(), "USDT", "", 10)
            .await
            .expect("listed");
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["new", "ordering-1", "old"],
            "newest first by timestamp, not by insertion site"
        );
        let times: Vec<i64> = rows.iter().map(|r| r.time_ms).collect();
        assert!(times.windows(2).all(|w| w[0] >= w[1]), "descending: {times:?}");
    }

    /// A subscription must be answered in the variant it asked for. Answering
    /// every channel with a ticker made an orderbook or trade strategy impossible
    /// to exercise offline: it decodes the event it subscribed for and finds
    /// something else entirely.
    ///
    /// Pinned through `market_event_key`, the single owner of "which variant is
    /// this" (it is also what the `Coalesce` policy dedupes on, so a mismatch
    /// would additionally let an orderbook evict a ticker).
    #[test]
    fn a_channel_is_answered_in_the_variant_it_asked_for() {
        let cases = [
            (market::StreamChannel::Ticker, "ticker:BTC/USDT"),
            (market::StreamChannel::Orderbook, "book:BTC/USDT"),
            (market::StreamChannel::Trades, "trade:BTC/USDT"),
            // OHLCV carries no symbol of its own, so its key has an empty half.
            (market::StreamChannel::Ohlcv, "ohlcv:"),
        ];
        for (channel, expected_key) in cases {
            let event = synthetic_event(
                channel,
                "BTC/USDT".to_string(),
                dec!(100),
                common::EventHeader { sequence: 1, ..common::EventHeader::default() },
            );
            let wrapped = market::MarketDataEvent { event: Some(event), ..Default::default() };
            assert_eq!(
                crate::adapters::market_event_key(&wrapped),
                expected_key,
                "channel {channel:?} was answered with the wrong variant"
            );
        }
    }

    /// A book with no depth is not a book: a depth strategy reading
    /// `bids.first()` must get a level, symmetric around the synthetic price.
    #[test]
    fn an_orderbook_event_carries_depth_around_the_synthetic_price() {
        let event = synthetic_event(
            market::StreamChannel::Orderbook,
            "BTC/USDT".to_string(),
            dec!(100),
            common::EventHeader::default(),
        );
        let book = match &event {
            market::market_data_event::Event::Orderbook(book) => Some(book.as_ref()),
            _ => None,
        }
        .expect("ORDERBOOK must be answered with a book");
        assert_eq!(book.symbol, "BTC/USDT");
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        let price_of = |level: &market::PriceLevel| {
            longtrader_contract::ext::common_to_decimal(
                level.price.as_option().expect("level price"),
            )
            .expect("the price decodes")
        };
        assert_eq!(price_of(&book.bids[0]), dec!(99.99), "the bid sits below the price");
        assert_eq!(price_of(&book.asks[0]), dec!(100.01), "the ask sits above the price");
    }

    /// An unspecified channel is the mock's ticker feed: it must still answer
    /// rather than go silent, and carry the synthetic price.
    #[test]
    fn an_unspecified_channel_falls_back_to_a_ticker() {
        let event = synthetic_event(
            market::StreamChannel::Unspecified,
            "BTC/USDT".to_string(),
            dec!(100),
            common::EventHeader { sequence: 7, ..common::EventHeader::default() },
        );
        let ticker = match &event {
            market::market_data_event::Event::Ticker(ticker) => Some(ticker.as_ref()),
            _ => None,
        }
        .expect("an unspecified channel must fall back to a ticker");
        assert_eq!(ticker.symbol, "BTC/USDT");
        assert_eq!(ticker.header.as_option().expect("header").sequence, 7);
        let last = ticker.last.as_option().expect("the synthetic price");
        assert_eq!(
            longtrader_contract::ext::common_to_decimal(last).expect("the price decodes"),
            dec!(100)
        );
    }

    /// The port's documented contract is "empty `symbols` = every contract the
    /// venue reports funding for". Iterating the request list made the one
    /// request that asks for the most return nothing, which a carry strategy
    /// reads as "this venue has no perps".
    #[tokio::test]
    async fn an_empty_symbol_list_returns_every_funded_contract() {
        let a = adapter();
        a.set_funding_rate(dec!(0.0001)).await;
        let every = a
            .list_funding_rates(&common::ExchangeId::default(), &[])
            .await
            .expect("an empty list means every contract the venue prices");
        let symbols: Vec<&str> = every.iter().map(|s| s.symbol.as_str()).collect();
        assert_eq!(symbols, vec!["MOCK-USDT"], "the mock venue lists exactly one contract");

        // A narrowed venue prices exactly that set, and an empty request follows
        // it rather than reporting the venue's listed-but-unpriced symbols.
        a.set_funding_symbols(vec!["BTC/USDT".to_string()]).await;
        let narrowed = a
            .list_funding_rates(&common::ExchangeId::default(), &[])
            .await
            .expect("the priced set");
        let symbols: Vec<&str> = narrowed.iter().map(|s| s.symbol.as_str()).collect();
        assert_eq!(symbols, vec!["BTC/USDT"]);
    }
}

// ---------------------------------------------------------------------------
// Extended capability ports (P2): fully scripted implementations so every
// strategy is testable offline.
// ---------------------------------------------------------------------------

use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TransferReceipt,
    TriggerOrder, TriggerOrderGateway, TriggerOrderRequest, TriggerOrderStatus, VenueOpDescriptor,
    VenueOpInvoker, VenueOpParam, VenueOpParamType, WalletGateway,
};

/// The funding settlement interval the mock models, in milliseconds.
const MOCK_FUNDING_INTERVAL_MS: i64 = 8 * 60 * 60 * 1000;

#[async_trait]
impl FundingRateSource for MockAdapter {
    async fn fetch_funding_rate(
        &self,
        _exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError> {
        let state = self.state.lock().await;
        if !state.funding_symbols.is_empty() && !state.funding_symbols.iter().any(|s| s == symbol) {
            // A venue tracks perps for a subset of its symbols. Answering for
            // an unlisted one would hand a carry strategy another contract's
            // rate, and it would act on it.
            return Err(PortError::NotFound(format!(
                "the mock venue lists no funding for {symbol}"
            )));
        }
        Ok(FundingRateSnapshot {
            symbol: symbol.to_string(),
            rate: state.funding_rate,
            // The next settlement is snapped to the interval grid, so repeated
            // calls agree instead of drifting with wall time.
            next_funding_time_ms: next_funding_boundary(now_ms()),
            mark_price: Some(state.price),
        })
    }

    async fn fetch_funding_rate_history(
        &self,
        _exchange_id: &common::ExchangeId,
        _symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError> {
        let state = self.state.lock().await;
        // Deterministic, evenly spaced settlements on the interval grid. Wall
        // time is deliberately not used: a strategy that backtests against the
        // mock must see the same history on every run.
        let now = now_ms();
        let grid = now.div_euclid(MOCK_FUNDING_INTERVAL_MS) * MOCK_FUNDING_INTERVAL_MS;
        Ok((0..limit.min(64))
            .map(|i| FundingRatePoint {
                rate: state.funding_rate,
                time_ms: grid - i64::from(i) * MOCK_FUNDING_INTERVAL_MS,
            })
            .collect())
    }

    async fn list_funding_rates(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<FundingRateSnapshot>, PortError> {
        // The documented contract is "empty = every contract the venue reports
        // funding for". Iterating the request list made the one request that asks
        // for the *most* return nothing at all, which reads as "this venue has no
        // perps" — the opposite of what an empty list means everywhere else.
        let requested =
            if symbols.is_empty() { self.funding_universe().await } else { symbols.to_vec() };
        let mut out = Vec::new();
        for symbol in &requested {
            match self.fetch_funding_rate(exchange_id, symbol).await {
                Ok(snapshot) => out.push(snapshot),
                // An unpriced symbol is simply absent from the batch, matching
                // the "empty = everything the venue reports" contract.
                Err(PortError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }
}

impl MockAdapter {
    /// Every contract the mock venue reports funding for.
    ///
    /// A narrowed `funding_symbols` *is* that set — it is what the venue prices —
    /// and otherwise it is every symbol the venue lists. Resolved by reading the
    /// state and releasing the lock before the `list_symbols` round trip, which
    /// takes the same lock.
    async fn funding_universe(&self) -> Vec<String> {
        let narrowed = self.state.lock().await.funding_symbols.clone();
        if !narrowed.is_empty() {
            return narrowed;
        }
        self.list_symbols(market::ListSymbolsRequest::default()).await.map_or_else(
            |_| Vec::new(),
            |listed| listed.symbols.into_iter().map(|s| s.name).collect(),
        )
    }
}

/// The next settlement boundary strictly after `now_ms`.
fn next_funding_boundary(now: i64) -> i64 {
    (now / MOCK_FUNDING_INTERVAL_MS + 1) * MOCK_FUNDING_INTERVAL_MS
}

#[async_trait]
impl TriggerOrderGateway for MockAdapter {
    async fn create_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<TriggerOrder, PortError> {
        validate_trigger_request(&req)?;
        let mut state = self.state.lock().await;
        let id = format!("trigger-{}", self.ids.fetch_add(1, Ordering::Relaxed));
        let created_at_ms = now_ms();
        state.trigger_orders.push((
            id.clone(),
            req.clone(),
            TriggerOrderStatus::Open,
            created_at_ms,
        ));
        Ok(TriggerOrder {
            id,
            client_order_id: req.client_order_id,
            symbol: req.symbol,
            is_buy: req.is_buy,
            trigger_price: req.trigger_price,
            qty: req.qty,
            reduce_only: req.reduce_only,
            status: TriggerOrderStatus::Open,
            created_at_ms,
            order_id: None,
            triggered_at_ms: None,
        })
    }

    async fn cancel_trigger_order(
        &self,
        _exchange_id: &common::ExchangeId,
        order_id: &str,
        _symbol: &str,
    ) -> Result<TriggerOrder, PortError> {
        let mut state = self.state.lock().await;
        let Some((_, req, status, created_at_ms)) =
            state.trigger_orders.iter_mut().find(|(id, _, _, _)| id == order_id)
        else {
            return Err(PortError::NotFound(format!("trigger order {order_id}")));
        };
        // A fired order is live on the venue; reporting a successful cancel
        // would tell a strategy its backstop is gone when it is not.
        if *status != TriggerOrderStatus::Open {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {order_id} is {status} and can no longer be cancelled"
            )));
        }
        *status = TriggerOrderStatus::Canceled;
        Ok(TriggerOrder {
            id: order_id.to_string(),
            client_order_id: req.client_order_id.clone(),
            symbol: req.symbol.clone(),
            is_buy: req.is_buy,
            trigger_price: req.trigger_price,
            qty: req.qty,
            reduce_only: req.reduce_only,
            status: TriggerOrderStatus::Canceled,
            created_at_ms: *created_at_ms,
            order_id: None,
            triggered_at_ms: None,
        })
    }

    async fn list_trigger_orders(
        &self,
        _exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<TriggerOrder>, PortError> {
        let state = self.state.lock().await;
        Ok(state
            .trigger_orders
            .iter()
            .filter(|(_, req, _, _)| symbols.is_empty() || symbols.contains(&req.symbol))
            .map(|(id, req, status, created_at_ms)| TriggerOrder {
                id: id.clone(),
                client_order_id: req.client_order_id.clone(),
                symbol: req.symbol.clone(),
                is_buy: req.is_buy,
                trigger_price: req.trigger_price,
                qty: req.qty,
                reduce_only: req.reduce_only,
                status: *status,
                created_at_ms: *created_at_ms,
                order_id: None,
                triggered_at_ms: None,
            })
            .collect())
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
        let state = self.state.lock().await;
        tracing::warn!(
            op = op,
            "VenueOpInvoker invoked on MockAdapter — dry-run stub; no real venue operation is performed"
        );
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
    ) -> Result<Vec<VenueOpDescriptor>, PortError> {
        // Descriptors, not bare names: `mutating` is what a client reads to
        // decide whether a call needs a confirmation prompt, and `account.
        // transfer` must not be reported as read-only.
        Ok(mock_ops())
    }
}

#[async_trait]
impl WalletGateway for MockAdapter {
    async fn list_ledger_entries(
        &self,
        _exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        let state = self.state.lock().await;
        let mut rows: Vec<LedgerEntry> = state
            .deposits
            .iter()
            .filter(|e| currency.is_empty() || e.currency == currency)
            .filter(|e| entry_type.is_empty() || e.entry_type == entry_type)
            .cloned()
            .collect();
        // Newest first, as a property of the read rather than of where a row
        // happened to be inserted. `push_deposit` appends and `transfer`
        // prepends, so relying on insertion order would report seeded deposits
        // and transfer rows in opposite orders.
        rows.sort_by_key(|e| std::cmp::Reverse(e.time_ms));
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn transfer(
        &self,
        _exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<TransferReceipt, PortError> {
        if asset.trim().is_empty() {
            return Err(PortError::MissingField("asset".to_string()));
        }
        if amount.is_sign_negative() || amount.is_zero() {
            return Err(PortError::InvalidArgument(format!(
                "transfer amount must be positive, got {amount:?}"
            )));
        }
        if dest_label.trim().is_empty() {
            return Err(PortError::MissingField("dest_label".to_string()));
        }
        let mut state = self.state.lock().await;
        // Idempotency first: a retry must not move funds twice.
        if !client_transfer_id.is_empty() &&
            let Some(receipt) = state
                .transfer_receipts
                .iter()
                .find(|(_, r)| r.transfer_id == client_transfer_id)
                .map(|(_, r)| r.clone())
        {
            return Ok(receipt);
        }
        state.transfers.push((asset.to_string(), amount, dest_label.to_string()));
        let transfer_id = if client_transfer_id.is_empty() {
            format!("transfer-{}", self.ids.fetch_add(1, Ordering::Relaxed))
        } else {
            client_transfer_id.to_string()
        };
        // The transfer appears on the ledger as an outgoing row, so a
        // strategy that scans the ledger sees its own capital movements.
        let entry = LedgerEntry {
            id: transfer_id.clone(),
            currency: asset.to_string(),
            amount: -amount,
            entry_type: "transfer".to_string(),
            completed: true,
            time_ms: now_ms(),
        };
        state.deposits.insert(0, entry.clone());
        let receipt = TransferReceipt { transfer_id, entry: Some(entry) };
        state.transfer_receipts.push((client_transfer_id.to_string(), receipt.clone()));
        Ok(receipt)
    }
}

/// Reject the two ways a conditional order is silently useless: a non-positive
/// quantity, and a non-positive trigger price (which would fire immediately and
/// degenerate into a market order).
fn validate_trigger_request(req: &TriggerOrderRequest) -> Result<(), PortError> {
    if req.symbol.trim().is_empty() {
        return Err(PortError::MissingField("symbol".to_string()));
    }
    if req.qty.is_sign_negative() || req.qty.is_zero() {
        return Err(PortError::InvalidArgument(format!(
            "trigger order qty must be positive, got {:?}",
            req.qty
        )));
    }
    if req.trigger_price.is_sign_negative() || req.trigger_price.is_zero() {
        return Err(PortError::InvalidArgument(format!(
            "trigger order trigger_price must be positive, got {:?}",
            req.trigger_price
        )));
    }
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_default(|d| i64::try_from(d.as_millis()).unwrap_or_default())
}

impl MockAdapter {
    /// Sets the funding rate returned by [`FundingRateSource`].
    pub async fn set_funding_rate(&self, rate: Decimal) {
        self.state.lock().await.funding_rate = rate;
    }

    /// Restrict funding to `symbols`, so a strategy cannot read another
    /// contract's rate.
    pub async fn set_funding_symbols(&self, symbols: Vec<String>) {
        self.state.lock().await.funding_symbols = symbols;
    }

    /// Sets the balance returned by the `account.balance` venue op.
    pub async fn set_op_balance(&self, balance: Decimal) {
        self.state.lock().await.op_balance = balance;
    }

    /// Pushes a completed deposit into the fake ledger.
    pub async fn push_deposit(&self, entry: LedgerEntry) {
        self.state.lock().await.deposits.push(entry);
    }

    /// Snapshot of transfers performed through [`WalletGateway::transfer`].
    pub async fn transfers(&self) -> Vec<(String, Decimal, String)> {
        self.state.lock().await.transfers.clone()
    }

    /// Snapshot of trigger orders that are still resting, as `(id, request)`.
    ///
    /// Canceled and fired orders are excluded: this is the "what protective
    /// orders do I have live right now?" view, which is the question a
    /// strategy actually asks.
    pub async fn trigger_orders(&self) -> Vec<(String, TriggerOrderRequest)> {
        self.state
            .lock()
            .await
            .trigger_orders
            .iter()
            .filter(|(_, _, status, _)| *status == TriggerOrderStatus::Open)
            .map(|(id, req, _, _)| (id.clone(), req.clone()))
            .collect()
    }

    /// Every trigger order ever placed, including canceled ones.
    pub async fn all_trigger_orders(&self) -> Vec<(String, TriggerOrderStatus)> {
        self.state
            .lock()
            .await
            .trigger_orders
            .iter()
            .map(|(id, _, status, _)| (id.clone(), *status))
            .collect()
    }
}
