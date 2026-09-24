//! `TerminalAdapter`: speaks `longtrader.terminal.v1` (venue-string surface)
//! via the shared `TerminalClient`, mapping onto the unified
//! `longtrader.{trading,market}.v1` ports.
//!
//! Use when the backend is a terminal (`backend = "terminal"`, e.g.
//! `tradingcharts-server` or `longtrader-api` terminal router). Use
//! [`super::RemoteAdapter`] when the backend natively serves the unified
//! `longtrader.{trading,market}.v1` contract.
//!
//! Conversions are explicit and lossy where the surfaces differ:
//! - terminal string decimals <-> unified `common.Decimal` (via `rust_decimal`);
//! - terminal `Side` <-> unified `OrderSide` (`Buy`/`Sell` same values);
//! - unified `Position.side: OrderSide` carries terminal `PositionSide` (`Long`->`Buy`,
//!   `Short`->`Sell`); reverse maps back;
//! - order status: terminal `Pending/Filled/Canceled` <-> unified `Open/Filled/Canceled`; other
//!   unified statuses are never produced here;
//! - order types beyond `Market/Limit/Stop` are rejected explicitly instead of silently downgraded;
//! - unified `Order` bookkeeping fields without terminal counterparts
//!   (`remaining/cost/average/fee/timestamps`) are derived (`remaining = amount - filled`, stamps =
//!   now) and documented as lossy.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::MessageField;
use longtrader_proto::{
    client::{TerminalClient, TerminalClientError},
    proto::longtrader::terminal::v1 as tproto,
};
use rust_decimal::Decimal;

use crate::{
    overflow,
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, trading, worker},
};

fn map_client_err(e: TerminalClientError) -> PortError {
    match e {
        TerminalClientError::Http(m) => PortError::Transport(m),
        TerminalClientError::Rpc { code, message } => PortError::Rpc { code, message },
        TerminalClientError::Decode(m) => PortError::Transport(m),
        TerminalClientError::MissingField(f) => PortError::MissingField(f),
    }
}

fn venue_of(exchange_id: &common::ExchangeId) -> Result<String, PortError> {
    if exchange_id.id.is_empty() {
        return Err(PortError::InvalidArgument(
            "exchange_id.id (venue) is empty; set strategy.params.exchange_id or --venue (e.g. binance)".to_string(),
        ));
    }
    Ok(exchange_id.id.clone())
}

fn parse_dec(s: &str, field: &str) -> Result<Decimal, PortError> {
    if s.is_empty() {
        return Err(PortError::MissingField(field.to_string()));
    }
    s.parse::<Decimal>()
        .map_err(|_| PortError::InvalidArgument(format!("bad decimal {field}={s:?}")))
}

fn opt_dec(s: &Option<String>) -> Result<Option<Decimal>, PortError> {
    match s {
        None => Ok(None),
        Some(v) if v.is_empty() => Ok(None),
        Some(v) => Ok(Some(
            v.parse::<Decimal>()
                .map_err(|_| PortError::InvalidArgument(format!("bad decimal {v:?}")))?,
        )),
    }
}

fn dec_to_common(v: Decimal) -> common::Decimal {
    longtrader_contract::ext::decimal_to_common(v)
}

fn str_to_common(s: &str, field: &str) -> Result<common::Decimal, PortError> {
    Ok(dec_to_common(parse_dec(s, field)?))
}

fn now_ts() -> buffa_types::google::protobuf::Timestamp {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    buffa_types::google::protobuf::Timestamp {
        seconds: i64::try_from(dur.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(dur.subsec_nanos()).expect("subsec_nanos fits i32"),
        ..Default::default()
    }
}

fn ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn ts_from_ms(ms: i64) -> buffa_types::google::protobuf::Timestamp {
    buffa_types::google::protobuf::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).expect("ms remainder fits i32"),
        ..Default::default()
    }
}

// ---- enum mapping (explicit, no silent downgrade) ----

fn side_to_unified(s: tproto::Side) -> Result<trading::OrderSide, PortError> {
    match s {
        tproto::Side::Buy => Ok(trading::OrderSide::Buy),
        tproto::Side::Sell => Ok(trading::OrderSide::Sell),
        _ => Err(PortError::InvalidArgument("terminal Side is unspecified".to_string())),
    }
}

fn side_to_terminal(s: trading::OrderSide) -> Result<tproto::Side, PortError> {
    match s {
        trading::OrderSide::Buy => Ok(tproto::Side::Buy),
        trading::OrderSide::Sell => Ok(tproto::Side::Sell),
        _ => Err(PortError::InvalidArgument("unified OrderSide is unspecified".to_string())),
    }
}

fn order_type_to_unified(t: tproto::OrderType) -> Result<trading::OrderType, PortError> {
    match t {
        tproto::OrderType::Market => Ok(trading::OrderType::Market),
        tproto::OrderType::Limit => Ok(trading::OrderType::Limit),
        tproto::OrderType::Stop => Ok(trading::OrderType::Stop),
        tproto::OrderType::StopLimit => Ok(trading::OrderType::StopLimit),
        _ => Err(PortError::InvalidArgument(
            "terminal OrderType is unspecified/unsupported".to_string(),
        )),
    }
}

fn order_type_to_terminal_type(t: trading::OrderType) -> Result<tproto::OrderType, PortError> {
    match t {
        trading::OrderType::Market => Ok(tproto::OrderType::Market),
        trading::OrderType::Limit => Ok(tproto::OrderType::Limit),
        trading::OrderType::Stop => Ok(tproto::OrderType::Stop),
        trading::OrderType::StopLimit => Ok(tproto::OrderType::StopLimit),
        _ => Err(PortError::InvalidArgument(format!(
            "order type {t:?} not supported by terminal backends (supported: Market/Limit/Stop/StopLimit)"
        ))),
    }
}

fn status_to_unified(s: tproto::OrderStatus) -> trading::OrderStatus {
    match s {
        tproto::OrderStatus::Pending => trading::OrderStatus::Open,
        tproto::OrderStatus::Filled => trading::OrderStatus::Filled,
        tproto::OrderStatus::Canceled => trading::OrderStatus::Canceled,
        _ => trading::OrderStatus::Unspecified,
    }
}

fn position_side_to_unified(
    s: longtrader_proto::proto::longtrader::trading::v1::PositionSide,
) -> Result<trading::OrderSide, PortError> {
    use longtrader_proto::proto::longtrader::trading::v1::PositionSide as PS;
    match s {
        PS::Long => Ok(trading::OrderSide::Buy),
        PS::Short => Ok(trading::OrderSide::Sell),
        _ => Err(PortError::InvalidArgument("PositionSide is unspecified".to_string())),
    }
}

// ---- message mapping (lossy parts documented) ----

fn order_to_unified(o: &tproto::Order) -> Result<trading::Order, PortError> {
    let side = side_to_unified(match o.side {
        buffa::EnumValue::Known(s) => s,
        buffa::EnumValue::Unknown(_) => {
            return Err(PortError::InvalidArgument("terminal Order.side unknown".to_string()));
        }
    })?;
    let otype = order_type_to_unified(match o.order_type {
        buffa::EnumValue::Known(t) => t,
        buffa::EnumValue::Unknown(_) => {
            return Err(PortError::InvalidArgument("terminal Order.type unknown".to_string()));
        }
    })?;
    let status = status_to_unified(match o.status {
        buffa::EnumValue::Known(s) => s,
        buffa::EnumValue::Unknown(_) => tproto::OrderStatus::Unspecified,
    });
    let amount = parse_dec(&o.quantity, "quantity")?;
    let price = opt_dec(&o.price)?.unwrap_or(Decimal::ZERO);
    let filled = if o.filled_quantity.is_empty() {
        Decimal::ZERO
    } else {
        parse_dec(&o.filled_quantity, "filled_quantity")?
    };
    let average = opt_dec(&o.avg_fill_price)?.unwrap_or(Decimal::ZERO);
    let remaining = (amount - filled).max(Decimal::ZERO);
    let ts = now_ts();
    Ok(trading::Order {
        id: o.id.clone(),
        client_order_id: o.id.clone(),
        symbol: o.symbol.clone(),
        r#type: buffa::EnumValue::Known(otype),
        side: buffa::EnumValue::Known(side),
        status: buffa::EnumValue::Known(status),
        amount: MessageField::some(dec_to_common(amount)),
        price: MessageField::some(dec_to_common(price)),
        filled: MessageField::some(dec_to_common(filled)),
        remaining: MessageField::some(dec_to_common(remaining)),
        cost: MessageField::some(dec_to_common(average * filled)),
        average: MessageField::some(dec_to_common(average)),
        fee: MessageField::some(dec_to_common(Decimal::ZERO)),
        timestamp: MessageField::some(ts.clone()),
        created_at: MessageField::some(ts.clone()),
        updated_at: MessageField::some(ts),
        ..Default::default()
    })
}

fn position_to_unified(p: &tproto::Position) -> Result<trading::Position, PortError> {
    use longtrader_proto::proto::longtrader::trading::v1::PositionSide as PS;
    let ps = match p.side {
        buffa::EnumValue::Known(s) => s,
        buffa::EnumValue::Unknown(_) => PS::Unspecified,
    };
    let side = position_side_to_unified(ps)?;
    let contracts = parse_dec(&p.quantity, "quantity")?;
    let entry = parse_dec(&p.entry_price, "entry_price")?;
    let mark = if p.current_price.is_empty() {
        entry
    } else {
        parse_dec(&p.current_price, "current_price")?
    };
    let pnl = if p.unrealized_pnl.is_empty() {
        Decimal::ZERO
    } else {
        parse_dec(&p.unrealized_pnl, "unrealized_pnl")?
    };
    Ok(trading::Position {
        id: p.id.clone(),
        symbol: p.symbol.clone(),
        contracts: MessageField::some(dec_to_common(contracts)),
        contract_size: MessageField::some(dec_to_common(Decimal::ONE)),
        side: buffa::EnumValue::Known(side),
        entry_price: MessageField::some(dec_to_common(entry)),
        mark_price: MessageField::some(dec_to_common(mark)),
        unrealized_pnl: MessageField::some(dec_to_common(pnl)),
        order_id: if p.order_id.is_empty() { None } else { Some(p.order_id.clone()) },
        current_price: MessageField::some(dec_to_common(mark)),
        timestamp: MessageField::some(now_ts()),
        ..Default::default()
    })
}

fn closed_to_unified(p: &tproto::ClosedPosition) -> Result<trading::ClosedPosition, PortError> {
    use longtrader_proto::proto::longtrader::trading::v1::{
        CloseReason as TCR, PositionSide as TPS,
    };
    let side = match p.side {
        buffa::EnumValue::Known(TPS::Long) => trading::PositionSide::Long,
        buffa::EnumValue::Known(TPS::Short) => trading::PositionSide::Short,
        _ => trading::PositionSide::Unspecified,
    };
    Ok(trading::ClosedPosition {
        id: p.id.clone(),
        order_id: p.order_id.clone(),
        symbol: p.symbol.clone(),
        side: buffa::EnumValue::Known(side),
        quantity: MessageField::some(str_to_common(&p.quantity, "quantity")?),
        entry_price: MessageField::some(str_to_common(&p.entry_price, "entry_price")?),
        exit_price: MessageField::some(str_to_common(&p.exit_price, "exit_price")?),
        realized_pnl: MessageField::some({
            if p.realized_pnl.is_empty() {
                dec_to_common(Decimal::ZERO)
            } else {
                str_to_common(&p.realized_pnl, "realized_pnl")?
            }
        }),
        close_reason: buffa::EnumValue::Known(match p.close_reason {
            buffa::EnumValue::Known(TCR::Manual) => trading::CloseReason::Manual,
            buffa::EnumValue::Known(TCR::TakeProfit) => trading::CloseReason::TakeProfit,
            buffa::EnumValue::Known(TCR::StopLoss) => trading::CloseReason::StopLoss,
            buffa::EnumValue::Known(TCR::StopOut) => trading::CloseReason::StopOut,
            _ => trading::CloseReason::Unspecified,
        }),
        ..Default::default()
    })
}

fn ticker_to_unified(symbol: &str, t: &tproto::Ticker) -> Result<market::Ticker, PortError> {
    let dec = |s: &str| {
        if s.is_empty() {
            dec_to_common(Decimal::ZERO)
        } else {
            str_to_common(s, "ticker").unwrap_or_else(|_| dec_to_common(Decimal::ZERO))
        }
    };
    Ok(market::Ticker {
        symbol: if symbol.is_empty() { t.symbol.clone() } else { symbol.to_string() },
        timestamp: MessageField::some(ts_from_ms(t.timestamp_ms)),
        bid: MessageField::some(dec(&t.bid)),
        ask: MessageField::some(dec(&t.ask)),
        last: MessageField::some(dec(&t.last)),
        high: MessageField::some(dec(&t.high)),
        low: MessageField::some(dec(&t.low)),
        ..Default::default()
    })
}

fn book_to_unified(symbol: &str, b: &tproto::Book) -> Result<market::OrderBook, PortError> {
    let level = |l: &tproto::PriceLevel| -> Result<market::PriceLevel, PortError> {
        Ok(market::PriceLevel {
            price: MessageField::some(str_to_common(&l.price, "price")?),
            amount: MessageField::some(str_to_common(&l.amount, "amount")?),
            ..Default::default()
        })
    };
    Ok(market::OrderBook {
        symbol: if symbol.is_empty() { b.symbol.clone() } else { symbol.to_string() },
        timestamp: MessageField::some(ts_from_ms(b.timestamp_ms)),
        bids: b.bids.iter().map(level).collect::<Result<Vec<_>, _>>()?,
        asks: b.asks.iter().map(level).collect::<Result<Vec<_>, _>>()?,
        ..Default::default()
    })
}

/// Connect client over `terminal.v1`, mapped onto unified ports.
#[derive(Clone)]
pub struct TerminalAdapter {
    client: TerminalClient,
    snapshot_seq: Arc<AtomicU64>,
}

impl TerminalAdapter {
    #[must_use]
    pub fn new(base_url: &str, token: &str) -> Self {
        let client = if token.is_empty() {
            TerminalClient::new(base_url)
        } else {
            TerminalClient::new_with_token(base_url, token)
        };
        Self { client, snapshot_seq: Arc::new(AtomicU64::new(0)) }
    }

    #[must_use]
    pub fn from_client(client: TerminalClient) -> Self {
        Self { client, snapshot_seq: Arc::new(AtomicU64::new(0)) }
    }
}

#[async_trait]
impl TradingGateway for TerminalAdapter {
    async fn create_order(
        &self,
        req: trading::CreateOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let inner = req
            .order
            .as_option()
            .ok_or_else(|| PortError::MissingField("order".to_string()))?
            .clone();
        let side = side_to_terminal(match inner.side {
            buffa::EnumValue::Known(s) => s,
            buffa::EnumValue::Unknown(_) => {
                return Err(PortError::InvalidArgument("order side unknown".to_string()))
            }
        })?;
        let otype = order_type_to_terminal_type(match inner.r#type {
            buffa::EnumValue::Known(t) => t,
            buffa::EnumValue::Unknown(_) => {
                return Err(PortError::InvalidArgument("order type unknown".to_string()))
            }
        })?;
        let amount = longtrader_contract::ext::common_to_decimal(
            inner
                .amount
                .as_option()
                .ok_or_else(|| PortError::MissingField("amount".to_string()))?,
        )?;
        let price = match inner.price.as_option() {
            Some(p) => {
                let v = longtrader_contract::ext::common_to_decimal(p)?;
                Some(v.to_string())
            }
            None => None,
        };
        let order = self
            .client
            .place_order(
                &venue,
                &inner.symbol,
                match side {
                    tproto::Side::Buy => tproto::Side::Buy,
                    tproto::Side::Sell => tproto::Side::Sell,
                    _ => return Err(PortError::InvalidArgument("bad side".to_string())),
                },
                otype,
                &amount.to_string(),
                price.as_deref(),
                None,
                None,
                &inner.client_order_id,
            )
            .await
            .map_err(map_client_err)?;
        order_to_unified(&order)
    }

    async fn batch_create_orders(
        &self,
        req: trading::CreateOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let mut out = Vec::with_capacity(req.orders.len());
        for o in &req.orders {
            let single = trading::CreateOrderRequest {
                exchange_id: req.exchange_id.clone(),
                order: MessageField::some(o.clone()),
                ..Default::default()
            };
            out.push(self.create_order(single).await?);
        }
        Ok(out)
    }

    async fn cancel_order(
        &self,
        req: trading::CancelOrderRequest,
    ) -> Result<trading::Order, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let order =
            self.client.cancel_order(&venue, &req.order_id).await.map_err(map_client_err)?;
        order_to_unified(&order)
    }

    async fn cancel_all_orders(
        &self,
        req: trading::CancelAllOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let symbol = if req.symbol.is_empty() { None } else { Some(req.symbol.as_str()) };
        let orders = self.client.cancel_all(&venue, symbol).await.map_err(map_client_err)?;
        orders.iter().map(order_to_unified).collect()
    }

    async fn fetch_open_orders(
        &self,
        req: trading::FetchOpenOrdersRequest,
    ) -> Result<Vec<trading::Order>, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let symbol = if req.symbol.is_empty() { None } else { Some(req.symbol.as_str()) };
        let orders = self.client.get_open_orders(&venue, symbol).await.map_err(map_client_err)?;
        orders.iter().map(order_to_unified).collect()
    }

    async fn get_account(
        &self,
        req: trading::GetAccountRequest,
    ) -> Result<trading::GetAccountResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let acc = self.client.get_account(&venue).await.map_err(map_client_err)?;
        Ok(trading::GetAccountResponse {
            account: MessageField::some(trading::Account {
                balance: MessageField::some(str_to_common(&acc.balance, "balance")?),
                equity: MessageField::some(str_to_common(&acc.equity, "equity")?),
                margin_used: MessageField::some(str_to_common(&acc.margin_used, "margin_used")?),
                free_margin: MessageField::some(str_to_common(&acc.free_margin, "free_margin")?),
                margin_frozen: MessageField::some(str_to_common(
                    &acc.margin_frozen,
                    "margin_frozen",
                )?),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn get_positions(
        &self,
        req: trading::GetPositionsRequest,
    ) -> Result<trading::GetPositionsResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let positions = self.client.get_positions(&venue).await.map_err(map_client_err)?;
        Ok(trading::GetPositionsResponse {
            positions: positions.iter().map(position_to_unified).collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }

    async fn get_order_history(
        &self,
        req: trading::GetOrderHistoryRequest,
    ) -> Result<trading::GetOrderHistoryResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let limit = req.pagination.as_option().map_or(100, |p| p.limit.min(1000) as u32);
        let orders = self.client.get_order_history(&venue, limit).await.map_err(map_client_err)?;
        Ok(trading::GetOrderHistoryResponse {
            orders: orders.iter().map(order_to_unified).collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }

    async fn get_closed_positions(
        &self,
        req: trading::GetClosedPositionsRequest,
    ) -> Result<trading::GetClosedPositionsResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let limit = req.pagination.as_option().map_or(100, |p| p.limit.min(1000) as u32);
        let positions =
            self.client.get_closed_positions(&venue, limit).await.map_err(map_client_err)?;
        Ok(trading::GetClosedPositionsResponse {
            positions: positions.iter().map(closed_to_unified).collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }

    async fn close_position(
        &self,
        req: trading::ClosePositionRequest,
    ) -> Result<trading::ClosePositionResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        self.client.close_position(&venue, &req.position_id).await.map_err(map_client_err)?;
        Ok(trading::ClosePositionResponse::default())
    }

    async fn close_all_positions(
        &self,
        req: trading::CloseAllPositionsRequest,
    ) -> Result<trading::CloseAllPositionsResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        self.client.close_all_positions(&venue).await.map_err(map_client_err)?;
        Ok(trading::CloseAllPositionsResponse::default())
    }

    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        // Best-effort snapshot over three terminal reads (NOT atomic).
        // ponytail: same caveat as RemoteAdapter; server atomic snapshot when available.
        let started_ms = ms_now();
        let venue = venue_of(exchange_id)?;
        let acc = self.client.get_account(&venue).await.map_err(map_client_err)?;
        let balances = vec![account::Balance {
            currency: "USD".to_string(),
            free: MessageField::some(str_to_common(&acc.free_margin, "free_margin")?),
            used: MessageField::some(str_to_common(&acc.margin_used, "margin_used")?),
            total: MessageField::some(str_to_common(&acc.equity, "equity")?),
            ..Default::default()
        }];
        let positions = self.client.get_positions(&venue).await.map_err(map_client_err)?;
        let orders = self.client.get_open_orders(&venue, None).await.map_err(map_client_err)?;
        Ok(worker::ReconcileStateResponse {
            snapshot_sequence: self.snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1,
            snapshot_time: MessageField::some(ts_from_ms(started_ms)),
            balances,
            positions: positions.iter().map(position_to_unified).collect::<Result<Vec<_>, _>>()?,
            open_orders: orders.iter().map(order_to_unified).collect::<Result<Vec<_>, _>>()?,
            ..Default::default()
        })
    }
}

fn timeframe_to_terminal(tf: &str) -> tproto::Timeframe {
    match tf {
        "S100" => tproto::Timeframe::S100,
        "S1" => tproto::Timeframe::S1,
        "M1" => tproto::Timeframe::M1,
        "M5" => tproto::Timeframe::M5,
        "M15" => tproto::Timeframe::M15,
        "M30" => tproto::Timeframe::M30,
        "H1" => tproto::Timeframe::H1,
        "H4" => tproto::Timeframe::H4,
        "D1" => tproto::Timeframe::D1,
        "W1" => tproto::Timeframe::W1,
        _ => tproto::Timeframe::M1,
    }
}

#[async_trait]
impl MarketDataSource for TerminalAdapter {
    async fn fetch_ticker(
        &self,
        req: market::FetchTickerRequest,
    ) -> Result<market::Ticker, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let tickers = self
            .client
            .get_tickers(&venue, std::slice::from_ref(&req.symbol))
            .await
            .map_err(map_client_err)?;
        let t = tickers
            .into_iter()
            .next()
            .ok_or_else(|| PortError::MissingField("ticker".to_string()))?;
        ticker_to_unified(&req.symbol, &t)
    }

    async fn fetch_order_book(
        &self,
        req: market::FetchOrderBookRequest,
    ) -> Result<market::OrderBook, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let depth = req.pagination.as_option().map_or(10, |p| p.limit.min(1000) as u32);
        let book =
            self.client.get_book(&venue, &req.symbol, depth).await.map_err(map_client_err)?;
        book_to_unified(&req.symbol, &book)
    }

    async fn get_candles(
        &self,
        req: market::GetCandlesRequest,
    ) -> Result<market::GetCandlesResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let limit = req.pagination.as_option().map_or(200, |p| p.limit.min(1000) as u32);
        let candles = self
            .client
            .get_candles(&venue, &req.symbol, timeframe_to_terminal(&req.timeframe), limit)
            .await
            .map_err(map_client_err)?;
        Ok(market::GetCandlesResponse {
            candles: candles
                .iter()
                .map(|c| market::Candle {
                    timestamp_ms: c.timestamp_ms,
                    open: MessageField::some(
                        str_to_common(&c.open, "open")
                            .unwrap_or_else(|_| dec_to_common(Decimal::ZERO)),
                    ),
                    high: MessageField::some(
                        str_to_common(&c.high, "high")
                            .unwrap_or_else(|_| dec_to_common(Decimal::ZERO)),
                    ),
                    low: MessageField::some(
                        str_to_common(&c.low, "low")
                            .unwrap_or_else(|_| dec_to_common(Decimal::ZERO)),
                    ),
                    close: MessageField::some(
                        str_to_common(&c.close, "close")
                            .unwrap_or_else(|_| dec_to_common(Decimal::ZERO)),
                    ),
                    volume: MessageField::some(
                        str_to_common(&c.volume, "volume")
                            .unwrap_or_else(|_| dec_to_common(Decimal::ZERO)),
                    ),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    async fn list_symbols(
        &self,
        req: market::ListSymbolsRequest,
    ) -> Result<market::ListSymbolsResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let symbols = self.client.get_symbols(&venue).await.map_err(map_client_err)?;
        Ok(market::ListSymbolsResponse {
            symbols: symbols
                .iter()
                .map(|s| market::SymbolInfo {
                    name: s.name.clone(),
                    display_name: s.display_name.clone(),
                    base_asset: s.base_asset.clone(),
                    quote_asset: s.quote_asset.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    async fn subscribe_market_data(
        &self,
        req: market::StreamMarketDataRequest,
        policy: OverflowPolicy,
    ) -> Result<MarketEventStream, PortError> {
        const POLL_INTERVAL_MS: u64 = 1_000;
        const BUFFER_CAP: usize = 16;
        let seq = Arc::new(AtomicU64::new(0));
        let (tx, rx) =
            overflow::policy_channel(BUFFER_CAP, policy, crate::adapters::market_event_key);
        for sub in req.subscriptions {
            let channel = match sub.channel {
                buffa::EnumValue::Known(c) => c,
                buffa::EnumValue::Unknown(_) => market::StreamChannel::Unspecified,
            };
            if sub.symbol.is_empty() {
                continue;
            }
            let exchange_id = req.exchange_id.clone();
            let client = self.clone();
            let tx = tx.clone();
            let seq = Arc::clone(&seq);
            let symbol = sub.symbol;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(POLL_INTERVAL_MS));
                loop {
                    if tx.is_closed() {
                        break;
                    }
                    ticker.tick().await;
                    if tx.is_closed() {
                        break;
                    }
                    match client.poll_snapshot(channel, &exchange_id, &symbol, &seq).await {
                        Ok(Some(event)) => {
                            tx.send(event).await;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, symbol = %symbol, "terminal poll failed");
                        }
                    }
                }
            });
        }
        Ok(rx)
    }
}

impl TerminalAdapter {
    async fn poll_snapshot(
        &self,
        channel: market::StreamChannel,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        seq: &AtomicU64,
    ) -> Result<Option<market::MarketDataEvent>, PortError> {
        let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
        let header = common::EventHeader { sequence: next, ..Default::default() };
        let event = if channel == market::StreamChannel::Orderbook {
            let book = self
                .fetch_order_book(market::FetchOrderBookRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.to_string(),
                    pagination: common::Pagination { limit: 100, ..Default::default() }.into(),
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
            resume_token: next.to_string(),
            ..Default::default()
        }))
    }
}

// Extended caps: terminal backends don't expose these; explicit Unsupported.
use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TriggerOrderGateway,
    TriggerOrderRequest, VenueOpInvoker, WalletGateway,
};

fn unsupported(capability: &str) -> PortError {
    PortError::Unsupported(format!(
        "terminal backend does not expose '{capability}' yet; use mock or venue daemon"
    ))
}

#[async_trait]
impl FundingRateSource for TerminalAdapter {
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
impl TriggerOrderGateway for TerminalAdapter {
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
impl VenueOpInvoker for TerminalAdapter {
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
impl WalletGateway for TerminalAdapter {
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
