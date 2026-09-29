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
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use buffa::MessageField;
use longtrader_proto::{
    client::{TerminalClient, TerminalClientError},
    proto::longtrader::terminal::v1 as tproto,
};
use rust_decimal::Decimal;

use crate::{
    ports::{MarketDataSource, MarketEventStream, OverflowPolicy, PortError, TradingGateway},
    proto::{account, common, market, ops, trading, worker},
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
        // `trading.v1.Position.order_id` is optional: an empty terminal string
        // means the venue did not report an originating order.
        order_id: (!p.order_id.is_empty()).then(|| p.order_id.clone()),
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
        // The unified contract's `trigger_price` maps onto the terminal
        // surface's `stop_price`; it is mapped here rather than dropped.
        let stop_price = match inner.trigger_price.as_option() {
            Some(p) => {
                let v = longtrader_contract::ext::common_to_decimal(p)?;
                Some(v.to_string())
            }
            None => None,
        };
        // A conditional order with no trigger can never fire, so reject it
        // rather than sending the venue an ordinary order that would execute
        // at market — the worst possible failure mode for a STOP.
        if matches!(otype, tproto::OrderType::Stop | tproto::OrderType::StopLimit) &&
            stop_price.is_none()
        {
            return Err(PortError::InvalidArgument(
                "conditional order requires trigger_price".to_string(),
            ));
        }
        // `reduce_only` is mapped, not dropped: silently ignoring it turns a
        // risk-reducing close into an order that can open opposite exposure.
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
                stop_price.as_deref(),
                None,
                None,
                &inner.client_order_id,
                inner.reduce_only,
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

    async fn modify_position(
        &self,
        req: trading::ModifyPositionRequest,
    ) -> Result<trading::ModifyPositionResponse, PortError> {
        let venue = venue_of(
            req.exchange_id
                .as_option()
                .ok_or_else(|| PortError::MissingField("exchange_id".to_string()))?,
        )?;
        let take_profit = req
            .take_profit
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .map(|v| v.to_string());
        let stop_loss = req
            .stop_loss
            .as_option()
            .map(longtrader_contract::ext::common_to_decimal)
            .transpose()?
            .map(|v| v.to_string());
        let position = self
            .client
            .modify_position(&venue, &req.position_id, take_profit.as_deref(), stop_loss.as_deref())
            .await
            .map_err(map_client_err)?;
        Ok(trading::ModifyPositionResponse {
            position: MessageField::some(position_to_unified(&position)?),
            ..Default::default()
        })
    }

    async fn sync_state(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<worker::ReconcileStateResponse, PortError> {
        // Best-effort snapshot over three terminal reads (NOT atomic).
        // SECURITY NOTE: Same caveat as RemoteAdapter — the snapshot is NOT
        // atomic and `snapshot_sequence` is a local monotonic counter.
        // Strategies MUST replay deltas after `snapshot_sequence` to converge.
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

/// Map the contract's timeframe string onto the terminal enum.
///
/// An unknown timeframe is an error, not a silent `M1`: a strategy asking for
/// `H4` and receiving 1-minute candles gets a signal that looks plausible and
/// is wrong, which is far worse than a rejected request.
fn timeframe_to_terminal(tf: &str) -> Result<tproto::Timeframe, PortError> {
    match tf.trim().to_ascii_uppercase().as_str() {
        "" | "M1" => Ok(tproto::Timeframe::M1),
        "S100" => Ok(tproto::Timeframe::S100),
        "S1" => Ok(tproto::Timeframe::S1),
        "M5" => Ok(tproto::Timeframe::M5),
        "M15" => Ok(tproto::Timeframe::M15),
        "M30" => Ok(tproto::Timeframe::M30),
        "H1" => Ok(tproto::Timeframe::H1),
        "H4" => Ok(tproto::Timeframe::H4),
        "D1" => Ok(tproto::Timeframe::D1),
        "W1" => Ok(tproto::Timeframe::W1),
        other => Err(PortError::InvalidArgument(format!(
            "unsupported timeframe {other:?} (supported: S1/S100/M1/M5/M15/M30/H1/H4/D1/W1)"
        ))),
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
            .get_candles(&venue, &req.symbol, timeframe_to_terminal(&req.timeframe)?, limit)
            .await
            .map_err(map_client_err)?;
        // A malformed OHLCV field is propagated as an error instead of being
        // coerced to zero. A zero price is a plausible-looking candle that
        // corrupts every downstream indicator, whereas a failed request is
        // something a strategy can retry or degrade from.
        let mut out = Vec::with_capacity(candles.len());
        for c in &candles {
            out.push(market::Candle {
                timestamp_ms: c.timestamp_ms,
                open: MessageField::some(str_to_common(&c.open, "open")?),
                high: MessageField::some(str_to_common(&c.high, "high")?),
                low: MessageField::some(str_to_common(&c.low, "low")?),
                close: MessageField::some(str_to_common(&c.close, "close")?),
                volume: MessageField::some(str_to_common(&c.volume, "volume")?),
                ..Default::default()
            });
        }
        Ok(market::GetCandlesResponse { candles: out, ..Default::default() })
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
        let this = self.clone();
        Ok(crate::adapters::poll_market_data(
            req,
            policy,
            move |channel, exchange_id, symbol, seq| {
                let this = this.clone();
                async move { this.poll_snapshot(channel, &exchange_id, symbol, &seq).await }
            },
        ))
    }
}

impl TerminalAdapter {
    async fn poll_snapshot(
        &self,
        channel: market::StreamChannel,
        exchange_id: &common::ExchangeId,
        symbol: String,
        seq: &AtomicU64,
    ) -> Result<Option<market::MarketDataEvent>, PortError> {
        let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
        let header = common::EventHeader { sequence: next, ..Default::default() };
        let event = if channel == market::StreamChannel::Orderbook {
            let book = self
                .fetch_order_book(market::FetchOrderBookRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.clone(),
                    pagination: common::Pagination { limit: 100, ..Default::default() }.into(),
                    ..Default::default()
                })
                .await?;
            market::market_data_event::Event::Orderbook(Box::new(book))
        } else {
            let ticker = self
                .fetch_ticker(market::FetchTickerRequest {
                    exchange_id: MessageField::some(exchange_id.clone()),
                    symbol: symbol.clone(),
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

// Extended capabilities: funding, venue-side conditional orders, venue ops and
// wallet. These map onto the `terminal.v1` funding / trigger / ledger / transfer
// RPCs, so a terminal backend supports exactly the same strategies as the
// paper venue. Every conversion is explicit: a decimal that fails to parse is
// an error, and an `optional` field the venue left unset stays `None` rather
// than becoming a zero that a strategy would read as a real measurement.
use crate::ports::{
    FundingRatePoint, FundingRateSnapshot, FundingRateSource, LedgerEntry, TransferReceipt,
    TriggerOrder, TriggerOrderGateway, TriggerOrderRequest, TriggerOrderStatus, VenueOpDescriptor,
    VenueOpInvoker, VenueOpParam, VenueOpParamType, WalletGateway,
};

/// The server bounds `limit` itself; this is the value a client asks for when
/// it has no opinion, matching the historical default.
const DEFAULT_LIMIT: u32 = 100;

/// Parse an optional string decimal, treating an empty string as absent.
///
/// The terminal surface carries decimals as strings, so "not reported" and
/// "reported as empty" are the same thing on the wire.
fn opt_dec_str(s: &str, field: &str) -> Result<Option<Decimal>, PortError> {
    if s.is_empty() {
        Ok(None)
    } else {
        s.parse::<Decimal>()
            .map(Some)
            .map_err(|_| PortError::InvalidArgument(format!("bad decimal {field}={s:?}")))
    }
}

/// Map a terminal `FundingRate` message onto the port snapshot.
fn funding_rate_to_port(rate: &tproto::FundingRate) -> Result<FundingRateSnapshot, PortError> {
    Ok(FundingRateSnapshot {
        symbol: rate.symbol.clone(),
        rate: parse_dec(&rate.rate, "funding_rate.rate")?,
        next_funding_time_ms: rate.next_funding_ts_ms,
        mark_price: opt_dec_str(&rate.mark_price, "funding_rate.mark_price")?,
    })
}

/// Map an `account.v1.LedgerEntry` onto the port record.
///
/// `direction` is deliberately dropped: the port's `amount` is signed, so a
/// "withdrawal" must arrive negative. Trusting a venue's unsigned amount
/// instead would let a withdrawal look like a deposit.
fn ledger_entry_to_port(entry: &account::LedgerEntry) -> Result<LedgerEntry, PortError> {
    let amount = entry
        .amount
        .as_option()
        .ok_or_else(|| PortError::MissingField("ledger_entry.amount".to_string()))?;
    let magnitude = longtrader_contract::ext::common_to_decimal(amount)?;
    let out_of = entry.direction.eq_ignore_ascii_case("out");
    Ok(LedgerEntry {
        id: entry.id.clone(),
        currency: entry.currency.clone(),
        amount: if out_of { -magnitude } else { magnitude },
        entry_type: entry.r#type.clone(),
        completed: entry.status.eq_ignore_ascii_case("completed"),
        time_ms: entry
            .timestamp
            .as_option()
            .map_or(0, |ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
    })
}

/// Map an `ops.v1.OpDescriptor` onto the port descriptor.
///
/// `mutating` is preserved rather than defaulted: a client reads it to decide
/// whether a call needs a confirmation prompt, so reporting a fund-moving
/// operation as read-only would be a real safety regression.
fn op_descriptor_to_port(d: &ops::OpDescriptor) -> Result<VenueOpDescriptor, PortError> {
    Ok(VenueOpDescriptor {
        name: d.name.clone(),
        category: d.category.clone(),
        summary: d.summary.clone(),
        mutating: d.mutating,
        params: d
            .params
            .iter()
            .map(|p| VenueOpParam {
                name: p.name.clone(),
                r#type: param_type_from_wire(p.r#type),
                required: p.required,
                doc: p.doc.clone(),
                enum_values: p.enum_values.clone(),
            })
            .collect(),
    })
}

/// Map an `ops.v1.ParamType` onto the port enum; unknown degrades to
/// `Unspecified` rather than being guessed at.
fn param_type_from_wire(t: buffa::EnumValue<ops::ParamType>) -> VenueOpParamType {
    use ops::ParamType as W;
    match t {
        buffa::EnumValue::Known(W::String) => VenueOpParamType::String,
        buffa::EnumValue::Known(W::Int64) => VenueOpParamType::Int64,
        buffa::EnumValue::Known(W::Decimal) => VenueOpParamType::Decimal,
        buffa::EnumValue::Known(W::Bool) => VenueOpParamType::Bool,
        buffa::EnumValue::Known(W::Enum) => VenueOpParamType::Enum,
        buffa::EnumValue::Known(W::List) => VenueOpParamType::List,
        buffa::EnumValue::Known(W::Map) => VenueOpParamType::Map,
        buffa::EnumValue::Known(W::Unspecified) | buffa::EnumValue::Unknown(_) => {
            VenueOpParamType::Unspecified
        }
    }
}

/// Map a terminal `TriggerOrder` message onto the port record.
///
/// An unknown status is an error rather than defaulting to `Open`: reporting a
/// dead backstop as live is the one failure mode this port cannot tolerate.
fn trigger_order_to_port(order: &tproto::TriggerOrder) -> Result<TriggerOrder, PortError> {
    let is_buy = match order.side {
        buffa::EnumValue::Known(trading::OrderSide::Buy) => true,
        buffa::EnumValue::Known(trading::OrderSide::Sell) => false,
        buffa::EnumValue::Known(other) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has invalid side {other:?}",
                id = order.id
            )));
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has unknown side discriminant {v}",
                id = order.id
            )));
        }
    };
    let status = match order.status {
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Open) => TriggerOrderStatus::Open,
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Triggered) => {
            TriggerOrderStatus::Triggered
        }
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Canceled) => {
            TriggerOrderStatus::Canceled
        }
        buffa::EnumValue::Known(trading::TriggerOrderStatus::Rejected) => {
            TriggerOrderStatus::Rejected
        }
        buffa::EnumValue::Known(other) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has invalid status {other:?}",
                id = order.id
            )));
        }
        buffa::EnumValue::Unknown(v) => {
            return Err(PortError::InvalidArgument(format!(
                "trigger order {id} has unknown status discriminant {v}",
                id = order.id
            )));
        }
    };
    Ok(TriggerOrder {
        id: order.id.clone(),
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        is_buy,
        trigger_price: parse_dec(&order.trigger_price, "trigger_price")?,
        qty: parse_dec(&order.qty, "qty")?,
        reduce_only: order.reduce_only,
        status,
        created_at_ms: order
            .created_at
            .as_option()
            .map_or(0, |ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
        order_id: order.order_id.clone(),
        triggered_at_ms: order
            .triggered_at
            .as_option()
            .map(|ts| ts.seconds * 1000 + i64::from(ts.nanos / 1_000_000)),
    })
}

/// Map the port's trigger request onto a terminal `TriggerOrderRequest`.
fn trigger_request_to_terminal(
    req: &TriggerOrderRequest,
) -> Result<tproto::TriggerOrderRequest, PortError> {
    if req.symbol.trim().is_empty() {
        return Err(PortError::InvalidArgument("trigger order requires a symbol".to_string()));
    }
    if req.qty.is_sign_negative() || req.qty.is_zero() {
        let qty = req.qty;
        return Err(PortError::InvalidArgument(format!(
            "trigger order qty must be positive, got {qty}"
        )));
    }
    if req.trigger_price.is_sign_negative() || req.trigger_price.is_zero() {
        // A zero trigger fires immediately and silently becomes a market
        // order, which is the exact opposite of what a STOP is for.
        let trigger_price = req.trigger_price;
        return Err(PortError::InvalidArgument(format!(
            "trigger order trigger_price must be positive, got {trigger_price}"
        )));
    }
    Ok(tproto::TriggerOrderRequest {
        client_order_id: req.client_order_id.clone(),
        symbol: req.symbol.clone(),
        side: buffa::EnumValue::Known(if req.is_buy {
            trading::OrderSide::Buy
        } else {
            trading::OrderSide::Sell
        }),
        trigger_price: req.trigger_price.to_string(),
        qty: req.qty.to_string(),
        reduce_only: req.reduce_only,
        trigger_type: buffa::EnumValue::Known(trading::TriggerPriceType::Last),
        // `order_price` stays unset and `order_type` is MARKET: a protective
        // stop should fire into liquidity rather than rest unfilled at a price
        // the market has already passed through.
        order_price: None,
        order_type: buffa::EnumValue::Known(tproto::OrderType::Market),
        ..Default::default()
    })
}

#[async_trait]
impl FundingRateSource for TerminalAdapter {
    async fn fetch_funding_rate(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
    ) -> Result<FundingRateSnapshot, PortError> {
        let venue = venue_of(exchange_id)?;
        let rates = self
            .client
            .get_funding_rates(&venue, std::slice::from_ref(&symbol.to_string()))
            .await
            .map_err(map_client_err)?;
        // An empty list is a legitimate answer: the venue tracks no perps for
        // that symbol. It is distinct from the venue having no funding surface
        // at all, which arrives as an error.
        rates
            .iter()
            .find(|r| r.symbol == symbol)
            .ok_or_else(|| PortError::NotFound(format!("no funding rate for {venue}/{symbol}")))
            .and_then(funding_rate_to_port)
    }

    async fn fetch_funding_rate_history(
        &self,
        exchange_id: &common::ExchangeId,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRatePoint>, PortError> {
        let venue = venue_of(exchange_id)?;
        let points = self
            .client
            .get_funding_rate_history(&venue, symbol, limit)
            .await
            .map_err(map_client_err)?;
        points
            .iter()
            .map(|p| {
                Ok(FundingRatePoint {
                    rate: p.rate.parse::<Decimal>().map_err(|_| {
                        PortError::InvalidArgument(format!(
                            "bad decimal funding_rate_point.rate={:?}",
                            p.rate
                        ))
                    })?,
                    time_ms: p.funding_time_ms,
                })
            })
            .collect()
    }
}

#[async_trait]
impl TriggerOrderGateway for TerminalAdapter {
    async fn create_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        req: TriggerOrderRequest,
    ) -> Result<TriggerOrder, PortError> {
        let venue = venue_of(exchange_id)?;
        let order = self
            .client
            .create_trigger_order(&venue, trigger_request_to_terminal(&req)?)
            .await
            .map_err(map_client_err)?;
        if order.id.is_empty() {
            // Returning an empty id would leave the caller unable to cancel the
            // backstop it just placed, which is the one thing it must be able
            // to do.
            return Err(PortError::Transport(
                "venue returned a trigger order with no id".to_string(),
            ));
        }
        trigger_order_to_port(&order)
    }

    async fn cancel_trigger_order(
        &self,
        exchange_id: &common::ExchangeId,
        order_id: &str,
        symbol: &str,
    ) -> Result<TriggerOrder, PortError> {
        let venue = venue_of(exchange_id)?;
        self.client
            .cancel_trigger_order(&venue, order_id, symbol)
            .await
            .map_err(map_client_err)
            .and_then(|o| trigger_order_to_port(&o))
    }

    async fn list_trigger_orders(
        &self,
        exchange_id: &common::ExchangeId,
        symbols: &[String],
    ) -> Result<Vec<TriggerOrder>, PortError> {
        let venue = venue_of(exchange_id)?;
        self.client
            .list_trigger_orders(&venue, symbols)
            .await
            .map_err(map_client_err)?
            .iter()
            .map(trigger_order_to_port)
            .collect()
    }
}

#[async_trait]
impl VenueOpInvoker for TerminalAdapter {
    async fn invoke_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
        params: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, PortError> {
        if op.trim().is_empty() {
            return Err(PortError::MissingField("op".to_string()));
        }
        self.client
            .invoke_venue_op(exchange_id, op, &serde_json::Value::Object(params))
            .await
            .map_err(map_client_err)
    }

    async fn list_venue_ops(
        &self,
        exchange_id: &common::ExchangeId,
    ) -> Result<Vec<VenueOpDescriptor>, PortError> {
        self.client
            .list_venue_ops(exchange_id)
            .await
            .map_err(map_client_err)?
            .iter()
            .map(op_descriptor_to_port)
            .collect()
    }

    async fn describe_venue_op(
        &self,
        exchange_id: &common::ExchangeId,
        op: &str,
    ) -> Result<VenueOpDescriptor, PortError> {
        if op.trim().is_empty() {
            return Err(PortError::MissingField("op".to_string()));
        }
        self.client
            .describe_venue_op(exchange_id, op)
            .await
            .map_err(map_client_err)
            .and_then(|d| op_descriptor_to_port(&d))
    }
}

#[async_trait]
impl WalletGateway for TerminalAdapter {
    async fn list_ledger_entries(
        &self,
        exchange_id: &common::ExchangeId,
        currency: &str,
        entry_type: &str,
        limit: u32,
    ) -> Result<Vec<LedgerEntry>, PortError> {
        let venue = venue_of(exchange_id)?;
        let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };
        self.client
            .get_ledger_entries(&venue, currency, entry_type, limit)
            .await
            .map_err(map_client_err)?
            .iter()
            .map(ledger_entry_to_port)
            .collect()
    }

    async fn transfer(
        &self,
        exchange_id: &common::ExchangeId,
        asset: &str,
        amount: Decimal,
        dest_label: &str,
        client_transfer_id: &str,
    ) -> Result<TransferReceipt, PortError> {
        let venue = venue_of(exchange_id)?;
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
        let resp = self
            .client
            .transfer(&venue, asset, &amount.to_string(), dest_label, client_transfer_id)
            .await
            .map_err(map_client_err)?;
        Ok(TransferReceipt {
            transfer_id: resp.transfer_id,
            entry: resp.entry.as_option().map(ledger_entry_to_port).transpose()?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// These conversions sit between the unified contract and the venue
    /// surface, where a silent default is the dangerous outcome: a wrong
    /// timeframe or a zeroed price looks like valid data to a strategy.

    #[test]
    fn timeframe_accepts_documented_values() {
        for tf in ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "S1", "S100"] {
            assert!(timeframe_to_terminal(tf).is_ok(), "{tf} must be supported");
        }
    }

    #[test]
    fn timeframe_is_case_and_space_insensitive() {
        assert_eq!(timeframe_to_terminal("h4").expect("h4"), tproto::Timeframe::H4);
        assert_eq!(timeframe_to_terminal(" H4 ").expect("padded H4"), tproto::Timeframe::H4);
    }

    #[test]
    fn empty_timeframe_defaults_to_m1() {
        assert_eq!(timeframe_to_terminal("").expect("empty"), tproto::Timeframe::M1);
    }

    #[test]
    fn unknown_timeframe_is_rejected_not_defaulted() {
        // The previous behaviour silently returned M1, so a strategy asking
        // for H4 received 1-minute candles and computed a plausible, wrong
        // signal. An error is the only safe answer.
        let err = timeframe_to_terminal("H3").expect_err("H3 must not silently map to M1");
        assert!(matches!(err, PortError::InvalidArgument(_)), "got {err:?}");
        assert!(err.to_string().contains("H4"), "message should list valid options: {err}");
    }

    #[test]
    fn str_to_common_rejects_garbage() {
        assert!(str_to_common("not-a-number", "close").is_err());
        assert!(str_to_common("", "close").is_err());
    }

    #[test]
    fn str_to_common_accepts_valid_decimal_strings() {
        let d = str_to_common("1.25", "close").expect("valid");
        assert_eq!(d.unscaled, 125);
        assert_eq!(d.scale, 2);
    }

    #[test]
    fn venue_of_requires_a_non_empty_id() {
        let empty = common::ExchangeId::default();
        assert!(venue_of(&empty).is_err());
        let named = common::ExchangeId { id: "mock".to_string(), ..Default::default() };
        assert_eq!(venue_of(&named).expect("named"), "mock");
    }

    #[test]
    fn order_type_rejects_unsupported_variants() {
        // Anything outside Market/Limit/Stop/StopLimit must be an explicit
        // error rather than being coerced.
        assert!(order_type_to_terminal_type(trading::OrderType::Unspecified).is_err());
        for t in [trading::OrderType::Market, trading::OrderType::Limit, trading::OrderType::Stop] {
            assert!(order_type_to_terminal_type(t).is_ok(), "{t:?}");
        }
    }

    /// `reduce_only` must be carried, not dropped. Silently ignoring it turns a
    /// risk-reducing close into an order that can open opposite exposure — the
    /// single most dangerous thing to lose on a conditional order.
    #[test]
    fn trigger_request_carries_reduce_only_and_a_market_leg() {
        use rust_decimal_macros::dec;
        let req = TriggerOrderRequest {
            client_order_id: "coid-1".into(),
            symbol: "BTC/USDT".into(),
            is_buy: false,
            trigger_price: dec!(95000),
            qty: dec!(0.001),
            reduce_only: true,
        };
        let wire = trigger_request_to_terminal(&req).expect("valid");
        assert!(wire.reduce_only, "reduce_only must reach the venue");
        assert_eq!(wire.symbol, "BTC/USDT");
        assert_eq!(wire.client_order_id, "coid-1");
        assert_eq!(wire.trigger_price, "95000");
        assert_eq!(wire.qty, "0.001");
        assert_eq!(wire.side, buffa::EnumValue::Known(trading::OrderSide::Sell));
        // A protective stop fires into liquidity; a limit leg with no price
        // would rest unfilled at a level the market has already passed.
        assert_eq!(wire.order_type, buffa::EnumValue::Known(tproto::OrderType::Market));
        assert!(wire.order_price.is_none(), "no invented limit price");
        assert_eq!(wire.trigger_type, buffa::EnumValue::Known(trading::TriggerPriceType::Last));
    }

    #[test]
    fn trigger_request_rejects_a_degenerate_order() {
        use rust_decimal_macros::dec;
        let base = TriggerOrderRequest {
            client_order_id: String::new(),
            symbol: "BTC/USDT".into(),
            is_buy: true,
            trigger_price: dec!(95000),
            qty: dec!(1),
            reduce_only: false,
        };
        assert!(trigger_request_to_terminal(&base).is_ok());

        // A closed-book "add" of each degenerate case, rather than a label
        // match: the label indirection made the intent harder to read than the
        // explicit variants.
        let mut zero_qty = base.clone();
        zero_qty.qty = dec!(0);
        let mut negative_qty = base.clone();
        negative_qty.qty = dec!(-1);
        let mut zero_trigger = base.clone();
        zero_trigger.trigger_price = dec!(0);
        let mut negative_trigger = base.clone();
        negative_trigger.trigger_price = dec!(-95000);
        let mut blank_symbol = base;
        blank_symbol.symbol = "   ".into();

        for (label, req) in [
            ("zero qty", zero_qty),
            ("negative qty", negative_qty),
            ("zero trigger", zero_trigger),
            ("negative trigger", negative_trigger),
            ("blank symbol", blank_symbol),
        ] {
            assert!(
                trigger_request_to_terminal(&req).is_err(),
                "{label} must be rejected before it reaches the venue"
            );
        }
    }

    /// A withdrawal must arrive negative. Trusting the venue's unsigned amount
    /// would let a withdrawal read as a deposit.
    #[test]
    fn ledger_entry_sign_comes_from_direction() {
        use rust_decimal_macros::dec;
        let mk = |direction: &str, amount: &str| account::LedgerEntry {
            id: "l-1".into(),
            currency: "USDT".into(),
            direction: direction.into(),
            r#type: "transfer".into(),
            amount: str_to_common(amount, "amount").expect("decimal").into(),
            timestamp: buffa::MessageField::none(),
            status: "completed".into(),
            ..Default::default()
        };
        assert_eq!(ledger_entry_to_port(&mk("out", "100")).expect("mapped").amount, dec!(-100));
        assert_eq!(ledger_entry_to_port(&mk("in", "100")).expect("mapped").amount, dec!(100));
    }

    /// A pending row must not be reported as settled, or a strategy would sweep
    /// funds that have not arrived.
    #[test]
    fn ledger_entry_completed_is_case_insensitive() {
        let mk = |status: &str| account::LedgerEntry {
            id: "l-1".into(),
            currency: "USDT".into(),
            direction: "in".into(),
            r#type: "deposit".into(),
            amount: str_to_common("1", "amount").expect("decimal").into(),
            timestamp: buffa::MessageField::none(),
            status: status.into(),
            ..Default::default()
        };
        assert!(ledger_entry_to_port(&mk("COMPLETED")).expect("mapped").completed);
        assert!(!ledger_entry_to_port(&mk("pending")).expect("mapped").completed);
    }

    /// An unreported optional stays `None`; it must not become a zero that a
    /// strategy reads as a real measurement.
    #[test]
    fn an_unreported_mark_price_stays_absent() {
        let rate = tproto::FundingRate {
            symbol: "BTC/USDT".into(),
            rate: "0.0001".into(),
            next_funding_ts_ms: 1_700_000_000_000,
            mark_price: String::new(),
            venue: "binance".into(),
            ..Default::default()
        };
        let snapshot = funding_rate_to_port(&rate).expect("mapped");
        assert_eq!(snapshot.rate, rust_decimal_macros::dec!(0.0001));
        assert_eq!(snapshot.mark_price, None);
    }

    /// An unknown status is an error, never `Open`: reporting a dead backstop as
    /// live is the one failure this port cannot tolerate.
    #[test]
    fn a_trigger_order_with_an_unknown_status_is_rejected() {
        let order = tproto::TriggerOrder {
            id: "t-1".into(),
            side: buffa::EnumValue::Known(trading::OrderSide::Sell),
            trigger_price: "95000".into(),
            qty: "1".into(),
            status: buffa::EnumValue::Unknown(99),
            ..Default::default()
        };
        assert!(trigger_order_to_port(&order).is_err());
    }

    /// A side-less conditional order must be rejected, not defaulted to Buy —
    /// which would silently invert a stop.
    #[test]
    fn a_trigger_order_with_an_unset_side_is_rejected() {
        let order = tproto::TriggerOrder {
            id: "t-1".into(),
            side: buffa::EnumValue::Known(trading::OrderSide::Unspecified),
            trigger_price: "95000".into(),
            qty: "1".into(),
            status: buffa::EnumValue::Known(trading::TriggerOrderStatus::Open),
            ..Default::default()
        };
        assert!(trigger_order_to_port(&order).is_err());
    }
}
