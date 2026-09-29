package contract

// OrderRequest is an order submission, as carried by CreateOrder/CreateOrders.
type OrderRequest struct {
	// ClientOrderID is the idempotency key the backend dedupes on; a retry
	// after a timeout MUST reuse it.
	ClientOrderID string
	// Symbol is the instrument, e.g. "BTC/USDT".
	Symbol string
	// Type is the order type.
	Type OrderType
	// Side is the order side as seen by the submitter.
	Side OrderSide
	// Amount is the order size.
	Amount Decimal
	// Price is the limit price; zero for market orders.
	Price Decimal
	// TriggerPrice arms a stop order on the main book; zero when unused.
	TriggerPrice Decimal
	// TimeInForce is the order lifetime.
	TimeInForce TimeInForce
	// PostOnly rejects the order if it would cross the book.
	PostOnly bool
	// ReduceOnly never increases exposure.
	ReduceOnly bool
	// Params carries venue-specific order parameters.
	Params map[string]string
}

// MarshalTo implements Message.
func (m *OrderRequest) MarshalTo(e *Encoder) {
	e.String(1, m.ClientOrderID)
	e.String(2, m.Symbol)
	e.Int32(3, int32(m.Type))
	e.Int32(4, int32(m.Side))
	e.Message(5, &m.Amount)
	e.Message(6, &m.Price)
	e.Message(7, &m.TriggerPrice)
	e.Int32(8, int32(m.TimeInForce))
	e.Bool(9, m.PostOnly)
	e.Bool(10, m.ReduceOnly)
	encodeMap(e, 11, m.Params)
}

// Unmarshal implements Message.
func (m *OrderRequest) Unmarshal(data []byte) error {
	*m = OrderRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ClientOrderID = f.AsString()
		case 2:
			m.Symbol = f.AsString()
		case 3:
			m.Type = OrderType(f.AsInt32())
		case 4:
			m.Side = OrderSide(f.AsInt32())
		case 5:
			return decodeSub(f, &m.Amount)
		case 6:
			return decodeSub(f, &m.Price)
		case 7:
			return decodeSub(f, &m.TriggerPrice)
		case 8:
			m.TimeInForce = TimeInForce(f.AsInt32())
		case 9:
			m.PostOnly = f.AsBool()
		case 10:
			m.ReduceOnly = f.AsBool()
		case 11:
			return decodeMapEntry(f, 11, &m.Params)
		}
		return nil
	})
}

// Order is a venue order and its execution state.
type Order struct {
	// ID is the venue order id used by CancelOrder.
	ID string
	// ClientOrderID is the submitter's idempotency key.
	ClientOrderID string
	// Symbol is the instrument.
	Symbol string
	// Type is the order type.
	Type OrderType
	// Side is the order side.
	Side OrderSide
	// Status is the current lifecycle status.
	Status OrderStatus
	// Amount is the requested size.
	Amount Decimal
	// Price is the limit price.
	Price Decimal
	// Filled is the executed size.
	Filled Decimal
	// Remaining is the unfilled size.
	Remaining Decimal
	// Cost is the filled notional.
	Cost Decimal
	// Average is the average fill price.
	Average Decimal
	// Fee is the paid fee.
	Fee Decimal
	// FeeCurrency is the asset the fee was charged in.
	FeeCurrency string
	// TimeInForce is the order lifetime.
	TimeInForce TimeInForce
	// Timestamp is the venue event time.
	Timestamp *Timestamp
	// LastTradeTimestamp is the last fill time.
	LastTradeTimestamp *Timestamp
	// PostOnly reflects the submitted post-only flag.
	PostOnly bool
	// ReduceOnly reflects the submitted reduce-only flag.
	ReduceOnly bool
	// Info carries venue-specific order metadata.
	Info map[string]string
	// CreatedAt is the creation time.
	CreatedAt *Timestamp
	// UpdatedAt is the last update time.
	UpdatedAt *Timestamp
	// TakeProfit is the attached bracket target, when set.
	TakeProfit *Decimal
	// StopLoss is the attached bracket stop, when set.
	StopLoss *Decimal
}

// MarshalTo implements Message.
func (m *Order) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.ClientOrderID)
	e.String(3, m.Symbol)
	e.Int32(4, int32(m.Type))
	e.Int32(5, int32(m.Side))
	e.Int32(6, int32(m.Status))
	e.Message(7, &m.Amount)
	e.Message(8, &m.Price)
	e.Message(9, &m.Filled)
	e.Message(10, &m.Remaining)
	e.Message(11, &m.Cost)
	e.Message(12, &m.Average)
	e.Message(13, &m.Fee)
	e.String(14, m.FeeCurrency)
	e.Int32(15, int32(m.TimeInForce))
	e.Message(16, m.Timestamp)
	e.Message(17, m.LastTradeTimestamp)
	e.Bool(18, m.PostOnly)
	e.Bool(19, m.ReduceOnly)
	encodeMap(e, 20, m.Info)
	e.Message(22, m.CreatedAt)
	e.Message(23, m.UpdatedAt)
	e.Message(24, m.TakeProfit)
	e.Message(25, m.StopLoss)
}

// Unmarshal implements Message.
func (m *Order) Unmarshal(data []byte) error {
	*m = Order{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.ClientOrderID = f.AsString()
		case 3:
			m.Symbol = f.AsString()
		case 4:
			m.Type = OrderType(f.AsInt32())
		case 5:
			m.Side = OrderSide(f.AsInt32())
		case 6:
			m.Status = OrderStatus(f.AsInt32())
		case 7:
			return decodeSub(f, &m.Amount)
		case 8:
			return decodeSub(f, &m.Price)
		case 9:
			return decodeSub(f, &m.Filled)
		case 10:
			return decodeSub(f, &m.Remaining)
		case 11:
			return decodeSub(f, &m.Cost)
		case 12:
			return decodeSub(f, &m.Average)
		case 13:
			return decodeSub(f, &m.Fee)
		case 14:
			m.FeeCurrency = f.AsString()
		case 15:
			m.TimeInForce = TimeInForce(f.AsInt32())
		case 16:
			return decodeTime(f, &m.Timestamp)
		case 17:
			return decodeTime(f, &m.LastTradeTimestamp)
		case 18:
			m.PostOnly = f.AsBool()
		case 19:
			m.ReduceOnly = f.AsBool()
		case 20:
			return decodeMapEntry(f, 20, &m.Info)
		case 22:
			return decodeTime(f, &m.CreatedAt)
		case 23:
			return decodeTime(f, &m.UpdatedAt)
		case 24:
			return decodeDecimal(f, &m.TakeProfit)
		case 25:
			return decodeDecimal(f, &m.StopLoss)
		}
		return nil
	})
}

// Account is an account-level margin summary.
type Account struct {
	// Balance is the total balance.
	Balance Decimal
	// Equity is balance plus unrealized PnL.
	Equity Decimal
	// MarginUsed is the margin consumed by open positions.
	MarginUsed Decimal
	// FreeMargin is margin still available.
	FreeMargin Decimal
	// MarginFrozen is margin locked in pending orders.
	MarginFrozen Decimal
}

// MarshalTo implements Message.
func (m *Account) MarshalTo(e *Encoder) {
	e.Message(1, &m.Balance)
	e.Message(2, &m.Equity)
	e.Message(3, &m.MarginUsed)
	e.Message(4, &m.FreeMargin)
	e.Message(5, &m.MarginFrozen)
}

// Unmarshal implements Message.
func (m *Account) Unmarshal(data []byte) error {
	*m = Account{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			return decodeSub(f, &m.Balance)
		case 2:
			return decodeSub(f, &m.Equity)
		case 3:
			return decodeSub(f, &m.MarginUsed)
		case 4:
			return decodeSub(f, &m.FreeMargin)
		case 5:
			return decodeSub(f, &m.MarginFrozen)
		}
		return nil
	})
}

// Trade is one fill against an order.
type Trade struct {
	// ID is the venue trade id.
	ID string
	// OrderID is the venue order id it filled.
	OrderID string
	// Symbol is the instrument.
	Symbol string
	// Side is the taker side.
	Side OrderSide
	// Price is the fill price.
	Price Decimal
	// Amount is the filled size.
	Amount Decimal
	// Cost is the fill notional.
	Cost Decimal
	// Fee is the paid fee.
	Fee Decimal
	// FeeCurrency is the asset the fee was charged in.
	FeeCurrency string
	// Timestamp is the fill time.
	Timestamp *Timestamp
	// Maker reports whether the order was resting when it filled.
	Maker bool
}

// MarshalTo implements Message.
func (m *Trade) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.OrderID)
	e.String(3, m.Symbol)
	e.Int32(4, int32(m.Side))
	e.Message(5, &m.Price)
	e.Message(6, &m.Amount)
	e.Message(7, &m.Cost)
	e.Message(8, &m.Fee)
	e.String(9, m.FeeCurrency)
	e.Message(10, m.Timestamp)
	e.Bool(11, m.Maker)
}

// Unmarshal implements Message.
func (m *Trade) Unmarshal(data []byte) error {
	*m = Trade{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.OrderID = f.AsString()
		case 3:
			m.Symbol = f.AsString()
		case 4:
			m.Side = OrderSide(f.AsInt32())
		case 5:
			return decodeSub(f, &m.Price)
		case 6:
			return decodeSub(f, &m.Amount)
		case 7:
			return decodeSub(f, &m.Cost)
		case 8:
			return decodeSub(f, &m.Fee)
		case 9:
			m.FeeCurrency = f.AsString()
		case 10:
			return decodeTime(f, &m.Timestamp)
		case 11:
			m.Maker = f.AsBool()
		}
		return nil
	})
}

// Position is one open position.
type Position struct {
	// ID is the position id used by ClosePosition and ModifyPosition.
	ID string
	// Symbol is the instrument.
	Symbol string
	// Contracts is the size in contracts.
	Contracts Decimal
	// ContractSize is the notional per contract.
	ContractSize Decimal
	// Side is the position side.
	Side OrderSide
	// EntryPrice is the average entry price.
	EntryPrice Decimal
	// MarkPrice is the venue mark price.
	MarkPrice Decimal
	// UnrealizedPnL is the open PnL.
	UnrealizedPnL Decimal
	// OrderID is the originating order, when the venue reports one.
	OrderID *string
	// CurrentPrice is the last traded price.
	CurrentPrice Decimal
	// Timestamp is the venue event time.
	Timestamp *Timestamp
	// TakeProfit is the attached bracket target, when set.
	TakeProfit *Decimal
	// StopLoss is the attached bracket stop, when set.
	StopLoss *Decimal
	// OpenedAt is the position open time.
	OpenedAt *Timestamp
}

// MarshalTo implements Message.
func (m *Position) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.Symbol)
	e.Message(3, &m.Contracts)
	e.Message(4, &m.ContractSize)
	e.Int32(5, int32(m.Side))
	e.Message(6, &m.EntryPrice)
	e.Message(7, &m.MarkPrice)
	e.Message(8, &m.UnrealizedPnL)
	encodeOptString(e, 9, m.OrderID)
	e.Message(10, &m.CurrentPrice)
	e.Message(11, m.Timestamp)
	e.Message(12, m.TakeProfit)
	e.Message(13, m.StopLoss)
	e.Message(17, m.OpenedAt)
}

// Unmarshal implements Message.
func (m *Position) Unmarshal(data []byte) error {
	*m = Position{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.Symbol = f.AsString()
		case 3:
			return decodeSub(f, &m.Contracts)
		case 4:
			return decodeSub(f, &m.ContractSize)
		case 5:
			m.Side = OrderSide(f.AsInt32())
		case 6:
			return decodeSub(f, &m.EntryPrice)
		case 7:
			return decodeSub(f, &m.MarkPrice)
		case 8:
			return decodeSub(f, &m.UnrealizedPnL)
		case 9:
			if f.Wire == WireBytes {
				v := f.AsString()
				m.OrderID = &v
			}
		case 10:
			return decodeSub(f, &m.CurrentPrice)
		case 11:
			return decodeTime(f, &m.Timestamp)
		case 12:
			return decodeDecimal(f, &m.TakeProfit)
		case 13:
			return decodeDecimal(f, &m.StopLoss)
		case 17:
			return decodeTime(f, &m.OpenedAt)
		}
		return nil
	})
}

// ClosedPosition is one completed position with its realized outcome.
type ClosedPosition struct {
	// ID is the position id.
	ID string
	// OrderID is the closing order, when the venue reports one.
	OrderID string
	// Symbol is the instrument.
	Symbol string
	// Side is the position side that was closed.
	Side PositionSide
	// Quantity is the closed size.
	Quantity Decimal
	// EntryPrice is the average entry price.
	EntryPrice Decimal
	// ExitPrice is the average exit price.
	ExitPrice Decimal
	// RealizedPnL is the booked profit or loss.
	RealizedPnL Decimal
	// OpenedAt is the position open time.
	OpenedAt *Timestamp
	// ClosedAt is the close time.
	ClosedAt *Timestamp
	// CloseReason is the canonical close reason.
	CloseReason CloseReason
}

// MarshalTo implements Message.
func (m *ClosedPosition) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.OrderID)
	e.String(3, m.Symbol)
	e.Int32(4, int32(m.Side))
	e.Message(5, &m.Quantity)
	e.Message(6, &m.EntryPrice)
	e.Message(7, &m.ExitPrice)
	e.Message(8, &m.RealizedPnL)
	e.Message(9, m.OpenedAt)
	e.Message(10, m.ClosedAt)
	e.Int32(11, int32(m.CloseReason))
}

// Unmarshal implements Message.
func (m *ClosedPosition) Unmarshal(data []byte) error {
	*m = ClosedPosition{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.OrderID = f.AsString()
		case 3:
			m.Symbol = f.AsString()
		case 4:
			m.Side = PositionSide(f.AsInt32())
		case 5:
			return decodeSub(f, &m.Quantity)
		case 6:
			return decodeSub(f, &m.EntryPrice)
		case 7:
			return decodeSub(f, &m.ExitPrice)
		case 8:
			return decodeSub(f, &m.RealizedPnL)
		case 9:
			return decodeTime(f, &m.OpenedAt)
		case 10:
			return decodeTime(f, &m.ClosedAt)
		case 11:
			m.CloseReason = CloseReason(f.AsInt32())
		}
		return nil
	})
}

// CreateOrderRequest submits one order.
//
// SessionID is what binds the order to the calling strategy session: the host
// rejects a pre-ACTIVE submission with reason SYNC_IN_PROGRESS and records the
// resulting order id so KillSwitchPolicy.SCOPE_SESSION_ORDERS and
// StopStrategy(cancel_open_orders) can cancel exactly this session's orders.
// An empty value marks an unscoped operator call, which is neither gated nor
// tracked -- so a strategy must always send it.
type CreateOrderRequest struct {
	// ExchangeID selects the backend instance; nil leaves the host default.
	ExchangeID *ExchangeId
	// Order is the order to submit.
	Order *OrderRequest
	// SessionID is the submitting session, as issued by AttachSession.
	SessionID string
}

// MarshalTo implements Message.
func (m *CreateOrderRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.Message(2, m.Order)
	e.String(3, m.SessionID)
}

// Unmarshal implements Message.
func (m *CreateOrderRequest) Unmarshal(data []byte) error {
	*m = CreateOrderRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Order = &OrderRequest{}
			return m.Order.Unmarshal(f.Data)
		case 3:
			m.SessionID = f.AsString()
		}
		return nil
	})
}

// CreateOrderResponse carries the accepted order.
type CreateOrderResponse struct {
	// Order is the venue order, including its assigned id.
	Order *Order
}

// MarshalTo implements Message.
func (m *CreateOrderResponse) MarshalTo(e *Encoder) { e.Message(1, m.Order) }

// Unmarshal implements Message.
func (m *CreateOrderResponse) Unmarshal(data []byte) error {
	*m = CreateOrderResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Order = &Order{}
			return m.Order.Unmarshal(f.Data)
		}
		return nil
	})
}

// CreateOrdersRequest submits a batch of orders under one session gate.
type CreateOrdersRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Orders are the orders to submit.
	Orders []*OrderRequest
	// SessionID is the submitting session; the gate is all-or-nothing.
	SessionID string
}

// MarshalTo implements Message.
func (m *CreateOrdersRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	for _, o := range m.Orders {
		e.Message(2, o)
	}
	e.String(3, m.SessionID)
}

// Unmarshal implements Message.
func (m *CreateOrdersRequest) Unmarshal(data []byte) error {
	*m = CreateOrdersRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			o := &OrderRequest{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Orders = append(m.Orders, o)
		case 3:
			m.SessionID = f.AsString()
		}
		return nil
	})
}

// CreateOrdersResponse carries the accepted batch.
type CreateOrdersResponse struct {
	// Orders are the venue orders, in request order.
	Orders []*Order
}

// MarshalTo implements Message.
func (m *CreateOrdersResponse) MarshalTo(e *Encoder) {
	for _, o := range m.Orders {
		e.Message(1, o)
	}
}

// Unmarshal implements Message.
func (m *CreateOrdersResponse) Unmarshal(data []byte) error {
	*m = CreateOrdersResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			o := &Order{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Orders = append(m.Orders, o)
		}
		return nil
	})
}

// CancelOrderRequest cancels one order by venue order id.
type CancelOrderRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// OrderID is the venue order id.
	OrderID string
	// Symbol is the instrument; optional but helps venue-side lookup.
	Symbol string
}

// MarshalTo implements Message.
func (m *CancelOrderRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.OrderID)
	e.String(3, m.Symbol)
}

// Unmarshal implements Message.
func (m *CancelOrderRequest) Unmarshal(data []byte) error {
	*m = CancelOrderRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.OrderID = f.AsString()
		case 3:
			m.Symbol = f.AsString()
		}
		return nil
	})
}

// CancelOrderResponse carries the cancelled order.
type CancelOrderResponse struct {
	// Order is the order in its post-cancel state.
	Order *Order
}

// MarshalTo implements Message.
func (m *CancelOrderResponse) MarshalTo(e *Encoder) { e.Message(1, m.Order) }

// Unmarshal implements Message.
func (m *CancelOrderResponse) Unmarshal(data []byte) error {
	*m = CancelOrderResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Order = &Order{}
			return m.Order.Unmarshal(f.Data)
		}
		return nil
	})
}

// CancelAllOrdersRequest cancels every open order; an empty symbol spans all
// symbols. This is the kill-switch execution path.
type CancelAllOrdersRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbol restricts the cancellation to one instrument.
	Symbol string
}

// MarshalTo implements Message.
func (m *CancelAllOrdersRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.Symbol)
}

// Unmarshal implements Message.
func (m *CancelAllOrdersRequest) Unmarshal(data []byte) error {
	*m = CancelAllOrdersRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		}
		return nil
	})
}

// CancelAllOrdersResponse carries every cancelled order.
type CancelAllOrdersResponse struct {
	// Orders are the cancelled orders.
	Orders []*Order
}

// MarshalTo implements Message.
func (m *CancelAllOrdersResponse) MarshalTo(e *Encoder) {
	for _, o := range m.Orders {
		e.Message(1, o)
	}
}

// Unmarshal implements Message.
func (m *CancelAllOrdersResponse) Unmarshal(data []byte) error {
	*m = CancelAllOrdersResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			o := &Order{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Orders = append(m.Orders, o)
		}
		return nil
	})
}

// FetchOpenOrdersRequest lists currently open orders.
type FetchOpenOrdersRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbol restricts the listing to one instrument.
	Symbol string
	// Pagination bounds the page.
	Pagination *Pagination
}

// MarshalTo implements Message.
func (m *FetchOpenOrdersRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.Symbol)
	e.Message(3, m.Pagination)
}

// Unmarshal implements Message.
func (m *FetchOpenOrdersRequest) Unmarshal(data []byte) error {
	*m = FetchOpenOrdersRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		case 3:
			if f.Wire != WireBytes {
				return nil
			}
			m.Pagination = &Pagination{}
			return m.Pagination.Unmarshal(f.Data)
		}
		return nil
	})
}

// FetchOpenOrdersResponse is one page of open orders.
type FetchOpenOrdersResponse struct {
	// Orders are the open orders in this page.
	Orders []*Order
	// Page carries the next cursor and total.
	Page *Page
}

// MarshalTo implements Message.
func (m *FetchOpenOrdersResponse) MarshalTo(e *Encoder) {
	for _, o := range m.Orders {
		e.Message(1, o)
	}
	e.Message(2, m.Page)
}

// Unmarshal implements Message.
func (m *FetchOpenOrdersResponse) Unmarshal(data []byte) error {
	*m = FetchOpenOrdersResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			o := &Order{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Orders = append(m.Orders, o)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Page = &Page{}
			return m.Page.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetAccountRequest fetches the account margin summary.
type GetAccountRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
}

// MarshalTo implements Message.
func (m *GetAccountRequest) MarshalTo(e *Encoder) { e.Message(1, m.ExchangeID) }

// Unmarshal implements Message.
func (m *GetAccountRequest) Unmarshal(data []byte) error {
	*m = GetAccountRequest{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetAccountResponse carries the account summary.
type GetAccountResponse struct {
	// Account is the venue account state.
	Account *Account
}

// MarshalTo implements Message.
func (m *GetAccountResponse) MarshalTo(e *Encoder) { e.Message(1, m.Account) }

// Unmarshal implements Message.
func (m *GetAccountResponse) Unmarshal(data []byte) error {
	*m = GetAccountResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Account = &Account{}
			return m.Account.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetPositionsRequest lists open positions.
type GetPositionsRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbols restricts the listing; empty means every symbol.
	Symbols []string
}

// MarshalTo implements Message.
func (m *GetPositionsRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	for _, s := range m.Symbols {
		e.String(2, s)
	}
}

// Unmarshal implements Message.
func (m *GetPositionsRequest) Unmarshal(data []byte) error {
	*m = GetPositionsRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire == WireBytes {
				m.Symbols = append(m.Symbols, f.AsString())
			}
		}
		return nil
	})
}

// GetPositionsResponse carries the open positions.
type GetPositionsResponse struct {
	// Positions are the open positions.
	Positions []*Position
}

// MarshalTo implements Message.
func (m *GetPositionsResponse) MarshalTo(e *Encoder) {
	for _, p := range m.Positions {
		e.Message(1, p)
	}
}

// Unmarshal implements Message.
func (m *GetPositionsResponse) Unmarshal(data []byte) error {
	*m = GetPositionsResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			p := &Position{}
			if err := p.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Positions = append(m.Positions, p)
		}
		return nil
	})
}

// GetOrderHistoryRequest lists historical orders.
type GetOrderHistoryRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Pagination bounds the page.
	Pagination *Pagination
}

// MarshalTo implements Message.
func (m *GetOrderHistoryRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.Message(2, m.Pagination)
}

// Unmarshal implements Message.
func (m *GetOrderHistoryRequest) Unmarshal(data []byte) error {
	*m = GetOrderHistoryRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Pagination = &Pagination{}
			return m.Pagination.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetOrderHistoryResponse is one page of historical orders.
type GetOrderHistoryResponse struct {
	// Orders are the historical orders in this page.
	Orders []*Order
	// Page carries the next cursor and total.
	Page *Page
}

// MarshalTo implements Message.
func (m *GetOrderHistoryResponse) MarshalTo(e *Encoder) {
	for _, o := range m.Orders {
		e.Message(1, o)
	}
	e.Message(2, m.Page)
}

// Unmarshal implements Message.
func (m *GetOrderHistoryResponse) Unmarshal(data []byte) error {
	*m = GetOrderHistoryResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			o := &Order{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Orders = append(m.Orders, o)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Page = &Page{}
			return m.Page.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetClosedPositionsRequest lists closed positions.
type GetClosedPositionsRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Pagination bounds the page.
	Pagination *Pagination
}

// MarshalTo implements Message.
func (m *GetClosedPositionsRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.Message(2, m.Pagination)
}

// Unmarshal implements Message.
func (m *GetClosedPositionsRequest) Unmarshal(data []byte) error {
	*m = GetClosedPositionsRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Pagination = &Pagination{}
			return m.Pagination.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetClosedPositionsResponse is one page of closed positions.
type GetClosedPositionsResponse struct {
	// Positions are the closed positions in this page.
	Positions []*ClosedPosition
	// Page carries the next cursor and total.
	Page *Page
}

// MarshalTo implements Message.
func (m *GetClosedPositionsResponse) MarshalTo(e *Encoder) {
	for _, p := range m.Positions {
		e.Message(1, p)
	}
	e.Message(2, m.Page)
}

// Unmarshal implements Message.
func (m *GetClosedPositionsResponse) Unmarshal(data []byte) error {
	*m = GetClosedPositionsResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			p := &ClosedPosition{}
			if err := p.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Positions = append(m.Positions, p)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Page = &Page{}
			return m.Page.Unmarshal(f.Data)
		}
		return nil
	})
}

// ClosePositionRequest closes one position by id.
type ClosePositionRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// PositionID is the position to close.
	PositionID string
}

// MarshalTo implements Message.
func (m *ClosePositionRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.PositionID)
}

// Unmarshal implements Message.
func (m *ClosePositionRequest) Unmarshal(data []byte) error {
	*m = ClosePositionRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.PositionID = f.AsString()
		}
		return nil
	})
}

// ClosePositionResponse carries the closed position.
type ClosePositionResponse struct {
	// Position is the position after the close request.
	Position *Position
}

// MarshalTo implements Message.
func (m *ClosePositionResponse) MarshalTo(e *Encoder) { e.Message(1, m.Position) }

// Unmarshal implements Message.
func (m *ClosePositionResponse) Unmarshal(data []byte) error {
	*m = ClosePositionResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Position = &Position{}
			return m.Position.Unmarshal(f.Data)
		}
		return nil
	})
}

// CloseAllPositionsRequest closes every open position; this is the
// kill-switch execution path.
type CloseAllPositionsRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
}

// MarshalTo implements Message.
func (m *CloseAllPositionsRequest) MarshalTo(e *Encoder) { e.Message(1, m.ExchangeID) }

// Unmarshal implements Message.
func (m *CloseAllPositionsRequest) Unmarshal(data []byte) error {
	*m = CloseAllPositionsRequest{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		}
		return nil
	})
}

// CloseAllPositionsResponse is the (empty) acknowledgement.
type CloseAllPositionsResponse struct{}

// MarshalTo implements Message.
func (m *CloseAllPositionsResponse) MarshalTo(*Encoder) {}

// Unmarshal implements Message.
func (m *CloseAllPositionsResponse) Unmarshal(data []byte) error {
	*m = CloseAllPositionsResponse{}
	return Scan(data, func(Field) error { return nil })
}

// ModifyPositionRequest edits a position's bracket orders.
//
// Both brackets are `optional`: an absent field leaves the corresponding
// bracket unchanged, so a caller can move just the stop without clearing the
// target.
type ModifyPositionRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// PositionID is the position to modify.
	PositionID string
	// TakeProfit sets the bracket target when non-nil.
	TakeProfit *Decimal
	// StopLoss sets the bracket stop when non-nil.
	StopLoss *Decimal
}

// MarshalTo implements Message.
func (m *ModifyPositionRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.PositionID)
	e.Message(3, m.TakeProfit)
	e.Message(4, m.StopLoss)
}

// Unmarshal implements Message.
func (m *ModifyPositionRequest) Unmarshal(data []byte) error {
	*m = ModifyPositionRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.PositionID = f.AsString()
		case 3:
			return decodeDecimal(f, &m.TakeProfit)
		case 4:
			return decodeDecimal(f, &m.StopLoss)
		}
		return nil
	})
}

// ModifyPositionResponse carries the updated position.
type ModifyPositionResponse struct {
	// Position is the position with its new brackets.
	Position *Position
}

// MarshalTo implements Message.
func (m *ModifyPositionResponse) MarshalTo(e *Encoder) { e.Message(1, m.Position) }

// Unmarshal implements Message.
func (m *ModifyPositionResponse) Unmarshal(data []byte) error {
	*m = ModifyPositionResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Position = &Position{}
			return m.Position.Unmarshal(f.Data)
		}
		return nil
	})
}
