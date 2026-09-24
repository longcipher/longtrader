// Package ports mirrors longtrader ports 1:1 (Session, TradingPort, MarketPort,
// sync_state, OverflowPolicy — design doc §6.8).
package ports

// OverflowPolicy governs event-queue behavior under a slow consumer.
type OverflowPolicy int

const (
	DropOldest OverflowPolicy = iota // ticker/trades/ohlcv
	Coalesce                         // orderbook
	Block                            // orders/balances/positions
)

// String matches Python (lowercase) wire labels for logs; Go uses int on the wire.
func (p OverflowPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop_oldest"
	case Coalesce:
		return "coalesce"
	case Block:
		return "block"
	default:
		return "drop_oldest"
	}
}

// TradingPort is the order management surface (mirrors longtrader.trading.v1).
// Payloads are pre-encoded protobuf bytes so this package stays gen-free;
// callers with ../gen decode into typed responses.
type TradingPort interface {
	CreateOrder(req []byte) ([]byte, error)
	CancelOrder(req []byte) ([]byte, error)
	CancelAllOrders(req []byte) ([]byte, error)
	FetchOpenOrders(req []byte) ([]byte, error)
	SyncState(req []byte) ([]byte, error)
}

// MarketPort is the market data surface (mirrors longtrader.market.v1).
type MarketPort interface {
	FetchTicker(req []byte) ([]byte, error)
	FetchOrderBook(req []byte) ([]byte, error)
	GetCandles(req []byte) ([]byte, error)
	ListSymbols(req []byte) ([]byte, error)
}
