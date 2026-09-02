// Package ports mirrors longtrader ports 1:1 (Session, TradingPort, MarketPort,
// sync_state, OverflowPolicy — design doc §6.8).
package ports

// OverflowPolicy governs event-queue behavior under a slow consumer.
type OverflowPolicy int

const (
	DropOldest OverflowPolicy = iota // ticker/trades/ohlcv
	Coalesce                          // orderbook
	Block                             // orders/balances/positions
)

// TradingPort is the order management surface (mirrors longtrader.trading.v1).
type TradingPort interface {
	SyncState() (interface{}, error)
}

// MarketPort is the market data surface (mirrors longtrader.market.v1).
type MarketPort interface{}
