// Package ports mirrors longtrader ports 1:1 (Session, TradingPort, MarketPort,
// ReconcileState, OverflowPolicy — design doc §6.8).
//
// The interfaces are the hexagonal seam: a strategy depends on them and never
// on the transport, so the same strategy runs against a session, a mock, or
// any future backend. SessionTradingPort and SessionMarketPort adapt a
// *session.Session to both, which is what makes the seam usable rather than
// declarative.
package ports

import (
	"context"
	"strings"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
	"github.com/longcipher/longtrader/sdks/go/session"
)

// OverflowPolicy governs event-queue behavior under a slow consumer.
type OverflowPolicy int

// Overflow policies. Go names them in CamelCase where Python and TypeScript
// use DROP_OLDEST / COALESCE / BLOCK; the behavior is identical.
const (
	// DropOldest discards the oldest buffered event and preserves the newest.
	// Sequence gaps then trigger a snapshot resync or a reconcile. Map it to
	// idempotent, high-frequency market data where the newest tick subsumes the
	// older ones.
	DropOldest OverflowPolicy = iota
	// Coalesce folds updates into the latest state per key, so stale
	// intermediates are merged instead of queued. Map it to order books.
	Coalesce
	// Block never drops; sustained blockage backpressures the producer and,
	// eventually, trips the lease into the kill-switch. Map it to private
	// order, balance and position streams.
	Block
)

// String returns the Python wire label, for logs that are compared across
// SDKs.
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

// Per-stream-kind default policies, mirroring the worker's ports::OVERFLOW_*
// constants (design doc §6.5): a book must be coalesced because intermediate
// snapshots are worthless, a ticker may drop because only the newest matters,
// and private order/balance/position streams must never drop.
var DefaultOverflow = map[string]OverflowPolicy{
	"ticker":    DropOldest,
	"trades":    DropOldest,
	"ohlcv":     DropOldest,
	"orderbook": Coalesce,
	"orders":    Block,
	"balances":  Block,
	"positions": Block,
}

// OverflowPolicyForChannel returns the default policy for a stream channel
// name ("TICKER", "ORDERBOOK", ...). An unknown name falls back to
// DropOldest, the safe choice for market data.
func OverflowPolicyForChannel(channel string) OverflowPolicy {
	if p, ok := DefaultOverflow[strings.ToLower(strings.TrimSpace(channel))]; ok {
		return p
	}
	return DropOldest
}

// IsSequenceGap reports whether next does not immediately follow prev, which
// means the stream dropped events and the consumer must resync.
//
// A zero on either side means "no sequence yet", which is not a gap: the first
// event of a subscription has no predecessor to compare against.
func IsSequenceGap(prev, next uint64) bool {
	return next > 0 && prev > 0 && next != prev+1
}

// TradingPort is the order management surface, mirroring the Rust
// TradingGateway (13 methods) and the contract's trading.v1 service.
type TradingPort interface {
	// CreateOrder submits one order; the backend dedupes on ClientOrderID.
	CreateOrder(ctx context.Context, spec session.OrderSpec) (*contract.Order, error)
	// BatchCreateOrders submits many orders under one all-or-nothing gate,
	// which is what a grid or a market maker needs.
	BatchCreateOrders(ctx context.Context, specs []session.OrderSpec) ([]*contract.Order, error)
	// CancelOrder cancels one order by venue order id.
	CancelOrder(ctx context.Context, orderID, symbol string) (*contract.Order, error)
	// CancelAllOrders cancels every open order; an empty symbol spans all
	// symbols. This is the kill-switch execution path.
	CancelAllOrders(ctx context.Context, symbol string) ([]*contract.Order, error)
	// FetchOpenOrders lists open orders, optionally filtered by symbol.
	FetchOpenOrders(ctx context.Context, symbol string, opts ...session.Option) ([]*contract.Order, error)
	// GetAccount fetches the account margin summary.
	GetAccount(ctx context.Context, opts ...session.Option) (*contract.GetAccountResponse, error)
	// GetPositions lists open positions; nil means every symbol.
	GetPositions(ctx context.Context, symbols []string, opts ...session.Option) ([]*contract.Position, error)
	// GetOrderHistory lists historical orders.
	GetOrderHistory(ctx context.Context, opts ...session.Option) (*contract.GetOrderHistoryResponse, error)
	// GetClosedPositions lists closed positions.
	GetClosedPositions(ctx context.Context, opts ...session.Option) (*contract.GetClosedPositionsResponse, error)
	// ClosePosition closes one position by id.
	ClosePosition(ctx context.Context, positionID string, opts ...session.Option) (*contract.Position, error)
	// CloseAllPositions closes every open position; kill-switch execution path.
	CloseAllPositions(ctx context.Context, opts ...session.Option) error
	// ModifyPosition attaches or replaces a position's bracket orders.
	ModifyPosition(ctx context.Context, positionID string, brackets session.Brackets, opts ...session.Option) (*contract.Position, error)
	// SyncState is the authoritative atomic snapshot for recovery; the session
	// gate out of SYNCING.
	SyncState(ctx context.Context) (*contract.ReconcileStateResponse, error)
}

// MarketPort is the market data surface, mirroring the Rust
// MarketDataSource and the contract's market.v1 service.
type MarketPort interface {
	// FetchTicker returns the latest ticker snapshot.
	FetchTicker(ctx context.Context, symbol string) (*contract.Ticker, error)
	// FetchOrderBook returns an order book snapshot.
	FetchOrderBook(ctx context.Context, symbol string, opts ...session.Option) (*contract.OrderBook, error)
	// GetCandles returns OHLCV candles; WithTimeframe selects the interval.
	GetCandles(ctx context.Context, symbol string, opts ...session.Option) (*contract.GetCandlesResponse, error)
	// ListSymbols lists the tradable symbols on the bound venue.
	ListSymbols(ctx context.Context) ([]*contract.SymbolInfo, error)
	// SubscribeMarketData streams market data events; each carries a
	// resume token for reconnect-with-replay and a gap-free header sequence.
	// The final item carries Err when the host rejected the subscription.
	SubscribeMarketData(ctx context.Context, symbols []string, channel contract.StreamChannel) (<-chan session.MarketDataEvent, error)
}

// SessionTradingPort adapts a *session.Session to TradingPort.
type SessionTradingPort struct {
	// Session is the adapted session; it must be attached and reconciled
	// before order submission is admitted.
	Session *session.Session
}

// Compile-time proof that both adapters satisfy the seam.
var (
	_ TradingPort = (*SessionTradingPort)(nil)
	_ MarketPort  = (*SessionMarketPort)(nil)
)

// CreateOrder submits one order through the session.
func (p *SessionTradingPort) CreateOrder(ctx context.Context, spec session.OrderSpec) (*contract.Order, error) {
	return p.Session.CreateOrder(ctx, spec)
}

// BatchCreateOrders submits a batch through the session.
func (p *SessionTradingPort) BatchCreateOrders(ctx context.Context, specs []session.OrderSpec) ([]*contract.Order, error) {
	return p.Session.CreateOrders(ctx, specs)
}

// CancelOrder cancels one order through the session.
func (p *SessionTradingPort) CancelOrder(ctx context.Context, orderID, symbol string) (*contract.Order, error) {
	return p.Session.CancelOrder(ctx, orderID, symbol)
}

// CancelAllOrders cancels every open order through the session.
func (p *SessionTradingPort) CancelAllOrders(ctx context.Context, symbol string) ([]*contract.Order, error) {
	return p.Session.CancelAllOrders(ctx, symbol)
}

// FetchOpenOrders lists open orders through the session.
func (p *SessionTradingPort) FetchOpenOrders(ctx context.Context, symbol string, opts ...session.Option) ([]*contract.Order, error) {
	return p.Session.FetchOpenOrders(ctx, symbol, opts...)
}

// GetAccount fetches the account summary through the session.
func (p *SessionTradingPort) GetAccount(ctx context.Context, opts ...session.Option) (*contract.GetAccountResponse, error) {
	return p.Session.GetAccount(ctx, opts...)
}

// GetPositions lists open positions through the session.
func (p *SessionTradingPort) GetPositions(ctx context.Context, symbols []string, opts ...session.Option) ([]*contract.Position, error) {
	return p.Session.GetPositions(ctx, symbols, opts...)
}

// GetOrderHistory lists historical orders through the session.
func (p *SessionTradingPort) GetOrderHistory(ctx context.Context, opts ...session.Option) (*contract.GetOrderHistoryResponse, error) {
	return p.Session.GetOrderHistory(ctx, opts...)
}

// GetClosedPositions lists closed positions through the session.
func (p *SessionTradingPort) GetClosedPositions(ctx context.Context, opts ...session.Option) (*contract.GetClosedPositionsResponse, error) {
	return p.Session.GetClosedPositions(ctx, opts...)
}

// ClosePosition closes one position through the session.
func (p *SessionTradingPort) ClosePosition(ctx context.Context, positionID string, opts ...session.Option) (*contract.Position, error) {
	return p.Session.ClosePosition(ctx, positionID, opts...)
}

// CloseAllPositions closes every position through the session.
func (p *SessionTradingPort) CloseAllPositions(ctx context.Context, opts ...session.Option) error {
	return p.Session.CloseAllPositions(ctx, opts...)
}

// ModifyPosition edits a position's brackets through the session.
func (p *SessionTradingPort) ModifyPosition(ctx context.Context, positionID string, brackets session.Brackets, opts ...session.Option) (*contract.Position, error) {
	return p.Session.ModifyPosition(ctx, positionID, brackets, opts...)
}

// SyncState reconciles through the session; it is also the gate out of
// SYNCING, so a strategy calls it before its first submission.
func (p *SessionTradingPort) SyncState(ctx context.Context) (*contract.ReconcileStateResponse, error) {
	return p.Session.ReconcileState(ctx)
}

// SessionMarketPort adapts a *session.Session to MarketPort.
type SessionMarketPort struct {
	// Session is the adapted session.
	Session *session.Session
}

// FetchTicker returns the latest ticker snapshot through the session.
func (p *SessionMarketPort) FetchTicker(ctx context.Context, symbol string) (*contract.Ticker, error) {
	return p.Session.FetchTicker(ctx, symbol)
}

// FetchOrderBook returns an order book snapshot through the session.
func (p *SessionMarketPort) FetchOrderBook(ctx context.Context, symbol string, opts ...session.Option) (*contract.OrderBook, error) {
	return p.Session.FetchOrderBook(ctx, symbol, opts...)
}

// GetCandles returns OHLCV candles through the session.
func (p *SessionMarketPort) GetCandles(ctx context.Context, symbol string, opts ...session.Option) (*contract.GetCandlesResponse, error) {
	return p.Session.GetCandles(ctx, symbol, opts...)
}

// ListSymbols lists tradable symbols through the session.
func (p *SessionMarketPort) ListSymbols(ctx context.Context) ([]*contract.SymbolInfo, error) {
	return p.Session.ListSymbols(ctx)
}

// SubscribeMarketData streams market data through the session.
func (p *SessionMarketPort) SubscribeMarketData(ctx context.Context, symbols []string, channel contract.StreamChannel) (<-chan session.MarketDataEvent, error) {
	return p.Session.StreamMarketData(ctx, symbols, channel)
}

// HeartbeatInterval is the helper a strategy needs to size its own timers when
// it drives the session lifecycle itself: a KeepAlive loop and the lease
// watchdog should both wake on it.
func HeartbeatInterval(s *session.Session) time.Duration {
	return time.Duration(s.HeartbeatIntervalMS()) * time.Millisecond
}
