package contract

import (
	"fmt"
	"strings"
)

// The contract's proto3 enums are sequential from zero, so each one is
// described by its ordered value-name table. Enum lookup therefore reduces to
// an index, and the tables double as the source for the `String` methods and
// for the "did you mean" lists the parsers print.
var (
	orderTypeNames       = []string{"UNSPECIFIED", "MARKET", "LIMIT", "STOP", "STOP_LIMIT"}
	orderSideNames       = []string{"UNSPECIFIED", "BUY", "SELL"}
	orderStatusNames     = []string{"UNSPECIFIED", "OPEN", "FILLED", "CANCELED", "REJECTED"}
	timeInForceNames     = []string{"UNSPECIFIED", "GTC", "IOC", "FOK", "GTD"}
	positionSideNames    = []string{"UNSPECIFIED", "LONG", "SHORT"}
	closeReasonNames     = []string{"UNSPECIFIED", "MANUAL", "TAKE_PROFIT", "STOP_LOSS", "STOP_OUT"}
	sessionStateNames    = []string{"UNSPECIFIED", "ATTACHED", "SYNCING", "ACTIVE", "KILL_SWITCH_TRIPPED", "GRACEFUL_SHUTDOWN"}
	logLevelNames        = []string{"UNSPECIFIED", "DEBUG", "INFO", "WARN", "ERROR"}
	streamChannelNames   = []string{"UNSPECIFIED", "TICKER", "ORDERBOOK", "TRADES", "OHLCV"}
	killSwitchScopeNames = []string{"UNSPECIFIED", "SESSION_ORDERS", "ALL_ORDERS", "NONE"}
	tradeSideNames       = []string{"UNSPECIFIED", "BUY", "SELL"}
)

// OrderType is the contract's order type enum.
type OrderType int32

// Order type values.
const (
	OrderTypeUnspecified OrderType = 0
	OrderTypeMarket      OrderType = 1
	OrderTypeLimit       OrderType = 2
	OrderTypeStop        OrderType = 3
	OrderTypeStopLimit   OrderType = 4
)

// String returns the enum's short name.
func (v OrderType) String() string { return enumName(orderTypeNames, int32(v)) }

// ParseOrderType resolves "LIMIT", "limit" or "ORDER_TYPE_LIMIT" to an
// OrderType. A typo yields an error naming the valid options.
func ParseOrderType(value string) (OrderType, error) {
	n, err := lookupEnum("ORDER_TYPE_", "OrderType", value, orderTypeNames)
	return OrderType(n), err
}

// OrderSide is the contract's order side enum, seen from the submitter.
type OrderSide int32

// Order side values.
const (
	OrderSideUnspecified OrderSide = 0
	OrderSideBuy         OrderSide = 1
	OrderSideSell        OrderSide = 2
)

// String returns the enum's short name.
func (v OrderSide) String() string { return enumName(orderSideNames, int32(v)) }

// ParseOrderSide resolves "BUY", "buy" or "ORDER_SIDE_BUY" to an OrderSide.
func ParseOrderSide(value string) (OrderSide, error) {
	n, err := lookupEnum("ORDER_SIDE_", "OrderSide", value, orderSideNames)
	return OrderSide(n), err
}

// OrderStatus is the contract's order status enum.
type OrderStatus int32

// Order status values.
const (
	OrderStatusUnspecified OrderStatus = 0
	OrderStatusOpen        OrderStatus = 1
	OrderStatusFilled      OrderStatus = 2
	OrderStatusCanceled    OrderStatus = 3
	OrderStatusRejected    OrderStatus = 4
)

// String returns the enum's short name.
func (v OrderStatus) String() string { return enumName(orderStatusNames, int32(v)) }

// ParseOrderStatus resolves "FILLED", "filled" or "ORDER_STATUS_FILLED".
func ParseOrderStatus(value string) (OrderStatus, error) {
	n, err := lookupEnum("ORDER_STATUS_", "OrderStatus", value, orderStatusNames)
	return OrderStatus(n), err
}

// TimeInForce is the contract's order lifetime enum.
type TimeInForce int32

// Time-in-force values.
const (
	TimeInForceUnspecified TimeInForce = 0
	TimeInForceGTC         TimeInForce = 1
	TimeInForceIOC         TimeInForce = 2
	TimeInForceFOK         TimeInForce = 3
	TimeInForceGTD         TimeInForce = 4
)

// String returns the enum's short name.
func (v TimeInForce) String() string { return enumName(timeInForceNames, int32(v)) }

// ParseTimeInForce resolves "GTC", "gtc" or "TIME_IN_FORCE_GTC".
func ParseTimeInForce(value string) (TimeInForce, error) {
	n, err := lookupEnum("TIME_IN_FORCE_", "TimeInForce", value, timeInForceNames)
	return TimeInForce(n), err
}

// PositionSide is the position side as seen from the holder's perspective.
type PositionSide int32

// Position side values.
const (
	PositionSideUnspecified PositionSide = 0
	PositionSideLong        PositionSide = 1
	PositionSideShort       PositionSide = 2
)

// String returns the enum's short name.
func (v PositionSide) String() string { return enumName(positionSideNames, int32(v)) }

// CloseReason is the canonical reason a position or order was closed.
type CloseReason int32

// Close reason values.
const (
	CloseReasonUnspecified CloseReason = 0
	CloseReasonManual      CloseReason = 1
	CloseReasonTakeProfit  CloseReason = 2
	CloseReasonStopLoss    CloseReason = 3
	CloseReasonStopOut     CloseReason = 4
)

// String returns the enum's short name.
func (v CloseReason) String() string { return enumName(closeReasonNames, int32(v)) }

// SessionState is the server-enforced session lifecycle state.
type SessionState int32

// Session state values.
const (
	SessionStateUnspecified       SessionState = 0
	SessionStateAttached          SessionState = 1
	SessionStateSyncing           SessionState = 2
	SessionStateActive            SessionState = 3
	SessionStateKillSwitchTripped SessionState = 4
	SessionStateGracefulShutdown  SessionState = 5
)

// String returns the enum's short name without the SESSION_STATE_ prefix.
func (v SessionState) String() string { return enumName(sessionStateNames, int32(v)) }

// ParseSessionState resolves "ACTIVE", "active" or "SESSION_STATE_ACTIVE".
func ParseSessionState(value string) (SessionState, error) {
	n, err := lookupEnum("SESSION_STATE_", "SessionState", value, sessionStateNames)
	return SessionState(n), err
}

// LogLevel is the strategy log severity.
type LogLevel int32

// Log level values.
const (
	LogLevelUnspecified LogLevel = 0
	LogLevelDebug       LogLevel = 1
	LogLevelInfo        LogLevel = 2
	LogLevelWarn        LogLevel = 3
	LogLevelError       LogLevel = 4
)

// String returns the enum's short name.
func (v LogLevel) String() string { return enumName(logLevelNames, int32(v)) }

// ParseLogLevel resolves "INFO", "info" or "LOG_LEVEL_INFO".
func ParseLogLevel(value string) (LogLevel, error) {
	n, err := lookupEnum("LOG_LEVEL_", "LogLevel", value, logLevelNames)
	return LogLevel(n), err
}

// StreamChannel is a market-data stream kind.
type StreamChannel int32

// Stream channel values.
const (
	StreamChannelUnspecified StreamChannel = 0
	StreamChannelTicker      StreamChannel = 1
	StreamChannelOrderbook   StreamChannel = 2
	StreamChannelTrades      StreamChannel = 3
	StreamChannelOhlcv       StreamChannel = 4
)

// String returns the enum's short name.
func (v StreamChannel) String() string { return enumName(streamChannelNames, int32(v)) }

// ParseStreamChannel resolves "TICKER", "ticker" or "STREAM_CHANNEL_TICKER".
func ParseStreamChannel(value string) (StreamChannel, error) {
	n, err := lookupEnum("STREAM_CHANNEL_", "StreamChannel", value, streamChannelNames)
	return StreamChannel(n), err
}

// KillSwitchScope selects which orders a tripped kill-switch cancels.
type KillSwitchScope int32

// Kill-switch scope values.
const (
	// KillSwitchScopeUnspecified lets the server pick the default.
	KillSwitchScopeUnspecified KillSwitchScope = 0
	// KillSwitchScopeSessionOrders cancels only this session's orders.
	KillSwitchScopeSessionOrders KillSwitchScope = 1
	// KillSwitchScopeAllOrders cancels every order of the bound accounts.
	KillSwitchScopeAllOrders KillSwitchScope = 2
	// KillSwitchScopeNone logs only and cancels nothing.
	KillSwitchScopeNone KillSwitchScope = 3
)

// String returns the enum's short name.
func (v KillSwitchScope) String() string { return enumName(killSwitchScopeNames, int32(v)) }

// ParseKillSwitchScope resolves "SESSION_ORDERS" or "SCOPE_SESSION_ORDERS".
func ParseKillSwitchScope(value string) (KillSwitchScope, error) {
	n, err := lookupEnum("SCOPE_", "KillSwitchScope", value, killSwitchScopeNames)
	return KillSwitchScope(n), err
}

// TradeSide is a public trade side, seen from the taker's perspective.
type TradeSide int32

// Trade side values.
const (
	TradeSideUnspecified TradeSide = 0
	TradeSideBuy         TradeSide = 1
	TradeSideSell        TradeSide = 2
)

// String returns the enum's short name.
func (v TradeSide) String() string { return enumName(tradeSideNames, int32(v)) }

func enumName(names []string, v int32) string {
	if v < 0 || int(v) >= len(names) {
		return fmt.Sprintf("UNKNOWN(%d)", v)
	}
	return names[v]
}

// lookupEnum resolves a human or prefixed enum name against an ordered table.
//
// Both the short form ("LIMIT") and the fully prefixed contract constant
// ("ORDER_TYPE_LIMIT") are accepted, case-insensitively, so a caller never has
// to know which spelling the generated stubs use.
func lookupEnum(prefix, kind, value string, names []string) (int32, error) {
	key := strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_").Replace(strings.TrimSpace(value)))
	if !strings.HasPrefix(key, prefix) {
		key = prefix + key
	}
	short := strings.TrimPrefix(key, prefix)
	for i, name := range names {
		if name == short {
			return int32(i), nil
		}
	}
	options := append([]string(nil), names[1:]...)
	return 0, decodeErrorf("unknown %s %q; expected one of %s", kind, value, strings.Join(options, ", "))
}
