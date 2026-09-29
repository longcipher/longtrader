package ports

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
	"github.com/longcipher/longtrader/sdks/go/session"
)

func TestOverflowPolicyString(t *testing.T) {
	// The labels are the Python wire spelling, so a log line is comparable
	// across SDKs.
	cases := map[OverflowPolicy]string{
		DropOldest: "drop_oldest",
		Coalesce:   "coalesce",
		Block:      "block",
	}
	for policy, want := range cases {
		if got := policy.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", policy, got, want)
		}
	}
}

func TestDefaultOverflowTable(t *testing.T) {
	// Mirrors the worker's ports::OVERFLOW_* constants: an order book must be
	// coalesced, a ticker may drop, private streams must never drop.
	want := map[string]OverflowPolicy{
		"ticker":    DropOldest,
		"trades":    DropOldest,
		"ohlcv":     DropOldest,
		"orderbook": Coalesce,
		"orders":    Block,
		"balances":  Block,
		"positions": Block,
	}
	if len(DefaultOverflow) != len(want) {
		t.Errorf("DefaultOverflow has %d entries, want %d", len(DefaultOverflow), len(want))
	}
	for name, policy := range want {
		if got := DefaultOverflow[name]; got != policy {
			t.Errorf("DefaultOverflow[%q] = %v, want %v", name, got, policy)
		}
	}
}

func TestOverflowPolicyForChannel(t *testing.T) {
	cases := map[string]OverflowPolicy{
		"ticker":    DropOldest,
		"trades":    DropOldest,
		"ohlcv":     DropOldest,
		"orderbook": Coalesce,
		"orders":    Block,
		"balances":  Block,
		"positions": Block,
		// The lookup is case- and whitespace-insensitive, so both the
		// contract enum name and a lowercase channel name resolve.
		"TICKER":    DropOldest,
		"ORDERBOOK": Coalesce,
		"  Orders ": Block,
		// An unknown channel is market data by assumption, which is the safe
		// direction to drop in.
		"unknown": DropOldest,
		"":        DropOldest,
	}
	for channel, want := range cases {
		if got := OverflowPolicyForChannel(channel); got != want {
			t.Errorf("OverflowPolicyForChannel(%q) = %v, want %v", channel, got, want)
		}
	}
}

func TestIsSequenceGap(t *testing.T) {
	cases := []struct {
		prev, next uint64
		want       bool
	}{
		{0, 0, false},
		{0, 1, false}, // no previous watermark yet
		{1, 0, false}, // a producer that stopped numbering
		{1, 2, false}, // contiguous
		{1, 3, true},  // one dropped
		{1, 1, true},  // a repeat is still a discontinuity
		{7, 6, true},  // going backwards
		{99, 100, false},
	}
	for _, tc := range cases {
		if got := IsSequenceGap(tc.prev, tc.next); got != tc.want {
			t.Errorf("IsSequenceGap(%d, %d) = %v, want %v", tc.prev, tc.next, got, tc.want)
		}
	}
}

// The seam has to be usable, not declarative: these compile-time assertions
// fail the build if an adapter drifts from the interface.
func TestSessionAdaptersSatisfyTheSeam(t *testing.T) {
	var trading TradingPort = &SessionTradingPort{}
	var market MarketPort = &SessionMarketPort{}
	if trading == nil || market == nil {
		t.Fatal("adapters must be assignable to the ports")
	}
}

// Both adapters must work against a live session, so a strategy written
// against the ports needs no knowledge of the transport.
func TestSessionPortsDriveASession(t *testing.T) {
	host := newPortHost(t)
	s, err := session.Attach(context.Background(), host.URL, "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	trading := &SessionTradingPort{Session: s}
	market := &SessionMarketPort{Session: s}

	// The gate is the adapter's job too: an unreconciled session refuses.
	if _, err := trading.CreateOrder(ctx, session.OrderSpec{Symbol: "BTC/USDT", Amount: "0.001"}); err == nil {
		t.Error("an unreconciled session must refuse order submission through the port")
	}
	snapshot, err := trading.SyncState(ctx)
	if err != nil {
		t.Fatalf("SyncState: %v", err)
	}
	if snapshot.SnapshotSequence != 11 {
		t.Errorf("snapshot sequence = %d, want 11", snapshot.SnapshotSequence)
	}

	order, err := trading.CreateOrder(ctx, session.OrderSpec{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideBuy})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if order == nil || order.ID != "ord-1" {
		t.Fatalf("order = %+v", order)
	}
	if _, err := trading.BatchCreateOrders(ctx, []session.OrderSpec{
		{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideBuy},
		{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideSell},
	}); err != nil {
		t.Fatalf("BatchCreateOrders: %v", err)
	}
	if _, err := trading.CancelOrder(ctx, "ord-1", "BTC/USDT"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if _, err := trading.CancelAllOrders(ctx, "BTC/USDT"); err != nil {
		t.Fatalf("CancelAllOrders: %v", err)
	}
	if _, err := trading.FetchOpenOrders(ctx, "BTC/USDT"); err != nil {
		t.Fatalf("FetchOpenOrders: %v", err)
	}
	if _, err := trading.GetAccount(ctx); err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if _, err := trading.GetPositions(ctx, nil); err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if _, err := trading.GetOrderHistory(ctx); err != nil {
		t.Fatalf("GetOrderHistory: %v", err)
	}
	if _, err := trading.GetClosedPositions(ctx); err != nil {
		t.Fatalf("GetClosedPositions: %v", err)
	}
	if _, err := trading.ClosePosition(ctx, "pos-1"); err != nil {
		t.Fatalf("ClosePosition: %v", err)
	}
	if err := trading.CloseAllPositions(ctx); err != nil {
		t.Fatalf("CloseAllPositions: %v", err)
	}
	stopLoss := "1"
	if _, err := trading.ModifyPosition(ctx, "pos-1", session.Brackets{StopLoss: &stopLoss}); err != nil {
		t.Fatalf("ModifyPosition: %v", err)
	}

	ticker, err := market.FetchTicker(ctx, "BTC/USDT")
	if err != nil {
		t.Fatalf("FetchTicker: %v", err)
	}
	if ticker == nil || ticker.Symbol != "BTC/USDT" {
		t.Fatalf("ticker = %+v", ticker)
	}
	if _, err := market.FetchOrderBook(ctx, "BTC/USDT", session.WithDepth(5)); err != nil {
		t.Fatalf("FetchOrderBook: %v", err)
	}
	if _, err := market.GetCandles(ctx, "BTC/USDT", session.WithTimeframe("1m")); err != nil {
		t.Fatalf("GetCandles: %v", err)
	}
	symbols, err := market.ListSymbols(ctx)
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 1 {
		t.Errorf("symbols = %+v", symbols)
	}
	stream, err := market.SubscribeMarketData(ctx, []string{"BTC/USDT"}, contract.StreamChannelTicker)
	if err != nil {
		t.Fatalf("SubscribeMarketData: %v", err)
	}
	got := 0
	for range stream {
		got++
	}
	if got != 1 {
		t.Errorf("streamed %d events, want 1", got)
	}
}

func TestHeartbeatIntervalHelper(t *testing.T) {
	host := newPortHost(t)
	s, err := session.Attach(context.Background(), host.URL, "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if got := HeartbeatInterval(s); got != 750*time.Millisecond {
		t.Errorf("HeartbeatInterval = %v, want 750ms", got)
	}
}

// newPortHost serves the handful of calls the adapter test needs.
func newPortHost(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case hasSuffix(r.URL.Path, "/AttachSession"):
			writeMsg(t, w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 750})
		case hasSuffix(r.URL.Path, "/ReconcileState"):
			writeMsg(t, w, &contract.ReconcileStateResponse{SnapshotSequence: 11})
		case hasSuffix(r.URL.Path, "/CreateOrder"):
			writeMsg(t, w, &contract.CreateOrderResponse{Order: &contract.Order{ID: "ord-1"}})
		case hasSuffix(r.URL.Path, "/CreateOrders"):
			writeMsg(t, w, &contract.CreateOrdersResponse{Orders: []*contract.Order{{ID: "ord-1"}, {ID: "ord-2"}}})
		case hasSuffix(r.URL.Path, "/CancelOrder"):
			writeMsg(t, w, &contract.CancelOrderResponse{Order: &contract.Order{ID: "ord-1"}})
		case hasSuffix(r.URL.Path, "/CancelAllOrders"):
			writeMsg(t, w, &contract.CancelAllOrdersResponse{Orders: []*contract.Order{{ID: "ord-1"}}})
		case hasSuffix(r.URL.Path, "/FetchOpenOrders"):
			writeMsg(t, w, &contract.FetchOpenOrdersResponse{Orders: []*contract.Order{{ID: "ord-1"}}})
		case hasSuffix(r.URL.Path, "/GetAccount"):
			writeMsg(t, w, &contract.GetAccountResponse{Account: &contract.Account{}})
		case hasSuffix(r.URL.Path, "/GetPositions"):
			writeMsg(t, w, &contract.GetPositionsResponse{Positions: []*contract.Position{{ID: "pos-1"}}})
		case hasSuffix(r.URL.Path, "/GetOrderHistory"):
			writeMsg(t, w, &contract.GetOrderHistoryResponse{Orders: []*contract.Order{{ID: "ord-1"}}})
		case hasSuffix(r.URL.Path, "/GetClosedPositions"):
			writeMsg(t, w, &contract.GetClosedPositionsResponse{})
		case hasSuffix(r.URL.Path, "/ClosePosition"):
			writeMsg(t, w, &contract.ClosePositionResponse{Position: &contract.Position{ID: "pos-1"}})
		case hasSuffix(r.URL.Path, "/CloseAllPositions"):
			writeMsg(t, w, &contract.CloseAllPositionsResponse{})
		case hasSuffix(r.URL.Path, "/ModifyPosition"):
			writeMsg(t, w, &contract.ModifyPositionResponse{Position: &contract.Position{ID: "pos-1"}})
		case hasSuffix(r.URL.Path, "/FetchTicker"):
			writeMsg(t, w, &contract.FetchTickerResponse{Ticker: &contract.Ticker{Symbol: "BTC/USDT"}})
		case hasSuffix(r.URL.Path, "/FetchOrderBook"):
			writeMsg(t, w, &contract.FetchOrderBookResponse{Orderbook: &contract.OrderBook{Symbol: "BTC/USDT"}})
		case hasSuffix(r.URL.Path, "/GetCandles"):
			writeMsg(t, w, &contract.GetCandlesResponse{Candles: []*contract.Candle{{TimestampMS: 1}}})
		case hasSuffix(r.URL.Path, "/ListSymbols"):
			writeMsg(t, w, &contract.ListSymbolsResponse{Symbols: []*contract.SymbolInfo{{Name: "BTCUSDT"}}})
		case hasSuffix(r.URL.Path, "/StreamMarketData"):
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(session.EncodeMessage(contract.Marshal(&contract.MarketDataEvent{
				Header: &contract.EventHeader{Sequence: 1},
				Ticker: &contract.Ticker{Symbol: "BTC/USDT"},
			})))
			_, _ = w.Write(session.EncodeEndOfStream([]byte(`{"error":null}`)))
		default:
			t.Errorf("unexpected RPC path %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeMsg(t *testing.T, w http.ResponseWriter, msg contract.Message) {
	t.Helper()
	w.Header().Set("Content-Type", "application/proto")
	if _, err := w.Write(contract.Marshal(msg)); err != nil {
		t.Errorf("writing reply: %v", err)
	}
}

func hasSuffix(path, suffix string) bool {
	return len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix
}
