package main

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

// The mock venue fills only unscaled/scale and leaves raw_str empty, so a
// reader that consults the text form alone sees no price at all.
func TestDecimalToFloatReadsBothRepresentations(t *testing.T) {
	cases := []struct {
		name string
		in   contract.Decimal
		want float64
		ok   bool
	}{
		{"raw_str only", contract.Decimal{RawStr: "64000.25"}, 64000.25, true},
		{"numeric only", contract.Decimal{Unscaled: 6400025, Scale: 2}, 64000.25, true},
		{"negative numeric only", contract.Decimal{Unscaled: -5, Scale: 1}, -0.5, true},
		{"both", contract.Decimal{Unscaled: 125, Scale: 2, RawStr: "1.25"}, 1.25, true},
		{"zero", contract.Decimal{}, 0, true},
		{"malformed raw_str", contract.Decimal{RawStr: "abc"}, 0, false},
		{"negative scale", contract.Decimal{Unscaled: 1, Scale: -1}, 0, false},
		{"absurd scale", contract.Decimal{Unscaled: 1, Scale: 40}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := decimalToFloat(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("value = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFormatPriceDropsTrailingZeros(t *testing.T) {
	cases := map[float64]string{
		64000:        "64000",
		64000.25:     "64000.25",
		64000.000001: "64000.000001",
		0.5:          "0.5",
	}
	for in, want := range cases {
		if got := formatPrice(in); got != want {
			t.Errorf("formatPrice(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestClientOrderIDIsULIDShaped(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := clientOrderID()
		if len(id) != 26 {
			t.Fatalf("clientOrderID = %q, want 26 characters", id)
		}
		if id != strings.ToUpper(id) {
			t.Fatalf("clientOrderID = %q, want upper case", id)
		}
		if seen[id] {
			t.Fatalf("clientOrderID repeated %q within 100 calls", id)
		}
		seen[id] = true
	}
}

// The full lifecycle against an offline stand-in for the worker: attach,
// register, reconcile, place a grid, then stop with cancellation. This is the
// path the example exists to demonstrate, so it is worth executing rather than
// only compiling.
func TestRunAgainstAnOfflineHost(t *testing.T) {
	var placed [][]*contract.CreateOrdersRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
			return
		}
		reply := func(msg contract.Message) {
			w.Header().Set("Content-Type", "application/proto")
			_, _ = w.Write(contract.Marshal(msg))
		}
		switch {
		case hasSuffix(r.URL.Path, "/AttachSession"):
			reply(&contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 5000, Capabilities: []string{"reconcile"}})
		case hasSuffix(r.URL.Path, "/RegisterStrategy"):
			reply(&contract.RegisterStrategyResponse{StrategyID: "strategy-1"})
		case hasSuffix(r.URL.Path, "/ReconcileState"):
			reply(&contract.ReconcileStateResponse{
				SnapshotSequence: 5,
				Balances:         []*contract.Balance{{Currency: "USDT"}},
			})
		case hasSuffix(r.URL.Path, "/FetchTicker"):
			reply(&contract.FetchTickerResponse{Ticker: &contract.Ticker{
				Symbol: "BTC/USDT",
				// The mock venue shape: unscaled/scale only.
				Bid: contract.Decimal{Unscaled: 639999925, Scale: 4},
				Ask: contract.Decimal{Unscaled: 640000075, Scale: 4},
			}})
		case hasSuffix(r.URL.Path, "/CreateOrders"):
			var req contract.CreateOrdersRequest
			if err := req.Unmarshal(body); err != nil {
				t.Errorf("decoding CreateOrdersRequest: %v", err)
				return
			}
			placed = append(placed, []*contract.CreateOrdersRequest{&req})
			orders := make([]*contract.Order, 0, len(req.Orders))
			for i := range req.Orders {
				orders = append(orders, &contract.Order{ID: "ord-" + string(rune('a'+i))})
			}
			reply(&contract.CreateOrdersResponse{Orders: orders})
		case hasSuffix(r.URL.Path, "/CancelOrder"):
			reply(&contract.CancelOrderResponse{Order: &contract.Order{ID: "ord-a"}})
		case hasSuffix(r.URL.Path, "/StopStrategy"):
			reply(&contract.StopStrategyResponse{FinalState: contract.SessionStateGracefulShutdown})
		case hasSuffix(r.URL.Path, "/KeepAlive"):
			reply(&contract.KeepAliveResponse{HeartbeatIntervalMS: 5000})
		default:
			t.Errorf("unexpected RPC %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotImplemented)
		}
	}))
	defer srv.Close()

	opts := options{
		baseURL:     srv.URL,
		symbol:      "BTC/USDT",
		levels:      2,
		stepPct:     0.5,
		amount:      "0.001",
		iterations:  2,
		refreshSecs: 0,
	}
	if err := run(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(placed) != 2 {
		t.Fatalf("the example placed %d grids, want 2", len(placed))
	}
	first := placed[0][0]
	if len(first.Orders) != 4 {
		t.Errorf("the first grid placed %d rungs, want 2 levels x 2 sides", len(first.Orders))
	}
	if first.SessionID != "sess-1" {
		t.Errorf("CreateOrdersRequest.session_id = %q; the kill-switch would not be scoped to this run", first.SessionID)
	}
	if first.Orders[0].Amount.RawStr != "0.001" || first.Orders[0].Amount.Unscaled != 1 || first.Orders[0].Amount.Scale != 3 {
		t.Errorf("the rung amount decoded as %+v", first.Orders[0].Amount)
	}
	if first.Orders[0].Price.String() == "" {
		t.Errorf("the buy rung has no price: %+v", first.Orders[0])
	}
	// The grid is symmetric around the mid price (64000).
	buy, _ := contract.ParseDecimal(first.Orders[0].Price.String())
	sell, _ := contract.ParseDecimal(first.Orders[1].Price.String())
	buyPrice, err := buy.Float64()
	if err != nil {
		t.Fatalf("buy price: %v", err)
	}
	sellPrice, err := sell.Float64()
	if err != nil {
		t.Fatalf("sell price: %v", err)
	}
	if buyPrice >= 64000 || sellPrice <= 64000 {
		t.Errorf("grid is not two-sided around 64000: buy %v sell %v", buyPrice, sellPrice)
	}
	if math.Abs((buyPrice+sellPrice)/2-64000) > 1 {
		t.Errorf("grid is not symmetric: buy %v sell %v", buyPrice, sellPrice)
	}
}

func TestRunValidatesFlags(t *testing.T) {
	cases := []options{
		{},
		{baseURL: "http://127.0.0.1:1", levels: 0},
		{baseURL: "http://127.0.0.1:1", levels: 1, stepPct: 0},
		{baseURL: "http://127.0.0.1:1", levels: 1, stepPct: 1, amount: "abc"},
	}
	for i, opts := range cases {
		if err := run(opts); err == nil {
			t.Errorf("case %d: expected a validation error for %+v", i, opts)
		}
	}
}

// --exchange-id must reach every RPC it is supposed to: the ticker the grid is
// priced from and the orders placed and cancelled from it. Pricing off one
// backend and trading on another is the failure this guards.
func TestRunWiresTheExchangeIDThroughEveryRPC(t *testing.T) {
	var (
		mu               sync.Mutex
		tickerExchangeID *contract.ExchangeId
		ordersExchangeID *contract.ExchangeId
		cancelExchangeID *contract.ExchangeId
	)
	record := func(dst **contract.ExchangeId) func(*contract.ExchangeId) {
		return func(id *contract.ExchangeId) {
			mu.Lock()
			defer mu.Unlock()
			*dst = id
		}
	}
	noteTicker, noteOrders, noteCancel := record(&tickerExchangeID), record(&ordersExchangeID), record(&cancelExchangeID)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
			return
		}
		reply := func(msg contract.Message) {
			w.Header().Set("Content-Type", "application/proto")
			_, _ = w.Write(contract.Marshal(msg))
		}
		switch {
		case hasSuffix(r.URL.Path, "/AttachSession"):
			reply(&contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 5000})
		case hasSuffix(r.URL.Path, "/RegisterStrategy"):
			reply(&contract.RegisterStrategyResponse{StrategyID: "strategy-1"})
		case hasSuffix(r.URL.Path, "/ReconcileState"):
			reply(&contract.ReconcileStateResponse{SnapshotSequence: 1})
		case hasSuffix(r.URL.Path, "/FetchTicker"):
			var req contract.FetchTickerRequest
			if err := req.Unmarshal(body); err != nil {
				t.Errorf("decoding FetchTickerRequest: %v", err)
				return
			}
			noteTicker(req.ExchangeID)
			reply(&contract.FetchTickerResponse{Ticker: &contract.Ticker{
				Symbol: "BTC/USDT",
				Bid:    contract.Decimal{Unscaled: 639999925, Scale: 4},
				Ask:    contract.Decimal{Unscaled: 640000075, Scale: 4},
			}})
		case hasSuffix(r.URL.Path, "/CreateOrders"):
			var req contract.CreateOrdersRequest
			if err := req.Unmarshal(body); err != nil {
				t.Errorf("decoding CreateOrdersRequest: %v", err)
				return
			}
			noteOrders(req.ExchangeID)
			reply(&contract.CreateOrdersResponse{Orders: []*contract.Order{{ID: "ord-a"}}})
		case hasSuffix(r.URL.Path, "/CancelOrder"):
			var req contract.CancelOrderRequest
			if err := req.Unmarshal(body); err != nil {
				t.Errorf("decoding CancelOrderRequest: %v", err)
				return
			}
			noteCancel(req.ExchangeID)
			reply(&contract.CancelOrderResponse{Order: &contract.Order{ID: "ord-a"}})
		case hasSuffix(r.URL.Path, "/StopStrategy"):
			reply(&contract.StopStrategyResponse{FinalState: contract.SessionStateGracefulShutdown})
		case hasSuffix(r.URL.Path, "/KeepAlive"):
			reply(&contract.KeepAliveResponse{HeartbeatIntervalMS: 5000})
		default:
			t.Errorf("unexpected RPC %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotImplemented)
		}
	}))
	defer srv.Close()

	// Two iterations so the second one also cancels the rungs the first placed.
	opts := options{
		baseURL:         srv.URL,
		exchangeID:      "binance-main",
		symbol:          "BTC/USDT",
		levels:          1,
		stepPct:         0.5,
		amount:          "0.001",
		iterations:      2,
		leaseTimeoutSec: 7,
	}
	if err := run(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	for name, got := range map[string]*contract.ExchangeId{
		"FetchTicker":  tickerExchangeID,
		"CreateOrders": ordersExchangeID,
		"CancelOrder":  cancelExchangeID,
	} {
		if got == nil || got.ID != "binance-main" {
			t.Errorf("%s exchange_id = %+v, want binance-main", name, got)
		}
	}
}

// buildPolicy is what carries --lease-timeout-secs to the host, and the flag
// used to reach the host while the local watchdog got a hardcoded 0.
func TestBuildPolicyCarriesTheLeaseTimeout(t *testing.T) {
	policy := buildPolicy(options{leaseTimeoutSec: 7})
	if policy.LeaseTimeout.AsDuration() != 7*time.Second {
		t.Errorf("lease timeout = %v, want 7s", policy.LeaseTimeout.AsDuration())
	}
	if policy.Scope != contract.KillSwitchScopeSessionOrders {
		t.Errorf("scope = %v", policy.Scope)
	}
	// Zero means "keep the server default", which the contract expresses by
	// leaving the field unset.
	if got := buildPolicy(options{}).LeaseTimeout; got != nil {
		t.Errorf("lease timeout = %+v, want unset", got)
	}
}

func TestParseFlags(t *testing.T) {
	opts, err := parseFlags([]string{"--base-url", "http://x", "--levels", "5", "--step-pct", "0.25", "--amount", "0.5", "--iterations", "3"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.baseURL != "http://x" || opts.levels != 5 || opts.stepPct != 0.25 || opts.amount != "0.5" || opts.iterations != 3 {
		t.Errorf("parsed %+v", opts)
	}
	if _, err := parseFlags([]string{"--help"}); err == nil {
		t.Error("--help must report flag.ErrHelp so main exits cleanly")
	} else if err.Error() != "flag: help requested" {
		t.Errorf("--help error = %v, want flag.ErrHelp", err)
	}
}

func hasSuffix(path, suffix string) bool {
	return len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix
}
