package session

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

// recordedCall is one HTTP request the fake host received.
type recordedCall struct {
	// Service is the Connect service segment of the path.
	Service string
	// Method is the RPC name.
	Method string
	// Body is the raw request body.
	Body []byte
	// Authorization is the Authorization header, if any.
	Authorization string
	// ContentType is the request Content-Type.
	ContentType string
	// ConnectVersion is the Connect-Protocol-Version header, if any.
	ConnectVersion string
}

// fakeHost is an offline stand-in for the worker control plane. Every test
// drives the SDK against it with httptest, so the suite needs no network and
// no worker process.
type fakeHost struct {
	t      *testing.T
	server *httptest.Server

	mu    sync.Mutex
	calls []recordedCall

	// handle answers a call. It runs on the server goroutine, so it may use
	// t.Errorf (never t.Fatalf) to observe mid-flight client state.
	handle func(h *fakeHost, w http.ResponseWriter, call recordedCall)
}

// newFakeHost starts a server whose calls are answered by handle.
func newFakeHost(t *testing.T, handle func(h *fakeHost, w http.ResponseWriter, call recordedCall)) *fakeHost {
	t.Helper()
	h := &fakeHost{t: t, handle: handle}
	h.server = httptest.NewServer(h)
	t.Cleanup(h.server.Close)
	return h
}

// url is the base URL to attach to.
func (h *fakeHost) url() string { return h.server.URL }

func (h *fakeHost) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.t.Errorf("reading request body: %v", err)
		return
	}
	service, method := splitPath(r.URL.Path)
	call := recordedCall{
		Service:        service,
		Method:         method,
		Body:           body,
		Authorization:  r.Header.Get("Authorization"),
		ContentType:    r.Header.Get("Content-Type"),
		ConnectVersion: r.Header.Get("Connect-Protocol-Version"),
	}
	h.mu.Lock()
	h.calls = append(h.calls, call)
	h.mu.Unlock()
	if h.handle == nil {
		h.writeError(w, http.StatusNotImplemented, "unimplemented", "no handler")
		return
	}
	h.handle(h, w, call)
}

func (h *fakeHost) record() []recordedCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]recordedCall(nil), h.calls...)
}

// countCalls returns how many calls of one RPC method were received.
func (h *fakeHost) countCalls(method string) int {
	n := 0
	for _, c := range h.record() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// lastCall returns the most recent call of an RPC method.
func (h *fakeHost) lastCall(method string) (recordedCall, bool) {
	calls := h.record()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Method == method {
			return calls[i], true
		}
	}
	return recordedCall{}, false
}

// writeProto replies with a raw protobuf body.
func (h *fakeHost) writeProto(w http.ResponseWriter, msg contract.Message) {
	w.Header().Set("Content-Type", "application/proto")
	if _, err := w.Write(contract.Marshal(msg)); err != nil {
		h.t.Errorf("writing protobuf reply: %v", err)
	}
}

// writeError replies with a Connect error frame in JSON.
func (h *fakeHost) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := io.WriteString(w, `{"code":"`+code+`","message":"`+message+`"}`); err != nil {
		h.t.Errorf("writing error reply: %v", err)
	}
}

// writeFrames streams pre-framed payloads and then an end-of-stream frame,
// flushing between frames the way a live stream would.
func (h *fakeHost) writeFrames(w http.ResponseWriter, payloads ...[]byte) {
	w.Header().Set("Content-Type", "application/connect+proto")
	flusher, _ := w.(http.Flusher)
	for _, p := range payloads {
		if _, err := w.Write(EncodeMessage(p)); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	_, _ = w.Write(EncodeEndOfStream([]byte(`{"error":null,"metadata":{}}`)))
	if flusher != nil {
		flusher.Flush()
	}
}

// splitPath splits "/longtrader.worker.v1.WorkerSessionService/KeepAlive" into
// its service and method segments.
func splitPath(path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", strings.Trim(path, "/")
	}
	return parts[0], parts[len(parts)-1]
}

// defaultHost answers the calls the common tests need and lets the test
// override individual methods through overrides.
func defaultHost(t *testing.T, overrides map[string]func(h *fakeHost, w http.ResponseWriter, call recordedCall)) *fakeHost {
	t.Helper()
	return newFakeHost(t, func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
		if override, ok := overrides[call.Method]; ok {
			override(h, w, call)
			return
		}
		switch call.Method {
		case "AttachSession":
			h.writeProto(w, &contract.AttachSessionResponse{
				SessionID:           "sess-1",
				HeartbeatIntervalMS: 5000,
				ServerTime:          &contract.Timestamp{Seconds: 1_700_000_000},
				Capabilities:        []string{"reconcile", "kill_switch"},
			})
		case "KeepAlive":
			h.writeProto(w, &contract.KeepAliveResponse{ServerTimeNS: 1, HeartbeatIntervalMS: 5000})
		case "ReconcileState":
			h.writeProto(w, &contract.ReconcileStateResponse{
				SnapshotSequence: 77,
				Balances:         []*contract.Balance{{Currency: "USDT"}},
			})
		case "RegisterStrategy":
			h.writeProto(w, &contract.RegisterStrategyResponse{StrategyID: "strategy-1"})
		case "StrategyStatus":
			h.writeProto(w, &contract.StrategyStatusResponse{State: contract.SessionStateActive, StrategyID: "strategy-1"})
		case "StopStrategy":
			h.writeProto(w, &contract.StopStrategyResponse{FinalState: contract.SessionStateGracefulShutdown})
		case "SetKillSwitchPolicy", "CloseAllPositions":
			h.writeProto(w, &contract.SetKillSwitchPolicyResponse{})
		case "CreateOrder":
			var req contract.CreateOrderRequest
			if err := req.Unmarshal(call.Body); err != nil {
				h.writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
			h.writeProto(w, &contract.CreateOrderResponse{Order: &contract.Order{ID: "ord-1", Symbol: req.Order.Symbol}})
		case "CreateOrders":
			var req contract.CreateOrdersRequest
			if err := req.Unmarshal(call.Body); err != nil {
				h.writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
				return
			}
			orders := make([]*contract.Order, 0, len(req.Orders))
			for i, o := range req.Orders {
				orders = append(orders, &contract.Order{ID: "ord-" + string(rune('a'+i)), Symbol: o.Symbol})
			}
			h.writeProto(w, &contract.CreateOrdersResponse{Orders: orders})
		case "CancelOrder":
			h.writeProto(w, &contract.CancelOrderResponse{Order: &contract.Order{ID: "ord-1", Status: contract.OrderStatusCanceled}})
		case "CancelAllOrders":
			h.writeProto(w, &contract.CancelAllOrdersResponse{Orders: []*contract.Order{{ID: "ord-1"}}})
		case "FetchOpenOrders":
			h.writeProto(w, &contract.FetchOpenOrdersResponse{Orders: []*contract.Order{{ID: "ord-1"}}, Page: &contract.Page{Total: 1}})
		case "GetAccount":
			h.writeProto(w, &contract.GetAccountResponse{Account: &contract.Account{Balance: mustDecimal(t, "1000")}})
		case "GetPositions":
			h.writeProto(w, &contract.GetPositionsResponse{Positions: []*contract.Position{{ID: "pos-1"}}})
		case "GetOrderHistory":
			h.writeProto(w, &contract.GetOrderHistoryResponse{Orders: []*contract.Order{{ID: "ord-1"}}, Page: &contract.Page{Total: 1}})
		case "GetClosedPositions":
			h.writeProto(w, &contract.GetClosedPositionsResponse{Page: &contract.Page{Total: 0}})
		case "ClosePosition":
			h.writeProto(w, &contract.ClosePositionResponse{Position: &contract.Position{ID: "pos-1"}})
		case "ModifyPosition":
			h.writeProto(w, &contract.ModifyPositionResponse{Position: &contract.Position{ID: "pos-1"}})
		case "FetchTicker":
			h.writeProto(w, &contract.FetchTickerResponse{Ticker: &contract.Ticker{
				Symbol: "BTC/USDT",
				Bid:    contract.Decimal{Value: "63999.9925"},
				Ask:    contract.Decimal{Value: "64000.0075"},
			}})
		case "FetchOrderBook":
			h.writeProto(w, &contract.FetchOrderBookResponse{Orderbook: &contract.OrderBook{Symbol: "BTC/USDT"}})
		case "GetCandles":
			h.writeProto(w, &contract.GetCandlesResponse{Candles: []*contract.Candle{{TimestampMS: 1}}, Page: &contract.Page{Total: 1}})
		case "ListSymbols":
			h.writeProto(w, &contract.ListSymbolsResponse{Symbols: []*contract.SymbolInfo{{Name: "BTCUSDT"}}})
		case "ReportLog":
			body := append(EncodeMessage(contract.Marshal(&contract.ReportLogResponse{Accepted: 1})),
				EncodeEndOfStream([]byte(`{"error":null,"metadata":{}}`))...)
			_, _ = w.Write(body)
		default:
			h.writeError(w, http.StatusNotImplemented, "unimplemented", call.Method)
		}
	})
}

// mustDecimal parses a decimal literal or fails the test.
func mustDecimal(t *testing.T, text string) contract.Decimal {
	t.Helper()
	d, err := contract.ParseDecimal(text)
	if err != nil {
		t.Fatalf("ParseDecimal(%q): %v", text, err)
	}
	return d
}

// waitFor polls cond until it holds or the deadline passes. Polling (rather
// than sleeping a fixed amount) keeps the goroutine-driven tests fast and
// non-flaky.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
