package session

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

func TestAttachPopulatesSession(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url()+"/", "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	if got := s.SessionID(); got != "sess-1" {
		t.Errorf("SessionID = %q, want sess-1", got)
	}
	if got := s.HeartbeatIntervalMS(); got != 5000 {
		t.Errorf("HeartbeatIntervalMS = %d, want 5000", got)
	}
	if got := strings.Join(s.Capabilities(), ","); got != "reconcile,kill_switch" {
		t.Errorf("Capabilities = %q", got)
	}
	if got := s.State(); got != StateAttached {
		t.Errorf("State = %q, want %q", got, StateAttached)
	}
	if s.CanTrade() {
		t.Error("an attached but unreconciled session must not be able to trade")
	}
	// The trailing slash must not leak into the request path.
	if got := s.BaseURL(); got != host.url() {
		t.Errorf("BaseURL = %q, want %q", got, host.url())
	}
	call, ok := host.lastCall("AttachSession")
	if !ok {
		t.Fatal("AttachSession was never called")
	}
	if call.Service != WorkerService {
		t.Errorf("service = %q, want %q", call.Service, WorkerService)
	}
	if call.ContentType != "application/proto" {
		t.Errorf("Content-Type = %q, want application/proto", call.ContentType)
	}
}

// The host reads the token from the request body field, not from a header;
// sending only the header is how the pre-rewrite Go SDK authenticated
// nothing at all.
func TestAttachSendsTokenInRequestBody(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "super-secret", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	call, _ := host.lastCall("AttachSession")
	var req contract.AttachSessionRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding AttachSessionRequest: %v", err)
	}
	if req.Token != "super-secret" {
		t.Errorf("AttachSessionRequest.token = %q, want the API token", req.Token)
	}
	if req.ClientName != "longtrader-sdk-go" || req.ClientVersion != Version {
		t.Errorf("client identity = %q/%q", req.ClientName, req.ClientVersion)
	}
	// The header is still set: it is harmless and useful behind a proxy.
	if call.Authorization != "Bearer super-secret" {
		t.Errorf("Authorization = %q, want a bearer token", call.Authorization)
	}
}

func TestAttachResendsSessionID(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "resume-me")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	call, _ := host.lastCall("AttachSession")
	var req contract.AttachSessionRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding AttachSessionRequest: %v", err)
	}
	if req.SessionID != "resume-me" {
		t.Errorf("AttachSessionRequest.session_id = %q, want resume-me", req.SessionID)
	}
}

func TestAttachSendsPolicy(t *testing.T) {
	host := defaultHost(t, nil)
	policy := &contract.KillSwitchPolicy{
		LeaseTimeout: contract.DurationFrom(30 * time.Second),
		Scope:        contract.KillSwitchScopeSessionOrders,
	}
	s, err := Attach(context.Background(), host.url(), "tok", policy, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	call, _ := host.lastCall("AttachSession")
	var req contract.AttachSessionRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding AttachSessionRequest: %v", err)
	}
	if req.Policy == nil {
		t.Fatal("policy was dropped")
	}
	if req.Policy.Scope != contract.KillSwitchScopeSessionOrders {
		t.Errorf("scope = %v", req.Policy.Scope)
	}
	if req.Policy.LeaseTimeout.AsDuration() != 30*time.Second {
		t.Errorf("lease timeout = %v", req.Policy.LeaseTimeout.AsDuration())
	}
}

func TestAttachMapsErrorReplies(t *testing.T) {
	host := newFakeHost(t, func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
		h.writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid api token")
	})
	_, err := Attach(context.Background(), host.url(), "bad", nil, "")
	if err == nil {
		t.Fatal("expected an error")
	}
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("error = %T (%v), want *ConnectError", err, err)
	}
	if connectErr.Status != http.StatusUnauthorized || connectErr.Code != "unauthenticated" {
		t.Errorf("error = %+v", connectErr)
	}
	if !strings.Contains(connectErr.Message, "invalid api token") {
		t.Errorf("message = %q", connectErr.Message)
	}
	if !strings.Contains(connectErr.Error(), "http 401") {
		t.Errorf("Error() = %q", connectErr.Error())
	}
}

func TestAttachHandlesNonJSONErrorBody(t *testing.T) {
	host := newFakeHost(t, func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream is down"))
	})
	_, err := Attach(context.Background(), host.url(), "tok", nil, "")
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("error = %T (%v), want *ConnectError", err, err)
	}
	if connectErr.Code != "unknown" || !strings.Contains(connectErr.Message, "upstream is down") {
		t.Errorf("error = %+v", connectErr)
	}
}

// The host drives ATTACHED -> SYNCING inside the RPC, so the local view must
// already say SYNCING while the call is in flight; otherwise a concurrent
// reader would trade against a snapshot that has not arrived yet.
func TestReconcileStateSetsSyncingBeforeTheRPC(t *testing.T) {
	var sess *Session
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"ReconcileState": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			if got := sess.State(); got != StateSyncing {
				t.Errorf("state during ReconcileState = %q, want %q", got, StateSyncing)
			}
			if sess.CanTrade() {
				t.Error("a SYNCING session must not be able to trade")
			}
			h.writeProto(w, &contract.ReconcileStateResponse{
				SnapshotSequence: 4242,
				SnapshotTime:     &contract.Timestamp{Seconds: 1_700_000_000},
				Balances:         []*contract.Balance{{Currency: "USDT", Free: mustDecimal(t, "1000.5")}},
				Positions:        []*contract.Position{{ID: "pos-1"}},
				OpenOrders:       []*contract.Order{{ID: "ord-1"}},
			})
		},
	})
	var err error
	sess, err = Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer sess.Close()

	snapshot, err := sess.ReconcileState(context.Background())
	if err != nil {
		t.Fatalf("ReconcileState: %v", err)
	}
	if got := sess.State(); got != StateActive {
		t.Errorf("state after reconcile = %q, want %q", got, StateActive)
	}
	if !sess.CanTrade() {
		t.Error("a reconciled session must be able to trade")
	}
	if got := sess.SnapshotSequence(); got != 4242 {
		t.Errorf("SnapshotSequence = %d, want 4242", got)
	}
	if snapshot.SnapshotSequence != 4242 {
		t.Errorf("snapshot sequence = %d", snapshot.SnapshotSequence)
	}
	if len(snapshot.Balances) != 1 || len(snapshot.Positions) != 1 || len(snapshot.OpenOrders) != 1 {
		t.Errorf("snapshot contents = %d balances, %d positions, %d orders",
			len(snapshot.Balances), len(snapshot.Positions), len(snapshot.OpenOrders))
	}
}

func TestReconcileStateFailureLeavesStateSyncing(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"ReconcileState": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeError(w, http.StatusServiceUnavailable, "unavailable", "no backend")
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if _, err := s.ReconcileState(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if got := s.State(); got != StateSyncing {
		t.Errorf("state = %q, want %q: a failed snapshot must not unlock trading", got, StateSyncing)
	}
	if s.CanTrade() {
		t.Error("a failed reconcile must not make the session tradable")
	}
}

// The pre-ACTIVE gate is the local half of the host's SYNC_IN_PROGRESS
// rejection: a strategy must not have to burn a round trip to learn it is not
// reconciled yet.
func TestPreActiveGateRefusesOrdersInEveryNonActiveState(t *testing.T) {
	nonActive := []string{
		StateDisconnected,
		StateAttached,
		StateSyncing,
		StateKillSwitchTripped,
		StateGracefulShutdown,
	}
	for _, state := range nonActive {
		t.Run(state, func(t *testing.T) {
			host := defaultHost(t, nil)
			s, err := Attach(context.Background(), host.url(), "tok", nil, "")
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			defer s.Close()
			s.setState(state)

			spec := OrderSpec{Symbol: "BTC/USDT", Amount: "0.001", Type: contract.OrderTypeLimit, Side: contract.OrderSideBuy}
			_, err = s.CreateOrder(context.Background(), spec)
			assertSyncInProgress(t, err, state)
			_, err = s.CreateOrders(context.Background(), []OrderSpec{spec})
			assertSyncInProgress(t, err, state)

			// The gate is local: nothing must have been sent.
			if n := host.countCalls("CreateOrder"); n != 0 {
				t.Errorf("CreateOrder reached the host %d times while %q", n, state)
			}
			if n := host.countCalls("CreateOrders"); n != 0 {
				t.Errorf("CreateOrders reached the host %d times while %q", n, state)
			}
		})
	}
}

func assertSyncInProgress(t *testing.T, err error, state string) {
	t.Helper()
	if err == nil {
		t.Fatalf("state %q must refuse order submission", state)
	}
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("error = %T (%v), want *ConnectError", err, err)
	}
	if connectErr.Status != http.StatusPreconditionFailed || connectErr.Code != "failed_precondition" {
		t.Errorf("error = %+v, want 412/failed_precondition", connectErr)
	}
	if !strings.Contains(connectErr.Message, SyncInProgress) {
		t.Errorf("message = %q, want it to contain %q", connectErr.Message, SyncInProgress)
	}
	if !strings.Contains(connectErr.Message, state) {
		t.Errorf("message = %q, want it to name the state %q", connectErr.Message, state)
	}
}

func TestPreActiveGateAdmitsActiveState(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)
	order, err := s.CreateOrder(context.Background(), OrderSpec{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideBuy})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if order == nil || order.ID != "ord-1" {
		t.Fatalf("order = %+v", order)
	}
}

// The session id on the wire is what lets the host gate the submission and
// scope the kill-switch, so assert against the bytes the host received.
func TestCreateOrderEncodesSessionID(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if _, err := s.ReconcileState(context.Background()); err != nil {
		t.Fatalf("ReconcileState: %v", err)
	}

	price := "64000.25"
	_, err = s.CreateOrder(context.Background(), OrderSpec{
		Symbol:        "BTC/USDT",
		Amount:        "0.001",
		Price:         &price,
		Side:          contract.OrderSideBuy,
		Type:          contract.OrderTypeLimit,
		TimeInForce:   contract.TimeInForceGTC,
		ClientOrderID: "cid-1",
		PostOnly:      true,
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	call, _ := host.lastCall("CreateOrder")
	if call.Service != TradingService {
		t.Errorf("service = %q, want %q", call.Service, TradingService)
	}
	var req contract.CreateOrderRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding CreateOrderRequest: %v", err)
	}
	if req.SessionID != "sess-1" {
		t.Fatalf("CreateOrderRequest.session_id = %q on the wire, want sess-1", req.SessionID)
	}
	if req.Order == nil {
		t.Fatal("order is missing from the request")
	}
	if req.Order.Symbol != "BTC/USDT" || req.Order.ClientOrderID != "cid-1" {
		t.Errorf("order decoded as %+v", req.Order)
	}
	if req.Order.Side != contract.OrderSideBuy || req.Order.Type != contract.OrderTypeLimit {
		t.Errorf("enums decoded as %v/%v", req.Order.Side, req.Order.Type)
	}
	if !req.Order.PostOnly {
		t.Error("post_only was dropped")
	}
	// The amount must carry the contract's single decimal payload.
	if req.Order.Amount.Value != "0.001" {
		t.Errorf("amount decoded as %+v; the host reads one base-10 string", req.Order.Amount)
	}
	if got := req.Order.Price.String(); got != price {
		t.Errorf("price = %q, want %q", got, price)
	}
}

func TestCreateOrderRejectsUnparsableDecimal(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	if _, err := s.CreateOrder(context.Background(), OrderSpec{Symbol: "BTC/USDT", Amount: "not-a-number"}); err == nil {
		t.Fatal("expected an error for a malformed amount")
	}
	// An unrepresentable mantissa must error rather than wrap into a price in
	// the wrong universe. The contract's decimal holds a 96-bit coefficient, so
	// a 29-digit mantissa (one past Decimal::MAX) is out of range.
	overflowing := "79228162514264337593543950336"
	if _, err := s.CreateOrder(context.Background(), OrderSpec{Symbol: "BTC/USDT", Amount: overflowing}); err == nil {
		t.Fatal("expected an error for an overflowing amount")
	}
	if n := host.countCalls("CreateOrder"); n != 0 {
		t.Errorf("a malformed order reached the host %d times", n)
	}
}

func TestCreateOrdersEncodesSessionAndOrders(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	orders, err := s.CreateOrders(context.Background(), []OrderSpec{
		{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideBuy, Type: contract.OrderTypeLimit},
		{Symbol: "BTC/USDT", Amount: "0.001", Side: contract.OrderSideSell, Type: contract.OrderTypeLimit},
	})
	if err != nil {
		t.Fatalf("CreateOrders: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("got %d orders, want 2", len(orders))
	}
	call, _ := host.lastCall("CreateOrders")
	var req contract.CreateOrdersRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding CreateOrdersRequest: %v", err)
	}
	if req.SessionID != "sess-1" {
		t.Errorf("CreateOrdersRequest.session_id = %q, want sess-1", req.SessionID)
	}
	if len(req.Orders) != 2 {
		t.Fatalf("got %d orders on the wire, want 2", len(req.Orders))
	}
	if req.Orders[0].Side != contract.OrderSideBuy || req.Orders[1].Side != contract.OrderSideSell {
		t.Errorf("sides decoded as %v/%v", req.Orders[0].Side, req.Orders[1].Side)
	}
}

func TestKeepAliveRefreshesTheLeaseClock(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	before := s.LastKeepAliveOK()
	resp, err := s.KeepAlive(context.Background())
	if err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if resp.ServerTimeNS != 1 {
		t.Errorf("server time = %d", resp.ServerTimeNS)
	}
	if !s.LastKeepAliveOK().After(before) && s.LastKeepAliveOK().Equal(before) {
		t.Error("KeepAlive must refresh the lease clock")
	}
	call, _ := host.lastCall("KeepAlive")
	var req contract.KeepAliveRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding KeepAliveRequest: %v", err)
	}
	if req.SessionID != "sess-1" {
		t.Errorf("KeepAliveRequest.session_id = %q", req.SessionID)
	}
	if req.ClientTimeNS == 0 {
		t.Error("KeepAliveRequest.client_time_ns must be set")
	}
}

func TestRegisterStrategyReturnsID(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	id, err := s.RegisterStrategy(context.Background(), "go_grid", map[string]string{"symbol": "BTC/USDT", "levels": "3"})
	if err != nil {
		t.Fatalf("RegisterStrategy: %v", err)
	}
	if id != "strategy-1" {
		t.Errorf("strategy id = %q", id)
	}
	call, _ := host.lastCall("RegisterStrategy")
	var req contract.RegisterStrategyRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding RegisterStrategyRequest: %v", err)
	}
	if req.Name != "go_grid" || req.Params["levels"] != "3" {
		t.Errorf("request decoded as %+v", req)
	}
}

func TestStrategyStatusMirrorsHostState(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"StrategyStatus": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			// The host tripped its kill-switch behind our back.
			h.writeProto(w, &contract.StrategyStatusResponse{
				State: contract.SessionStateKillSwitchTripped, StrategyID: "strategy-1", OrdersSubmitted: 4,
			})
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	status, err := s.StrategyStatus(context.Background())
	if err != nil {
		t.Fatalf("StrategyStatus: %v", err)
	}
	if status.OrdersSubmitted != 4 {
		t.Errorf("orders submitted = %d", status.OrdersSubmitted)
	}
	if got := s.State(); got != StateKillSwitchTripped {
		t.Errorf("local state = %q, want the host's KILL_SWITCH_TRIPPED", got)
	}
	if s.CanTrade() {
		t.Error("a tripped session must not be able to trade")
	}
}

func TestStopStrategyMirrorsFinalState(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	resp, err := s.StopStrategy(context.Background(), true)
	if err != nil {
		t.Fatalf("StopStrategy: %v", err)
	}
	if resp.FinalState != contract.SessionStateGracefulShutdown {
		t.Errorf("final state = %v", resp.FinalState)
	}
	if got := s.State(); got != StateGracefulShutdown {
		t.Errorf("local state = %q, want GRACEFUL_SHUTDOWN", got)
	}
	call, _ := host.lastCall("StopStrategy")
	var req contract.StopStrategyRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding StopStrategyRequest: %v", err)
	}
	if !req.CancelOpenOrders {
		t.Error("cancel_open_orders must be set")
	}
}

func TestSetKillSwitchPolicy(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	err = s.SetKillSwitchPolicy(context.Background(), &contract.KillSwitchPolicy{Scope: contract.KillSwitchScopeAllOrders})
	if err != nil {
		t.Fatalf("SetKillSwitchPolicy: %v", err)
	}
	call, _ := host.lastCall("SetKillSwitchPolicy")
	var req contract.SetKillSwitchPolicyRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding SetKillSwitchPolicyRequest: %v", err)
	}
	if req.SessionID != "sess-1" || req.Policy == nil || req.Policy.Scope != contract.KillSwitchScopeAllOrders {
		t.Errorf("request decoded as %+v", req)
	}
}

// ReportLog is client-streaming: the reply is one data frame followed by an
// end-of-stream JSON frame, and the data frame is the one that carries the
// protobuf.
func TestReportLogReadsTheDataFrameBeforeTheEndOfStreamFrame(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"ReportLog": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			w.Header().Set("Content-Type", "application/connect+proto")
			w.Header().Set("Connect-Protocol-Version", "1")
			body := append(EncodeMessage(contract.Marshal(&contract.ReportLogResponse{Accepted: 2})),
				EncodeEndOfStream([]byte(`{"error":null,"metadata":{}}`))...)
			_, _ = w.Write(body)
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	accepted, err := s.ReportLog(context.Background(), contract.LogLevelInfo, "grid leg 3 placed", map[string]string{"leg": "3"})
	if err != nil {
		t.Fatalf("ReportLog: %v", err)
	}
	if accepted != 2 {
		t.Errorf("accepted = %d, want 2", accepted)
	}
	call, _ := host.lastCall("ReportLog")
	if call.ContentType != "application/connect+proto" {
		t.Errorf("Content-Type = %q, want application/connect+proto", call.ContentType)
	}
	if call.ConnectVersion != "1" {
		t.Errorf("Connect-Protocol-Version = %q", call.ConnectVersion)
	}
	payload, err := FirstMessagePayload(call.Body)
	if err != nil {
		t.Fatalf("decoding the request frame: %v", err)
	}
	var ev contract.LogEvent
	if err := ev.Unmarshal(payload); err != nil {
		t.Fatalf("decoding LogEvent: %v", err)
	}
	if ev.SessionID != "sess-1" || ev.Level != contract.LogLevelInfo || ev.Message != "grid leg 3 placed" {
		t.Errorf("log event decoded as %+v", ev)
	}
	if ev.Fields["leg"] != "3" {
		t.Errorf("fields = %v", ev.Fields)
	}
	if ev.Timestamp == nil || ev.Timestamp.Seconds == 0 {
		t.Error("the log event must carry a timestamp")
	}
}

func TestStreamEventsReportsSequenceGaps(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"StreamStrategyEvents": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			seqs := []uint64{1, 2, 4, 5}
			payloads := make([][]byte, 0, len(seqs))
			for _, seq := range seqs {
				payloads = append(payloads, contract.Marshal(&contract.StrategyEvent{
					Header:      &contract.EventHeader{Sequence: seq},
					StateChange: &contract.StateChange{To: contract.SessionStateActive},
				}))
			}
			h.writeFrames(w, payloads...)
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events, err := s.StreamEvents(ctx, "cursor-0")
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	var gaps, messages int
	var sequences []uint64
	for ev := range events {
		if ev.Gap {
			gaps++
			if ev.Message != nil {
				t.Error("a gap marker must not carry a message")
			}
			continue
		}
		if ev.Message == nil || ev.Message.Header == nil {
			t.Fatalf("event without a header: %+v", ev)
		}
		messages++
		sequences = append(sequences, ev.Message.Header.Sequence)
	}
	if messages != 4 {
		t.Errorf("got %d messages, want 4", messages)
	}
	if gaps != 1 {
		t.Errorf("got %d gap markers, want exactly 1 (between 2 and 4)", gaps)
	}
	want := []uint64{1, 2, 4, 5}
	for i := range want {
		if i < len(sequences) && sequences[i] != want[i] {
			t.Errorf("sequence %d = %d, want %d", i, sequences[i], want[i])
		}
	}
	// The channel must close when the stream ends.
	select {
	case _, open := <-events:
		if open {
			t.Error("the channel must be closed after the end-of-stream frame")
		}
	case <-time.After(time.Second):
		t.Error("the channel was not closed after the stream ended")
	}
}

func TestStreamEventsSurfacesErrorReplies(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"StreamStrategyEvents": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeError(w, http.StatusForbidden, "permission_denied", "stream not allowed")
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if _, err := s.StreamEvents(context.Background(), ""); err == nil {
		t.Fatal("expected an error")
	} else {
		var connectErr *ConnectError
		if !errors.As(err, &connectErr) || connectErr.Status != http.StatusForbidden {
			t.Errorf("error = %v, want a 403 ConnectError", err)
		}
	}
}

// Connect reports a streaming failure as HTTP 200 plus an end-of-stream frame
// whose payload is the JSON error, so a status check alone sees success. The
// 200 must be read as the error it is, or the caller cannot tell a rejected
// subscription from a stream that finished.
func TestStreamRepliesCarryTheirErrorInA200(t *testing.T) {
	errorFrame := `{"error":{"code":"failed_precondition","message":"session is not ACTIVE"},"metadata":{}}`

	t.Run("StreamEvents", func(t *testing.T) {
		host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
			"StreamStrategyEvents": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
				w.Header().Set("Content-Type", "application/connect+proto")
				_, _ = w.Write(EncodeEndOfStream([]byte(errorFrame)))
			},
		})
		s, err := Attach(context.Background(), host.url(), "tok", nil, "")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		defer s.Close()

		events, err := s.StreamEvents(context.Background(), "")
		if err != nil {
			t.Fatalf("StreamEvents: %v", err)
		}
		// A closed channel is the failure this guards: without the error item
		// a rejected subscription looks exactly like a clean end of stream.
		var streamErr error
		for ev := range events {
			if ev.Err != nil {
				streamErr = ev.Err
			}
			if ev.Message != nil {
				t.Fatalf("an errored stream produced a message: %+v", ev)
			}
		}
		if streamErr == nil {
			t.Fatal("a 200 whose only frame is an error must reach the caller")
		}
		var connectErr *ConnectError
		if !errors.As(streamErr, &connectErr) {
			t.Fatalf("error = %T, want *ConnectError", streamErr)
		}
		if connectErr.Code != "failed_precondition" || connectErr.Message != "session is not ACTIVE" {
			t.Errorf("error = %+v, want the host's code and message", connectErr)
		}
	})

	t.Run("StreamMarketData", func(t *testing.T) {
		host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
			"StreamMarketData": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
				w.Header().Set("Content-Type", "application/connect+proto")
				_, _ = w.Write(EncodeEndOfStream([]byte(errorFrame)))
			},
		})
		s, err := Attach(context.Background(), host.url(), "tok", nil, "")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		defer s.Close()

		items, err := s.StreamMarketData(context.Background(), []string{"BTC/USDT"}, contract.StreamChannelTicker)
		if err != nil {
			t.Fatalf("StreamMarketData: %v", err)
		}
		var streamErr error
		for item := range items {
			if item.Err != nil {
				streamErr = item.Err
			}
			if item.Event != nil {
				t.Fatalf("an errored stream produced an event: %+v", item.Event)
			}
		}
		var connectErr *ConnectError
		if !errors.As(streamErr, &connectErr) || connectErr.Code != "failed_precondition" {
			t.Errorf("error = %v, want the host's failed_precondition", streamErr)
		}
	})

	// A clean stream must not grow a spurious error item.
	t.Run("a clean stream is not an error", func(t *testing.T) {
		host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
			"StreamStrategyEvents": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
				h.writeFrames(w, contract.Marshal(&contract.StrategyEvent{Header: &contract.EventHeader{Sequence: 1}}))
			},
		})
		s, err := Attach(context.Background(), host.url(), "tok", nil, "")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		defer s.Close()

		events, err := s.StreamEvents(context.Background(), "")
		if err != nil {
			t.Fatalf("StreamEvents: %v", err)
		}
		for ev := range events {
			if ev.Err != nil {
				t.Errorf("a clean stream reported %v", ev.Err)
			}
		}
	})
}

// ReportLog is client-streaming, so its error arrives the same way: HTTP 200
// with an end-of-stream error frame and no data frame at all. Reading it as
// "no message frame" would throw the host's reason away.
func TestReportLogSurfacesA200ErrorReply(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"ReportLog": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(EncodeEndOfStream([]byte(
				`{"error":{"code":"resource_exhausted","message":"log rate limited"},"metadata":{}}`)))
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	_, err = s.ReportLog(context.Background(), contract.LogLevelInfo, "hello", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("error = %T, want *ConnectError", err)
	}
	if connectErr.Code != "resource_exhausted" || connectErr.Message != "log rate limited" {
		t.Errorf("error = %+v, want the host's code and message", connectErr)
	}
	// The old failure mode was ErrNoMessageFrame, which discards the reason.
	if errors.Is(err, ErrNoMessageFrame) {
		t.Error("the error text was thrown away; a 200 error frame is not a missing message frame")
	}
}

// ReportLog reads a finite body, so it must be bounded by the request timeout.
// It previously went through the streaming client, which has none: a wedged
// host could block the call forever, and because that client is also the
// heartbeat's, the whole process.
func TestReportLogTimesOutOnAWedgedHost(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"ReportLog": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			// Accept the request and stall without answering it, the way a
			// wedged host does: the connection stays open with nothing in
			// flight. The stall is bounded so the handler cannot outlive the
			// test and block the fake host's shutdown.
			time.Sleep(2 * time.Second)
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	// Shrink the budget so the test does not have to wait the production 10s;
	// what is under test is that a bound applies at all.
	s.client.Timeout = 150 * time.Millisecond

	start := time.Now()
	if _, err := s.ReportLog(context.Background(), contract.LogLevelInfo, "hello", nil); err == nil {
		t.Fatal("a wedged host must not make ReportLog block forever")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("ReportLog blocked for %s; it must be bounded by the request timeout", elapsed)
	}
}

func TestStreamEventsStopsOnContextCancel(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"StreamStrategyEvents": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeFrames(w, contract.Marshal(&contract.StrategyEvent{Header: &contract.EventHeader{Sequence: 1}}))
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	events, err := s.StreamEvents(ctx, "")
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	<-events
	cancel()
	// The producer must exit and close the channel rather than leak a
	// goroutine parked on a read that will never end.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-events:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("the stream goroutine did not exit after the context was cancelled")
		}
	}
}

func TestStreamMarketData(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"StreamMarketData": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeFrames(w,
				contract.Marshal(&contract.MarketDataEvent{
					Header:      &contract.EventHeader{Sequence: 1},
					Ticker:      &contract.Ticker{Symbol: "BTC/USDT", Last: mustDecimal(t, "64000")},
					ResumeToken: "rt-1",
				}),
				contract.Marshal(&contract.MarketDataEvent{
					Header: &contract.EventHeader{Sequence: 2},
					Ticker: &contract.Ticker{Symbol: "BTC/USDT", Last: mustDecimal(t, "64001")},
				}),
			)
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	events, err := s.StreamMarketData(context.Background(), []string{"BTC/USDT"}, contract.StreamChannelTicker)
	if err != nil {
		t.Fatalf("StreamMarketData: %v", err)
	}
	var got int
	for item := range events {
		if item.Err != nil {
			t.Fatalf("event %d ended with %v, want a clean stream", got, item.Err)
		}
		ev := item.Event
		got++
		if ev.Header == nil || ev.Ticker == nil {
			t.Fatalf("event %d = %+v", got, ev)
		}
	}
	if got != 2 {
		t.Errorf("got %d events, want 2", got)
	}
	call, _ := host.lastCall("StreamMarketData")
	if call.Service != MarketService {
		t.Errorf("service = %q, want %q", call.Service, MarketService)
	}
	payload, err := FirstMessagePayload(call.Body)
	if err != nil {
		t.Fatalf("decoding the request frame: %v", err)
	}
	var req contract.StreamMarketDataRequest
	if err := req.Unmarshal(payload); err != nil {
		t.Fatalf("decoding StreamMarketDataRequest: %v", err)
	}
	if len(req.Subscriptions) != 1 || req.Subscriptions[0].Symbol != "BTC/USDT" {
		t.Fatalf("subscriptions = %+v", req.Subscriptions)
	}
	if req.Subscriptions[0].Channel != contract.StreamChannelTicker {
		t.Errorf("channel = %v", req.Subscriptions[0].Channel)
	}
}

func TestTradingAndMarketMethodsRoundTrip(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	stopLoss := "29000"

	t.Run("CancelOrder", func(t *testing.T) {
		order, err := s.CancelOrder(ctx, "ord-1", "BTC/USDT")
		if err != nil {
			t.Fatalf("CancelOrder: %v", err)
		}
		if order.Status != contract.OrderStatusCanceled {
			t.Errorf("status = %v", order.Status)
		}
		call, _ := host.lastCall("CancelOrder")
		var req contract.CancelOrderRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.OrderID != "ord-1" || req.Symbol != "BTC/USDT" {
			t.Errorf("request = %+v", req)
		}
	})

	t.Run("CancelAllOrders", func(t *testing.T) {
		orders, err := s.CancelAllOrders(ctx, "BTC/USDT")
		if err != nil {
			t.Fatalf("CancelAllOrders: %v", err)
		}
		if len(orders) != 1 {
			t.Errorf("got %d orders, want 1", len(orders))
		}
	})

	t.Run("FetchOpenOrders", func(t *testing.T) {
		orders, err := s.FetchOpenOrders(ctx, "BTC/USDT", WithLimit(25))
		if err != nil {
			t.Fatalf("FetchOpenOrders: %v", err)
		}
		if len(orders) != 1 {
			t.Errorf("got %d orders, want 1", len(orders))
		}
		call, _ := host.lastCall("FetchOpenOrders")
		var req contract.FetchOpenOrdersRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.Pagination == nil || req.Pagination.Limit != 25 {
			t.Errorf("pagination = %+v", req.Pagination)
		}
	})

	t.Run("GetAccount", func(t *testing.T) {
		resp, err := s.GetAccount(ctx, WithExchangeID("okx"))
		if err != nil {
			t.Fatalf("GetAccount: %v", err)
		}
		if resp.Account == nil || resp.Account.Balance.String() != "1000" {
			t.Errorf("account = %+v", resp.Account)
		}
		call, _ := host.lastCall("GetAccount")
		var req contract.GetAccountRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID == nil || req.ExchangeID.ID != "okx" {
			t.Errorf("exchange id = %+v; WithExchangeID was dropped", req.ExchangeID)
		}
	})

	t.Run("GetPositions", func(t *testing.T) {
		positions, err := s.GetPositions(ctx, []string{"BTC/USDT"})
		if err != nil {
			t.Fatalf("GetPositions: %v", err)
		}
		if len(positions) != 1 || positions[0].ID != "pos-1" {
			t.Errorf("positions = %+v", positions)
		}
		call, _ := host.lastCall("GetPositions")
		var req contract.GetPositionsRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(req.Symbols) != 1 || req.Symbols[0] != "BTC/USDT" {
			t.Errorf("symbols = %q", req.Symbols)
		}
	})

	t.Run("GetOrderHistory", func(t *testing.T) {
		resp, err := s.GetOrderHistory(ctx, WithLimit(10))
		if err != nil {
			t.Fatalf("GetOrderHistory: %v", err)
		}
		if len(resp.Orders) != 1 || resp.Page == nil {
			t.Errorf("history = %+v", resp)
		}
	})

	t.Run("GetClosedPositions", func(t *testing.T) {
		if _, err := s.GetClosedPositions(ctx, WithLimit(10)); err != nil {
			t.Fatalf("GetClosedPositions: %v", err)
		}
	})

	t.Run("ClosePosition", func(t *testing.T) {
		position, err := s.ClosePosition(ctx, "pos-1")
		if err != nil {
			t.Fatalf("ClosePosition: %v", err)
		}
		if position.ID != "pos-1" {
			t.Errorf("position = %+v", position)
		}
	})

	t.Run("CloseAllPositions", func(t *testing.T) {
		if err := s.CloseAllPositions(ctx); err != nil {
			t.Fatalf("CloseAllPositions: %v", err)
		}
	})

	t.Run("ModifyPosition", func(t *testing.T) {
		position, err := s.ModifyPosition(ctx, "pos-1", Brackets{StopLoss: &stopLoss})
		if err != nil {
			t.Fatalf("ModifyPosition: %v", err)
		}
		if position.ID != "pos-1" {
			t.Errorf("position = %+v", position)
		}
		call, _ := host.lastCall("ModifyPosition")
		var req contract.ModifyPositionRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.StopLoss == nil || req.StopLoss.String() != stopLoss {
			t.Errorf("stop loss = %+v", req.StopLoss)
		}
		if req.TakeProfit != nil {
			t.Errorf("take profit = %+v, want absent so the target is unchanged", req.TakeProfit)
		}
	})

	t.Run("FetchTicker", func(t *testing.T) {
		ticker, err := s.FetchTicker(ctx, "BTC/USDT")
		if err != nil {
			t.Fatalf("FetchTicker: %v", err)
		}
		if ticker.Symbol != "BTC/USDT" {
			t.Errorf("symbol = %q", ticker.Symbol)
		}
		// The venue fills the one decimal payload, and every price in it is
		// readable; a blank or malformed one would be an error here, not a
		// silent zero.
		bid, err := ticker.Bid.Float64()
		if err != nil {
			t.Fatalf("bid: %v", err)
		}
		ask, err := ticker.Ask.Float64()
		if err != nil {
			t.Fatalf("ask: %v", err)
		}
		if bid == 0 || ask == 0 {
			t.Fatalf("bid/ask = %v/%v; the decimal payload was dropped", bid, ask)
		}
		if mid := (bid + ask) / 2; mid < 63000 || mid > 65000 {
			t.Errorf("mid = %v, out of the BTC/USDT range", mid)
		}
	})

	t.Run("FetchOrderBook", func(t *testing.T) {
		book, err := s.FetchOrderBook(ctx, "BTC/USDT", WithDepth(5))
		if err != nil {
			t.Fatalf("FetchOrderBook: %v", err)
		}
		if book.Symbol != "BTC/USDT" {
			t.Errorf("symbol = %q", book.Symbol)
		}
		call, _ := host.lastCall("FetchOrderBook")
		var req contract.FetchOrderBookRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.Pagination == nil || req.Pagination.Limit != 5 {
			t.Errorf("depth = %+v", req.Pagination)
		}
	})

	t.Run("GetCandles", func(t *testing.T) {
		resp, err := s.GetCandles(ctx, "BTC/USDT", WithTimeframe("1h"), WithLimit(50))
		if err != nil {
			t.Fatalf("GetCandles: %v", err)
		}
		if len(resp.Candles) != 1 {
			t.Errorf("got %d candles, want 1", len(resp.Candles))
		}
		call, _ := host.lastCall("GetCandles")
		var req contract.GetCandlesRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.Timeframe != "1H" {
			t.Errorf("timeframe = %q, want the upper-cased contract form", req.Timeframe)
		}
		if req.Pagination == nil || req.Pagination.Limit != 50 {
			t.Errorf("limit = %+v", req.Pagination)
		}
	})

	t.Run("ListSymbols", func(t *testing.T) {
		symbols, err := s.ListSymbols(ctx)
		if err != nil {
			t.Fatalf("ListSymbols: %v", err)
		}
		if len(symbols) != 1 || symbols[0].Name != "BTCUSDT" {
			t.Errorf("symbols = %+v", symbols)
		}
	})
}

// The four order methods were the only ones that took no Option at all, so
// CreateOrdersRequest.exchange_id and CancelOrderRequest.exchange_id were never
// populated: a strategy that priced a grid off exchange A (WithExchangeID on
// FetchTicker) submitted and cancelled on the host's default exchange instead.
func TestOrderMethodsCarryTheExchangeID(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.ReconcileState(ctx); err != nil {
		t.Fatalf("ReconcileState: %v", err)
	}
	pin := WithExchangeID("okx-main")

	t.Run("CreateOrder", func(t *testing.T) {
		if _, err := s.CreateOrder(ctx, OrderSpec{Symbol: "BTC/USDT", Amount: "0.001"}, pin); err != nil {
			t.Fatalf("CreateOrder: %v", err)
		}
		call, _ := host.lastCall("CreateOrder")
		var req contract.CreateOrderRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID == nil || req.ExchangeID.ID != "okx-main" {
			t.Errorf("exchange_id = %+v; WithExchangeID was dropped", req.ExchangeID)
		}
	})

	t.Run("CreateOrders", func(t *testing.T) {
		specs := []OrderSpec{{Symbol: "BTC/USDT", Amount: "0.001"}}
		if _, err := s.CreateOrders(ctx, specs, pin); err != nil {
			t.Fatalf("CreateOrders: %v", err)
		}
		call, _ := host.lastCall("CreateOrders")
		var req contract.CreateOrdersRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID == nil || req.ExchangeID.ID != "okx-main" {
			t.Errorf("exchange_id = %+v; WithExchangeID was dropped", req.ExchangeID)
		}
	})

	t.Run("CancelOrder", func(t *testing.T) {
		if _, err := s.CancelOrder(ctx, "ord-1", "BTC/USDT", pin); err != nil {
			t.Fatalf("CancelOrder: %v", err)
		}
		call, _ := host.lastCall("CancelOrder")
		var req contract.CancelOrderRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID == nil || req.ExchangeID.ID != "okx-main" {
			t.Errorf("exchange_id = %+v; WithExchangeID was dropped", req.ExchangeID)
		}
	})

	t.Run("CancelAllOrders", func(t *testing.T) {
		if _, err := s.CancelAllOrders(ctx, "BTC/USDT", pin); err != nil {
			t.Fatalf("CancelAllOrders: %v", err)
		}
		call, _ := host.lastCall("CancelAllOrders")
		var req contract.CancelAllOrdersRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID == nil || req.ExchangeID.ID != "okx-main" {
			t.Errorf("exchange_id = %+v; WithExchangeID was dropped", req.ExchangeID)
		}
	})

	t.Run("no option leaves the host default", func(t *testing.T) {
		if _, err := s.CancelAllOrders(ctx, "BTC/USDT"); err != nil {
			t.Fatalf("CancelAllOrders: %v", err)
		}
		call, _ := host.lastCall("CancelAllOrders")
		var req contract.CancelAllOrdersRequest
		if err := req.Unmarshal(call.Body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.ExchangeID != nil {
			t.Errorf("exchange_id = %+v, want absent so the host picks its default", req.ExchangeID)
		}
	})
}

func TestTerminalStates(t *testing.T) {
	for _, state := range []string{StateKillSwitchTripped, StateGracefulShutdown} {
		if !IsTerminal(state) {
			t.Errorf("%q must be terminal", state)
		}
	}
	for _, state := range []string{StateDisconnected, StateAttached, StateSyncing, StateActive} {
		if IsTerminal(state) {
			t.Errorf("%q must not be terminal", state)
		}
	}
	if len(TerminalStates) != 2 {
		t.Errorf("TerminalStates = %v, want exactly the two terminal states", TerminalStates)
	}
	if SyncInProgress != "SYNC_IN_PROGRESS" {
		t.Errorf("SyncInProgress = %q", SyncInProgress)
	}
}

// Close must only release idle connections on a transport this Session owns.
// A remote base URL gets the process-wide http.DefaultTransport, which every
// other http.Client in the process shares: closing its idle connections here
// would drop unrelated keep-alives that merely run in the same binary.
func TestCloseOnlyReleasesOwnedIdleConnections(t *testing.T) {
	for _, tc := range []struct {
		name      string
		baseURL   string
		wantClose int
	}{
		// The shared process-wide transport belongs to nobody in particular.
		{"a remote base URL", "http://192.168.1.5:9000", 0},
		// A per-session transport is this session's to release.
		{"a loopback base URL", "http://127.0.0.1:9000", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSession(tc.baseURL, "tok")
			if want := tc.wantClose > 0; s.ownsTransport != want {
				t.Fatalf("ownsTransport = %v, want %v", s.ownsTransport, want)
			}
			var closed int
			counter := &countingTransport{closed: &closed}
			s.client.Transport = counter
			s.stream.Transport = counter

			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if closed != tc.wantClose {
				t.Errorf("CloseIdleConnections called %d times, want %d (one per client)", closed, tc.wantClose)
			}
		})
	}
}

// The transport choice itself: a loopback host gets a private transport (and
// not an ambient proxy), a remote one gets the process-wide default.
func TestTransportOwnershipFollowsTheBaseURL(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })

	var closed int
	http.DefaultTransport = &countingTransport{closed: &closed}
	// Not an *http.Transport, so even a loopback URL cannot clone it.
	shared, owned := transportFor("http://127.0.0.1:9000")
	if owned || shared != http.DefaultTransport {
		t.Errorf("an unclonable default must stay shared, got owned=%v", owned)
	}

	def, ok := original.(*http.Transport)
	if !ok {
		t.Skip("the default transport is not an *http.Transport")
	}
	http.DefaultTransport = def
	loopback, owned := transportFor("http://127.0.0.1:9000")
	if !owned || loopback == http.DefaultTransport {
		t.Error("a loopback session must get its own transport")
	}
	if loopback.(*http.Transport).Proxy != nil {
		t.Error("a loopback host must not be routed through an ambient proxy")
	}
	remote, owned := transportFor("http://192.168.1.5:9000")
	if owned || remote != http.DefaultTransport {
		t.Error("a remote session must share the process-wide transport")
	}
}

// countingTransport records CloseIdleConnections calls so the test can prove
// the shared pool was left alone.
type countingTransport struct {
	http.RoundTripper
	closed *int
}

func (c *countingTransport) CloseIdleConnections() { *c.closed++ }

func TestCloseAndStopAreIdempotent(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	s.StartHeartbeat(context.Background())
	s.Stop()
	s.Stop()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestStartHeartbeatIsIdempotent(t *testing.T) {
	host := defaultHost(t, nil)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.StartHeartbeat(context.Background())
	s.StartHeartbeat(context.Background())
	// Both calls share one background context; the second must be a no-op
	// rather than a second heartbeat loop.
	if !s.backgroundRunning() {
		t.Error("the heartbeat should be running")
	}
}

func TestContextCancellationStopsTheHeartbeat(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 1})
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	s.StartHeartbeat(ctx)
	waitFor(t, "at least one KeepAlive", 2*time.Second, func() bool { return host.countCalls("KeepAlive") > 0 })
	cancel()
	// The loop must notice the cancellation and return.
	waitFor(t, "the heartbeat to notice cancellation", 2*time.Second, func() bool { return !s.backgroundRunning() })
}

func TestIsLoopback(t *testing.T) {
	cases := map[string]bool{
		"http://127.0.0.1:9000":   true,
		"http://localhost:9000":   true,
		"http://[::1]:9000":       true,
		"http://192.168.1.5:9000": false,
		"https://trade.example":   false,
		"://bad":                  false,
	}
	for in, want := range cases {
		if got := isLoopback(in); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", in, got, want)
		}
	}
}
