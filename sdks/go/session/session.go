// Package session mirrors the worker session semantics over bare Connect
// calls: attach -> heartbeat -> reconcile -> trade.
//
// Concept names match the Python/TypeScript/Rust SDKs 1:1 (Session,
// TradingPort, ReconcileState, OverflowPolicy).
//
// Wire format (see ../../docs/bare-protocol-guide.md):
//
//	POST {baseURL}/longtrader.worker.v1.WorkerSessionService/{Method}
//	Content-Type: application/proto (unary) or application/connect+proto (streaming)
//
// A unary body is raw protobuf in and raw protobuf out; a streaming body is a
// sequence of 5-byte Connect envelopes (see Envelope).
//
// The protobuf types come from the sibling contract package, which encodes the
// wire format by hand. This package therefore has no third-party dependency
// and builds in a fresh checkout, with or without `buf generate` output in
// ../gen.
package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

// Version is the SDK version reported to the host in AttachSession.
const Version = "0.2.0"

// Service paths of the three contract services this SDK calls.
const (
	// WorkerService is the control plane of the local strategy host.
	WorkerService = "longtrader.worker.v1.WorkerSessionService"
	// TradingService is the unified order/account/position surface.
	TradingService = "longtrader.trading.v1.TradingService"
	// MarketService is the unified market data surface.
	MarketService = "longtrader.market.v1.MarketDataService"
)

// Lifecycle states, in the server-enforced order
// DISCONNECTED -> ATTACHED -> SYNCING -> ACTIVE, with two terminal branches.
// The two terminal states are named in the Python/TypeScript SDKs as
// TERMINAL_STATES.
const (
	// StateDisconnected is the zero value, before a successful Attach.
	StateDisconnected = "DISCONNECTED"
	// StateAttached is negotiated but not yet reconciled.
	StateAttached = "ATTACHED"
	// StateSyncing is a snapshot in flight.
	StateSyncing = "SYNCING"
	// StateActive is the only state that may submit orders.
	StateActive = "ACTIVE"
	// StateKillSwitchTripped is terminal: the lease lapsed or the host tripped
	// its kill-switch.
	StateKillSwitchTripped = "KILL_SWITCH_TRIPPED"
	// StateGracefulShutdown is terminal: the strategy stopped cleanly.
	StateGracefulShutdown = "GRACEFUL_SHUTDOWN"
)

// SyncInProgress is the stable reason the host returns when a session-scoped
// order is submitted before the session reaches ACTIVE (see
// SessionManager::authorize_order_submission). The Python and TypeScript SDKs
// spell the same constant SYNC_IN_PROGRESS.
const SyncInProgress = "SYNC_IN_PROGRESS"

// TerminalStates holds the states from which no further transition is
// possible. A strategy that observes either must not trade again on the
// session it observed them on.
var TerminalStates = map[string]struct{}{
	StateKillSwitchTripped: {},
	StateGracefulShutdown:  {},
}

// IsTerminal reports whether state is one of TerminalStates.
func IsTerminal(state string) bool {
	_, ok := TerminalStates[state]
	return ok
}

// ConnectError is a non-200 Connect reply: a Connect error frame or a plain
// JSON error body.
type ConnectError struct {
	// Status is the HTTP status code, or 0 for a transport failure.
	Status int
	// Code is the Connect error code, e.g. "failed_precondition".
	Code string
	// Message is the human-readable error message.
	Message string
	// Details is the raw JSON detail payload, when the host sent one.
	Details json.RawMessage
}

// Error implements error.
func (e *ConnectError) Error() string {
	return fmt.Sprintf("%s: %s (http %d)", e.Code, e.Message, e.Status)
}

// Option customizes a single trading or market RPC.
type Option func(*callOptions)

// callOptions are the per-call parameters of the trading and market methods.
type callOptions struct {
	exchangeID string
	pagination *contract.Pagination
	timeframe  string
}

// WithExchangeID pins the RPC to one registered backend instance. The default
// leaves the field unset so the host applies its own default exchange.
func WithExchangeID(id string) Option {
	return func(o *callOptions) { o.exchangeID = id }
}

// WithPagination bounds a list RPC's page.
func WithPagination(p *contract.Pagination) Option {
	return func(o *callOptions) { o.pagination = p }
}

// WithLimit caps a list RPC's page size.
func WithLimit(limit uint64) Option {
	return func(o *callOptions) { o.pagination = &contract.Pagination{Limit: limit} }
}

// WithDepth caps the number of order book levels returned.
func WithDepth(depth uint64) Option { return WithLimit(depth) }

// WithTimeframe selects the candle interval, in the contract's string form
// ("1m", "M1", "H4", ...). It is upper-cased before it goes on the wire.
func WithTimeframe(timeframe string) Option {
	return func(o *callOptions) { o.timeframe = strings.ToUpper(strings.TrimSpace(timeframe)) }
}

func newCallOptions(opts []Option) callOptions {
	var o callOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// exchangeID builds the contract ExchangeId; nil leaves the host default unset.
func (o callOptions) exchangeId() *contract.ExchangeId {
	if o.exchangeID == "" {
		return nil
	}
	return &contract.ExchangeId{ID: o.exchangeID}
}

// OrderSpec is an ergonomic order submission: plain decimal strings in, one
// order out. Amount and Price are decimal literals such as "0.001" or
// "90000.25"; they are validated and encoded through contract.ParseDecimal,
// which rejects anything the contract's decimal grammar does not allow before
// it reaches the wire.
type OrderSpec struct {
	// Symbol is the instrument, e.g. "BTC/USDT".
	Symbol string
	// Amount is the order size, as a decimal literal.
	Amount string
	// Price is the limit price; nil for market orders.
	Price *string
	// TriggerPrice arms a stop order on the main book; nil when unused.
	TriggerPrice *string
	// Side is the order side; the zero value is UNSPECIFIED.
	Side contract.OrderSide
	// Type is the order type; the zero value is UNSPECIFIED.
	Type contract.OrderType
	// TimeInForce is the order lifetime; the zero value is UNSPECIFIED.
	TimeInForce contract.TimeInForce
	// ClientOrderID is the idempotency key: a retry after a timeout MUST
	// reuse it, because the backend dedupes on it.
	ClientOrderID string
	// PostOnly rejects the order if it would cross the book.
	PostOnly bool
	// ReduceOnly never increases exposure.
	ReduceOnly bool
}

// MarketDataEvent is one item of a market data stream: either a decoded event
// or, as the final item, the error that ended the stream.
//
// It exists because Connect reports a rejected subscription as HTTP 200 whose
// only frame is an end-of-stream error, so a channel of bare messages cannot
// distinguish "the host refused this subscription" from "the stream finished".
type MarketDataEvent struct {
	// Event is the decoded market data update; nil on an error item.
	Event *contract.MarketDataEvent
	// Err is set on the final item when the host ended the stream with a
	// Connect error; nil on every other item.
	Err error
}

// Brackets carries the optional take-profit and stop-loss of ModifyPosition.
// A nil field leaves the corresponding bracket unchanged, which is the
// contract's `optional` semantics -- so a caller can move just the stop
// without clearing the target.
type Brackets struct {
	// TakeProfit sets the bracket target when non-nil.
	TakeProfit *string
	// StopLoss sets the bracket stop when non-nil.
	StopLoss *string
}

// Event is one item of the strategy event stream.
//
// Gap items are synthetic markers emitted before the event that follows them
// when the stream's sequence jumped: the host's ring buffer overflowed, so the
// consumer must re-run ReconcileState instead of assuming continuity.
type Event struct {
	// Gap reports that events were dropped before the next Message.
	Gap bool
	// Message is the decoded event; nil on a gap marker or an error item.
	Message *contract.StrategyEvent
	// Err is set on the final item when the host ended the stream with a
	// Connect error, which Connect delivers as HTTP 200 plus an end-of-stream
	// error frame. It is what distinguishes a rejected subscription from a
	// stream that simply finished; Err is nil on every other item.
	Err error
}

// Lease and heartbeat defaults. heartbeatFloor keeps a chatty host from
// saturating the control plane, and defaultLeaseFloor is the minimum
// three-heartbeat budget when the caller names none.
const (
	heartbeatFloor    = 50 * time.Millisecond
	defaultLeaseFloor = 1500 * time.Millisecond
	unaryTimeout      = 10 * time.Second
	streamBuffer      = 64
)

// The two independent lease tiers, named so each can be started exactly once
// without a shared latch letting one suppress the other.
const (
	// heartbeatLoop is the StartHeartbeat KeepAlive loop.
	heartbeatLoop = "heartbeat"
	// watchdogLoop is the SpawnLeaseWatchdog lease guard.
	watchdogLoop = "lease-watchdog"
)

// Session is one attached strategy session against the worker control plane.
//
// A Session is safe for concurrent use: the lifecycle view, the negotiated
// lease and the snapshot watermark are all guarded, so a background heartbeat
// can run while the strategy trades.
type Session struct {
	mu      sync.Mutex
	baseURL string
	token   string
	state   string
	lastOK  time.Time
	client  *http.Client
	stream  *http.Client

	// Guarded by mu.
	sessionID           string
	heartbeatIntervalMS uint32
	capabilities        []string
	snapshotSequence    uint64
	leaseTripped        bool

	// Background loops.
	//
	// bgMu guards bgCtx, bgCancel, bgStopped, bgKinds and bgLoops, and it is
	// also the lock bgWG.Add runs under: Stop takes the same lock, so it can
	// never observe a zero counter for a loop that is about to be registered.
	bgMu      sync.Mutex
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	bgWG      sync.WaitGroup
	bgStopped bool
	bgKinds   map[string]struct{}
	bgLoops   int
	stopOnce  sync.Once

	// ownsTransport records whether this session created its own HTTP
	// transport. A shared process-wide one must never be closed from here.
	ownsTransport bool
}

// Attach validates the terminal API token and negotiates lease parameters.
//
// policy is optional; omit it to accept the server defaults (scope
// SESSION_ORDERS, lease timeout three times the negotiated heartbeat).
//
// sessionID resumes a previous session after a network drop: the host reuses
// it (refreshing the lease and re-admitting a non-terminal state) instead of
// issuing a new one, which is what keeps the kill-switch's tracked-order set
// intact across a reconnect. Passing the id of a session the host already
// killed raises rather than silently creating a fresh session.
func Attach(ctx context.Context, baseURL, token string, policy *contract.KillSwitchPolicy, sessionID string) (*Session, error) {
	s := newSession(baseURL, token)
	// The host authenticates from the request body field, not from a header, so
	// the token is authoritative here; the Authorization header is still set
	// for intermediaries.
	req := &contract.AttachSessionRequest{
		Token:         token,
		ClientName:    "longtrader-sdk-go",
		ClientVersion: Version,
		Policy:        policy,
		SessionID:     sessionID,
	}
	resp := &contract.AttachSessionResponse{}
	if err := s.unaryInto(ctx, WorkerService, "AttachSession", req, resp); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.sessionID = resp.SessionID
	// A resumed session keeps its negotiated lease rather than resetting to
	// zero, which would stall the heartbeat and the watchdog.
	if resp.HeartbeatIntervalMS != 0 {
		s.heartbeatIntervalMS = resp.HeartbeatIntervalMS
	}
	s.capabilities = append([]string(nil), resp.Capabilities...)
	s.lastOK = time.Now()
	s.state = StateAttached
	s.mu.Unlock()
	return s, nil
}

// newSession builds an unattached session: DISCONNECTED, with a clock start so
// the lease budget is measured from now rather than from the zero time.
func newSession(baseURL, token string) *Session {
	trimmed := strings.TrimRight(baseURL, "/")
	transport, owned := transportFor(trimmed)
	return &Session{
		baseURL:       trimmed,
		token:         token,
		state:         StateDisconnected,
		lastOK:        time.Now(),
		client:        &http.Client{Transport: transport, Timeout: unaryTimeout},
		stream:        &http.Client{Transport: transport},
		ownsTransport: owned,
	}
}

// transportFor picks the HTTP transport and reports whether the caller owns it
// exclusively. LongTrader is local-first: a host on loopback must not be routed
// through an ambient HTTP(S)/SOCKS proxy just because one is exported in the
// environment, so a loopback endpoint gets a private transport this SDK may
// close. Remote endpoints keep the process-wide default transport, which this
// SDK does not own and must not tear down when one session ends.
func transportFor(baseURL string) (http.RoundTripper, bool) {
	if !isLoopback(baseURL) {
		return http.DefaultTransport, false
	}
	def, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport, false
	}
	t := def.Clone()
	t.Proxy = nil
	return t, true
}

// isLoopback reports whether rawURL targets the local machine.
func isLoopback(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.Trim(u.Hostname(), "[]")
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return host == ""
}

// BaseURL returns the worker control-plane base URL, without a trailing slash.
func (s *Session) BaseURL() string { return s.baseURL }

// SessionID returns the id the host issued, to be sent on every later call.
func (s *Session) SessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionID
}

// HeartbeatIntervalMS returns the negotiated KeepAlive interval in
// milliseconds; 0 means the host has not negotiated one yet.
func (s *Session) HeartbeatIntervalMS() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatIntervalMS
}

// Capabilities returns the features the host advertised at attach (reconcile,
// kill_switch, event_replay, ...), so callers can feature-detect instead of
// guessing.
func (s *Session) Capabilities() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.capabilities...)
}

// SnapshotSequence returns the watermark of the last ReconcileState, used to
// discard replayed deltas the snapshot already covers.
func (s *Session) SnapshotSequence() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotSequence
}

// State returns the local view of the lifecycle state.
func (s *Session) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "" {
		return StateDisconnected
	}
	return s.state
}

// CanTrade reports whether the session has reconciled and the host considers
// it ACTIVE, which is the precondition for submitting an order.
func (s *Session) CanTrade() bool { return s.State() == StateActive }

// LastKeepAliveOK returns the instant of the last successful KeepAlive, or of
// the attach when none has succeeded yet. The lease guard measures from here,
// so it is worth exposing to a strategy that wants to report lease health.
func (s *Session) LastKeepAliveOK() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastOK
}

// backgroundRunning reports whether a background goroutine is still live.
func (s *Session) backgroundRunning() bool { return s.backgroundLoops() > 0 }

func (s *Session) setState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

// syncStateFromHost mirrors a host-reported state into the local view, so a
// kill-switch trip or a server-side stop is visible locally instead of being
// reported ACTIVE. SESSION_STATE_UNSPECIFIED is ignored: it means "the host did
// not say", not "disconnected".
func (s *Session) syncStateFromHost(state contract.SessionState) {
	if state == contract.SessionStateUnspecified {
		return
	}
	s.setState(state.String())
}

// KeepAlive feeds the host's lease watchdog; call it at the negotiated
// interval. StartHeartbeat does that automatically.
func (s *Session) KeepAlive(ctx context.Context) (*contract.KeepAliveResponse, error) {
	req := &contract.KeepAliveRequest{SessionID: s.SessionID(), ClientTimeNS: time.Now().UnixNano()}
	resp := &contract.KeepAliveResponse{}
	if err := s.unaryInto(ctx, WorkerService, "KeepAlive", req, resp); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.lastOK = time.Now()
	if resp.HeartbeatIntervalMS != 0 {
		s.heartbeatIntervalMS = resp.HeartbeatIntervalMS
	}
	s.mu.Unlock()
	return resp, nil
}

// ReconcileState fetches the authoritative atomic snapshot: balances,
// positions and open orders stamped with the stream watermark at snapshot
// time.
//
// The host drives ATTACHED -> SYNCING inside this call, so the local view is
// updated first: a concurrent reader then sees SYNCING and refuses to trade
// rather than racing ahead of the snapshot. A successful reconcile is the
// gate out of syncing; orders submitted before ACTIVE are rejected with
// reason SYNC_IN_PROGRESS.
func (s *Session) ReconcileState(ctx context.Context) (*contract.ReconcileStateResponse, error) {
	s.setState(StateSyncing)
	req := &contract.ReconcileStateRequest{SessionID: s.SessionID()}
	resp := &contract.ReconcileStateResponse{}
	if err := s.unaryInto(ctx, WorkerService, "ReconcileState", req, resp); err != nil {
		return nil, err
	}
	s.mu.Lock()
	// Remember the watermark so a resumed event stream can drop the deltas the
	// snapshot already contains.
	s.snapshotSequence = resp.SnapshotSequence
	s.state = StateActive
	s.mu.Unlock()
	return resp, nil
}

// SetKillSwitchPolicy parameterizes cancel-on-disconnect behavior for this
// session.
func (s *Session) SetKillSwitchPolicy(ctx context.Context, policy *contract.KillSwitchPolicy) error {
	req := &contract.SetKillSwitchPolicyRequest{SessionID: s.SessionID(), Policy: policy}
	return s.unaryInto(ctx, WorkerService, "SetKillSwitchPolicy", req, &contract.SetKillSwitchPolicyResponse{})
}

// RegisterStrategy registers this session's strategy and returns the assigned
// strategy id, so host-side status and counters cover the run.
func (s *Session) RegisterStrategy(ctx context.Context, name string, params map[string]string) (string, error) {
	req := &contract.RegisterStrategyRequest{SessionID: s.SessionID(), Name: name, Params: params}
	resp := &contract.RegisterStrategyResponse{}
	if err := s.unaryInto(ctx, WorkerService, "RegisterStrategy", req, resp); err != nil {
		return "", err
	}
	return resp.StrategyID, nil
}

// StrategyStatus returns the host's lifecycle state, strategy id and
// submission counters, and mirrors the reported state into the local view.
func (s *Session) StrategyStatus(ctx context.Context) (*contract.StrategyStatusResponse, error) {
	req := &contract.StrategyStatusRequest{SessionID: s.SessionID()}
	resp := &contract.StrategyStatusResponse{}
	if err := s.unaryInto(ctx, WorkerService, "StrategyStatus", req, resp); err != nil {
		return nil, err
	}
	s.syncStateFromHost(resp.State)
	return resp, nil
}

// StopStrategy stops the strategy, optionally cancelling the session's open
// orders -- the same RPC the host uses to guarantee nothing is left resting,
// and the same path the kill-switch takes.
func (s *Session) StopStrategy(ctx context.Context, cancelOpenOrders bool) (*contract.StopStrategyResponse, error) {
	req := &contract.StopStrategyRequest{SessionID: s.SessionID(), CancelOpenOrders: cancelOpenOrders}
	resp := &contract.StopStrategyResponse{}
	if err := s.unaryInto(ctx, WorkerService, "StopStrategy", req, resp); err != nil {
		return nil, err
	}
	s.syncStateFromHost(resp.FinalState)
	return resp, nil
}

// ReportLog sends one log event to the host and returns the number of events
// it accepted.
//
// The RPC is client-streaming, so the request is Connect-framed, but the reply
// is a finite body: one data frame followed by an end-of-stream frame carrying
// JSON, so the data frame is located before decoding. That makes this a bounded
// request, and it is sent through the timeout-carrying client rather than the
// streaming one -- a wedged host must not be able to block a log call forever
// on a client that the heartbeat also uses.
//
// Connect reports a streaming failure as HTTP 200 plus an end-of-stream frame
// whose payload is the JSON error, so a 200 whose only frame is end-of-stream
// is an error, not an empty reply; errorFromResponse is reused to keep the
// code and message the host sent.
func (s *Session) ReportLog(ctx context.Context, level contract.LogLevel, message string, fields map[string]string) (uint64, error) {
	event := &contract.LogEvent{
		SessionID: s.SessionID(),
		Level:     level,
		Message:   message,
		Timestamp: contract.NowTimestamp(),
		Fields:    fields,
	}
	body := EncodeMessage(contract.Marshal(event))
	// Connect framing, bounded request: post routes a finite body through the
	// timeout-carrying client and reads a 200-with-an-error end-of-stream frame
	// as the error it is.
	data, err := s.post(ctx, WorkerService, "ReportLog", body, true)
	if err != nil {
		return 0, err
	}
	payload, err := FirstMessagePayload(data)
	if err != nil {
		return 0, err
	}
	resp := &contract.ReportLogResponse{}
	if err := resp.Unmarshal(payload); err != nil {
		return 0, err
	}
	return resp.Accepted, nil
}

// StreamEvents opens the unified strategy event stream, resuming after
// resumeToken.
//
// A returned channel is closed when the stream ends or ctx is cancelled.
// Gap items mark a sequence discontinuity: the host's ring buffer overflowed,
// so the consumer must re-run ReconcileState rather than assume continuity.
//
// If the host rejects the subscription it answers HTTP 200 with a single
// end-of-stream frame carrying the error, so a closed channel alone would look
// like a clean end of stream. Such a rejection arrives as a final item with Err
// set; consumers that do not care about the reason can keep ignoring it.
func (s *Session) StreamEvents(ctx context.Context, resumeToken string) (<-chan Event, error) {
	req := &contract.StreamStrategyEventsRequest{SessionID: s.SessionID(), ResumeToken: resumeToken}
	resp, err := s.openStream(ctx, WorkerService, "StreamStrategyEvents", req)
	if err != nil {
		return nil, err
	}
	events := make(chan Event, streamBuffer)
	go s.pumpStrategyEvents(ctx, resp.Body, events)
	return events, nil
}

func (s *Session) pumpStrategyEvents(ctx context.Context, body io.ReadCloser, out chan<- Event) {
	defer close(out)
	defer body.Close()
	dec := NewStreamDecoder(body)
	var prev uint64
	for {
		payload, err := dec.Next()
		if err != nil {
			// A Connect 200 that ends in an error frame is a rejection, not a
			// clean end of stream. Report it as a final Err item so the caller
			// can tell "the host refused this subscription" from "the stream
			// finished".
			if cerr := endStreamFrameError(dec.EndOfStreamPayload()); cerr != nil {
				_ = sendEvent(ctx, out, Event{Err: cerr})
			}
			return
		}
		ev := &contract.StrategyEvent{}
		if err := ev.Unmarshal(payload); err != nil {
			return
		}
		seq := uint64(0)
		if ev.Header != nil {
			seq = ev.Header.Sequence
		}
		if seq > 0 {
			if prev > 0 && seq != prev+1 {
				if !sendEvent(ctx, out, Event{Gap: true}) {
					return
				}
			}
			prev = seq
		}
		if !sendEvent(ctx, out, Event{Message: ev}) {
			return
		}
	}
}

func sendEvent(ctx context.Context, out chan<- Event, ev Event) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// StartHeartbeat spawns a goroutine sending KeepAlive at the negotiated
// interval until ctx is cancelled or Stop is called.
//
// It reports whether the loop was actually spawned: false when this session
// already runs a heartbeat, or when Stop has already run. The heartbeat and
// SpawnLeaseWatchdog are independent tiers, so starting one never suppresses
// the other.
func (s *Session) StartHeartbeat(ctx context.Context) bool {
	return s.startBackground(heartbeatLoop, ctx, func(runCtx context.Context) {
		lease := s.leaseBudget(0)
		for {
			interval := s.heartbeatInterval()
			timer := time.NewTimer(interval)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			// Transient failures are fine; the next tick retries, and the lease
			// check below decides whether retrying is still meaningful.
			_, _ = s.KeepAlive(runCtx)
			if s.checkLease(runCtx, lease) {
				return
			}
		}
	})
}

// LeaseWatchdog is the handle of a spawned lease watchdog.
//
// It exists because "the watchdog finished" and "the watchdog never started"
// must not look the same: the single-slot latch this replaces returned a
// closed Done for a watchdog that had silently failed to spawn, so a caller
// waiting on it read success for a loop that did not exist. Check Started;
// Done only reports that a watchdog which *did* start has stopped.
type LeaseWatchdog struct {
	// Done is closed once the watchdog loop has returned, whether because the
	// lease lapsed, because ctx was cancelled, or because Stop was called. It
	// is already closed when Started is false, so a caller that only waits
	// never blocks forever.
	Done <-chan struct{}
	// Started reports whether the loop was actually spawned. It is false when
	// this session already runs a watchdog, or when Stop has already run.
	Started bool
}

// SpawnLeaseWatchdog starts the strategy-side lease guard: it wakes every
// heartbeat interval and checks how long ago a KeepAlive last succeeded. On
// timeout it stops this strategy trading locally and asks the host to cancel
// this session's orders.
//
// It mirrors the Rust spawn_strategy_lease_guard helper (the L_session tier of
// the session/daemon/exchange watchdog) and is an independent tier of
// StartHeartbeat, mirroring the two independent threads the Python SDK runs:
// either may be started first, and both are stopped by one Stop call.
//
// Cancellation is owned by whichever tier started FIRST, so pass the same ctx to
// both unless you want the first one to control the lifetime; see
// startBackground.
//
// leaseTimeout overrides the budget; pass 0 for three heartbeat intervals with
// a floor. Read the returned handle's Started to tell a running watchdog from
// one that was never spawned.
func (s *Session) SpawnLeaseWatchdog(ctx context.Context, leaseTimeout time.Duration) LeaseWatchdog {
	done := make(chan struct{})
	lease := s.leaseBudget(leaseTimeout)
	started := s.startBackground(watchdogLoop, ctx, func(runCtx context.Context) {
		defer close(done)
		for {
			interval := s.heartbeatInterval()
			timer := time.NewTimer(interval)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if s.checkLease(runCtx, lease) {
				return
			}
		}
	})
	if !started {
		close(done)
	}
	return LeaseWatchdog{Done: done, Started: started}
}

// startBackground registers one named background loop under a cancellable
// context shared by every loop of this session.
//
// The FIRST caller's context wins: later loops join it, and their own context is
// ignored for cancellation purposes. So
//
//	StartHeartbeat(ctxA); SpawnLeaseWatchdog(ctxB)
//
// runs both tiers, but cancelling ctxB alone does not stop the watchdog -- only
// cancelling ctxA or calling Stop does. Pass the same context to both unless you
// specifically want the first one to own the lifetime.
//
// It reports false when this session already runs a loop of the same name, or
// when Stop has already run -- and only then. The WaitGroup is incremented
// while bgMu is held, so Stop cannot pass Wait with a zero counter and return
// before a freshly spawned loop has been registered.
func (s *Session) startBackground(kind string, ctx context.Context, loop func(context.Context)) bool {
	s.bgMu.Lock()
	if s.bgStopped {
		s.bgMu.Unlock()
		return false
	}
	if _, ok := s.bgKinds[kind]; ok {
		s.bgMu.Unlock()
		return false
	}
	if s.bgCtx == nil {
		// The first loop owns the lifetime every later loop joins: one Stop, or
		// one cancellation of this first context, tears every tier down. See the
		// doc comment above for what that implies for a later caller's ctx.
		s.bgCtx, s.bgCancel = context.WithCancel(ctx)
	}
	runCtx := s.bgCtx
	if s.bgKinds == nil {
		s.bgKinds = make(map[string]struct{}, 2)
	}
	s.bgKinds[kind] = struct{}{}
	s.bgLoops++
	s.bgWG.Add(1)
	s.bgMu.Unlock()

	go func() {
		defer s.bgWG.Done()
		defer func() {
			s.bgMu.Lock()
			s.bgLoops--
			s.bgMu.Unlock()
		}()
		loop(runCtx)
	}()
	return true
}

// backgroundLoops reports how many background goroutines are still running.
func (s *Session) backgroundLoops() int {
	s.bgMu.Lock()
	defer s.bgMu.Unlock()
	return s.bgLoops
}

// leaseBudget resolves the lease timeout: the caller's value when positive,
// otherwise three negotiated heartbeats with a floor.
func (s *Session) leaseBudget(explicit time.Duration) time.Duration {
	if explicit > 0 {
		return explicit
	}
	d := 3 * time.Duration(s.HeartbeatIntervalMS()) * time.Millisecond
	if d < defaultLeaseFloor {
		return defaultLeaseFloor
	}
	return d
}

// heartbeatInterval is the KeepAlive tick, floored so a sub-50ms negotiated
// interval cannot saturate the control plane.
func (s *Session) heartbeatInterval() time.Duration {
	d := time.Duration(s.HeartbeatIntervalMS()) * time.Millisecond
	if d < heartbeatFloor {
		return heartbeatFloor
	}
	return d
}

// checkLease trips the local kill-switch when the lease has lapsed, and
// reports whether the caller should stop its loop.
//
// Stopping trading comes first: the host may already have tripped its own
// kill-switch on lease expiry, and a strategy that keeps trading past its
// lease is exactly the failure this tier exists to prevent. The cancellation
// RPC is then best-effort -- it usually fails here, since the missing lease is
// why we tripped -- and the host's watchdog remains the authority.
func (s *Session) checkLease(ctx context.Context, lease time.Duration) bool {
	s.mu.Lock()
	alreadyTripped := s.leaseTripped
	terminal := IsTerminal(s.state)
	elapsed := time.Since(s.lastOK)
	if !alreadyTripped && !terminal && elapsed > lease {
		s.leaseTripped = true
		s.state = StateKillSwitchTripped
	}
	tripped := s.leaseTripped
	s.mu.Unlock()
	if !tripped || alreadyTripped {
		return false
	}
	_, _ = s.StopStrategy(ctx, true)
	return true
}

// Stop cancels the background heartbeat and watchdog goroutines and waits for
// them to return. It is idempotent, and it stops both lease tiers: the
// registration lock is taken before the context is cancelled, so a loop can
// neither be spawned after Stop nor be counted in a WaitGroup whose counter
// was already zero when Stop passed it.
func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		s.bgMu.Lock()
		// Mark the session stopped so a later StartHeartbeat or
		// SpawnLeaseWatchdog cannot spawn a goroutine that nothing would ever
		// cancel.
		s.bgStopped = true
		cancel := s.bgCancel
		s.bgMu.Unlock()
		if cancel != nil {
			cancel()
		}
		s.bgWG.Wait()
	})
}

// Close stops the background loops and releases idle connections.
//
// It only releases connections on a transport this Session owns (a loopback
// base URL gets a private one). For a remote host the transport is the
// process-wide http.DefaultTransport, shared with every other http.Client in
// the process: closing its idle connections from here would drop unrelated
// keep-alives that merely happen to run in the same binary.
func (s *Session) Close() error {
	s.Stop()
	if s.ownsTransport {
		s.client.CloseIdleConnections()
		s.stream.CloseIdleConnections()
	}
	return nil
}

// -- transport ---------------------------------------------------------------

// unaryInto performs one Connect unary call and decodes the reply into resp.
func (s *Session) unaryInto(ctx context.Context, service, method string, req contract.Message, resp contract.Message) error {
	data, err := s.post(ctx, service, method, contract.Marshal(req), false)
	if err != nil {
		return err
	}
	return resp.Unmarshal(data)
}

// post performs one HTTP call and returns the raw response body.
//
// Every call through here reads a finite body, so every call goes through the
// client that carries unaryTimeout. A wedged host must not be able to block a
// caller forever, and because that client is also the heartbeat's, a hang
// there would wedge the process. framed selects the Connect streaming framing
// (a client-streaming request such as ReportLog) without changing the bound.
func (s *Session) post(ctx context.Context, service, method string, body []byte, framed bool) ([]byte, error) {
	req, err := s.newRequest(ctx, service, method, body, framed)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &ConnectError{Status: resp.StatusCode, Code: "read", Message: err.Error()}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errorFromResponse(resp.StatusCode, data)
	}
	// A streaming RPC reports its own errors inside a 200, so the status check
	// above is not the whole story for a framed body.
	if framed {
		if cerr := endOfStreamError(data); cerr != nil {
			return nil, cerr
		}
	}
	return data, nil
}

// openStream performs one streaming call and hands back the still-open
// response body.
//
// A genuine server-streaming RPC has no deadline -- it stays open for as long
// as the caller wants it, which is why it uses the client with no Timeout. It
// is not read here: a stream that has been accepted but has nothing to say
// yet must not block the caller. The first frame is therefore drained by the
// pump, which reports an end-of-stream error as a terminal error item rather
// than as a channel that closed with no reason.
func (s *Session) openStream(ctx context.Context, service, method string, req contract.Message) (*http.Response, error) {
	body := EncodeMessage(contract.Marshal(req))
	httpReq, err := s.newRequest(ctx, service, method, body, true)
	if err != nil {
		return nil, err
	}
	resp, err := s.stream.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return nil, errorFromResponse(resp.StatusCode, data)
	}
	return resp, nil
}

func (s *Session) newRequest(ctx context.Context, service, method string, body []byte, framed bool) (*http.Request, error) {
	endpoint := fmt.Sprintf("%s/%s/%s", s.baseURL, service, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if framed {
		req.Header.Set("Content-Type", "application/connect+proto")
		req.Header.Set("Connect-Protocol-Version", "1")
	} else {
		req.Header.Set("Content-Type", "application/proto")
	}
	// The host authenticates from AttachSessionRequest.token, not from this
	// header; sending it anyway is harmless and helps when the worker sits
	// behind an authenticating proxy.
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	return req, nil
}

// endOfStreamError maps a Connect 200 whose last frame is end-of-stream to a
// ConnectError, or returns nil when the stream ended cleanly.
//
// This is the failure mode the status code hides: Connect reports a streaming
// error as HTTP 200 followed by a single end-of-stream frame whose payload is
// the JSON error body. Without this, a rejected subscription is
// indistinguishable from a stream that simply finished -- the caller sees a
// closed channel and no reason at all.
func endOfStreamError(data []byte) *ConnectError {
	for pos := 0; pos+EnvelopeHeaderSize <= len(data); {
		length := int(binary.BigEndian.Uint32(data[pos+1 : pos+EnvelopeHeaderSize]))
		if length > MaxFrameLength || pos+EnvelopeHeaderSize+length > len(data) {
			return nil
		}
		flags := data[pos]
		if flags&FlagEndOfStream != 0 {
			return endStreamFrameError(data[pos+EnvelopeHeaderSize : pos+EnvelopeHeaderSize+length])
		}
		pos += EnvelopeHeaderSize + length
	}
	return nil
}

// endStreamFrameError maps one end-of-stream frame payload to a ConnectError,
// or returns nil when the frame is an ordinary clean close-out.
func endStreamFrameError(payload []byte) *ConnectError {
	var frame struct {
		Error *struct {
			Code    string          `json:"code"`
			Message string          `json:"message"`
			Details json.RawMessage `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &frame); err != nil || frame.Error == nil {
		// A clean end-of-stream carries {"error":null,"metadata":{}}.
		return nil
	}
	code := frame.Error.Code
	if code == "" {
		code = "unknown"
	}
	return &ConnectError{
		Status:  http.StatusOK,
		Code:    code,
		Message: frame.Error.Message,
		Details: frame.Error.Details,
	}
}

// errorFromResponse maps a non-200 reply to a ConnectError, preserving the
// Connect code and message when the body is a JSON error frame.
func errorFromResponse(status int, body []byte) *ConnectError {
	var payload struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		msg := string(body)
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return &ConnectError{Status: status, Code: "unknown", Message: msg}
	}
	if payload.Code == "" {
		payload.Code = "unknown"
	}
	return &ConnectError{Status: status, Code: payload.Code, Message: payload.Message, Details: payload.Details}
}

// requireActive is the local pre-ACTIVE gate on order submission.
//
// It fails fast, before the round trip, with the same reason the host would
// return: a submission before ACTIVE is rejected with SYNC_IN_PROGRESS. The
// session id on the wire is what the host uses to enforce that, so a strategy
// that skipped the gate would learn about it only as a remote error.
func (s *Session) requireActive() error {
	state := s.State()
	if state == StateActive {
		return nil
	}
	return &ConnectError{
		Status:  http.StatusPreconditionFailed,
		Code:    "failed_precondition",
		Message: fmt.Sprintf("%s: local session is %s, not %s; call ReconcileState first", SyncInProgress, state, StateActive),
	}
}

// -- trading -----------------------------------------------------------------

// CreateOrder places one order, attributed to this session.
//
// The session id is what lets the host reject a pre-ACTIVE submission
// (SYNC_IN_PROGRESS) and record the order so the kill-switch can cancel it,
// so it is always sent. WithExchangeID pins the submission to one registered
// backend, exactly as it pins the ticker a strategy priced it from.
// Requires a reconciled (ACTIVE) session.
func (s *Session) CreateOrder(ctx context.Context, spec OrderSpec, opts ...Option) (*contract.Order, error) {
	if err := s.requireActive(); err != nil {
		return nil, err
	}
	order, err := buildOrderRequest(spec)
	if err != nil {
		return nil, err
	}
	o := newCallOptions(opts)
	req := &contract.CreateOrderRequest{ExchangeID: o.exchangeId(), Order: order, SessionID: s.SessionID()}
	resp := &contract.CreateOrderResponse{}
	if err := s.unaryInto(ctx, TradingService, "CreateOrder", req, resp); err != nil {
		return nil, err
	}
	if resp.Order == nil {
		return nil, errors.New("session: CreateOrder returned no order")
	}
	return resp.Order, nil
}

// CreateOrders places a batch, attributed to this session.
//
// The gate is all-or-nothing: if the session is not ACTIVE the whole batch is
// rejected, so a grid is never half-placed. WithExchangeID pins the whole batch
// to one registered backend.
func (s *Session) CreateOrders(ctx context.Context, specs []OrderSpec, opts ...Option) ([]*contract.Order, error) {
	if err := s.requireActive(); err != nil {
		return nil, err
	}
	orders := make([]*contract.OrderRequest, 0, len(specs))
	for _, spec := range specs {
		order, err := buildOrderRequest(spec)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	o := newCallOptions(opts)
	req := &contract.CreateOrdersRequest{ExchangeID: o.exchangeId(), Orders: orders, SessionID: s.SessionID()}
	resp := &contract.CreateOrdersResponse{}
	if err := s.unaryInto(ctx, TradingService, "CreateOrders", req, resp); err != nil {
		return nil, err
	}
	return resp.Orders, nil
}

func buildOrderRequest(spec OrderSpec) (*contract.OrderRequest, error) {
	amount, err := contract.ParseDecimal(spec.Amount)
	if err != nil {
		return nil, fmt.Errorf("session: order amount: %w", err)
	}
	req := &contract.OrderRequest{
		ClientOrderID: spec.ClientOrderID,
		Symbol:        spec.Symbol,
		Type:          spec.Type,
		Side:          spec.Side,
		Amount:        amount,
		TimeInForce:   spec.TimeInForce,
		PostOnly:      spec.PostOnly,
		ReduceOnly:    spec.ReduceOnly,
	}
	if spec.Price != nil {
		price, err := contract.ParseDecimal(*spec.Price)
		if err != nil {
			return nil, fmt.Errorf("session: order price: %w", err)
		}
		req.Price = price
	}
	if spec.TriggerPrice != nil {
		trigger, err := contract.ParseDecimal(*spec.TriggerPrice)
		if err != nil {
			return nil, fmt.Errorf("session: order trigger price: %w", err)
		}
		req.TriggerPrice = trigger
	}
	return req, nil
}

// CancelOrder cancels one order by venue order id. WithExchangeID must name the
// same backend the order was placed on, or the host looks on its default
// exchange and does not find the order.
func (s *Session) CancelOrder(ctx context.Context, orderID, symbol string, opts ...Option) (*contract.Order, error) {
	o := newCallOptions(opts)
	req := &contract.CancelOrderRequest{ExchangeID: o.exchangeId(), OrderID: orderID, Symbol: symbol}
	resp := &contract.CancelOrderResponse{}
	if err := s.unaryInto(ctx, TradingService, "CancelOrder", req, resp); err != nil {
		return nil, err
	}
	if resp.Order == nil {
		return nil, errors.New("session: CancelOrder returned no order")
	}
	return resp.Order, nil
}

// CancelAllOrders cancels every open order; an empty symbol spans all
// symbols. WithExchangeID bounds the sweep to one registered backend. This is
// the kill-switch execution path, which the host scopes to the session itself.
func (s *Session) CancelAllOrders(ctx context.Context, symbol string, opts ...Option) ([]*contract.Order, error) {
	o := newCallOptions(opts)
	req := &contract.CancelAllOrdersRequest{ExchangeID: o.exchangeId(), Symbol: symbol}
	resp := &contract.CancelAllOrdersResponse{}
	if err := s.unaryInto(ctx, TradingService, "CancelAllOrders", req, resp); err != nil {
		return nil, err
	}
	return resp.Orders, nil
}

// FetchOpenOrders lists currently open orders; an empty symbol spans all
// symbols and WithLimit bounds the page.
func (s *Session) FetchOpenOrders(ctx context.Context, symbol string, opts ...Option) ([]*contract.Order, error) {
	o := newCallOptions(opts)
	req := &contract.FetchOpenOrdersRequest{ExchangeID: o.exchangeId(), Symbol: symbol, Pagination: o.pagination}
	resp := &contract.FetchOpenOrdersResponse{}
	if err := s.unaryInto(ctx, TradingService, "FetchOpenOrders", req, resp); err != nil {
		return nil, err
	}
	return resp.Orders, nil
}

// GetAccount fetches the account margin summary.
func (s *Session) GetAccount(ctx context.Context, opts ...Option) (*contract.GetAccountResponse, error) {
	o := newCallOptions(opts)
	resp := &contract.GetAccountResponse{}
	err := s.unaryInto(ctx, TradingService, "GetAccount", &contract.GetAccountRequest{ExchangeID: o.exchangeId()}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetPositions lists open positions; a nil or empty symbols list means every
// symbol.
func (s *Session) GetPositions(ctx context.Context, symbols []string, opts ...Option) ([]*contract.Position, error) {
	o := newCallOptions(opts)
	req := &contract.GetPositionsRequest{ExchangeID: o.exchangeId(), Symbols: symbols}
	resp := &contract.GetPositionsResponse{}
	if err := s.unaryInto(ctx, TradingService, "GetPositions", req, resp); err != nil {
		return nil, err
	}
	return resp.Positions, nil
}

// GetOrderHistory lists historical orders.
func (s *Session) GetOrderHistory(ctx context.Context, opts ...Option) (*contract.GetOrderHistoryResponse, error) {
	o := newCallOptions(opts)
	req := &contract.GetOrderHistoryRequest{ExchangeID: o.exchangeId(), Pagination: o.pagination}
	resp := &contract.GetOrderHistoryResponse{}
	if err := s.unaryInto(ctx, TradingService, "GetOrderHistory", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetClosedPositions lists closed positions.
func (s *Session) GetClosedPositions(ctx context.Context, opts ...Option) (*contract.GetClosedPositionsResponse, error) {
	o := newCallOptions(opts)
	req := &contract.GetClosedPositionsRequest{ExchangeID: o.exchangeId(), Pagination: o.pagination}
	resp := &contract.GetClosedPositionsResponse{}
	if err := s.unaryInto(ctx, TradingService, "GetClosedPositions", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ClosePosition closes one position by id.
func (s *Session) ClosePosition(ctx context.Context, positionID string, opts ...Option) (*contract.Position, error) {
	o := newCallOptions(opts)
	req := &contract.ClosePositionRequest{ExchangeID: o.exchangeId(), PositionID: positionID}
	resp := &contract.ClosePositionResponse{}
	if err := s.unaryInto(ctx, TradingService, "ClosePosition", req, resp); err != nil {
		return nil, err
	}
	if resp.Position == nil {
		return nil, errors.New("session: ClosePosition returned no position")
	}
	return resp.Position, nil
}

// CloseAllPositions closes every open position. This is the kill-switch
// execution path.
func (s *Session) CloseAllPositions(ctx context.Context, opts ...Option) error {
	o := newCallOptions(opts)
	req := &contract.CloseAllPositionsRequest{ExchangeID: o.exchangeId()}
	return s.unaryInto(ctx, TradingService, "CloseAllPositions", req, &contract.CloseAllPositionsResponse{})
}

// ModifyPosition attaches or replaces a position's bracket orders. A nil
// bracket field leaves that bracket unchanged, so a caller can move just the
// stop without clearing the target.
func (s *Session) ModifyPosition(ctx context.Context, positionID string, brackets Brackets, opts ...Option) (*contract.Position, error) {
	o := newCallOptions(opts)
	req := &contract.ModifyPositionRequest{ExchangeID: o.exchangeId(), PositionID: positionID}
	if brackets.TakeProfit != nil {
		value, err := contract.ParseDecimal(*brackets.TakeProfit)
		if err != nil {
			return nil, fmt.Errorf("session: take profit: %w", err)
		}
		req.TakeProfit = &value
	}
	if brackets.StopLoss != nil {
		value, err := contract.ParseDecimal(*brackets.StopLoss)
		if err != nil {
			return nil, fmt.Errorf("session: stop loss: %w", err)
		}
		req.StopLoss = &value
	}
	resp := &contract.ModifyPositionResponse{}
	if err := s.unaryInto(ctx, TradingService, "ModifyPosition", req, resp); err != nil {
		return nil, err
	}
	if resp.Position == nil {
		return nil, errors.New("session: ModifyPosition returned no position")
	}
	return resp.Position, nil
}

// -- market data -------------------------------------------------------------

// FetchTicker fetches one ticker snapshot.
func (s *Session) FetchTicker(ctx context.Context, symbol string, opts ...Option) (*contract.Ticker, error) {
	o := newCallOptions(opts)
	req := &contract.FetchTickerRequest{ExchangeID: o.exchangeId(), Symbol: symbol}
	resp := &contract.FetchTickerResponse{}
	if err := s.unaryInto(ctx, MarketService, "FetchTicker", req, resp); err != nil {
		return nil, err
	}
	if resp.Ticker == nil {
		return nil, errors.New("session: FetchTicker returned no ticker")
	}
	return resp.Ticker, nil
}

// FetchOrderBook fetches one order book snapshot; WithDepth bounds the levels.
func (s *Session) FetchOrderBook(ctx context.Context, symbol string, opts ...Option) (*contract.OrderBook, error) {
	o := newCallOptions(opts)
	req := &contract.FetchOrderBookRequest{ExchangeID: o.exchangeId(), Symbol: symbol, Pagination: o.pagination}
	resp := &contract.FetchOrderBookResponse{}
	if err := s.unaryInto(ctx, MarketService, "FetchOrderBook", req, resp); err != nil {
		return nil, err
	}
	if resp.Orderbook == nil {
		return nil, errors.New("session: FetchOrderBook returned no order book")
	}
	return resp.Orderbook, nil
}

// GetCandles fetches historical OHLCV candles; WithTimeframe selects the
// interval and WithLimit the page size.
func (s *Session) GetCandles(ctx context.Context, symbol string, opts ...Option) (*contract.GetCandlesResponse, error) {
	o := newCallOptions(opts)
	if o.timeframe == "" {
		o.timeframe = "M1"
	}
	req := &contract.GetCandlesRequest{
		ExchangeID: o.exchangeId(),
		Symbol:     symbol,
		Timeframe:  o.timeframe,
		Pagination: o.pagination,
	}
	resp := &contract.GetCandlesResponse{}
	if err := s.unaryInto(ctx, MarketService, "GetCandles", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ListSymbols lists the tradable instruments of the bound venue.
func (s *Session) ListSymbols(ctx context.Context, opts ...Option) ([]*contract.SymbolInfo, error) {
	o := newCallOptions(opts)
	resp := &contract.ListSymbolsResponse{}
	err := s.unaryInto(ctx, MarketService, "ListSymbols", &contract.ListSymbolsRequest{ExchangeID: o.exchangeId()}, resp)
	if err != nil {
		return nil, err
	}
	return resp.Symbols, nil
}

// StreamMarketData opens a server-streaming market data subscription and
// returns a channel of decoded events, closed when the stream ends or ctx is
// cancelled.
//
// Unlike the strategy stream, this one does not emit gap markers: a market
// data consumer detects a gap from the header sequence itself (and an order
// book consumer must refetch a snapshot when it does).
//
// A subscription the host refuses arrives as a final item with Err set rather
// than as a silently closed channel, for the same reason as StreamEvents: the
// rejection is an HTTP 200 whose only frame is an end-of-stream error.
func (s *Session) StreamMarketData(ctx context.Context, symbols []string, channel contract.StreamChannel) (<-chan MarketDataEvent, error) {
	req := &contract.StreamMarketDataRequest{}
	for _, symbol := range symbols {
		req.Subscriptions = append(req.Subscriptions, &contract.StreamSubscription{Symbol: symbol, Channel: channel})
	}
	resp, err := s.openStream(ctx, MarketService, "StreamMarketData", req)
	if err != nil {
		return nil, err
	}
	events := make(chan MarketDataEvent, streamBuffer)
	go func() {
		defer close(events)
		defer resp.Body.Close()
		dec := NewStreamDecoder(resp.Body)
		for {
			payload, err := dec.Next()
			if err != nil {
				if cerr := endStreamFrameError(dec.EndOfStreamPayload()); cerr != nil {
					select {
					case events <- MarketDataEvent{Err: cerr}:
					case <-ctx.Done():
					}
				}
				return
			}
			ev := &contract.MarketDataEvent{}
			if err := ev.Unmarshal(payload); err != nil {
				return
			}
			select {
			case events <- MarketDataEvent{Event: ev}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, nil
}
