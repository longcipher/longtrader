// Package session mirrors the worker session semantics over bare Connect
// unary calls: attach -> heartbeat -> reconcile -> trade. Concept names match
// Python/TypeScript/Rust 1:1 (Session, TradingPort, sync_state, OverflowPolicy).
//
// Wire format: POST {base}/longtrader.worker.v1.WorkerSessionService/{Method}
// Content-Type: application/proto (unary) or application/connect+proto (streaming);
// see docs/bare-protocol-guide.md sections 4-5.
//
// Generated protobuf types live in ../gen (produced by `buf generate`, never
// hand-edited). This file stays dependency-free (stdlib only) so `go build`
// works even before generation: payloads are pre-encoded protobuf bytes.
package session

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Lifecycle states (proto/longtrader/worker/v1/worker.proto).
const (
	StateDisconnected = "DISCONNECTED"
	StateAttached     = "ATTACHED"
	StateSyncing      = "SYNCING"
	StateActive       = "ACTIVE"
)

// WorkerService is the Connect service path.
const WorkerService = "longtrader.worker.v1.WorkerSessionService"

// ConnectError is a non-200 unary error reply.
type ConnectError struct {
	Status  int
	Code    string
	Message string
}

func (e *ConnectError) Error() string {
	return fmt.Sprintf("%s: %s (http %d)", e.Code, e.Message, e.Status)
}

// Session is one attached strategy session against the worker control plane.
type Session struct {
	mu                  sync.Mutex
	baseURL             string
	http                *http.Client
	SessionID           string
	HeartbeatIntervalMs uint32
	state               string
	lastKeepAliveOk     time.Time
	stopCh              chan struct{}
	stopOnce            sync.Once
}

// Attach validates the token and negotiates lease parameters.
// reqBody is a pre-encoded AttachSessionRequest (from ../gen after generation);
// respBody is filled with the raw AttachSessionResponse bytes for the caller
// to decode. Use AttachEncoded when gen types are unavailable.
func Attach(baseURL, token string, reqBody []byte) (*Session, []byte, error) {
	s := &Session{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
		state:   StateDisconnected,
		stopCh:  make(chan struct{}),
	}
	resp, err := s.unary("AttachSession", token, reqBody)
	if err != nil {
		return nil, nil, err
	}
	s.state = StateAttached
	s.lastKeepAliveOk = time.Now()
	return s, resp, nil
}

// State returns the local lifecycle view.
func (s *Session) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "" {
		return StateDisconnected
	}
	return s.state
}

// SetAttached populates session metadata after decoding AttachSessionResponse.
func (s *Session) SetAttached(sessionID string, heartbeatMs uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SessionID = sessionID
	s.HeartbeatIntervalMs = heartbeatMs
	s.state = StateAttached
	s.lastKeepAliveOk = time.Now()
}

// MarkSyncing transitions ATTACHED -> SYNCING before snapshot fetch.
func (s *Session) MarkSyncing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = StateSyncing
}

// MarkActive transitions -> ACTIVE after successful reconcile.
func (s *Session) MarkActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = StateActive
}

// KeepAlive feeds the lease watchdog with a pre-encoded KeepAliveRequest.
func (s *Session) KeepAlive(token string, reqBody []byte) ([]byte, error) {
	resp, err := s.unary("KeepAlive", token, reqBody)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.lastKeepAliveOk = time.Now()
	s.mu.Unlock()
	return resp, nil
}

// Reconcile fetches the atomic snapshot with a pre-encoded ReconcileStateRequest.
func (s *Session) Reconcile(token string, reqBody []byte) ([]byte, error) {
	s.MarkSyncing()
	resp, err := s.unary("ReconcileState", token, reqBody)
	if err != nil {
		return nil, err
	}
	s.MarkActive()
	return resp, nil
}

// SetKillSwitchPolicy updates the session cancel-on-disconnect behavior.
func (s *Session) SetKillSwitchPolicy(token string, reqBody []byte) ([]byte, error) {
	return s.unary("SetKillSwitchPolicy", token, reqBody)
}

// StartHeartbeat spawns a lease watchdog that calls keepAlive every interval.
// keepAlive must capture the session + token and return an error on failure;
// on lease timeout the watchdog stops and reports via onExpired (may be nil).
func (s *Session) StartHeartbeat(interval time.Duration, keepAlive func() error, onExpired func()) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		// Lease defaults to 3x heartbeat when the server omits it.
		lease := 3 * interval
		for {
			select {
			case <-s.stopCh:
				return
			case <-t.C:
				if err := keepAlive(); err != nil {
					continue
				}
				s.mu.Lock()
				elapsed := time.Since(s.lastKeepAliveOk)
				s.mu.Unlock()
				if elapsed > lease {
					if onExpired != nil {
						onExpired()
					}
					return
				}
			}
		}
	}()
}

// Close stops heartbeat/watchdog loops. Idempotent.
func (s *Session) Close() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

func (s *Session) unary(method, token string, body []byte) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%s", s.baseURL, WorkerService, method)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// ponytail: unary uses application/proto; streaming uses application/connect+proto.
	req.Header.Set("Content-Type", "application/proto")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, &ConnectError{Status: 0, Code: "transport", Message: err.Error()}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &ConnectError{Status: resp.StatusCode, Code: "read", Message: err.Error()}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := string(data)
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return nil, &ConnectError{Status: resp.StatusCode, Code: "rpc", Message: msg}
	}
	return data, nil
}
