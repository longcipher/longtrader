// Package session mirrors the worker session semantics over bare Connect
// unary calls: attach -> heartbeat -> reconcile -> trade. Concept names match
// Python/TypeScript/Rust 1:1 (Session, TradingPort, sync_state, OverflowPolicy).
package session

// Session is one attached strategy session against the worker control plane.
// Wire format: POST {base}/longtrader.worker.v1.WorkerSessionService/{Method}
// Content-Type: application/proto ; see docs/bare-protocol-guide.md §§4-5.
type Session struct {
	SessionID           string
	HeartbeatIntervalMs uint32
	baseURL             string
}

