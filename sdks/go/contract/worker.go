package contract

// KillSwitchPolicy parameterizes this session's cancel-on-disconnect behavior.
type KillSwitchPolicy struct {
	// LeaseTimeout is the missed-heartbeat budget; nil takes the server default
	// of three times the negotiated heartbeat interval.
	LeaseTimeout *Duration
	// Scope selects which orders a tripped kill-switch cancels.
	Scope KillSwitchScope
}

// MarshalTo implements Message.
func (m *KillSwitchPolicy) MarshalTo(e *Encoder) {
	e.Message(1, m.LeaseTimeout)
	e.Int32(2, int32(m.Scope))
}

// Unmarshal implements Message.
func (m *KillSwitchPolicy) Unmarshal(data []byte) error {
	*m = KillSwitchPolicy{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.LeaseTimeout = &Duration{}
			return m.LeaseTimeout.Unmarshal(f.Data)
		case 2:
			m.Scope = KillSwitchScope(f.AsInt32())
		}
		return nil
	})
}

// AttachSessionRequest validates a token and (re)attaches a strategy session.
//
// Token is the existing terminal API token; the host compares it in constant
// time from this request body field, not from a header. SessionID resumes a
// previous session after a network drop: the host reuses it (refreshing the
// lease and re-admitting a non-terminal state) so the kill-switch's
// tracked-order set survives the reconnect.
type AttachSessionRequest struct {
	// Token is the terminal API token.
	Token string
	// ClientName identifies the SDK.
	ClientName string
	// ClientVersion is the SDK version.
	ClientVersion string
	// Policy parameterizes the session; nil accepts server defaults.
	Policy *KillSwitchPolicy
	// SessionID resumes an existing session; empty creates a new one.
	SessionID string
}

// MarshalTo implements Message.
func (m *AttachSessionRequest) MarshalTo(e *Encoder) {
	e.String(1, m.Token)
	e.String(2, m.ClientName)
	e.String(3, m.ClientVersion)
	e.Message(4, m.Policy)
	e.String(5, m.SessionID)
}

// Unmarshal implements Message.
func (m *AttachSessionRequest) Unmarshal(data []byte) error {
	*m = AttachSessionRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Token = f.AsString()
		case 2:
			m.ClientName = f.AsString()
		case 3:
			m.ClientVersion = f.AsString()
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			m.Policy = &KillSwitchPolicy{}
			return m.Policy.Unmarshal(f.Data)
		case 5:
			m.SessionID = f.AsString()
		}
		return nil
	})
}

// AttachSessionResponse carries the negotiated session parameters.
type AttachSessionResponse struct {
	// SessionID is the session id to send on every subsequent call.
	SessionID string
	// HeartbeatIntervalMS is the negotiated KeepAlive interval.
	HeartbeatIntervalMS uint32
	// ServerTime is the host clock at attach.
	ServerTime *Timestamp
	// Capabilities are the optional features the host advertises, e.g.
	// "reconcile", "kill_switch", "event_replay"; feature-detect with them
	// instead of guessing.
	Capabilities []string
}

// MarshalTo implements Message.
func (m *AttachSessionResponse) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.Uint32(2, m.HeartbeatIntervalMS)
	e.Message(3, m.ServerTime)
	for _, c := range m.Capabilities {
		e.String(4, c)
	}
}

// Unmarshal implements Message.
func (m *AttachSessionResponse) Unmarshal(data []byte) error {
	*m = AttachSessionResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.HeartbeatIntervalMS = f.AsUint32()
		case 3:
			return decodeTime(f, &m.ServerTime)
		case 4:
			if f.Wire == WireBytes {
				m.Capabilities = append(m.Capabilities, f.AsString())
			}
		}
		return nil
	})
}

// KeepAliveRequest feeds the host's session lease watchdog.
type KeepAliveRequest struct {
	// SessionID is the attached session.
	SessionID string
	// ClientTimeNS is the client wall clock in Unix nanoseconds.
	ClientTimeNS int64
}

// MarshalTo implements Message.
func (m *KeepAliveRequest) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.Int64(2, m.ClientTimeNS)
}

// Unmarshal implements Message.
func (m *KeepAliveRequest) Unmarshal(data []byte) error {
	*m = KeepAliveRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.ClientTimeNS = f.AsInt64()
		}
		return nil
	})
}

// KeepAliveResponse acknowledges a heartbeat and may re-negotiate the interval.
type KeepAliveResponse struct {
	// ServerTimeNS is the host clock in Unix nanoseconds.
	ServerTimeNS int64
	// HeartbeatIntervalMS is the (possibly updated) negotiated interval.
	HeartbeatIntervalMS uint32
}

// MarshalTo implements Message.
func (m *KeepAliveResponse) MarshalTo(e *Encoder) {
	e.Int64(1, m.ServerTimeNS)
	e.Uint32(2, m.HeartbeatIntervalMS)
}

// Unmarshal implements Message.
func (m *KeepAliveResponse) Unmarshal(data []byte) error {
	*m = KeepAliveResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ServerTimeNS = f.AsInt64()
		case 2:
			m.HeartbeatIntervalMS = f.AsUint32()
		}
		return nil
	})
}

// ReconcileStateRequest asks for the authoritative snapshot.
type ReconcileStateRequest struct {
	// SessionID is the attached session.
	SessionID string
}

// MarshalTo implements Message.
func (m *ReconcileStateRequest) MarshalTo(e *Encoder) { e.String(1, m.SessionID) }

// Unmarshal implements Message.
func (m *ReconcileStateRequest) Unmarshal(data []byte) error {
	*m = ReconcileStateRequest{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.SessionID = f.AsString()
		}
		return nil
	})
}

// ReconcileStateResponse is one atomic snapshot stamped with the stream
// watermark at snapshot time, which avoids multi-RPC watermark races.
type ReconcileStateResponse struct {
	// SnapshotSequence is the watermark a resumed stream replays from.
	SnapshotSequence uint64
	// SnapshotTime is the snapshot instant.
	SnapshotTime *Timestamp
	// Balances are the account balances.
	Balances []*Balance
	// Positions are the open positions.
	Positions []*Position
	// OpenOrders are the resting orders.
	OpenOrders []*Order
}

// MarshalTo implements Message.
func (m *ReconcileStateResponse) MarshalTo(e *Encoder) {
	e.Uint64(1, m.SnapshotSequence)
	e.Message(2, m.SnapshotTime)
	for _, b := range m.Balances {
		e.Message(3, b)
	}
	for _, p := range m.Positions {
		e.Message(4, p)
	}
	for _, o := range m.OpenOrders {
		e.Message(5, o)
	}
}

// Unmarshal implements Message.
func (m *ReconcileStateResponse) Unmarshal(data []byte) error {
	*m = ReconcileStateResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SnapshotSequence = f.AsUint64()
		case 2:
			return decodeTime(f, &m.SnapshotTime)
		case 3:
			if f.Wire != WireBytes {
				return nil
			}
			b := &Balance{}
			if err := b.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Balances = append(m.Balances, b)
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			p := &Position{}
			if err := p.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Positions = append(m.Positions, p)
		case 5:
			if f.Wire != WireBytes {
				return nil
			}
			o := &Order{}
			if err := o.Unmarshal(f.Data); err != nil {
				return err
			}
			m.OpenOrders = append(m.OpenOrders, o)
		}
		return nil
	})
}

// SetKillSwitchPolicyRequest re-parameterizes cancel-on-disconnect behavior.
type SetKillSwitchPolicyRequest struct {
	// SessionID is the attached session.
	SessionID string
	// Policy is the new policy.
	Policy *KillSwitchPolicy
}

// MarshalTo implements Message.
func (m *SetKillSwitchPolicyRequest) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.Message(2, m.Policy)
}

// Unmarshal implements Message.
func (m *SetKillSwitchPolicyRequest) Unmarshal(data []byte) error {
	*m = SetKillSwitchPolicyRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Policy = &KillSwitchPolicy{}
			return m.Policy.Unmarshal(f.Data)
		}
		return nil
	})
}

// SetKillSwitchPolicyResponse is the empty acknowledgement.
type SetKillSwitchPolicyResponse struct{}

// MarshalTo implements Message.
func (m *SetKillSwitchPolicyResponse) MarshalTo(*Encoder) {}

// Unmarshal implements Message.
func (m *SetKillSwitchPolicyResponse) Unmarshal(data []byte) error {
	*m = SetKillSwitchPolicyResponse{}
	return Scan(data, func(Field) error { return nil })
}

// RegisterStrategyRequest names this session's strategy and its parameters.
type RegisterStrategyRequest struct {
	// SessionID is the attached session.
	SessionID string
	// Name is the strategy name.
	Name string
	// Params are the strategy parameters, as strings.
	Params map[string]string
}

// MarshalTo implements Message.
func (m *RegisterStrategyRequest) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.String(2, m.Name)
	encodeMap(e, 3, m.Params)
}

// Unmarshal implements Message.
func (m *RegisterStrategyRequest) Unmarshal(data []byte) error {
	*m = RegisterStrategyRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.Name = f.AsString()
		case 3:
			return decodeMapEntry(f, 3, &m.Params)
		}
		return nil
	})
}

// RegisterStrategyResponse carries the assigned strategy id.
type RegisterStrategyResponse struct {
	// StrategyID identifies the registered strategy.
	StrategyID string
}

// MarshalTo implements Message.
func (m *RegisterStrategyResponse) MarshalTo(e *Encoder) { e.String(1, m.StrategyID) }

// Unmarshal implements Message.
func (m *RegisterStrategyResponse) Unmarshal(data []byte) error {
	*m = RegisterStrategyResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.StrategyID = f.AsString()
		}
		return nil
	})
}

// StrategyStatusRequest asks for the host's view of this strategy.
type StrategyStatusRequest struct {
	// SessionID is the attached session.
	SessionID string
}

// MarshalTo implements Message.
func (m *StrategyStatusRequest) MarshalTo(e *Encoder) { e.String(1, m.SessionID) }

// Unmarshal implements Message.
func (m *StrategyStatusRequest) Unmarshal(data []byte) error {
	*m = StrategyStatusRequest{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.SessionID = f.AsString()
		}
		return nil
	})
}

// StrategyStatusResponse is the host-reported state, id and counters.
type StrategyStatusResponse struct {
	// State is the host's authoritative lifecycle state.
	State SessionState
	// StrategyID is the registered strategy id.
	StrategyID string
	// Name is the registered strategy name.
	Name string
	// StartedAt is the registration time.
	StartedAt *Timestamp
	// OrdersSubmitted counts accepted submissions.
	OrdersSubmitted uint64
	// LogEvents counts accepted log events.
	LogEvents uint64
}

// MarshalTo implements Message.
func (m *StrategyStatusResponse) MarshalTo(e *Encoder) {
	e.Int32(2, int32(m.State))
	e.String(3, m.StrategyID)
	e.String(4, m.Name)
	e.Message(5, m.StartedAt)
	e.Uint64(6, m.OrdersSubmitted)
	e.Uint64(7, m.LogEvents)
}

// Unmarshal implements Message.
func (m *StrategyStatusResponse) Unmarshal(data []byte) error {
	*m = StrategyStatusResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 2:
			m.State = SessionState(f.AsInt32())
		case 3:
			m.StrategyID = f.AsString()
		case 4:
			m.Name = f.AsString()
		case 5:
			return decodeTime(f, &m.StartedAt)
		case 6:
			m.OrdersSubmitted = f.AsUint64()
		case 7:
			m.LogEvents = f.AsUint64()
		}
		return nil
	})
}

// StopStrategyRequest stops the strategy, optionally cancelling its orders.
type StopStrategyRequest struct {
	// SessionID is the attached session.
	SessionID string
	// CancelOpenOrders asks the host to cancel this session's resting orders;
	// this is the same path the kill-switch uses.
	CancelOpenOrders bool
}

// MarshalTo implements Message.
func (m *StopStrategyRequest) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.Bool(2, m.CancelOpenOrders)
}

// Unmarshal implements Message.
func (m *StopStrategyRequest) Unmarshal(data []byte) error {
	*m = StopStrategyRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.CancelOpenOrders = f.AsBool()
		}
		return nil
	})
}

// StopStrategyResponse carries the terminal state the host settled on.
type StopStrategyResponse struct {
	// FinalState is the state after the stop.
	FinalState SessionState
}

// MarshalTo implements Message.
func (m *StopStrategyResponse) MarshalTo(e *Encoder) { e.Int32(1, int32(m.FinalState)) }

// Unmarshal implements Message.
func (m *StopStrategyResponse) Unmarshal(data []byte) error {
	*m = StopStrategyResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.FinalState = SessionState(f.AsInt32())
		}
		return nil
	})
}

// LogEvent is one strategy log line sent to the host.
type LogEvent struct {
	// SessionID is the attached session.
	SessionID string
	// Level is the severity.
	Level LogLevel
	// Message is the log line.
	Message string
	// Timestamp is the client wall clock.
	Timestamp *Timestamp
	// Fields are structured key/value context.
	Fields map[string]string
}

// MarshalTo implements Message.
func (m *LogEvent) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.Int32(2, int32(m.Level))
	e.String(3, m.Message)
	e.Message(4, m.Timestamp)
	encodeMap(e, 5, m.Fields)
}

// Unmarshal implements Message.
func (m *LogEvent) Unmarshal(data []byte) error {
	*m = LogEvent{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.Level = LogLevel(f.AsInt32())
		case 3:
			m.Message = f.AsString()
		case 4:
			return decodeTime(f, &m.Timestamp)
		case 5:
			return decodeMapEntry(f, 5, &m.Fields)
		}
		return nil
	})
}

// ReportLogResponse reports how many log events the host accepted.
type ReportLogResponse struct {
	// Accepted is the number of accepted events.
	Accepted uint64
}

// MarshalTo implements Message.
func (m *ReportLogResponse) MarshalTo(e *Encoder) { e.Uint64(1, m.Accepted) }

// Unmarshal implements Message.
func (m *ReportLogResponse) Unmarshal(data []byte) error {
	*m = ReportLogResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.Accepted = f.AsUint64()
		}
		return nil
	})
}

// StreamStrategyEventsRequest opens the unified event stream.
type StreamStrategyEventsRequest struct {
	// SessionID is the attached session.
	SessionID string
	// ResumeToken is the opaque cursor from a previous stream's last event;
	// empty starts fresh.
	ResumeToken string
}

// MarshalTo implements Message.
func (m *StreamStrategyEventsRequest) MarshalTo(e *Encoder) {
	e.String(1, m.SessionID)
	e.String(2, m.ResumeToken)
}

// Unmarshal implements Message.
func (m *StreamStrategyEventsRequest) Unmarshal(data []byte) error {
	*m = StreamStrategyEventsRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.SessionID = f.AsString()
		case 2:
			m.ResumeToken = f.AsString()
		}
		return nil
	})
}

// StateChange is a lifecycle transition observed on the stream.
type StateChange struct {
	// From is the previous state.
	From SessionState
	// To is the new state.
	To SessionState
	// Reason is a human-readable explanation.
	Reason string
}

// MarshalTo implements Message.
func (m *StateChange) MarshalTo(e *Encoder) {
	e.Int32(1, int32(m.From))
	e.Int32(2, int32(m.To))
	e.String(3, m.Reason)
}

// Unmarshal implements Message.
func (m *StateChange) Unmarshal(data []byte) error {
	*m = StateChange{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.From = SessionState(f.AsInt32())
		case 2:
			m.To = SessionState(f.AsInt32())
		case 3:
			m.Reason = f.AsString()
		}
		return nil
	})
}

// OrderUpdate is an order lifecycle event on the strategy stream.
type OrderUpdate struct {
	// Order is the order in its post-update state.
	Order *Order
	// UpdateType is "new", "fill", "canceled" or "rejected".
	UpdateType string
}

// MarshalTo implements Message.
func (m *OrderUpdate) MarshalTo(e *Encoder) {
	e.Message(1, m.Order)
	e.String(2, m.UpdateType)
}

// Unmarshal implements Message.
func (m *OrderUpdate) Unmarshal(data []byte) error {
	*m = OrderUpdate{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Order = &Order{}
			return m.Order.Unmarshal(f.Data)
		case 2:
			m.UpdateType = f.AsString()
		}
		return nil
	})
}

// StrategyEvent is one item of the unified stream: private updates plus
// strategy events, replayable after a reconnect via ResumeToken.
type StrategyEvent struct {
	// Header carries the gap-free per-subscription watermark.
	Header *EventHeader
	// OrderUpdate is set for order events.
	OrderUpdate *OrderUpdate
	// BalanceUpdate is set for balance events.
	BalanceUpdate *Balance
	// PositionUpdate is set for position events.
	PositionUpdate *Position
	// TradeUpdate is set for fill events.
	TradeUpdate *Trade
	// Log is set for host-echoed log events.
	Log *LogEvent
	// StateChange is set for lifecycle transitions.
	StateChange *StateChange
	// Ticker is set for tickers multiplexed onto this stream.
	Ticker *Ticker
	// ResumeToken is the opaque cursor for reconnect-with-replay.
	ResumeToken string
}

// MarshalTo implements Message.
func (m *StrategyEvent) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.Message(2, m.OrderUpdate)
	e.Message(3, m.BalanceUpdate)
	e.Message(4, m.PositionUpdate)
	e.Message(5, m.TradeUpdate)
	e.Message(6, m.Log)
	e.Message(7, m.StateChange)
	e.Message(8, m.Ticker)
	e.String(9, m.ResumeToken)
}

// Unmarshal implements Message.
func (m *StrategyEvent) Unmarshal(data []byte) error {
	*m = StrategyEvent{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.OrderUpdate = &OrderUpdate{}
			return m.OrderUpdate.Unmarshal(f.Data)
		case 3:
			if f.Wire != WireBytes {
				return nil
			}
			m.BalanceUpdate = &Balance{}
			return m.BalanceUpdate.Unmarshal(f.Data)
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			m.PositionUpdate = &Position{}
			return m.PositionUpdate.Unmarshal(f.Data)
		case 5:
			if f.Wire != WireBytes {
				return nil
			}
			m.TradeUpdate = &Trade{}
			return m.TradeUpdate.Unmarshal(f.Data)
		case 6:
			if f.Wire != WireBytes {
				return nil
			}
			m.Log = &LogEvent{}
			return m.Log.Unmarshal(f.Data)
		case 7:
			if f.Wire != WireBytes {
				return nil
			}
			m.StateChange = &StateChange{}
			return m.StateChange.Unmarshal(f.Data)
		case 8:
			if f.Wire != WireBytes {
				return nil
			}
			m.Ticker = &Ticker{}
			return m.Ticker.Unmarshal(f.Data)
		case 9:
			m.ResumeToken = f.AsString()
		}
		return nil
	})
}
