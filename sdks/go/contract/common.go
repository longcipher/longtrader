package contract

// Symbol is a unified instrument symbol such as "BTC/USDT".
type Symbol struct {
	// Value is the symbol text.
	Value string
}

// MarshalTo implements Message.
func (m *Symbol) MarshalTo(e *Encoder) { e.String(1, m.Value) }

// Unmarshal implements Message.
func (m *Symbol) Unmarshal(data []byte) error {
	*m = Symbol{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			m.Value = f.AsString()
		}
		return nil
	})
}

// ExchangeId identifies one registered exchange instance on a backend.
type ExchangeId struct {
	// ID is the registered instance id.
	ID string
	// Label is the human-readable backend label.
	Label string
}

// MarshalTo implements Message.
func (m *ExchangeId) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.Label)
}

// Unmarshal implements Message.
func (m *ExchangeId) Unmarshal(data []byte) error {
	*m = ExchangeId{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.Label = f.AsString()
		}
		return nil
	})
}

// Pagination is the shared paging envelope for list methods.
//
// `Limit` caps the page size; `Since` is an optional inclusive lower bound in
// Unix milliseconds; `Cursor` is an opaque server-issued continuation token.
// Set at most one of Since/Cursor, preferring Cursor for stable paging.
type Pagination struct {
	// Limit caps the page size; 0 means "backend default".
	Limit uint64
	// Since is an inclusive lower bound in Unix milliseconds.
	Since uint64
	// Cursor is the opaque continuation token from a previous Page.NextCursor.
	Cursor string
}

// MarshalTo implements Message.
func (m *Pagination) MarshalTo(e *Encoder) {
	e.Uint64(1, m.Limit)
	e.Uint64(2, m.Since)
	e.String(3, m.Cursor)
}

// Unmarshal implements Message.
func (m *Pagination) Unmarshal(data []byte) error {
	*m = Pagination{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Limit = f.AsUint64()
		case 2:
			m.Since = f.AsUint64()
		case 3:
			m.Cursor = f.AsString()
		}
		return nil
	})
}

// Page is the paging metadata echoed on every list response.
type Page struct {
	// NextCursor is empty when the last page was returned.
	NextCursor string
	// Total is best-effort and may be 0 when the backend does not count.
	Total uint32
}

// MarshalTo implements Message.
func (m *Page) MarshalTo(e *Encoder) {
	e.String(1, m.NextCursor)
	e.Uint32(2, m.Total)
}

// Unmarshal implements Message.
func (m *Page) Unmarshal(data []byte) error {
	*m = Page{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.NextCursor = f.AsString()
		case 2:
			m.Total = f.AsUint32()
		}
		return nil
	})
}

// EventHeader is the multi-stage latency header attached to every streamed
// event. Sequence is the per-subscription watermark a consumer uses to detect
// drops; a jump means resync (order book) or reconcile (private streams).
type EventHeader struct {
	// TraceID is the W3C TraceContext trace id, propagated end-to-end.
	TraceID string
	// Sequence is monotonically increasing and gap-free per subscription.
	Sequence uint64
	// ExchangeTimeNS is the venue-reported event time.
	ExchangeTimeNS int64
	// GatewayInTimeNS is the daemon receive time.
	GatewayInTimeNS int64
	// LocalDispatchTimeNS is the worker dispatch-to-consumer time.
	LocalDispatchTimeNS int64
}

// MarshalTo implements Message.
func (m *EventHeader) MarshalTo(e *Encoder) {
	e.String(1, m.TraceID)
	e.Uint64(2, m.Sequence)
	e.Int64(3, m.ExchangeTimeNS)
	e.Int64(4, m.GatewayInTimeNS)
	e.Int64(5, m.LocalDispatchTimeNS)
}

// Unmarshal implements Message.
func (m *EventHeader) Unmarshal(data []byte) error {
	*m = EventHeader{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.TraceID = f.AsString()
		case 2:
			m.Sequence = f.AsUint64()
		case 3:
			m.ExchangeTimeNS = f.AsInt64()
		case 4:
			m.GatewayInTimeNS = f.AsInt64()
		case 5:
			m.LocalDispatchTimeNS = f.AsInt64()
		}
		return nil
	})
}
