package contract

import "time"

// Timestamp mirrors google.protobuf.Timestamp (seconds + nanos).
//
// It is modelled as a pointer on every message field so "absent" stays
// distinguishable from the epoch.
type Timestamp struct {
	// Seconds since the Unix epoch.
	Seconds int64
	// Nanos within the second, 0..999999999.
	Nanos int32
}

// MarshalTo implements Message.
func (m *Timestamp) MarshalTo(e *Encoder) {
	e.Int64(1, m.Seconds)
	e.Int32(2, m.Nanos)
}

// Unmarshal implements Message.
func (m *Timestamp) Unmarshal(data []byte) error {
	*m = Timestamp{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Seconds = f.AsInt64()
		case 2:
			m.Nanos = f.AsInt32()
		}
		return nil
	})
}

// AsTime converts the timestamp to a time.Time in UTC.
func (m *Timestamp) AsTime() time.Time { return time.Unix(m.Seconds, int64(m.Nanos)).UTC() }

// TimeFrom converts a time.Time to a Timestamp.
func TimeFrom(t time.Time) *Timestamp {
	return &Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}

// NowTimestamp returns the current wall-clock time as a Timestamp.
func NowTimestamp() *Timestamp { return TimeFrom(time.Now()) }

// Duration mirrors google.protobuf.Duration (seconds + nanos).
type Duration struct {
	// Seconds is the whole-second part; may be negative.
	Seconds int64
	// Nanos is the sub-second part with the same sign as Seconds.
	Nanos int32
}

// MarshalTo implements Message.
func (m *Duration) MarshalTo(e *Encoder) {
	e.Int64(1, m.Seconds)
	e.Int32(2, m.Nanos)
}

// Unmarshal implements Message.
func (m *Duration) Unmarshal(data []byte) error {
	*m = Duration{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Seconds = f.AsInt64()
		case 2:
			m.Nanos = f.AsInt32()
		}
		return nil
	})
}

// AsDuration converts the duration to a time.Duration.
func (m *Duration) AsDuration() time.Duration {
	return time.Duration(m.Seconds)*time.Second + time.Duration(m.Nanos)
}

// DurationFrom converts a time.Duration to a Duration.
func DurationFrom(d time.Duration) *Duration {
	return &Duration{Seconds: int64(d / time.Second), Nanos: int32(d % time.Second)}
}
