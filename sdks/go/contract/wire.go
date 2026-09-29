// Package contract is a dependency-free, hand-written protobuf codec for the
// longtrader contract (proto/longtrader/**).
//
// The generated stubs under ../gen are produced by `buf generate` and are never
// required: every type here encodes and decodes the exact wire format itself,
// so `go build`, `go vet` and `go test` all work in a fresh checkout and CI
// needs no codegen step. The module therefore has no `require` directives at
// all (stdlib only).
//
// Wire rules implemented here are the standard proto3 ones:
//
//   - varints, little-endian fixed32/fixed64, and length-delimited fields,
//   - proto3 default-value omission (zero scalars and empty strings are not
//     written),
//   - negative int32/int64 encoded as 10-byte sign-extended varints,
//   - unknown fields skipped, so a newer producer stays readable.
package contract

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// Protobuf wire types.
const (
	// WireVarint is the varint encoding (int32, int64, uint32, uint64, bool).
	WireVarint = 0
	// WireFixed64 is the 8-byte little-endian encoding (fixed64, double).
	WireFixed64 = 1
	// WireBytes is the length-delimited encoding (string, bytes, messages).
	WireBytes = 2
	// WireFixed32 is the 4-byte little-endian encoding (fixed32, float).
	WireFixed32 = 5
)

// DecodeError reports malformed protobuf input.
type DecodeError struct {
	// Msg describes what could not be decoded.
	Msg string
}

// Error implements error.
func (e *DecodeError) Error() string { return "contract: " + e.Msg }

func decodeErrorf(format string, args ...any) error {
	return &DecodeError{Msg: fmt.Sprintf(format, args...)}
}

// Message is a protobuf message this package can encode and decode.
type Message interface {
	// MarshalTo appends the message body to e, without a length prefix.
	MarshalTo(e *Encoder)
	// Unmarshal decodes a message body, replacing the receiver's contents.
	Unmarshal(data []byte) error
}

// Marshal returns the protobuf encoding of m.
func Marshal(m Message) []byte {
	e := &Encoder{}
	m.MarshalTo(e)
	return e.Bytes()
}

// Unmarshal decodes data into m, replacing its contents.
func Unmarshal(data []byte, m Message) error { return m.Unmarshal(data) }

// Encoder accumulates a protobuf message body.
type Encoder struct {
	buf []byte
}

// Bytes returns the encoded message body. The slice aliases the encoder's
// buffer and must not be modified by the caller.
func (e *Encoder) Bytes() []byte { return e.buf }

// Len returns the number of bytes written so far.
func (e *Encoder) Len() int { return len(e.buf) }

// Reset truncates the buffer, keeping its capacity.
func (e *Encoder) Reset() { e.buf = e.buf[:0] }

func (e *Encoder) putVarint(v uint64) {
	for v >= 0x80 {
		e.buf = append(e.buf, byte(v)|0x80)
		v >>= 7
	}
	e.buf = append(e.buf, byte(v))
}

func (e *Encoder) putTag(num, wire int) {
	e.putVarint(uint64(num)<<3 | uint64(wire))
}

func (e *Encoder) putLengthDelimited(num int, b []byte) {
	e.putTag(num, WireBytes)
	e.putVarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}

// Varint writes a varint field, omitted when zero (the proto3 default).
//
// Use the typed helpers below for readability; this is the raw form they are
// built on.
func (e *Encoder) Varint(num int, v uint64) {
	if v == 0 {
		return
	}
	e.putTag(num, WireVarint)
	e.putVarint(v)
}

// Int64 writes a signed 64-bit integer field. Negative values are sign-extended
// to ten bytes, as the protobuf spec requires.
func (e *Encoder) Int64(num int, v int64) { e.Varint(num, uint64(v)) }

// Int32 writes a signed 32-bit integer field, sign-extended like Int64.
func (e *Encoder) Int32(num int, v int32) { e.Varint(num, uint64(int64(v))) }

// Uint64 writes an unsigned 64-bit integer field.
func (e *Encoder) Uint64(num int, v uint64) { e.Varint(num, v) }

// Uint32 writes an unsigned 32-bit integer field.
func (e *Encoder) Uint32(num int, v uint32) { e.Varint(num, uint64(v)) }

// Bool writes a boolean field, omitted when false (the proto3 default).
func (e *Encoder) Bool(num int, v bool) {
	if !v {
		return
	}
	e.putTag(num, WireVarint)
	e.putVarint(1)
}

// String writes a string field, omitted when empty (the proto3 default).
//
// This is implicit-presence behaviour, which is right for a plain `string`
// field: absent and empty are the same thing on the wire. An `optional string`
// needs OptString instead, because there the two are different states.
func (e *Encoder) String(num int, v string) {
	if v == "" {
		return
	}
	e.putLengthDelimited(num, []byte(v))
}

// OptString writes an explicit-presence string field, omitted only when the
// pointer is nil.
//
// In proto3 an `optional` field records presence separately from value, so a
// field that is *set* to the zero value is a real, meaningful state: "the host
// told me the order id is empty" is not the same as "the host said nothing".
// Omitting a present-but-empty value collapses those two states, so a producer
// that sets the field to "" and a producer that never sets it decode
// identically. Writing the zero-length field is what preserves it.
func (e *Encoder) OptString(num int, v *string) {
	if v == nil {
		return
	}
	e.putLengthDelimited(num, []byte(*v))
}

// BytesField writes a raw length-delimited byte field, omitted when empty.
func (e *Encoder) BytesField(num int, v []byte) {
	if len(v) == 0 {
		return
	}
	e.putLengthDelimited(num, v)
}

// Fixed64 writes a little-endian 64-bit field, omitted when zero.
func (e *Encoder) Fixed64(num int, v uint64) {
	if v == 0 {
		return
	}
	e.putTag(num, WireFixed64)
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}

// Fixed32 writes a little-endian 32-bit field, omitted when zero.
func (e *Encoder) Fixed32(num int, v uint32) {
	if v == 0 {
		return
	}
	e.putTag(num, WireFixed32)
	e.buf = binary.LittleEndian.AppendUint32(e.buf, v)
}

// Message writes a nested message field, omitted when m is nil.
//
// The nil check covers a typed nil pointer held in the interface, which is the
// trap every `m.SubMessage` field of pointer type walks into; writing it would
// panic deep inside the nested encoder.
func (e *Encoder) Message(num int, m Message) {
	if isNilMessage(m) {
		return
	}
	sub := &Encoder{}
	m.MarshalTo(sub)
	e.putLengthDelimited(num, sub.Bytes())
}

// isNilMessage reports whether m is nil, either as a nil interface or as a nil
// pointer stored in a non-nil interface.
func isNilMessage(m Message) bool {
	if m == nil {
		return true
	}
	v := reflect.ValueOf(m)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// Field is one decoded field of a protobuf message.
type Field struct {
	// Number is the field number.
	Number int
	// Wire is the wire type of the encoding.
	Wire int
	// Varint holds varint, fixed32 and fixed64 payloads.
	Varint uint64
	// Data holds length-delimited payloads (still aliasing the input).
	Data []byte
}

// Scan walks every field of a protobuf message body, calling fn for each.
//
// Unknown fields are surfaced to fn like any other; a decoder's default branch
// simply ignores them, which is what keeps a newer producer readable by an
// older client.
func Scan(data []byte, fn func(Field) error) error {
	for len(data) > 0 {
		tag, n := consumeVarint(data)
		if n <= 0 {
			return decodeErrorf("truncated field tag")
		}
		data = data[n:]
		num := int(tag >> 3)
		wire := int(tag & 0x7)
		if num <= 0 {
			return decodeErrorf("invalid field number %d", num)
		}
		f := Field{Number: num, Wire: wire}
		switch wire {
		case WireVarint:
			v, n := consumeVarint(data)
			if n <= 0 {
				return decodeErrorf("field %d: truncated varint", num)
			}
			f.Varint, data = v, data[n:]
		case WireFixed64:
			if len(data) < 8 {
				return decodeErrorf("field %d: truncated fixed64", num)
			}
			f.Varint, data = binary.LittleEndian.Uint64(data), data[8:]
		case WireBytes:
			length, n := consumeVarint(data)
			if n <= 0 {
				return decodeErrorf("field %d: truncated length prefix", num)
			}
			data = data[n:]
			if length > uint64(len(data)) {
				return decodeErrorf("field %d: truncated payload, want %d have %d", num, length, len(data))
			}
			f.Data, data = data[:int(length)], data[int(length):]
		case WireFixed32:
			if len(data) < 4 {
				return decodeErrorf("field %d: truncated fixed32", num)
			}
			f.Varint, data = uint64(binary.LittleEndian.Uint32(data)), data[4:]
		case 3, 4:
			return decodeErrorf("field %d: groups are not supported", num)
		default:
			return decodeErrorf("field %d: unknown wire type %d", num, wire)
		}
		if err := fn(f); err != nil {
			return err
		}
	}
	return nil
}

// consumeVarint decodes the varint at the head of b, returning the value and
// the number of bytes read, or n <= 0 when b does not hold a complete varint.
func consumeVarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		v |= uint64(b[i]&0x7f) << (7 * uint(i))
		if b[i] < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

// AsInt32 reinterprets a decoded varint as a signed 32-bit value.
func (f Field) AsInt32() int32 { return int32(f.Varint) }

// AsInt64 reinterprets a decoded varint as a signed 64-bit value.
func (f Field) AsInt64() int64 { return int64(f.Varint) }

// AsBool reinterprets a decoded varint as a boolean.
func (f Field) AsBool() bool { return f.Varint != 0 }

// AsUint32 reinterprets a decoded varint as an unsigned 32-bit value.
func (f Field) AsUint32() uint32 { return uint32(f.Varint) }

// AsUint64 returns the decoded varint as an unsigned 64-bit value.
func (f Field) AsUint64() uint64 { return f.Varint }

// AsString returns the decoded length-delimited payload as a string.
func (f Field) AsString() string { return string(f.Data) }

// decodeSub decodes a length-delimited field into m. A field whose wire type
// does not match is ignored, so a malformed producer cannot make a decoder
// fail on a field it was going to skip anyway.
func decodeSub(f Field, m Message) error {
	if f.Wire != WireBytes {
		return nil
	}
	return m.Unmarshal(f.Data)
}

// decodeDecimal decodes an `optional Decimal` field into dst, allocating it
// only when the field is actually present so "unset" stays observable.
func decodeDecimal(f Field, dst **Decimal) error {
	if f.Wire != WireBytes {
		return nil
	}
	*dst = &Decimal{}
	return (*dst).Unmarshal(f.Data)
}

// decodeTime decodes an optional Timestamp field into dst.
func decodeTime(f Field, dst **Timestamp) error {
	if f.Wire != WireBytes {
		return nil
	}
	*dst = &Timestamp{}
	return (*dst).Unmarshal(f.Data)
}

// encodeOptString writes an `optional string` field, omitting it only when nil.
//
// It deliberately does not delegate to Encoder.String: that helper applies the
// implicit-presence default-omission rule, which is correct for a plain string
// field and wrong here, where a present-but-empty value is a distinct state.
func encodeOptString(e *Encoder, num int, v *string) {
	e.OptString(num, v)
}

// encodeMap writes a proto `map<string, string>` as one map-entry message per
// pair. Iteration order is unspecified; the contract never depends on it.
func encodeMap(e *Encoder, num int, m map[string]string) {
	for k, v := range m {
		entry := &Encoder{}
		entry.String(1, k)
		entry.String(2, v)
		e.putLengthDelimited(num, entry.Bytes())
	}
}

// decodeMapEntry decodes one map<string, string> entry, ignoring any other
// field number or wire type.
func decodeMapEntry(f Field, want int, dst *map[string]string) error {
	if f.Number != want || f.Wire != WireBytes {
		return nil
	}
	var k, v string
	err := Scan(f.Data, func(inner Field) error {
		switch inner.Number {
		case 1:
			k = inner.AsString()
		case 2:
			v = inner.AsString()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if *dst == nil {
		*dst = make(map[string]string)
	}
	(*dst)[k] = v
	return nil
}
