package contract

import (
	"bytes"
	"math"
	"testing"
)

func TestEncoderVarintWidths(t *testing.T) {
	cases := []struct {
		name  string
		field int
		value uint64
		want  []byte
	}{
		{"small", 1, 1, []byte{0x08, 0x01}},
		{"one byte max", 1, 0x7f, []byte{0x08, 0x7f}},
		{"two bytes", 1, 0x80, []byte{0x08, 0x80, 0x01}},
		{"max field number", 2047, 1, []byte{0xf8, 0x7f, 0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Encoder{}
			e.Varint(tc.field, tc.value)
			if !bytes.Equal(e.Bytes(), tc.want) {
				t.Errorf("got % x, want % x", e.Bytes(), tc.want)
			}
		})
	}
}

// An `optional string` has explicit presence: set-to-empty and unset are
// different states, so the field must be written whenever it is set. Reusing
// the implicit-presence String helper drops a present-but-empty value, and the
// two states become indistinguishable on the wire.
func TestOptStringWritesPresentButEmpty(t *testing.T) {
	empty := ""
	unset := (*string)(nil)

	e := &Encoder{}
	e.OptString(9, &empty)
	// Field 9, wire type 2, length 0: the field is present and carries the
	// zero-length value.
	if want := []byte{0x4a, 0x00}; !bytes.Equal(e.Bytes(), want) {
		t.Errorf("OptString of a set empty string = % x, want % x", e.Bytes(), want)
	}
	e.Reset()
	e.OptString(9, unset)
	if e.Len() != 0 {
		t.Errorf("an unset optional must not be written, got % x", e.Bytes())
	}
	// A present value is written exactly as the implicit-presence helper would.
	e.Reset()
	value := "ord-1"
	e.OptString(9, &value)
	if want := []byte{0x4a, 0x05, 'o', 'r', 'd', '-', '1'}; !bytes.Equal(e.Bytes(), want) {
		t.Errorf("OptString = % x, want % x", e.Bytes(), want)
	}
}

// The end-to-end shape of the same rule, on the one explicit-presence field in
// scope: trading.v1.Position.order_id = 9. A host that populates Some("") must
// read back as Some(""), not nil.
func TestPositionOrderIDRoundTripsPresentButEmpty(t *testing.T) {
	empty := ""
	for _, tc := range []struct {
		name  string
		order *string
	}{
		{"present and empty", &empty},
		{"present and set", ptr("ord-9")},
		{"absent", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := Marshal(&Position{ID: "pos-1", OrderID: tc.order})
			var got Position
			if err := got.Unmarshal(encoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if tc.order == nil {
				if got.OrderID != nil {
					t.Errorf("OrderID = %q, want nil for an absent field", *got.OrderID)
				}
				return
			}
			if got.OrderID == nil {
				t.Fatalf("OrderID = nil, want the set value %q: presence was lost", *tc.order)
			}
			if *got.OrderID != *tc.order {
				t.Errorf("OrderID = %q, want %q", *got.OrderID, *tc.order)
			}
		})
	}
}

// A field that is genuinely absent must stay absent: this is a presence fix, not
// a change to implicit-presence default omission.
func TestPlainStringStillOmitsTheEmptyDefault(t *testing.T) {
	pos := &Position{ID: "pos-1", Symbol: "BTC/USDT"}
	var got Position
	if err := got.Unmarshal(Marshal(pos)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Symbol != "BTC/USDT" {
		t.Errorf("Symbol = %q", got.Symbol)
	}
	if got.OrderID != nil {
		t.Errorf("OrderID = %q, want nil", *got.OrderID)
	}
}

func ptr(s string) *string { return &s }

func TestEncoderOmitsProto3Defaults(t *testing.T) {
	e := &Encoder{}
	e.Int64(1, 0)
	e.Int32(2, 0)
	e.Uint64(3, 0)
	e.Uint32(4, 0)
	e.Bool(5, false)
	e.String(6, "")
	e.BytesField(7, nil)
	e.Fixed64(8, 0)
	e.Fixed32(9, 0)
	e.Message(10, nil)
	if e.Len() != 0 {
		t.Errorf("proto3 defaults must not be written, got % x", e.Bytes())
	}
}

func TestEncoderNegativeIntIsSignExtended(t *testing.T) {
	e := &Encoder{}
	e.Int32(1, -1)
	got := e.Bytes()
	// An int32 negative is a 10-byte sign-extended varint; truncating it to
	// one byte would decode as 1 on a strict reader.
	if len(got) != 11 {
		t.Fatalf("int32 -1 encoded in %d bytes, want 11 (% x)", len(got), got)
	}
	if got[10] != 0x01 {
		t.Errorf("sign extension lost: % x", got)
	}
}

func TestEncoderFixedFieldsAreLittleEndian(t *testing.T) {
	e := &Encoder{}
	e.Fixed64(1, 0x0102030405060708)
	if got := e.Bytes(); !bytes.Equal(got[1:], []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}) {
		t.Errorf("fixed64 must be little-endian, got % x", got)
	}
	e.Reset()
	e.Fixed32(1, 0x01020304)
	if got := e.Bytes(); !bytes.Equal(got[1:], []byte{0x04, 0x03, 0x02, 0x01}) {
		t.Errorf("fixed32 must be little-endian, got % x", got)
	}
}

func TestScanAllWireTypes(t *testing.T) {
	e := &Encoder{}
	e.Varint(1, 42)
	e.Fixed64(2, 7)
	e.String(3, "hello")
	e.Fixed32(4, 9)
	want := []Field{
		{Number: 1, Wire: WireVarint, Varint: 42},
		{Number: 2, Wire: WireFixed64, Varint: 7},
		{Number: 3, Wire: WireBytes, Data: []byte("hello")},
		{Number: 4, Wire: WireFixed32, Varint: 9},
	}
	var got []Field
	if err := Scan(e.Bytes(), func(f Field) error {
		got = append(got, f)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d fields, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Number != want[i].Number || got[i].Wire != want[i].Wire || got[i].Varint != want[i].Varint {
			t.Errorf("field %d = %+v, want %+v", i, got[i], want[i])
		}
		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Errorf("field %d data = %q, want %q", i, got[i].Data, want[i].Data)
		}
	}
}

func TestScanSkipsUnknownFields(t *testing.T) {
	// A message written by a newer producer: known fields plus fields this
	// build has never heard of, in every wire type.
	e := &Encoder{}
	e.Message(1, &ExchangeId{ID: "BTC/USDT"})
	e.String(99, "future-field")
	e.Varint(100, 12345)
	e.Fixed64(101, 3)
	e.Fixed32(102, 4)
	e.BytesField(103, []byte{0xde, 0xad})
	e.String(2, "")

	var req GetPositionsRequest
	if err := req.Unmarshal(e.Bytes()); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if req.ExchangeID == nil || req.ExchangeID.ID != "BTC/USDT" {
		t.Errorf("known fields were lost: %+v", req)
	}
	if len(req.Symbols) != 0 {
		t.Errorf("an empty repeated field must stay empty, got %q", req.Symbols)
	}
}

// Bytes that are not valid protobuf must not panic a decoder, and must not be
// mistaken for known fields: raw text scanned as a message hits a bogus
// length prefix and must report a decode error rather than yield junk.
func TestScanTextAsNestedMessage(t *testing.T) {
	var id ExchangeId
	if err := id.Unmarshal([]byte("BTC/USDT")); err == nil {
		t.Fatalf("scanning %q as a message must fail, got %+v", "BTC/USDT", id)
	}
}

func TestScanRejectsMalformedInput(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"truncated tag", []byte{0x80}},
		{"truncated varint", []byte{0x08}},
		{"truncated length prefix", []byte{0x1a, 0x80}},
		{"truncated payload", []byte{0x1a, 0x05, 'a', 'b'}},
		{"truncated fixed64", []byte{0x09, 0x01, 0x02}},
		{"truncated fixed32", []byte{0x0d, 0x01}},
		{"start group", []byte{0x0b, 0x0c}},
		{"unknown wire type", []byte{0x0e, 0x00}},
		{"field number zero", []byte{0x00, 0x01}},
		{"unterminated varint", []byte{0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if err = Scan(tc.in, func(Field) error { return nil }); err == nil {
				t.Fatalf("Scan(% x) accepted malformed input", tc.in)
			}
			var decodeErr *DecodeError
			if !asDecodeError(err, &decodeErr) {
				t.Errorf("error = %T (%v), want *DecodeError", err, err)
			}
		})
	}
}

func TestScanPropagatesCallbackError(t *testing.T) {
	sentinel := errSentinel{}
	err := Scan([]byte{0x08, 0x01}, func(Field) error { return sentinel })
	if err != sentinel {
		t.Fatalf("Scan error = %v, want the callback error", err)
	}
}

func TestFieldAccessors(t *testing.T) {
	e := &Encoder{}
	e.Int64(1, -5)
	e.Varint(2, math.MaxUint64)
	e.String(3, "x")
	f := []Field{}
	if err := Scan(e.Bytes(), func(g Field) error {
		f = append(f, g)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if f[0].AsInt64() != -5 {
		t.Errorf("AsInt64 = %d, want -5", f[0].AsInt64())
	}
	if f[0].AsInt32() != -5 {
		t.Errorf("AsInt32 = %d, want -5", f[0].AsInt32())
	}
	if f[1].AsUint64() != math.MaxUint64 {
		t.Errorf("AsUint64 = %d", f[1].AsUint64())
	}
	if !f[0].AsBool() {
		t.Error("a non-zero varint must read as true")
	}
	if f[2].AsString() != "x" {
		t.Errorf("AsString = %q", f[2].AsString())
	}
}

type errSentinel struct{}

func (errSentinel) Error() string { return "sentinel" }

func asDecodeError(err error, target **DecodeError) bool {
	de, ok := err.(*DecodeError)
	if ok {
		*target = de
	}
	return ok
}
