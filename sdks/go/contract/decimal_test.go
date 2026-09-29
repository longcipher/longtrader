package contract

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestParseDecimalRoundTrip(t *testing.T) {
	cases := []struct {
		in       string
		unscaled int64
		scale    int32
		raw      string
	}{
		{"1.25", 125, 2, "1.25"},
		{"0.001", 1, 3, "0.001"},
		{"90000", 90000, 0, "90000"},
		{"-0.5", -5, 1, "-0.5"},
		{"0", 0, 0, "0"},
		{"0.000", 0, 3, "0"},
		{"-0.0000000000000000000000000001", -1, 28, "-0.0000000000000000000000000001"},
		{"90000.25", 9000025, 2, "90000.25"},
		// An exponent is normalized into the unscaled/scale pair rather than
		// rejected, so callers can hand over a computed value verbatim.
		{"1e3", 1000, 0, "1000"},
		{"1.5e-3", 15, 4, "0.0015"},
		{"-2.5E2", -2500, 1, "-250"},
		{"+7", 7, 0, "7"},
		{" 12.50 ", 1250, 2, "12.5"},
		// The most negative int64 mantissa is representable: the fast path
		// must not reject it, and formatting must not overflow.
		{"-92233720368547758.08", math.MinInt64, 2, "-92233720368547758.08"},
	}
	for _, tc := range cases {
		got, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): unexpected error %v", tc.in, err)
		}
		if got.Unscaled != tc.unscaled || got.Scale != tc.scale {
			t.Errorf("ParseDecimal(%q) = unscaled %d scale %d, want %d/%d",
				tc.in, got.Unscaled, got.Scale, tc.unscaled, tc.scale)
		}
		if got.RawStr != tc.raw {
			t.Errorf("ParseDecimal(%q).RawStr = %q, want %q", tc.in, got.RawStr, tc.raw)
		}
		if got.String() != tc.raw {
			t.Errorf("ParseDecimal(%q).String() = %q, want %q", tc.in, got.String(), tc.raw)
		}
		// The decoded value must be numerically the input.
		want, _ := ParseDecimal(tc.in)
		if !decimalsEqual(got, want) {
			t.Errorf("ParseDecimal(%q) is not self-consistent: %+v", tc.in, got)
		}
	}
}

func TestParseDecimalOverflow(t *testing.T) {
	cases := []string{
		"92233720368547758.08",                // 2^63, one past the max mantissa
		"-92233720368547758.09",               // one past the min mantissa
		"79228162514264337593543950335",       // 2^96-1, the rust_decimal ceiling
		"12345678901234567890123456789012345", // far past any fixed-width mantissa
	}
	for _, in := range cases {
		got, err := ParseDecimal(in)
		if err == nil {
			t.Fatalf("ParseDecimal(%q) = %+v, want an error", in, got)
		}
		var overflow *OverflowError
		if !errors.As(err, &overflow) {
			t.Fatalf("ParseDecimal(%q) error = %T (%v), want *OverflowError", in, err, err)
		}
		// Silently wrapping is the failure this guards: a wrapped mantissa
		// would price an order in the wrong universe.
		if got.Unscaled != 0 || got.Scale != 0 {
			t.Errorf("ParseDecimal(%q) returned a value on error: %+v", in, got)
		}
	}
}

func TestParseDecimalRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "   ", "-", ".", "abc", "1.2.3", "1,25", "1e", "1e+", "NaN", "Infinity", "0x10", "1 000"} {
		if got, err := ParseDecimal(in); err == nil {
			t.Errorf("ParseDecimal(%q) = %+v, want an error", in, got)
		}
	}
}

func TestDecimalMarshalWritesAllThreeFields(t *testing.T) {
	d, err := ParseDecimal("1.25")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	// 08 7d | 10 02 | 1a 04 "1.25"
	want := []byte{0x08, 0x7d, 0x10, 0x02, 0x1a, 0x04, '1', '.', '2', '5'}
	got := Marshal(&d)
	if string(got) != string(want) {
		t.Fatalf("Marshal(1.25) = % x, want % x", got, want)
	}

	// The representation invariant: the numeric pair must be on the wire, not
	// just the human-readable fallback, because the host decoder trusts it.
	var fields []int
	if err := Scan(got, func(f Field) error {
		fields = append(fields, f.Number)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, want := range []int{1, 2, 3} {
		found := false
		for _, got := range fields {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("decimal encoding is missing field %d; the host would decode %+v as zero", want, d)
		}
	}
}

func TestDecimalUnmarshalRoundTrip(t *testing.T) {
	original, err := ParseDecimal("-0.001")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	wire := Marshal(&original)
	var decoded Decimal
	if err := decoded.Unmarshal(wire); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded != original {
		t.Errorf("round trip = %+v, want %+v", decoded, original)
	}
}

// A producer that fills only unscaled/scale -- which is what the Rust host
// does whenever the mantissa fits -- must still read back correctly.
func TestDecimalUnmarshalNumericOnly(t *testing.T) {
	var d Decimal
	if err := d.Unmarshal([]byte{0x08, 0x02, 0x10, 0x02}); err != nil { // unscaled=2 scale=2
		t.Fatalf("Unmarshal: %v", err)
	}
	if d.RawStr != "" {
		t.Fatalf("expected an empty raw_str, got %q", d.RawStr)
	}
	if got, err := d.Float64(); err != nil || got != 0.02 {
		t.Errorf("Float64 = %v, %v; want 0.02", got, err)
	}
	if d.String() != "0.02" {
		t.Errorf("String = %q, want %q", d.String(), "0.02")
	}
}

// The mirror image: a producer that only filled raw_str must read back too.
func TestDecimalUnmarshalRawStrOnly(t *testing.T) {
	var d Decimal
	// Field 3 (raw_str) = "1.5"; the numeric pair stays at zero.
	if err := d.Unmarshal([]byte{0x1a, 0x03, '1', '.', '5'}); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if d.Unscaled != 0 || d.Scale != 0 {
		t.Fatalf("expected the numeric pair to stay zero, got %+v", d)
	}
	if got, err := d.Float64(); err != nil || got != 1.5 {
		t.Errorf("Float64 = %v, %v; want 1.5", got, err)
	}
	// DecodeDecimal repairs the pair so downstream code never has to branch.
	repaired, err := DecodeDecimal(d)
	if err != nil {
		t.Fatalf("DecodeDecimal: %v", err)
	}
	if repaired.Unscaled != 15 || repaired.Scale != 1 {
		t.Errorf("DecodeDecimal = %+v, want unscaled 15 scale 1", repaired)
	}
}

func TestDecodeDecimalRepairsMissingRawStr(t *testing.T) {
	got, err := DecodeDecimal(Decimal{Unscaled: 90000, Scale: 0})
	if err != nil {
		t.Fatalf("DecodeDecimal: %v", err)
	}
	if got.RawStr != "90000" {
		t.Errorf("RawStr = %q, want %q", got.RawStr, "90000")
	}
}

func TestDecimalStringFormatting(t *testing.T) {
	cases := []struct {
		unscaled int64
		scale    int32
		want     string
	}{
		{10, 0, "10"},
		{100, 2, "1"},
		{0, 0, "0"},
		{0, 3, "0"},
		{1, 3, "0.001"},
		{-5, 1, "-0.5"},
		{1, 0, "1"},
		{-1, 0, "-1"},
		{math.MaxInt64, 0, "9223372036854775807"},
		{math.MinInt64, 0, "-9223372036854775808"},
		{123, -2, "12300"},
	}
	for _, tc := range cases {
		if got := NewDecimal(tc.unscaled, tc.scale).String(); got != tc.want {
			t.Errorf("NewDecimal(%d, %d).String() = %q, want %q", tc.unscaled, tc.scale, got, tc.want)
		}
	}
}

// Unmarshal keeps whatever the producer wrote, so a self-inconsistent message
// is reachable from the wire. IsZero must agree with the accessors that read
// raw_str in preference to the numeric pair, or "is this zero?" and "what is
// its value?" give different answers on the same value.
func TestDecimalIsZeroAgreesWithItsSiblings(t *testing.T) {
	zeroByText := Decimal{Unscaled: 5, Scale: 0, RawStr: "0"}
	if !zeroByText.IsZero() {
		t.Errorf("IsZero = false for %+v, which every other accessor reads as zero", zeroByText)
	}
	if v := zeroByText.MustFloat64(); v != 0 {
		t.Errorf("Float64 = %v, so IsZero must agree", v)
	}
	// The mirror image: the text form is authoritative, so a non-zero
	// raw_str is non-zero even when the mantissa is zero.
	nonZeroByText := Decimal{Unscaled: 0, Scale: 0, RawStr: "5"}
	if nonZeroByText.IsZero() {
		t.Errorf("IsZero = true for %+v, whose raw_str is 5", nonZeroByText)
	}
	if v := nonZeroByText.MustFloat64(); v == 0 {
		t.Error("Float64 = 0, so IsZero must agree that this is non-zero")
	}
	// An unparsable text form falls back to the numeric pair: the text cannot
	// answer the question, so the mantissa does rather than the value being
	// called zero because its text was unreadable.
	if (Decimal{Unscaled: 7, RawStr: "nonsense"}).IsZero() {
		t.Error("an unreadable raw_str must fall back to unscaled, which is 7")
	}
	if !(Decimal{RawStr: "nonsense"}).IsZero() {
		t.Error("an unreadable raw_str with a zero mantissa must read as zero")
	}
}

func TestDecimalIsZero(t *testing.T) {
	if !(Decimal{}).IsZero() {
		t.Error("the zero Decimal must report IsZero")
	}
	if !(Decimal{RawStr: "0"}).IsZero() {
		t.Error("a raw_str zero must report IsZero")
	}
	if (Decimal{Unscaled: 1, Scale: 3}).IsZero() {
		t.Error("0.001 must not report IsZero")
	}
}

func TestDecimalFloat64ReadsBothRepresentations(t *testing.T) {
	cases := []struct {
		name string
		in   Decimal
		want float64
	}{
		{"raw_str only", Decimal{RawStr: "90000.25"}, 90000.25},
		{"numeric only", Decimal{Unscaled: 9000025, Scale: 2}, 90000.25},
		{"negative numeric only", Decimal{Unscaled: -5, Scale: 1}, -0.5},
		{"zero", Decimal{}, 0},
	}
	for _, tc := range cases {
		got, err := tc.in.Float64()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: Float64 = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDecimalFloat64RejectsBadScale(t *testing.T) {
	if _, err := (Decimal{Unscaled: 1, Scale: -3}).Float64(); err == nil {
		t.Error("a negative scale must be rejected rather than silently zeroed")
	}
	if _, err := (Decimal{RawStr: "not-a-number"}).Float64(); err == nil {
		t.Error("a malformed raw_str must be rejected")
	}
}

func TestDecimalRatIsExact(t *testing.T) {
	r, err := Decimal{Unscaled: 1, Scale: 3}.Rat()
	if err != nil {
		t.Fatalf("Rat: %v", err)
	}
	if r.RatString() != "1/1000" {
		t.Errorf("Rat = %s, want 1/1000", r.RatString())
	}
	if _, err := (Decimal{RawStr: "nope"}).Rat(); err == nil {
		t.Error("a malformed raw_str must be rejected")
	}
}

func TestOverflowErrorMessageNamesTheMantissa(t *testing.T) {
	_, err := ParseDecimal("92233720368547758.08")
	if err == nil {
		t.Fatal("expected an overflow error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "92233720368547758.08") || !strings.Contains(msg, "int64") {
		t.Errorf("error %q should name the literal and the limit", msg)
	}
}

func decimalsEqual(a, b Decimal) bool {
	if a.Unscaled == b.Unscaled && a.Scale == b.Scale {
		return true
	}
	ra, err := a.Rat()
	if err != nil {
		return false
	}
	rb, err := b.Rat()
	if err != nil {
		return false
	}
	return ra.Cmp(rb) == 0
}
