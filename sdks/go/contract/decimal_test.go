package contract

import (
	"errors"
	"strings"
	"testing"
)

func TestParseDecimalRoundTrip(t *testing.T) {
	// The payload is carried verbatim, so every literal below must come back
	// exactly as written -- trailing zeros included.
	cases := []string{
		"1.25",
		"0.001",
		"90000",
		"-0.5",
		"0",
		// Trailing zeros are part of the value: the host's "0.000" and "0" are
		// different decimals, so re-rendering would change what it means.
		"0.000",
		"1.100",
		"-0.0000000000000000000000000001", // 28 fractional digits, the ceiling
		"90000.25",
		// The widest values the contract can carry: a 96-bit coefficient with
		// no fraction, and the same coefficient split by a decimal point.
		"79228162514264337593543950335",
		"7922816251426433759354395033.5",
		"-79228162514264337593543950335",
	}
	for _, in := range cases {
		got, err := ParseDecimal(in)
		if err != nil {
			t.Errorf("ParseDecimal(%q): unexpected error %v", in, err)
			continue
		}
		if got.Value != in {
			t.Errorf("ParseDecimal(%q).Value = %q, want the payload verbatim", in, got.Value)
		}
		if got.String() != in {
			t.Errorf("ParseDecimal(%q).String() = %q, want %q", in, got.String(), in)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("ParseDecimal(%q) does not validate: %v", in, err)
		}
	}
}

// Trailing zeros are significant to the host's decimal, so ParseDecimal must
// not normalize them away: "1.100" and "1.1" decode to different values there.
func TestParseDecimalPreservesTrailingZeros(t *testing.T) {
	a, err := ParseDecimal("1.100")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	b, err := ParseDecimal("1.1")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	if a.Value == b.Value {
		t.Fatal("normalizing trailing zeros would make two different decimals equal")
	}
	ra, err := a.Rat()
	if err != nil {
		t.Fatalf("Rat: %v", err)
	}
	if ra.RatString() != "11/10" {
		t.Errorf("Rat(1.100) = %s, want 11/10", ra.RatString())
	}
}

func TestParseDecimalRejectsGarbage(t *testing.T) {
	cases := []struct {
		in     string
		reason string
	}{
		{"abc", "expected only digits before the decimal point"},
		{"1_000", "digit separators are not accepted"},
		{"1e3", "exponents are not accepted"},
		{"1E3", "exponents are not accepted"},
		{"1.5e-3", "exponents are not accepted"},
		{"1e", "exponents are not accepted"},
		{"+7", "a leading `+` is not accepted"},
		{".5", "expected at least one digit before the decimal point"},
		{"1.", "expected at least one digit after the decimal point"},
		{"-", "expected at least one digit before the decimal point"},
		{".", "expected at least one digit before the decimal point"},
		{"1.2.3", "expected only digits after the decimal point"},
		{"1,25", "expected only digits before the decimal point"},
		{"NaN", "expected only digits before the decimal point"},
		{"Infinity", "expected only digits before the decimal point"},
		{"0x10", "expected only digits before the decimal point"},
		// Surrounding whitespace is not trimmed away: the contract defines no
		// whitespace, and a payload that differs only in padding is a payload
		// the host will reject.
		{" 1", "expected only digits before the decimal point"},
		{"1 ", "expected only digits before the decimal point"},
		{" 12.50 ", "expected only digits before the decimal point"},
		{"0." + strings.Repeat("0", 29), "more fractional digits than a decimal can hold"},
	}
	for _, tc := range cases {
		got, err := ParseDecimal(tc.in)
		if err == nil {
			t.Errorf("ParseDecimal(%q) = %+v, want an error", tc.in, got)
			continue
		}
		var decErr *DecimalError
		if !errors.As(err, &decErr) {
			t.Errorf("ParseDecimal(%q) error = %T (%v), want *DecimalError", tc.in, err, err)
			continue
		}
		if decErr.Kind != DecimalNotBase10 {
			t.Errorf("ParseDecimal(%q) kind = %v, want DecimalNotBase10", tc.in, decErr.Kind)
		}
		if decErr.Reason != tc.reason {
			t.Errorf("ParseDecimal(%q) reason = %q, want %q", tc.in, decErr.Reason, tc.reason)
		}
		if decErr.Value != tc.in {
			t.Errorf("ParseDecimal(%q) echoed %q; the caller cannot log what arrived", tc.in, decErr.Value)
		}
	}
}

// A blank payload is the shape a message nobody populated has, so it is its own
// error rather than a grammar violation: it says "the writer populated nothing",
// not "the payload was garbage". Presence lives on the containing field, so it
// is never a zero.
func TestParseDecimalRejectsBlankAsEmpty(t *testing.T) {
	for _, in := range []string{"", "   ", "\t\n"} {
		got, err := ParseDecimal(in)
		if err == nil {
			t.Errorf("ParseDecimal(%q) = %+v, want an error", in, got)
			continue
		}
		var decErr *DecimalError
		if !errors.As(err, &decErr) {
			t.Errorf("ParseDecimal(%q) error = %T, want *DecimalError", in, err)
			continue
		}
		if decErr.Kind != DecimalEmpty {
			t.Errorf("ParseDecimal(%q) kind = %v, want DecimalEmpty", in, decErr.Kind)
		}
	}
}

// Well-formed base 10 that is too wide is a distinct case from a grammar
// violation: the payload looks like any other integer, so only the range check
// can catch it.
func TestParseDecimalOutOfRange(t *testing.T) {
	cases := []string{
		"79228162514264337593543950336", // 2^96, one past the coefficient
		"100000000000000000000000000000",
		"-79228162514264337593543950336",
		"7922816251426433759354395033.6", // the point moves, the width does not shrink
		"12345678901234567890123456789012345",
	}
	for _, in := range cases {
		got, err := ParseDecimal(in)
		if err == nil {
			t.Errorf("ParseDecimal(%q) = %+v, want an error", in, got)
			continue
		}
		var decErr *DecimalError
		if !errors.As(err, &decErr) {
			t.Errorf("ParseDecimal(%q) error = %T (%v), want *DecimalError", in, err, err)
			continue
		}
		if decErr.Kind != DecimalOutOfRange {
			t.Errorf("ParseDecimal(%q) kind = %v, want DecimalOutOfRange", in, decErr.Kind)
		}
	}
}

// Silently returning a value alongside an error is how a rejected literal turns
// into a priced order.
func TestParseDecimalReturnsNothingOnError(t *testing.T) {
	for _, in := range []string{"", "abc", "1e3", "79228162514264337593543950336"} {
		got, err := ParseDecimal(in)
		if err == nil {
			t.Fatalf("ParseDecimal(%q) unexpectedly succeeded", in)
		}
		if got.Value != "" {
			t.Errorf("ParseDecimal(%q) returned %+v on error", in, got)
		}
	}
}

func TestDecimalMarshalWritesOnlyTheValue(t *testing.T) {
	d, err := ParseDecimal("1.25")
	if err != nil {
		t.Fatalf("ParseDecimal: %v", err)
	}
	// 0a 04 "1.25": field 1, length-delimited, and nothing else.
	want := []byte{0x0a, 0x04, '1', '.', '2', '5'}
	got := Marshal(&d)
	if string(got) != string(want) {
		t.Fatalf("Marshal(1.25) = % x, want % x", got, want)
	}

	// One field on the wire, and it is the text: there is no numeric companion
	// a host could prefer over it.
	var fields []int
	if err := Scan(got, func(f Field) error {
		fields = append(fields, f.Number)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(fields) != 1 || fields[0] != 1 {
		t.Errorf("decimal encoding carries fields %v, want exactly field 1", fields)
	}
}

// An unpopulated message has one encoding: no bytes at all, which decodes back
// to the blank payload the contract forbids. That is why every writer here goes
// through ParseDecimal.
func TestDecimalMarshalOfTheZeroValueEncodesToNothing(t *testing.T) {
	if got := Marshal(&Decimal{}); len(got) != 0 {
		t.Errorf("Marshal(Decimal{}) = % x, want no bytes", got)
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

// Unknown fields are skipped, so a newer producer stays readable. The retired
// numeric pair lived in fields 1 and 2 and the retired text in field 3; none of
// them is a fallback, so a message written in the old shape decodes to the
// blank payload -- which every accessor rejects rather than answering zero.
func TestDecimalUnmarshalSkipsFieldsItDoesNotKnow(t *testing.T) {
	retired := map[string][]byte{
		// 08 02 | 10 02: the numeric pair (unscaled 2, scale 2).
		"numeric pair only": {0x08, 0x02, 0x10, 0x02},
		// 1a 03 "1.5": the text form, in the field it used to live in.
		"text only": {0x1a, 0x03, '1', '.', '5'},
	}
	for name, wire := range retired {
		var d Decimal
		if err := d.Unmarshal(wire); err != nil {
			t.Errorf("%s: Unmarshal: %v", name, err)
			continue
		}
		if d.Value != "" {
			t.Errorf("%s: value = %q; a payload in a retired field must not be read", name, d.Value)
			continue
		}
		var decErr *DecimalError
		if err := d.Validate(); !errors.As(err, &decErr) || decErr.Kind != DecimalEmpty {
			t.Errorf("%s: Validate = %v, want a DecimalEmpty", name, err)
		}
		if d.IsZero() {
			t.Errorf("%s: an empty payload must not report IsZero", name)
		}
	}

	var ok Decimal
	if err := ok.Unmarshal([]byte{0x0a, 0x03, '1', '.', '5'}); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if ok.Value != "1.5" {
		t.Errorf("value = %q, want %q", ok.Value, "1.5")
	}
}

func TestDecimalStringIsThePayload(t *testing.T) {
	cases := []string{"1.25", "-0.5", "0.000", "90000"}
	for _, in := range cases {
		d, err := ParseDecimal(in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", in, err)
		}
		if got := d.String(); got != in {
			t.Errorf("Decimal(%q).String() = %q", in, got)
		}
	}
	// The blank payload prints as blank: it is not a value, and rendering it
	// as "0" would assert a number nobody sent.
	if got := (Decimal{}).String(); got != "" {
		t.Errorf("Decimal{}.String() = %q, want the empty payload", got)
	}
}

func TestDecimalIsZero(t *testing.T) {
	zeros := []string{"0", "-0", "0.000", "-0.0000"}
	for _, in := range zeros {
		d, err := ParseDecimal(in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", in, err)
		}
		if !d.IsZero() {
			t.Errorf("IsZero = false for %q", in)
		}
	}
	nonZero := []string{"0.001", "-0.5", "90000", "0.0000000000000000000000000001"}
	for _, in := range nonZero {
		d, err := ParseDecimal(in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", in, err)
		}
		if d.IsZero() {
			t.Errorf("IsZero = true for %q", in)
		}
	}
	// A payload the reader cannot parse is not known to be zero, and reporting
	// it as zero is the direction that hides a price.
	for _, d := range []Decimal{{}, {Value: "nonsense"}, {Value: "1e3"}, {Value: " 1"}} {
		if d.IsZero() {
			t.Errorf("IsZero = true for the unreadable payload %q", d.Value)
		}
	}
}

func TestDecimalFloat64(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"64000.25", 64000.25},
		{"-0.5", -0.5},
		{"0", 0},
		{"0.001", 0.001},
		{"90000", 90000},
	}
	for _, tc := range cases {
		d, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", tc.in, err)
		}
		got, err := d.Float64()
		if err != nil {
			t.Errorf("Float64(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Float64(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// Every accessor validates first, so a payload the host would reject cannot be
// read as a number -- and least of all as zero.
func TestDecimalFloat64RejectsUnusablePayloads(t *testing.T) {
	for _, in := range []string{"", "   ", "abc", "1e3", "+7", ".5", "1.", " 1", "79228162514264337593543950336"} {
		d := Decimal{Value: in}
		got, err := d.Float64()
		if err == nil {
			t.Errorf("Float64(%q) = %v, want an error", in, got)
		}
		if got != 0 {
			t.Errorf("Float64(%q) returned %v alongside an error", in, got)
		}
	}
	// MustFloat64 is the deliberate exception, and it is for logs only.
	if got := (Decimal{Value: "abc"}).MustFloat64(); got != 0 {
		t.Errorf("MustFloat64 = %v, want 0", got)
	}
}

func TestDecimalRatIsExact(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0.001", "1/1000"},
		{"1.5", "3/2"},
		{"-0.5", "-1/2"},
		{"0", "0"},
		{"90000", "90000"},
		{"123.456", "15432/125"},
		// Wider than any fixed-width integer, and still exact here: this is the
		// accessor that survives what Float64 rounds.
		{"1234567890123456789012345678", "1234567890123456789012345678"},
		{"7922816251426433759354395033.5", "15845632502852867518708790067/2"},
	}
	for _, tc := range cases {
		d, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("ParseDecimal(%q): %v", tc.in, err)
		}
		r, err := d.Rat()
		if err != nil {
			t.Errorf("Rat(%q): %v", tc.in, err)
			continue
		}
		if got := r.RatString(); got != tc.want {
			t.Errorf("Rat(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{"", "abc", "1e3", "79228162514264337593543950336"} {
		if r, err := (Decimal{Value: in}).Rat(); err == nil {
			t.Errorf("Rat(%q) = %v, want an error", in, r)
		}
	}
}

func TestDecimalValidateMatchesTheAccessors(t *testing.T) {
	// A payload that Validate accepts is one every accessor can read, and one
	// it rejects is one none of them will answer for.
	for _, in := range []string{"0", "1.25", "-79228162514264337593543950335", "0." + strings.Repeat("0", 27) + "1"} {
		d := Decimal{Value: in}
		if err := d.Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", in, err)
		}
		if _, err := d.Float64(); err != nil {
			t.Errorf("Float64(%q) = %v, want nil", in, err)
		}
		if _, err := d.Rat(); err != nil {
			t.Errorf("Rat(%q) = %v, want nil", in, err)
		}
	}
	for _, in := range []string{"", "abc", "1e3", "79228162514264337593543950336"} {
		d := Decimal{Value: in}
		if err := d.Validate(); err == nil {
			t.Errorf("Validate(%q) = nil, want an error", in)
		}
	}
}

func TestDecimalErrorNamesThePayloadAndTheLimit(t *testing.T) {
	cases := []struct {
		in     string
		reason string
	}{
		{"abc", "expected only digits before the decimal point"},
		{"79228162514264337593543950336", "96 bits"},
	}
	for _, tc := range cases {
		_, err := ParseDecimal(tc.in)
		if err == nil {
			t.Fatalf("ParseDecimal(%q): expected an error", tc.in)
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.in) || !strings.Contains(msg, tc.reason) {
			t.Errorf("error %q should name the payload %q and %q", msg, tc.in, tc.reason)
		}
	}
	// The blank payload has no literal worth echoing, so it says what is wrong
	// instead: the writer populated nothing.
	_, err := ParseDecimal("")
	if err == nil {
		t.Fatal("ParseDecimal(\"\") unexpectedly succeeded")
	}
	if msg := err.Error(); !strings.Contains(msg, "empty") {
		t.Errorf("error %q should say the payload is empty", msg)
	}
}
