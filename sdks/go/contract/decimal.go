package contract

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Decimal is the contract's dual-representation high-precision decimal
// (common.v1.Decimal).
//
// A writer MUST populate all three fields: `unscaled * 10^-scale` is the fast
// path every host decoder trusts, and `raw_str` is the arbitrary-precision
// fallback for values whose mantissa does not fit an int64. Writing only the
// string form silently arrives as ZERO on the wire; writing only the numeric
// pair truncates anything past 19 significant digits.
//
// Use ParseDecimal (or NewDecimal) to build one safely, and DecodeDecimal when
// reading a value whose producer may have chosen either representation.
type Decimal struct {
	// Unscaled is the signed mantissa; the value is Unscaled * 10^-Scale.
	Unscaled int64
	// Scale is the number of fractional digits, never negative in practice.
	Scale int32
	// RawStr is the canonical plain-decimal text form of the same value.
	RawStr string
}

// OverflowError reports a decimal whose mantissa does not fit an int64.
//
// The contract's fast path is int64-based, so such a value cannot be written
// losslessly; callers must round it (or split it) rather than let it wrap.
type OverflowError struct {
	// Text is the offending decimal literal.
	Text string
	// Unscaled is the exact mantissa that did not fit.
	Unscaled string
	// Scale is the scale the mantissa was computed at.
	Scale int32
}

// Error implements error.
func (e *OverflowError) Error() string {
	return fmt.Sprintf("contract: decimal %q needs a %s mantissa, which exceeds int64", e.Text, e.Unscaled)
}

// NewDecimal builds a Decimal from an exact int64 mantissa and a scale.
func NewDecimal(unscaled int64, scale int32) Decimal {
	return Decimal{Unscaled: unscaled, Scale: scale, RawStr: formatPlain(unscaled, scale)}
}

// ParseDecimal builds a Decimal from a plain decimal literal such as "1.25",
// "-0.5" or "90000", populating the numeric pair and the canonical text form
// together.
//
// An optional exponent is accepted ("1.5e-3") and normalized into the
// unscaled/scale pair. An empty, malformed, non-finite or int64-overflowing
// literal returns an error instead of wrapping silently.
func ParseDecimal(text string) (Decimal, error) {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return Decimal{}, decodeErrorf("cannot interpret %q as a decimal", text)
	}
	body := raw
	neg := false
	switch body[0] {
	case '+':
		body = body[1:]
	case '-':
		neg = true
		body = body[1:]
	}

	// Optional exponent, applied by shifting the implied decimal point.
	exp := 0
	if i := strings.IndexAny(body, "eE"); i >= 0 {
		v, err := strconv.Atoi(body[i+1:])
		if err != nil {
			return Decimal{}, decodeErrorf("cannot interpret %q as a decimal: bad exponent", text)
		}
		if v < -4096 || v > 4096 {
			return Decimal{}, decodeErrorf("cannot interpret %q as a decimal: exponent out of range", text)
		}
		exp = v
		body = body[:i]
	}

	intPart, fracPart := body, ""
	if i := strings.IndexByte(body, '.'); i >= 0 {
		intPart, fracPart = body[:i], body[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return Decimal{}, decodeErrorf("cannot interpret %q as a decimal", text)
	}
	digits := intPart + fracPart
	if !isDigits(digits) {
		return Decimal{}, decodeErrorf("cannot interpret %q as a decimal", text)
	}

	// value = digits * 10^exp; a positive exponent is folded into the mantissa
	// so the scale stays non-negative (the contract's fast path rejects a
	// negative scale outright), and a negative one just widens the scale.
	unscaled, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Decimal{}, decodeErrorf("cannot interpret %q as a decimal", text)
	}
	if exp > 0 {
		unscaled.Mul(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
	}
	if neg {
		unscaled.Neg(unscaled)
	}
	scale := int64(len(fracPart))
	if exp < 0 {
		scale += int64(-exp)
	}
	if scale > math.MaxInt32 {
		return Decimal{}, decodeErrorf("decimal %q has an out-of-range scale", text)
	}
	if !unscaled.IsInt64() {
		return Decimal{}, &OverflowError{Text: text, Unscaled: unscaled.String(), Scale: int32(scale)}
	}
	return Decimal{
		Unscaled: unscaled.Int64(),
		Scale:    int32(scale),
		RawStr:   formatPlain(unscaled.Int64(), int32(scale)),
	}, nil
}

// DecodeDecimal is the counterpart of ParseDecimal for values read off the
// wire: it accepts an already-decoded message and returns the canonical form.
//
// It is a no-op for a message that is already internally consistent, and it
// repairs the other direction: a producer that wrote only `raw_str` gets its
// numeric pair filled in, and a producer that wrote only `unscaled`/`scale`
// gets its text filled in.
func DecodeDecimal(d Decimal) (Decimal, error) {
	if d.RawStr == "" {
		if d.Scale < 0 {
			return Decimal{}, decodeErrorf("decimal scale %d is negative", d.Scale)
		}
		return NewDecimal(d.Unscaled, d.Scale), nil
	}
	parsed, err := ParseDecimal(d.RawStr)
	if err != nil {
		return Decimal{}, err
	}
	return parsed, nil
}

// String returns the canonical plain-decimal text of d, deriving it from the
// numeric pair when the text form is absent.
func (d Decimal) String() string {
	if d.RawStr != "" {
		return d.RawStr
	}
	return formatPlain(d.Unscaled, d.Scale)
}

// IsZero reports whether d represents exactly zero.
//
// It reads the same representation String, Float64 and Rat prefer -- raw_str
// when it is set -- because a message can carry both forms and they can
// disagree: Decimal.Unmarshal keeps whatever the producer wrote, so
// {Unscaled: 5, Scale: 0, RawStr: "0"} is a self-inconsistent value that every
// other accessor resolves as zero. Reading the two representations
// independently is what made such a value non-zero here while Float64 reported
// 0.
//
// An unparsable raw_str falls back to the numeric pair: the text cannot answer
// the question, so the mantissa does. That reports the value as non-zero
// rather than zero, because treating an unreadable value as zero is the
// direction that hides a price.
func (d Decimal) IsZero() bool {
	if d.RawStr != "" {
		parsed, err := ParseDecimal(d.RawStr)
		if err != nil {
			return d.Unscaled == 0
		}
		return parsed.Unscaled == 0
	}
	return d.Unscaled == 0
}

// Float64 returns d as a float64.
//
// It reads both representations, because a producer populates only one: the
// mock venue, for example, fills `unscaled`/`scale` and leaves `raw_str` empty,
// so a reader that consults only the text form sees zero.
func (d Decimal) Float64() (float64, error) {
	if d.RawStr == "" {
		if d.Unscaled == 0 {
			return 0, nil
		}
		if d.Scale < 0 || d.Scale > 1024 {
			return 0, decodeErrorf("decimal scale %d is out of range", d.Scale)
		}
		v := float64(d.Unscaled) / math.Pow(10, float64(d.Scale))
		if math.IsInf(v, 0) {
			return 0, &OverflowError{Text: d.String(), Unscaled: strconv.FormatInt(d.Unscaled, 10), Scale: d.Scale}
		}
		return v, nil
	}
	v, err := strconv.ParseFloat(d.RawStr, 64)
	if err != nil {
		return 0, decodeErrorf("cannot interpret decimal %q as a float: %v", d.RawStr, err)
	}
	return v, nil
}

// MustFloat64 is Float64 without the error: a malformed or unrepresentable
// decimal reads as zero. Intended for logging and examples.
func (d Decimal) MustFloat64() float64 {
	v, err := d.Float64()
	if err != nil {
		return 0
	}
	return v
}

// Rat returns d as an exact rational number.
func (d Decimal) Rat() (*big.Rat, error) {
	if d.RawStr == "" {
		if d.Scale < 0 || d.Scale > 1024 {
			return nil, decodeErrorf("decimal scale %d is out of range", d.Scale)
		}
		r := new(big.Rat).SetInt64(d.Unscaled)
		if d.Scale == 0 {
			return r, nil
		}
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)
		return new(big.Rat).SetFrac(r.Num(), new(big.Int).Mul(r.Num(), pow)), nil
	}
	r, ok := new(big.Rat).SetString(d.RawStr)
	if !ok {
		return nil, decodeErrorf("cannot interpret decimal %q as a rational", d.RawStr)
	}
	return r, nil
}

// MarshalTo implements Message. All three fields are written, which is the
// contract's representation invariant for a writer.
func (d *Decimal) MarshalTo(e *Encoder) {
	e.Int64(1, d.Unscaled)
	e.Int32(2, d.Scale)
	e.String(3, d.RawStr)
}

// Unmarshal implements Message.
func (d *Decimal) Unmarshal(data []byte) error {
	*d = Decimal{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			d.Unscaled = f.AsInt64()
		case 2:
			d.Scale = f.AsInt32()
		case 3:
			d.RawStr = f.AsString()
		}
		return nil
	})
}

// formatPlain renders unscaled * 10^-scale in plain decimal notation, keeping
// the full scale so the text round-trips the numeric pair exactly.
func formatPlain(unscaled int64, scale int32) string {
	neg := unscaled < 0
	// Unsigned negation recovers the magnitude without overflowing on
	// math.MinInt64 (uint64(math.MinInt64) is 1<<63; negating it is 1<<63).
	mag := uint64(unscaled)
	if neg {
		mag = -mag
	}
	digits := strconv.FormatUint(mag, 10)
	if scale < 0 {
		// A negative scale means trailing zeros; the contract never produces
		// one, but keep the rendering total rather than panicking.
		digits += strings.Repeat("0", int(-int64(scale)))
		scale = 0
	}
	if int32(len(digits)) <= scale {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}
	cut := int32(len(digits)) - scale
	// Trim trailing zeros of the fraction only: "10" must not become "1".
	frac := strings.TrimRight(digits[cut:], "0")
	out := digits[:cut]
	if frac != "" {
		out += "." + frac
	}
	if neg && out != "0" {
		out = "-" + out
	}
	return out
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
