package contract

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Decimal is the contract's high-precision decimal (common.v1.Decimal).
//
// It carries exactly one field: the value in base 10. There is deliberately no
// numeric fast path beside it, because a second representation is what turned
// "which one is authoritative?" into a question every reader had to answer, and
// it is why the old int64 mantissa had to under-power the host decimal's own
// 96-bit coefficient.
//
// Presence lives on the *containing* field, never inside this message: an
// `optional Decimal` that is absent reports no value, and one that is present
// always carries a populated Value. An empty payload is therefore a contract
// violation rather than a zero, and every accessor below reports it as an error
// instead of quietly answering 0.
//
// Use ParseDecimal to build one from a literal; use Validate -- or Float64, Rat
// or IsZero, which all validate first -- to read one that arrived off the wire.
type Decimal struct {
	// Value is the number in base 10, e.g. "-0.0025" or "123.45". Never empty.
	Value string
}

// DecimalErrorKind classifies why a decimal payload was rejected.
type DecimalErrorKind int

const (
	// DecimalEmpty is a blank payload: a writer sent a Decimal it never
	// populated. It is not the value zero, and it is not "the field was absent"
	// either -- the containing field's presence already carries the latter.
	DecimalEmpty DecimalErrorKind = iota
	// DecimalNotBase10 is a payload outside the grammar the Decimal message
	// documents: a digit separator, an exponent, a leading `+`, a bare `.5` or
	// `1.`, surrounding whitespace, or too many fractional digits.
	DecimalNotBase10
	// DecimalOutOfRange is well-formed base 10 that is wider than the
	// contract's decimal can hold.
	DecimalOutOfRange
)

// DecimalError reports a decimal payload the contract does not allow.
//
// Every variant echoes the payload, so a caller can log what actually arrived
// rather than infer it from the message alone.
type DecimalError struct {
	// Kind is why the payload was rejected.
	Kind DecimalErrorKind
	// Value is the offending payload.
	Value string
	// Reason names the clause that was broken; empty for DecimalEmpty.
	Reason string
}

// Error implements error.
func (e *DecimalError) Error() string {
	switch e.Kind {
	case DecimalEmpty:
		return "contract: decimal payload is empty: a value is carried by the containing field's presence"
	case DecimalNotBase10:
		return fmt.Sprintf("contract: decimal %q is not a base-10 decimal: %s", e.Value, e.Reason)
	case DecimalOutOfRange:
		return fmt.Sprintf("contract: decimal %q is out of range for a decimal: %s", e.Value, e.Reason)
	default:
		return fmt.Sprintf("contract: decimal %q is invalid", e.Value)
	}
}

// maxDecimalScale is the number of fractional digits the contract's decimal
// keeps. A 29th digit would be accepted by a rounding parser and silently
// altered, so the grammar stops at the ceiling rather than at "some limit".
const maxDecimalScale = 28

// maxDecimalMantissa is the widest coefficient the contract's decimal holds: a
// 96-bit unsigned integer, the ceiling the host's decimal type enforces. It is
// what `Decimal::MAX` is, so the widest integer this accepts is exactly the
// widest one the host can read back.
var maxDecimalMantissa, _ = new(big.Int).SetString("79228162514264337593543950335", 10)

// ParseDecimal builds a Decimal from a decimal literal such as "1.25", "-0.5"
// or "90000".
//
// It is the validated constructor, and it is strict on purpose: every payload
// it rejects is one the host would reject too, so a bad literal surfaces here
// instead of as a rejected RPC. Rejected are a blank payload, a digit separator
// ("1_000"), an exponent ("1e3"), a leading "+", a bare ".5" or "1.",
// surrounding whitespace, more than 28 fractional digits, and a value whose
// coefficient does not fit 96 bits.
//
// The payload is carried verbatim rather than re-rendered. Trailing zeros are
// part of the value -- the host's "1.100" and "1.1" are different decimals --
// so normalizing the text would silently change what the number means.
func ParseDecimal(text string) (Decimal, error) {
	if _, err := validateDecimalText(text); err != nil {
		return Decimal{}, err
	}
	return Decimal{Value: text}, nil
}

// Validate reports whether the payload obeys the grammar the Decimal message
// documents.
//
// This is the read-path check. A Decimal decoded from the wire carries whatever
// the producer wrote, including nothing at all, so a reader that trusts it
// without validating prices an order from a message that never held a price.
func (d Decimal) Validate() error {
	_, err := validateDecimalText(d.Value)
	return err
}

// String returns the payload verbatim.
//
// It implements fmt.Stringer, so a Decimal formats as its number rather than as
// a struct. An empty payload prints as the empty string: it is not a value, and
// rendering it as "0" would assert a number nobody sent.
func (d Decimal) String() string { return d.Value }

// IsZero reports whether d is exactly zero.
//
// A payload that does not parse is reported as non-zero. That is the safe
// direction: "is this zero?" answered "yes" for a price the reader could not
// read is how a price disappears, and the caller that cares can call Validate
// to find out why.
func (d Decimal) IsZero() bool {
	r, err := d.Rat()
	return err == nil && r.Sign() == 0
}

// Float64 returns d as a float64.
//
// The payload is validated first, so a blank, out-of-grammar or too-wide
// payload is an error rather than a float. Note that a value the contract can
// carry does not always survive the conversion: 96 bits of coefficient over 28
// decimal places is exact in decimal and only rounded in binary. Use Rat when
// the number has to be exact.
func (d Decimal) Float64() (float64, error) {
	if _, err := validateDecimalText(d.Value); err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(d.Value, 64)
	if err != nil {
		return 0, decodeErrorf("cannot interpret decimal %q as a float: %v", d.Value, err)
	}
	return v, nil
}

// MustFloat64 is Float64 without the error: a decimal that does not parse reads
// as zero. Intended for logging and examples.
func (d Decimal) MustFloat64() float64 {
	v, err := d.Float64()
	if err != nil {
		return 0
	}
	return v
}

// Rat returns d as an exact rational number.
//
// Every decimal the contract can express is a rational with a power-of-ten
// denominator, so this conversion is lossless where Float64 rounds. The sign of
// a zero is not preserved, matching big.Rat.
func (d Decimal) Rat() (*big.Rat, error) {
	p, err := validateDecimalText(d.Value)
	if err != nil {
		return nil, err
	}
	mantissa, ok := new(big.Int).SetString(p.whole+p.fraction, 10)
	if !ok {
		return nil, grammarError(d.Value, "expected only digits")
	}
	if p.negative {
		mantissa.Neg(mantissa)
	}
	if !p.hasFraction {
		return new(big.Rat).SetInt(mantissa), nil
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(p.fraction))), nil)
	return new(big.Rat).SetFrac(mantissa, pow), nil
}

// MarshalTo implements Message. A Decimal has one field, so the encoding has
// one tag; an unpopulated message encodes to nothing, which is exactly the
// blank payload the contract forbids and why callers build these with
// ParseDecimal.
func (d *Decimal) MarshalTo(e *Encoder) {
	e.String(1, d.Value)
}

// Unmarshal implements Message.
func (d *Decimal) Unmarshal(data []byte) error {
	*d = Decimal{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 {
			d.Value = f.AsString()
		}
		return nil
	})
}

// decimalParts is a payload that already passed validateDecimalText, split into
// the pieces the accessors need. Keeping the split here means Rat validates and
// decomposes in one pass.
type decimalParts struct {
	negative    bool
	whole       string
	fraction    string
	hasFraction bool
}

// validateDecimalText enforces the grammar the Decimal message documents.
//
// Parsing is not validation, and neither is the host's own parser: Go's
// strconv would happily take "1e3", "+7" and " 12.5", each of which the contract
// does not define, so an unvalidated payload would reach the wire and come back
// rejected. The grammar is a subset of what strconv accepts, so this never
// rejects a value that would otherwise have decoded.
func validateDecimalText(text string) (decimalParts, error) {
	// Blank is its own case: it is the shape a message nobody populated has, so
	// it says "the writer populated nothing" rather than "the payload was
	// garbage".
	if strings.TrimSpace(text) == "" {
		return decimalParts{}, &DecimalError{Kind: DecimalEmpty, Value: text}
	}
	// Named before the digit checks so the diagnosis points at the actual
	// surprise rather than at "expected only digits".
	if strings.Contains(text, "_") {
		return decimalParts{}, grammarError(text, "digit separators are not accepted")
	}
	if strings.ContainsAny(text, "eE") {
		return decimalParts{}, grammarError(text, "exponents are not accepted")
	}
	if strings.HasPrefix(text, "+") {
		return decimalParts{}, grammarError(text, "a leading `+` is not accepted")
	}

	var p decimalParts
	body := strings.TrimPrefix(text, "-")
	p.negative = body != text
	p.whole, p.fraction, p.hasFraction = strings.Cut(body, ".")
	if p.whole == "" {
		return decimalParts{}, grammarError(text, "expected at least one digit before the decimal point")
	}
	if !isDigits(p.whole) {
		return decimalParts{}, grammarError(text, "expected only digits before the decimal point")
	}
	if p.hasFraction {
		if p.fraction == "" {
			return decimalParts{}, grammarError(text, "expected at least one digit after the decimal point")
		}
		if !isDigits(p.fraction) {
			return decimalParts{}, grammarError(text, "expected only digits after the decimal point")
		}
		if len(p.fraction) > maxDecimalScale {
			return decimalParts{}, grammarError(text, "more fractional digits than a decimal can hold")
		}
	}

	// Dropping the point leaves the coefficient, which has to fit the 96 bits
	// the contract's decimal has. This is the one check a grammar cannot make,
	// because "79228162514264337593543950336" looks like any other integer.
	mantissa, ok := new(big.Int).SetString(p.whole+p.fraction, 10)
	if !ok {
		return decimalParts{}, grammarError(text, "expected only digits")
	}
	if mantissa.Cmp(maxDecimalMantissa) > 0 {
		return decimalParts{}, &DecimalError{
			Kind:   DecimalOutOfRange,
			Value:  text,
			Reason: "the mantissa needs more than the 96 bits a decimal holds",
		}
	}
	return p, nil
}

func grammarError(text, reason string) error {
	return &DecimalError{Kind: DecimalNotBase10, Value: text, Reason: reason}
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
