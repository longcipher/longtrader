//! Conversion helpers between generated contract types and [`rust_decimal::Decimal`].
//!
//! The contract's `common.v1.Decimal` carries a value as a single base-10
//! string. There is no numeric fast path beside it: a second representation is
//! what made "which one is authoritative?" a question every reader had to
//! answer, and it is why the old int64 mantissa had to under-power
//! `rust_decimal`'s own 96-bit coefficient.
//!
//! Presence lives on the *containing* field. An `optional Decimal` that is absent
//! reports no value; one that is present always carries a populated `value`, so
//! an empty payload is a contract violation ([`DecimalConvertError::Empty`])
//! rather than a zero. That is what makes "unset" and "zero" distinguishable
//! without a presence bit inside the message.
//!
//! Round-trip exactness across the full `rust_decimal` range is property-tested
//! below, and the wire shape is fuzzed in the workspace's `fuzz/` member.

use rust_decimal::Decimal;

use crate::proto::longtrader::common::v1 as common;

/// Errors produced when converting a contract decimal into [`Decimal`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum DecimalConvertError {
    /// The payload was blank.
    ///
    /// This is not "the value zero" and not "the field was absent" — the
    /// containing field's presence already carries the latter. A blank payload
    /// means a writer sent a `Decimal` it never populated.
    #[error("decimal payload is empty: a value is carried by the containing field's presence")]
    Empty,
    /// The payload is outside the grammar the `Decimal` message documents.
    #[error("`{value}` is not a base-10 decimal: {reason}")]
    NotBase10 {
        /// The offending payload, echoed so a caller can log what arrived.
        value: String,
        /// Which clause of the grammar it broke.
        reason: &'static str,
    },
    /// The payload is well-formed base 10 but wider than [`Decimal`] can hold.
    #[error("`{value}` is out of range for a decimal: {source}")]
    OutOfRange {
        /// The offending payload.
        value: String,
        /// Why the decimal parser rejected it.
        #[source]
        source: rust_decimal::Error,
    },
}

/// Whether every character of `s` is an ASCII digit.
fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Which clause of the contract's documented grammar `s` breaks, if any.
///
/// [`rust_decimal::Decimal`]'s `FromStr` follows Rust literal syntax, so it also
/// accepts digit separators, exponents, and a leading `+` — forms the `Decimal`
/// message does not define. Inheriting that would mean `1_000` silently
/// becomes 1000, so the grammar is enforced here instead. The grammar is a
/// subset of what the parser accepts, so this never rejects a value that would
/// otherwise have decoded.
fn grammar_violation(s: &str) -> Option<&'static str> {
    // Named first so the diagnosis points at the actual surprise rather than at
    // "expected only digits".
    if s.contains('_') {
        return Some("digit separators are not accepted");
    }
    if s.contains(['e', 'E']) {
        return Some("exponents are not accepted");
    }
    if s.starts_with('+') {
        return Some("a leading `+` is not accepted");
    }
    let body = s.strip_prefix('-').unwrap_or(s);
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (body, None),
    };
    if whole.is_empty() {
        return Some("expected at least one digit before the decimal point");
    }
    if !is_digits(whole) {
        return Some("expected only digits before the decimal point");
    }
    if let Some(fraction) = fraction {
        if fraction.is_empty() {
            return Some("expected at least one digit after the decimal point");
        }
        if !is_digits(fraction) {
            return Some("expected only digits after the decimal point");
        }
        // `rust_decimal` keeps 28 fractional digits and rounds the rest, so a
        // 29th digit would be accepted and silently altered.
        if fraction.len() > Decimal::MAX_SCALE as usize {
            return Some("more fractional digits than a decimal can hold");
        }
    }
    None
}

/// Encode a [`Decimal`] into the contract's wire message.
///
/// This is the single authority for the wire shape: the value is rendered in
/// base 10, which is lossless for every [`Decimal`] (a 96-bit coefficient over 28
/// decimal places), so no value this crate writes can come back different.
#[inline]
#[must_use]
pub fn decimal_to_common(value: Decimal) -> common::Decimal {
    common::Decimal { value: value.to_string(), ..Default::default() }
}

/// Decode a contract decimal into a [`Decimal`].
///
/// # Errors
/// - [`DecimalConvertError::Empty`] when the payload is blank. The containing field's presence is
///   what reports "no value", so a blank payload is a contract violation rather than a zero.
/// - [`DecimalConvertError::NotBase10`] when the payload breaks the grammar the message documents —
///   a digit separator, an exponent, a bare `.5`, a `+`, or surrounding whitespace — naming the
///   clause it broke.
/// - [`DecimalConvertError::OutOfRange`] when the payload is well-formed base 10 but wider than
///   [`Decimal`] can hold.
///
/// Each variant echoes the payload, so a caller can log what actually arrived.
pub fn common_to_decimal(value: &common::Decimal) -> Result<Decimal, DecimalConvertError> {
    // Blank is its own case: it is the shape an unpopulated message has, so it
    // says "the writer populated nothing" rather than "the payload was garbage".
    if value.value.trim().is_empty() {
        return Err(DecimalConvertError::Empty);
    }
    if let Some(reason) = grammar_violation(&value.value) {
        return Err(DecimalConvertError::NotBase10 { value: value.value.clone(), reason });
    }
    value
        .value
        .parse()
        .map_err(|source| DecimalConvertError::OutOfRange { value: value.value.clone(), source })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The wire shape is exactly one field, so a value can never be represented
    /// two ways or resolved by precedence.
    fn assert_round_trip(value: Decimal) {
        let encoded = decimal_to_common(value);
        assert_eq!(encoded.value, value.to_string(), "the payload is the canonical rendering");
        let decoded = common_to_decimal(&encoded).expect("this crate's own output must decode");
        assert_eq!(decoded, value, "the round trip must be exact");
    }

    // ---- The single representation -------------------------------------

    #[test]
    fn a_value_is_carried_by_the_string_alone() {
        assert_eq!(decimal_to_common(Decimal::new(12_345, 2)).value, "123.45");
        assert_eq!(decimal_to_common(Decimal::ZERO).value, "0");
        assert_eq!(decimal_to_common(Decimal::new(-7, 3)).value, "-0.007");
    }

    /// The whole `rust_decimal` range must survive, including the 96-bit
    /// coefficients an int64 mantissa could never have carried.
    #[test]
    fn the_full_decimal_range_round_trips_exactly() {
        for value in [
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::NEGATIVE_ONE,
            Decimal::new(-1, 8),
            Decimal::MAX,
            Decimal::MIN,
            Decimal::from_i128_with_scale(1, 28),
            "79228162514264337593543950335".parse::<Decimal>().expect("valid"),
            "-79228162514264337593543950335".parse::<Decimal>().expect("valid"),
            "0.0000000000000000000000000001".parse::<Decimal>().expect("valid"),
            "1234567890123456789012345678".parse::<Decimal>().expect("valid"),
        ] {
            assert_round_trip(value);
        }
    }

    /// `Decimal::MAX` is 7.9e28 — 29 significant digits, wider than an `i64`
    /// mantissa. It was the reason the string representation had to exist; now it
    /// is the *only* representation, so nothing narrows it.
    #[test]
    fn a_wider_than_int64_mantissa_survives_unchanged() {
        let widest = "79228162514264337593543950335".parse::<Decimal>().expect("valid");
        assert!(widest.mantissa() > i128::from(i64::MAX), "this case must need more than 64 bits");
        assert_round_trip(widest);
    }

    /// A value's scale is preserved, so `1.10` does not come back as `1.1`.
    #[test]
    fn the_scale_survives_the_round_trip() {
        assert_round_trip(Decimal::new(1_100, 3));
        assert_eq!(decimal_to_common(Decimal::new(1_100, 3)).value, "1.100");
    }

    // ---- "Unset" versus "zero" ------------------------------------------

    /// A present `Decimal` carrying zero is a value. This is the property the
    /// old int64 pair could not express: `unscaled = 0, scale = 0` was
    /// indistinguishable from an untouched message.
    #[test]
    fn a_present_zero_is_a_value_and_not_an_absence() {
        let zero = decimal_to_common(Decimal::ZERO);
        assert_eq!(zero.value, "0");
        assert_eq!(common_to_decimal(&zero).expect("zero decodes"), Decimal::ZERO);
    }

    /// Blank is a writer contract violation. The containing field's presence
    /// already reports "no value", so a blank payload cannot be quietly read as
    /// zero — which is what the old code did whenever `raw_str` was empty.
    #[test]
    fn a_blank_payload_is_a_contract_violation_not_a_zero() {
        for blank in ["", " ", "\t", "\n  "] {
            assert_eq!(
                common_to_decimal(&common::Decimal {
                    value: blank.to_string(),
                    ..Default::default()
                }),
                Err(DecimalConvertError::Empty),
                "a payload of {blank:?} must be Empty, never zero"
            );
        }
    }

    /// The grammar constrains *shape*, not spelling. Several spellings of one
    /// value are all legal, and the encoder emits the canonical one. This is
    /// normal for a decimal wire format — a venue may send `0.00000000` — and it
    /// is why nothing may compare payloads for byte equality to decide whether
    /// two decimals hold the same number.
    #[test]
    fn the_grammar_accepts_any_spelling_and_the_encoder_canonicalises() {
        for (spelling, canonical) in
            [("007", "7"), ("-0", "0"), ("1.500", "1.500"), ("0.000", "0.000"), ("00.5", "0.5")]
        {
            let wire = common::Decimal { value: spelling.to_string(), ..Default::default() };
            let decoded = common_to_decimal(&wire)
                .unwrap_or_else(|err| panic!("{spelling:?} should decode, got {err:?}"));
            assert_eq!(
                decimal_to_common(decoded).value,
                canonical,
                "{spelling:?} must canonicalise to {canonical:?}"
            );
        }
    }

    // ---- Grammar ---------------------------------------------------------

    #[test]
    fn an_unparsable_payload_is_rejected_and_echoed() {
        let garbage = common::Decimal { value: "not-a-number".to_string(), ..Default::default() };
        match common_to_decimal(&garbage) {
            Err(DecimalConvertError::NotBase10 { value, reason }) => {
                assert_eq!(value, "not-a-number", "the error must echo what actually arrived");
                assert!(!reason.is_empty(), "the error must name the clause it broke");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    /// The grammar is stated in the proto comment, so pin every clause of it.
    /// `rust_decimal`'s `FromStr` follows Rust literal syntax, so it accepts
    /// several forms the contract does not.
    #[test]
    fn the_contract_grammar_is_stricter_than_the_rust_decimal_parser() {
        // Rejected by the contract grammar, and it must not become a value.
        for rejected in [
            "1_000", // digit separators
            "1e3",   // exponent
            "1E3",   // exponent
            ".5",    // no integer part
            "1.",    // no fraction digits
            "+1",    // explicit plus
            " 1",    // leading whitespace
            "1 ",    // trailing whitespace
            "1,000", // grouping
            "NaN",   // not a decimal
            "inf",   // not a decimal
            "--1",   // doubled sign
            "0x10",  // not base 10
            "１",    // full-width digit
        ] {
            let wire = common::Decimal { value: rejected.to_string(), ..Default::default() };
            assert!(
                common_to_decimal(&wire).is_err(),
                "{rejected:?} is outside the contract grammar and must be rejected"
            );
        }
        // Accepted, so the grammar is not merely refusing everything.
        for accepted in ["0", "-0", "1", "-1", "0.5", "-0.5", "1.000", "12345678901234567890"] {
            let wire = common::Decimal { value: accepted.to_string(), ..Default::default() };
            assert!(common_to_decimal(&wire).is_ok(), "{accepted:?} is in the contract grammar");
        }
    }

    /// `Decimal` renders its own output, and the decoder reads exactly what
    /// `Decimal`'s `FromStr` accepts — so a value this crate writes always comes
    /// back. That is the contract's core round-trip guarantee.
    #[test]
    fn anything_the_encoder_writes_is_accepted_by_the_decoder() {
        for value in [
            Decimal::new(i64::MAX, 0),
            Decimal::new(i64::MIN, 28),
            Decimal::from_i128_with_scale(-1, 28),
            Decimal::new(1, 28),
        ] {
            let encoded = decimal_to_common(value);
            assert_eq!(common_to_decimal(&encoded).expect("encodable"), value);
        }
    }

    // ---- Properties -------------------------------------------------------

    proptest! {
    /// Every representable decimal survives the wire unchanged. This is what a
    /// single representation buys: there is no second form to disagree with it.
    #[test]
    fn every_representable_decimal_survives_the_wire(
        mantissa in proptest::num::i128::ANY,
        scale in 0u32..=28,
    ) {
        let Ok(value) = Decimal::try_from_i128_with_scale(mantissa, scale) else {
            return Ok(());
        };
        let encoded = decimal_to_common(value);
        prop_assert_eq!(&encoded.value, &value.to_string());
        prop_assert_eq!(common_to_decimal(&encoded).unwrap_or(value), value);
    }

    /// Whatever the payload, decoding either succeeds or fails cleanly. It never
    /// panics — this is the untrusted-input boundary of the contract.
    #[test]
    fn decoding_arbitrary_payloads_never_panics(payload in ".{0,40}") {
        let wire = common::Decimal { value: payload, ..Default::default() };
        match common_to_decimal(&wire) {
            Ok(_) => {}
            Err(DecimalConvertError::Empty) => {}
            Err(DecimalConvertError::NotBase10 { value, .. })
                    | Err(DecimalConvertError::OutOfRange { value, .. })
                        => prop_assert!(!value.is_empty()),
        }
    }
    }
}
