#![no_main]

//! Fuzz the contract decimal decoder.
//!
//! `common_to_decimal` reads `common.v1.Decimal`, the value field carried by
//! essentially every message in the contract. Its input comes straight off the
//! wire from a remote venue or terminal, so it is a parser of untrusted input:
//! a hostile or buggy peer controls the payload in full.
//!
//! The invariants it must hold:
//!
//! 1. decoding never panics and never overflows, whatever the payload says;
//! 2. each error means exactly one thing — `Empty` iff the payload is blank,
//!    `NotBase10` iff it breaks the documented grammar, `OutOfRange` iff it is
//!    well-formed base 10 but wider than a decimal — and every one of them
//!    echoes the payload that caused it;
//! 3. a payload the decoder accepts is a fixed point: re-encoding it lands on
//!    the same wire bytes and the same number;
//! 4. a value this codec encodes always decodes back to the same number, so no
//!    message the worker *writes* can be rejected by its own reader.

use libfuzzer_sys::fuzz_target;
use longtrader_contract::ext::{DecimalConvertError, common_to_decimal, decimal_to_common};
use longtrader_contract::proto::longtrader::common::v1::Decimal as WireDecimal;
use rust_decimal::Decimal;

/// Both directions of the codec, driven from one input. A `cargo-fuzz` target
/// may hold only one `fuzz_target!`, and checking the writer and the reader
/// together is the point: a disagreement between them is the bug that matters.
#[derive(arbitrary::Arbitrary, Debug)]
struct FuzzInput {
    /// A payload exactly as a hostile or buggy peer would send it.
    payload: String,
    /// A value the encoder might be asked to write.
    mantissa: i128,
    scale: u32,
}

/// The grammar the `Decimal` message documents, re-derived here so the fuzz
/// target does not simply trust the implementation's own helper.
fn breaks_documented_grammar(payload: &str) -> bool {
    if payload.trim().is_empty() {
        return false; // that is `Empty`, not `NotBase10`
    }
    if payload.contains('_') || payload.contains(['e', 'E']) || payload.starts_with('+') {
        return true;
    }
    let body = payload.strip_prefix('-').unwrap_or(payload);
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (body, None),
    };
    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return true;
    }
    match fraction {
        None => false,
        Some(fraction) => {
            fraction.is_empty()
                || !fraction.bytes().all(|b| b.is_ascii_digit())
                || fraction.len() > Decimal::MAX_SCALE as usize
        }
    }
}

fuzz_target!(|input: FuzzInput| {
    // Property 4 from the writer's side: anything the encoder is willing to emit
    // must come back. Without this, a writer could produce a payload its own
    // reader rejects, and the bug would only surface against a real venue.
    if input.scale <= Decimal::MAX_SCALE {
        if let Ok(value) = Decimal::try_from_i128_with_scale(input.mantissa, input.scale) {
            let written = decimal_to_common(value);
            assert_eq!(written.value, value.to_string(), "the payload must be canonical");
            assert_eq!(
                common_to_decimal(&written).expect("the encoder must produce decodable output"),
                value,
                "the codec is not round-trip exact"
            );
        }
    }

    let input_text = input.payload.clone();
    let wire = WireDecimal { value: input_text.clone(), ..Default::default() };

    // Property 1 + 2: decode is total, and each error means one thing.
    match common_to_decimal(&wire) {
        Err(DecimalConvertError::Empty) => {
            assert!(
                input_text.trim().is_empty(),
                "Empty must mean a blank payload, not {:?}",
                input_text
            );
        }
        Err(DecimalConvertError::NotBase10 { value, reason }) => {
            assert_eq!(value, input_text, "the error must echo the offending payload");
            assert!(
                breaks_documented_grammar(&input_text),
                "NotBase10 for a payload that satisfies the grammar: {:?}",
                input_text
            );
            assert!(!reason.is_empty(), "the error must name the clause it broke");
        }
        Err(DecimalConvertError::OutOfRange { value, .. }) => {
            assert_eq!(value, input_text, "the error must echo the offending payload");
            assert!(
                !breaks_documented_grammar(&input_text),
                "OutOfRange for a payload that breaks the grammar: {:?}",
                input_text
            );
            assert!(
                Decimal::from_str_exact(&input_text).is_err(),
                "OutOfRange must mean the parser itself refused it: {:?}",
                input_text
            );
        }
        Ok(value) => {
            // A payload that decodes must be inside the grammar.
            assert!(
                !breaks_documented_grammar(&input_text),
                "a payload that breaks the grammar decoded to {value}: {:?}",
                input_text
            );
            // Property 3: the *canonical* form is a fixed point. The payload
            // itself need not be canonical — the grammar accepts any in-grammar
            // spelling (`007`, `-0`, `1.500`) and the encoder normalises it — so
            // the claim is about what the encoder emits, not about the input.
            let canonical = decimal_to_common(value);
            let decoded_again =
                common_to_decimal(&canonical).expect("the encoder must produce decodable output");
            assert_eq!(decoded_again, value, "the decimal codec is not round-trip stable");
            assert_eq!(
                decimal_to_common(decoded_again),
                canonical,
                "the canonical wire form must be a fixed point"
            );
            // And what the encoder emits must itself be a payload the decoder
            // accepts — otherwise a writer could outrun its own reader.
            assert!(Decimal::from_str_exact(&canonical.value).is_ok());
        }
    }
});
