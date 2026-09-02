//! Conversion helpers between generated contract types and [`rust_decimal::Decimal`].
//!
//! The contract's `common.v1.Decimal` uses a dual representation:
//!
//! - fast path: `unscaled * 10^-scale` (int64 mantissa), used whenever the value is representable —
//!   the overwhelming majority of market data;
//! - fallback: `raw_str`, for values whose 96-bit `rust_decimal` mantissa exceeds int64.
//!
//! Invariant: writers populate exactly one representation; readers support
//! both. Round-trip exactness across the full `rust_decimal` range is
//! property-tested below.

use rust_decimal::Decimal;

use crate::proto::longtrader::common::v1 as common;

/// Errors produced when converting contract decimals into [`Decimal`].
#[derive(Debug, thiserror::Error)]
pub enum DecimalConvertError {
    /// The `raw_str` fallback was populated but not parseable.
    #[error("invalid decimal string '{value}': {source}")]
    RawStr {
        value: String,
        #[source]
        source: rust_decimal::Error,
    },
    /// Fast-path fields were out of `rust_decimal` range.
    #[error("unscaled/scale out of range: unscaled={unscaled} scale={scale}")]
    OutOfRange { unscaled: i64, scale: i32 },
}

/// Encode a [`Decimal`] into the contract's dual-representation message.
///
/// Uses the int64 fast path whenever the mantissa fits; otherwise falls back
/// to `raw_str`. This is the single authority for the representation split.
#[inline]
pub fn decimal_to_common(value: Decimal) -> common::Decimal {
    let mantissa = value.mantissa();
    if let Ok(unscaled) = i64::try_from(mantissa) {
        common::Decimal {
            unscaled,
            scale: i32::try_from(value.scale()).unwrap_or_default(),
            raw_str: String::new(),
            ..Default::default()
        }
    } else {
        common::Decimal { unscaled: 0, scale: 0, raw_str: value.to_string(), ..Default::default() }
    }
}

/// Decode a contract decimal into a [`Decimal`], supporting both
/// representations. Returns an error when neither representation is valid.
pub fn common_to_decimal(value: &common::Decimal) -> Result<Decimal, DecimalConvertError> {
    if value.raw_str.is_empty() {
        let scale = u32::try_from(value.scale).map_err(|_| DecimalConvertError::OutOfRange {
            unscaled: value.unscaled,
            scale: value.scale,
        })?;
        Decimal::try_from_i128_with_scale(i128::from(value.unscaled), scale).map_err(|_| {
            DecimalConvertError::OutOfRange { unscaled: value.unscaled, scale: value.scale }
        })
    } else {
        value
            .raw_str
            .parse::<Decimal>()
            .map_err(|source| DecimalConvertError::RawStr { value: value.raw_str.clone(), source })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn assert_round_trip(value: Decimal) {
        let encoded = decimal_to_common(value);
        // Representation invariant: exactly one of {fast path, raw_str}.
        if encoded.raw_str.is_empty() {
            assert_eq!(
                i128::from(encoded.unscaled),
                value.mantissa(),
                "fast path must carry the full mantissa"
            );
            assert_eq!(i32::try_from(value.scale()).expect("scale fits i32"), encoded.scale);
        } else {
            assert_eq!(encoded.raw_str, value.to_string(), "raw_str must be canonical");
        }
        let decoded = common_to_decimal(&encoded).expect("decode must succeed");
        assert_eq!(decoded, value, "round trip must be exact");
    }

    #[test]
    fn special_values_round_trip_exactly() {
        for value in [
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::NEGATIVE_ONE,
            Decimal::new(-1, 8), // -0.00000001
            Decimal::MAX,        // 96-bit mantissa, needs raw_str
            Decimal::MIN,        // negative 96-bit mantissa
            Decimal::from_i128_with_scale(1, 28),
            "79228162514264337593543950335".parse::<Decimal>().expect("valid decimal"),
            "-0.0000000000000000000000000001".parse::<Decimal>().expect("valid decimal"),
        ] {
            assert_round_trip(value);
        }
    }

    #[test]
    fn max_mantissa_uses_raw_str_fallback() {
        let encoded = decimal_to_common(Decimal::MAX);
        assert!(!encoded.raw_str.is_empty(), "96-bit mantissa cannot use the fast path");
        assert_eq!(common_to_decimal(&encoded).expect("decode"), Decimal::MAX);
    }

    #[test]
    fn small_values_use_fast_path() {
        let encoded = decimal_to_common(Decimal::new(12345, 2)); // 123.45
        assert!(encoded.raw_str.is_empty());
        assert_eq!(encoded.unscaled, 12_345);
        assert_eq!(encoded.scale, 2);
    }

    #[test]
    fn invalid_raw_str_is_an_error() {
        let bad = common::Decimal {
            unscaled: 0,
            scale: 0,
            raw_str: "not-a-number".to_string(),
            ..Default::default()
        };
        assert!(matches!(common_to_decimal(&bad), Err(DecimalConvertError::RawStr { .. })));
    }

    // Arbitrary strategy over the representable rust_decimal space:
    // sign x 96-bit mantissa (two i64 halves) x scale.
    fn arb_decimal() -> impl Strategy<Value = Decimal> {
        (proptest::bool::ANY, proptest::num::i64::ANY, proptest::num::i64::ANY, 0u32..=28).prop_map(
            |(negative, hi, lo, scale)| {
                let bits = ((hi as u64 as u128) << 64) | (lo as u64 as u128);
                let magnitude = if negative { -(bits as i128) } else { bits as i128 };
                Decimal::try_from_i128_with_scale(magnitude, scale).unwrap_or(Decimal::ZERO)
            },
        )
    }

    proptest! {
        #[test]
        fn round_trip_is_exact(value in arb_decimal()) {
            assert_round_trip(value);
        }

        #[test]
        fn decode_never_panics(
            unscaled in proptest::num::i64::ANY,
            scale in -40i32..=40,
            raw in "[a-z0-9.+-e]{0,12}",
        ) {
            let encoded =
                common::Decimal { unscaled, scale, raw_str: raw, ..Default::default() };
            let _ = common_to_decimal(&encoded);
        }
    }
}
