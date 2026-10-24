//! Newton's-method square root on `Decimal`, shared by volatility bands.

use rust_decimal::Decimal;

/// Newton's-method square root on `Decimal`.
///
/// Returns `Decimal::ZERO` for any non-positive input.
///
/// Convergence contract: the answer is always a finite, non-negative `Decimal`,
/// but it is not always the exact root. Every step runs on `checked_*` because
/// `rust_decimal`'s `/` and `+` **panic** on overflow, and the Newton sequence
/// for a value near `Decimal::MAX` does leave the representable range — halving
/// `MAX` a second time needs one more mantissa digit than the 96-bit field
/// holds. In that case the iteration stops and returns the last finite iterate
/// rather than taking the process down.
#[must_use]
pub fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let two = Decimal::from(2);
    let seed = if value >= Decimal::ONE {
        // `value / 2` is itself unrepresentable for some inputs near
        // `Decimal::MAX`, so the seed falls back to `value`: a valid Newton
        // start, just a slower one.
        value.checked_div(two).unwrap_or(value)
    } else {
        // Below one the unit seed converges in far fewer steps than halving down
        // from the value.
        Decimal::ONE
    };
    let mut x = seed;
    for _ in 0..64 {
        let Some(quotient) = value.checked_div(x) else {
            return x;
        };
        let Some(next) = x.checked_add(quotient).and_then(|sum| sum.checked_div(two)) else {
            return x;
        };
        // A non-positive iterate means the quotient step went off the rails
        // (division by a zero `x`, or a rounding collapse). Stop here.
        if next <= Decimal::ZERO {
            return x;
        }
        if (next - x).abs() < Decimal::new(1, 8) {
            return next;
        }
        x = next;
    }
    x
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn sqrt_rejects_non_positive_input() {
        assert_eq!(decimal_sqrt(Decimal::ZERO), Decimal::ZERO);
        assert_eq!(decimal_sqrt(dec!(-1)), Decimal::ZERO);
        assert_eq!(decimal_sqrt(Decimal::MIN), Decimal::ZERO);
        assert_eq!(decimal_sqrt(dec!(-0.00000001)), Decimal::ZERO);
    }

    #[test]
    fn sqrt_of_perfect_squares() {
        for n in 1..=12u32 {
            let value = Decimal::from(n * n);
            let root = decimal_sqrt(value);
            // Newton stops at a 1e-8 delta, so allow one ulp of slack at 1e-8.
            assert!((root - Decimal::from(n)).abs() < dec!(0.00000001), "sqrt({n}^2) = {root}");
        }
    }

    #[test]
    fn sqrt_of_a_fraction_below_one_uses_the_unit_seed() {
        // Below 1.0 the seed is 1.0 rather than value/2; verify convergence.
        let root = decimal_sqrt(dec!(0.25));
        assert!((root - dec!(0.5)).abs() < dec!(0.00000001), "sqrt(0.25) = {root}");
    }

    /// The Newton sequence for `Decimal::MAX` leaves the representable range
    /// (the second halving needs a mantissa digit the 96-bit field does not
    /// have) and `rust_decimal`'s `/` **panics** on that overflow. The contract
    /// is "always a finite, non-negative answer", not "always the exact root".
    #[test]
    fn sqrt_at_the_decimal_boundaries_never_panics() {
        let edges = [Decimal::MAX, Decimal::MIN, Decimal::ONE];
        for value in edges {
            let root = decimal_sqrt(value);
            assert!(root >= Decimal::ZERO, "sqrt({value}) = {root} was negative");
            assert!(root <= Decimal::MAX, "sqrt({value}) = {root} was not finite");
        }
    }

    /// The bail-out is a corner case, not a retreat: an ordinary large value
    /// still converges to its root.
    #[test]
    fn sqrt_of_a_large_but_well_scaled_value_still_converges() {
        let root = decimal_sqrt(Decimal::new(1_000_000_000_000_000_000, 0)); // 1e18
        assert!((root - dec!(1000000000)).abs() < dec!(0.00000001), "sqrt(1e18) = {root}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// A non-negative input never yields a negative root, and squaring the
        /// root recovers the input to within the Newton tolerance.
        #[test]
        fn sqrt_squares_back_to_its_input(
            mantissa in 0i64..1_000_000_000_000_000i64,
            scale in 0u32..12,
        ) {
            let Ok(value) = Decimal::try_from_i128_with_scale(i128::from(mantissa), scale) else {
                return Ok(());
            };
            let root = decimal_sqrt(value);
            prop_assert!(root >= Decimal::ZERO, "sqrt({value}) = {root} was negative");
            let squared = root * root;
            let tolerance = Decimal::new(1, 4);
            prop_assert!(
                (squared - value).abs() <= tolerance,
                "sqrt({value}) = {root}, squared = {squared}"
            );
        }
    }
}
