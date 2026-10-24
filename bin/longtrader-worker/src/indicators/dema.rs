//! Double exponential moving average.

use rust_decimal::Decimal;

use super::ema::Ema;

/// Double exponential moving average.
#[derive(Debug, Clone)]
pub struct Dema {
    ema1: Ema,
    ema2: Ema,
}

impl Dema {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { ema1: Ema::new(window), ema2: Ema::new(window) }
    }

    pub fn push(&mut self, value: Decimal) {
        self.ema1.push(value);
        if let Some(v1) = self.ema1.value() {
            self.ema2.push(v1);
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<Decimal> {
        let v1 = self.ema1.value()?;
        let v2 = self.ema2.value()?;
        Some(v1 * Decimal::from(2) - v2)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::indicators::ema::Ema;

    #[test]
    fn dema_is_none_until_both_emas_are_warm() {
        let mut dema = Dema::new(3);
        assert_eq!(dema.value(), None);
        dema.push(dec!(1));
        dema.push(dec!(2));
        assert_eq!(dema.value(), None, "the outer EMA is still seeding");
        dema.push(dec!(3));
        assert_eq!(dema.value(), None, "the inner EMA has not started collecting");
        // The inner EMA only starts once the outer one is warm, so a
        // double-exponential needs strictly more than one window of samples.
        for v in 4..=8 {
            dema.push(Decimal::from(v));
        }
        assert!(dema.value().is_some(), "both EMAs are warm after 2*window-1 samples");
    }

    #[test]
    fn dema_converges_to_a_constant_series() {
        let mut dema = Dema::new(4);
        for _ in 0..20 {
            dema.push(dec!(42));
        }
        assert_eq!(dema.value(), Some(dec!(42)));
    }

    #[test]
    fn dema_is_twice_the_fast_ema_minus_the_slow_one() {
        let mut dema = Dema::new(2);
        let mut ema1 = Ema::new(2);
        let mut ema2 = Ema::new(2);
        for v in [dec!(1), dec!(2), dec!(5), dec!(9)] {
            dema.push(v);
            ema1.push(v);
            if let Some(v1) = ema1.value() {
                ema2.push(v1);
            }
        }
        let (v1, v2) = (ema1.value().expect("warm"), ema2.value().expect("warm"));
        assert_eq!(dema.value(), Some(v1 * Decimal::from(2) - v2));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// DEMA is `2 * ema1 - ema2` by construction, so once both EMAs are warm
        /// it must equal that identity exactly.
        #[test]
        fn dema_matches_its_definition(
            window in 1usize..12,
            values in prop::collection::vec(0i64..1_000, 1..40),
        ) {
            let mut dema = Dema::new(window);
            let mut ema1 = Ema::new(window);
            let mut ema2 = Ema::new(window);
            for value in values {
                dema.push(Decimal::from(value));
                ema1.push(Decimal::from(value));
                if let Some(v1) = ema1.value() {
                    ema2.push(v1);
                }
            }
            let (Some(expected), Some(actual)) = (ema2.value().map(|v2| {
                ema1.value().expect("inner ema is warm when the outer is") * Decimal::from(2) - v2
            }), dema.value()) else {
                return Ok(());
            };
            prop_assert_eq!(actual, expected);
        }
    }
}
