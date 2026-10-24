//! Average True Range (Wilder smoothing).

use rust_decimal::Decimal;

/// Average True Range (Wilder smoothing).
#[derive(Debug, Clone)]
pub struct Atr {
    rma_value: Option<Decimal>,
    prev_close: Option<Decimal>,
    period: usize,
    samples: usize,
    sum: Decimal,
}

impl Atr {
    #[must_use]
    pub fn new(period: usize) -> Self {
        // Same divide-by-zero guard as `Rsi`: `sum / n` below would panic on a
        // zero period.
        let period = period.max(1);
        Self { rma_value: None, prev_close: None, period, samples: 0, sum: Decimal::ZERO }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal, close: Decimal) {
        let tr = match self.prev_close {
            // The seed range has no previous close to widen it, so clamp: a bar
            // with `high < low` is malformed, and letting a negative true range
            // through would invert every band derived from it.
            None => (high - low).max(Decimal::ZERO),
            Some(prev) => (high - low).max((high - prev).abs()).max((low - prev).abs()),
        };
        self.prev_close = Some(close);
        let n = Decimal::from(self.period);
        match self.rma_value {
            None => {
                self.sum += tr;
                self.samples += 1;
                if self.samples >= self.period {
                    self.rma_value = Some(self.sum / n);
                }
            }
            Some(prev) => self.rma_value = Some((prev * (n - Decimal::ONE) + tr) / n),
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.rma_value
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn atr_is_none_until_the_period_fills() {
        let mut atr = Atr::new(3);
        atr.push(dec!(10), dec!(8), dec!(9));
        assert_eq!(atr.value(), None);
        atr.push(dec!(11), dec!(9), dec!(10));
        assert_eq!(atr.value(), None);
        atr.push(dec!(12), dec!(10), dec!(11));
        assert_eq!(atr.value(), Some(dec!(2)), "seed is the mean true range");
    }

    /// The first true range has no previous close, so it degrades to `high -
    /// low`.
    /// A malformed bar (`high < low`) has no previous close to widen the range,
    /// so the seed would be negative. Clamping keeps a negative true range — and
    /// the inverted Supertrend bands derived from it — out of the pipeline.
    #[test]
    fn a_malformed_seed_bar_does_not_produce_a_negative_range() {
        let mut atr = Atr::new(1);
        atr.push(dec!(1), dec!(5), dec!(3));
        assert_eq!(atr.value(), Some(Decimal::ZERO));
    }

    #[test]
    fn atr_first_sample_is_the_plain_high_low_range() {
        let mut atr = Atr::new(1);
        atr.push(dec!(15), dec!(5), dec!(10));
        assert_eq!(atr.value(), Some(dec!(10)));
    }

    #[test]
    fn atr_widens_on_a_gap_across_the_previous_close() {
        let mut atr = Atr::new(1);
        // First sample seeds prev_close = 10.
        atr.push(dec!(11), dec!(10), dec!(10));
        assert_eq!(atr.value(), Some(dec!(1)));
        // Opens far above the previous close: |high - prev_close| dominates.
        atr.push(dec!(50), dec!(48), dec!(49));
        assert_eq!(atr.value(), Some(dec!(40)));
    }

    #[test]
    fn atr_takes_the_widest_of_the_three_candidate_ranges() {
        let mut atr = Atr::new(1);
        atr.push(dec!(10), dec!(10), dec!(10));
        // prev_close is 10: intraday range 2, |high - prev| 32, |low - prev| 30.
        atr.push(dec!(42), dec!(40), dec!(41));
        assert_eq!(atr.value(), Some(dec!(32)));
    }

    /// A zero period used to divide `sum / n` with `n == 0`, panicking inside
    /// `rust_decimal`. It is now clamped to the smallest window.
    #[test]
    fn atr_with_a_zero_period_clamps_instead_of_dividing_by_zero() {
        let mut atr = Atr::new(0);
        atr.push(dec!(10), dec!(8), dec!(9));
        assert_eq!(atr.value(), Some(dec!(2)), "the first true range is high - low");
        // prev_close is now 9, so the second true range is max(2, 3, 1) = 3.
        atr.push(dec!(12), dec!(10), dec!(11));
        assert_eq!(atr.value(), Some(dec!(3)));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// ATR is non-negative for any OHLC triple, including inverted bars.
        #[test]
        fn atr_is_never_negative(
            period in 1usize..16,
            bars in prop::collection::vec((0i64..200, 0i64..200, 0i64..200), 1..30),
        ) {
            let mut atr = Atr::new(period);
            for (high, low, close) in bars {
                atr.push(Decimal::from(high), Decimal::from(low), Decimal::from(close));
                if let Some(value) = atr.value() {
                    prop_assert!(value >= Decimal::ZERO, "atr {value} was negative");
                }
            }
        }
    }
}
