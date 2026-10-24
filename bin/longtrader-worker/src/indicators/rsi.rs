//! Relative Strength Index (Wilder smoothing).

use rust_decimal::Decimal;

/// Relative Strength Index (Wilder smoothing).
#[derive(Debug, Clone)]
pub struct Rsi {
    period: usize,
    avg_gain: Option<Decimal>,
    avg_loss: Option<Decimal>,
    prev: Option<Decimal>,
    samples: usize,
}

impl Rsi {
    #[must_use]
    pub fn new(period: usize) -> Self {
        // A zero period would make the Wilder average divide by `n == 0`, which
        // panics inside `rust_decimal`. Clamp to the smallest meaningful window:
        // every sample is then both gain and loss of the prior one.
        Self { period: period.max(1), avg_gain: None, avg_loss: None, prev: None, samples: 0 }
    }

    pub fn push(&mut self, close: Decimal) {
        let Some(prev) = self.prev else {
            self.prev = Some(close);
            return;
        };
        self.prev = Some(close);
        let change = close - prev;
        let gain = change.max(Decimal::ZERO);
        let loss = (-change).max(Decimal::ZERO);
        let n = Decimal::from(self.period);
        if let (Some(g), Some(l)) = (self.avg_gain, self.avg_loss) {
            self.avg_gain = Some((g * (n - Decimal::ONE) + gain) / n);
            self.avg_loss = Some((l * (n - Decimal::ONE) + loss) / n);
        } else {
            // Seed phase: simple averages until `period` deltas seen.
            self.avg_gain = Some(self.avg_gain.unwrap_or_default() + gain);
            self.avg_loss = Some(self.avg_loss.unwrap_or_default() + loss);
        }
        self.samples += 1;
        if self.samples == self.period {
            self.avg_gain = self.avg_gain.map(|g| g / n);
            self.avg_loss = self.avg_loss.map(|l| l / n);
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<Decimal> {
        let gain = self.avg_gain?;
        let loss = self.avg_loss?;
        if loss.is_zero() {
            return Some(Decimal::from(100));
        }
        let rs = gain / loss;
        Some(Decimal::from(100) - Decimal::from(100) / (Decimal::ONE + rs))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn rsi_extremes() {
        let mut up = Rsi::new(5);
        for v in [1, 2, 3, 4, 5, 6, 7] {
            up.push(Decimal::from(v));
        }
        assert_eq!(up.value(), Some(dec!(100)));

        let mut down = Rsi::new(5);
        for v in [7, 6, 5, 4, 3, 2, 1] {
            down.push(Decimal::from(v));
        }
        assert_eq!(down.value(), Some(dec!(0)));
    }

    #[test]
    fn rsi_is_none_until_the_first_delta() {
        let rsi = Rsi::new(5);
        assert_eq!(rsi.value(), None);
        let mut rsi = rsi;
        rsi.push(dec!(100));
        assert_eq!(rsi.value(), None, "a single sample has no change to weigh");
    }

    #[test]
    fn rsi_of_a_flat_series_is_the_upper_bound() {
        let mut rsi = Rsi::new(4);
        for _ in 0..10 {
            rsi.push(dec!(50));
        }
        // Zero average loss means "never a downtick", which the indicator
        // reports as 100 rather than dividing by zero.
        assert_eq!(rsi.value(), Some(dec!(100)));
    }

    #[test]
    fn rsi_is_always_within_the_documented_range() {
        let series = [10, 9, 11, 8, 12, 7, 13, 6, 14, 5, 15, 4, 16, 3, 17, 2];
        let mut rsi = Rsi::new(3);
        for v in series {
            rsi.push(Decimal::from(v));
            if let Some(value) = rsi.value() {
                assert!(
                    (Decimal::ZERO..=Decimal::from(100)).contains(&value),
                    "rsi {value} outside 0..=100"
                );
            }
        }
    }

    /// A zero period used to divide the Wilder average by `n == 0`, which panics
    /// inside `rust_decimal`. It is now clamped to the smallest window.
    #[test]
    fn rsi_with_a_zero_period_clamps_instead_of_dividing_by_zero() {
        let mut rsi = Rsi::new(0);
        for v in [1, 2, 3, 2, 5] {
            rsi.push(Decimal::from(v));
        }
        let value = rsi.value().expect("a clamped period still produces a value");
        assert!((Decimal::ZERO..=Decimal::from(100)).contains(&value), "rsi {value}");
    }

    #[test]
    fn rsi_tracks_a_mixed_series_between_the_bounds() {
        let mut rsi = Rsi::new(5);
        for v in [100, 101, 99, 102, 98, 103, 97, 104] {
            rsi.push(Decimal::from(v));
        }
        let value = rsi.value().expect("warm");
        assert!(value > Decimal::ZERO && value < Decimal::from(100), "rsi {value}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// RSI is documented as a 0..=100 oscillator; clamp and warm-up paths
        /// included.
        #[test]
        fn rsi_stays_in_range(
            period in 1usize..24,
            closes in prop::collection::vec(-500i64..500, 2..40),
        ) {
            let mut rsi = Rsi::new(period);
            for close in closes {
                rsi.push(Decimal::from(close));
                if let Some(value) = rsi.value() {
                    prop_assert!(
                        (Decimal::ZERO..=Decimal::from(100)).contains(&value),
                        "rsi {value} escaped 0..=100"
                    );
                }
            }
        }
    }
}
