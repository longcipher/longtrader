//! Exponential moving average seeded by the first full-window SMA.

use std::collections::VecDeque;

use rust_decimal::Decimal;

/// Exponential moving average seeded by the first full-window SMA.
#[derive(Debug, Clone)]
pub struct Ema {
    window: usize,
    k: Decimal,
    value: Option<Decimal>,
    seed: VecDeque<Decimal>,
    seed_sum: Decimal,
}

impl Ema {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self {
            window,
            k: Decimal::from(2) / Decimal::from(window.saturating_add(1)),
            value: None,
            seed: VecDeque::with_capacity(window),
            seed_sum: Decimal::ZERO,
        }
    }

    pub fn push(&mut self, value: Decimal) {
        match self.value {
            None => {
                self.seed.push_back(value);
                self.seed_sum += value;
                if self.seed.len() >= self.window && self.window > 0 {
                    self.value = Some(self.seed_sum / Decimal::from(self.seed.len()));
                    self.seed.clear();
                    self.seed_sum = Decimal::ZERO;
                }
            }
            Some(prev) => self.value = Some((value - prev) * self.k + prev),
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn ema_converges_to_constant() {
        let mut ema = Ema::new(4);
        for _ in 0..8 {
            ema.push(dec!(10));
        }
        assert_eq!(ema.value(), Some(dec!(10)));
    }

    #[test]
    fn ema_is_none_until_the_window_fills() {
        let mut ema = Ema::new(3);
        assert_eq!(ema.value(), None);
        ema.push(dec!(1));
        assert_eq!(ema.value(), None, "one sample is not a window");
        ema.push(dec!(2));
        assert_eq!(ema.value(), None);
        ema.push(dec!(3));
        assert_eq!(ema.value(), Some(dec!(2)), "seed is the window SMA");
    }

    #[test]
    fn ema_window_of_one_echoes_every_sample() {
        let mut ema = Ema::new(1);
        ema.push(dec!(7));
        assert_eq!(ema.value(), Some(dec!(7)));
        ema.push(dec!(9));
        assert_eq!(ema.value(), Some(dec!(9)));
    }

    /// A zero window can never satisfy `seed.len() >= window && window > 0`, so
    /// the EMA stays unseeded forever instead of dividing by zero.
    #[test]
    fn ema_with_a_zero_window_never_seeds_and_never_panics() {
        let mut ema = Ema::new(0);
        for v in 1..=5 {
            ema.push(Decimal::from(v));
        }
        assert_eq!(ema.value(), None);
    }

    #[test]
    fn ema_weights_recent_samples_more_heavily() {
        let mut ema = Ema::new(2);
        ema.push(dec!(0));
        ema.push(dec!(0));
        assert_eq!(ema.value(), Some(dec!(0)));
        // k = 2/3, so three units of input move the value by 2. `2/3` is not
        // exactly representable in Decimal, so compare against the real product
        // rather than the ideal 2.
        ema.push(dec!(3));
        assert_eq!(ema.value(), Some(dec!(3) * (Decimal::from(2) / Decimal::from(3))));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// After a full window, an EMA seeded from a constant series stays
        /// exactly on that constant regardless of the window size.
        #[test]
        fn ema_of_a_constant_series_is_that_constant(
            window in 1usize..32,
            value in -1_000_000i64..1_000_000,
        ) {
            let Ok(constant) = Decimal::try_from_i128_with_scale(i128::from(value), 2) else {
                return Ok(());
            };
            let mut ema = Ema::new(window);
            for _ in 0..(window * 4).max(4) {
                ema.push(constant);
            }
            prop_assert_eq!(ema.value(), Some(constant));
        }
    }
}
