//! Z-score for mean-reversion detection over a rolling window.

use std::collections::VecDeque;

use rust_decimal::Decimal;

use super::decimal_sqrt;

/// Z-score for mean-reversion detection over a rolling window.
#[derive(Debug, Clone)]
pub struct ZScore {
    window: usize,
    buf: VecDeque<Decimal>,
}

impl ZScore {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, value: Decimal) {
        self.buf.push_back(value);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<Decimal> {
        let n = self.buf.len();
        if n < 2 {
            return None;
        }
        let count = Decimal::from(n);
        let mean = self.buf.iter().copied().sum::<Decimal>() / count;
        let variance = self
            .buf
            .iter()
            .try_fold(Decimal::ZERO, |acc, &x| {
                let d = x.checked_sub(mean)?;
                acc.checked_add(d.checked_mul(d)?)
            })?
            .checked_div(count)?;
        let std = decimal_sqrt(variance);
        if std.is_zero() {
            return None;
        }
        let last = *self.buf.back()?;
        last.checked_sub(mean)?.checked_div(std)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Z-score is zero when all values are equal and sign agrees with the
        /// last value versus the rolling window mean.
        #[test]
        fn zscore_detects_deviation(
            window in 2usize..12,
            values in prop::collection::vec(1i64..100, 2..20),
        ) {
            let mut zs = ZScore::new(window);
            let last = Decimal::from(*values.last().expect("non-empty"));
            for v in &values {
                zs.push(Decimal::from(*v));
            }
            let z = zs.value();
            if values.iter().all(|v| *v == values[0]) {
                prop_assert!(z.is_none() || z == Some(Decimal::ZERO));
                return Ok(());
            }
            // The indicator reads only the last `window` values.
            let tail: Vec<i64> = values.iter().rev().take(window).copied().collect();
            if tail.iter().all(|v| *v == tail[0]) {
                // Constant sub-window → std==0, z is None / zero.
                prop_assert!(z.is_none() || z == Some(Decimal::ZERO));
            } else {
                let tail_mean = Decimal::from(tail.iter().sum::<i64>()) / Decimal::from(tail.len() as i64);
                if last > tail_mean {
                    prop_assert!(z.expect("warmed") > Decimal::ZERO);
                } else if last < tail_mean {
                    prop_assert!(z.expect("warmed") < Decimal::ZERO);
                }
            }
        }
    }
}
