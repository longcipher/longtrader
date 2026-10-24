//! Donchian channel: rolling highest-high / lowest-low over a window.

use std::collections::VecDeque;

use rust_decimal::Decimal;

/// Donchian channel: rolling highest-high / lowest-low over a window.
#[derive(Debug, Clone)]
pub struct DonchianChannel {
    window: usize,
    buf: VecDeque<(Decimal, Decimal)>,
}

impl DonchianChannel {
    #[must_use]
    pub fn new(window: usize) -> Self {
        Self { window, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal) {
        self.buf.push_back((high, low));
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    #[must_use]
    pub fn upper(&self) -> Option<Decimal> {
        self.buf.iter().map(|(h, _)| *h).max()
    }

    #[must_use]
    pub fn lower(&self) -> Option<Decimal> {
        self.buf.iter().map(|(_, l)| *l).min()
    }

    #[must_use]
    pub fn middle(&self) -> Option<Decimal> {
        let u = self.upper()?;
        let l = self.lower()?;
        Some((u + l) / Decimal::from(2))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Donchian channel reflects the rolling highest-high / lowest-low.
        #[test]
        fn donchian_tracks_extremes(
            window in 1usize..12,
            bars in prop::collection::vec((0i64..200, 0i64..200), 2..30),
        ) {
            let bars: Vec<(Decimal, Decimal)> =
                bars.into_iter().map(|(h, l)| (Decimal::from(h), Decimal::from(l))).collect();
            let mut dc = DonchianChannel::new(window);
            for (h, l) in &bars {
                dc.push(*h, *l);
            }
            let start = bars.len().saturating_sub(window);
            let expected_upper = bars[start..].iter().map(|(h, _)| *h).max().expect("non-empty");
            let expected_lower = bars[start..].iter().map(|(_, l)| *l).min().expect("non-empty");
            prop_assert_eq!(dc.upper(), Some(expected_upper));
            prop_assert_eq!(dc.lower(), Some(expected_lower));
            prop_assert_eq!(dc.middle(), Some((expected_upper + expected_lower) / Decimal::from(2)));
        }
    }
}
