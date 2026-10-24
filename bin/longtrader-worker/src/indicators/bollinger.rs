//! Bollinger bands over a rolling close window.

use std::collections::VecDeque;

use rust_decimal::Decimal;

use super::decimal_sqrt;

/// Bollinger bands over a rolling close window.
#[derive(Debug, Clone)]
pub struct BollingerBands {
    window: usize,
    mult: Decimal,
    buf: VecDeque<Decimal>,
}

/// One Bollinger-band snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bands {
    pub middle: Decimal,
    pub upper: Decimal,
    pub lower: Decimal,
}

impl BollingerBands {
    #[must_use]
    pub fn new(window: usize, mult: Decimal) -> Self {
        Self { window, mult, buf: VecDeque::with_capacity(window) }
    }

    pub fn push(&mut self, close: Decimal) {
        self.buf.push_back(close);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
    }

    /// Recomputes bands from the whole window (stateless variant suited to
    /// poll-based strategies that refetch candle windows).
    ///
    /// `None` means "no bands": fewer than two closes, or an intermediate that
    /// `Decimal` cannot represent. The magnitudes here are venue-supplied, and
    /// squaring a deviation from a price of `1e14` and up produces values past
    /// the 96-bit mantissa (`7.9e28`) — where `rust_decimal`'s `+`/`*`/`/`
    /// **panic** on overflow. Every step is therefore `checked_*`, and an
    /// unrepresentable variance reports no bands instead of killing the process.
    #[must_use]
    pub fn compute(closes: &[Decimal], mult: Decimal) -> Option<Bands> {
        let n = closes.len();
        if n < 2 {
            return None;
        }
        let count = Decimal::from(n);
        let sum = closes.iter().try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))?;
        let mean = sum.checked_div(count)?;
        // Two-pass variance: subtracting the mean before squaring is what keeps
        // the products small (the one-pass `E[x^2] - E[x]^2` form cancels
        // catastrophically here). The deviations that survive are still
        // venue-sized, hence the checked accumulation.
        let squared = closes.iter().try_fold(Decimal::ZERO, |acc, &x| {
            let deviation = x.checked_sub(mean)?;
            acc.checked_add(deviation.checked_mul(deviation)?)
        })?;
        let variance = squared.checked_div(count)?;
        let spread = decimal_sqrt(variance).checked_mul(mult)?;
        Some(Bands {
            middle: mean,
            upper: mean.checked_add(spread)?,
            lower: mean.checked_sub(spread)?,
        })
    }

    #[must_use]
    pub fn bands(&self) -> Option<Bands> {
        if self.buf.len() < self.window || self.window < 2 {
            return None;
        }
        let closes: Vec<Decimal> = self.buf.iter().copied().collect();
        Self::compute(&closes, self.mult)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn bollinger_stateless_matches_incremental() {
        let closes = [dec!(1), dec!(2), dec!(3)];
        let mut boll = BollingerBands::new(3, dec!(2));
        for c in closes {
            boll.push(c);
        }
        assert_eq!(boll.bands(), BollingerBands::compute(&closes, dec!(2)));
    }

    #[test]
    fn bollinger_needs_two_closes_to_have_a_deviation() {
        assert_eq!(BollingerBands::compute(&[], dec!(2)), None);
        assert_eq!(BollingerBands::compute(&[dec!(5)], dec!(2)), None);
        assert!(BollingerBands::compute(&[dec!(5), dec!(5)], dec!(2)).is_some());
    }

    #[test]
    fn bollinger_of_a_flat_series_collapses_onto_the_mean() {
        let bands = BollingerBands::compute(&[dec!(3); 8], dec!(2)).expect("bands");
        assert_eq!(bands.middle, dec!(3));
        assert_eq!(bands.upper, dec!(3));
        assert_eq!(bands.lower, dec!(3));
    }

    #[test]
    fn bollinger_bands_straddle_the_middle() {
        let closes = [dec!(1), dec!(2), dec!(3), dec!(4), dec!(10)];
        let bands = BollingerBands::compute(&closes, dec!(2)).expect("bands");
        assert!(bands.lower <= bands.middle, "{:?}", bands);
        assert!(bands.middle <= bands.upper, "{:?}", bands);
        // The multiplier widens the band linearly around the mean.
        let narrow = BollingerBands::compute(&closes, dec!(1)).expect("bands");
        assert!((bands.upper - bands.lower) > (narrow.upper - narrow.lower));
    }

    #[test]
    fn bollinger_window_below_two_never_produces_bands() {
        let mut boll = BollingerBands::new(1, dec!(2));
        for v in 1..=10 {
            boll.push(Decimal::from(v));
        }
        assert_eq!(boll.bands(), None, "a window of one has no deviation");
    }

    #[test]
    fn bollinger_with_a_zero_window_never_produces_bands() {
        let mut boll = BollingerBands::new(0, dec!(2));
        for v in 1..=5 {
            boll.push(Decimal::from(v));
        }
        assert_eq!(boll.bands(), None);
    }

    #[test]
    fn bollinger_is_none_until_the_window_is_full() {
        let mut boll = BollingerBands::new(4, dec!(2));
        boll.push(dec!(1));
        boll.push(dec!(2));
        boll.push(dec!(3));
        assert_eq!(boll.bands(), None, "three of four samples is not a window");
        boll.push(dec!(4));
        assert_eq!(
            boll.bands(),
            BollingerBands::compute(&[dec!(1), dec!(2), dec!(3), dec!(4)], dec!(2))
        );
    }

    #[test]
    fn bollinger_window_rolls_forward() {
        let mut boll = BollingerBands::new(3, dec!(2));
        for v in [1, 2, 3, 9] {
            boll.push(Decimal::from(v));
        }
        // The oldest sample rolled out, so the window is exactly [2, 3, 9].
        assert_eq!(boll.bands(), BollingerBands::compute(&[dec!(2), dec!(3), dec!(9)], dec!(2)));
    }

    /// A deviation of `5e14` from a `1e15` price squares to `2.5e29`, past
    /// `Decimal::MAX` (`7.9e28`), so the plain `*` used to panic inside
    /// `rust_decimal` on a venue-supplied price. Both entry points must report
    /// "no bands" instead of taking the process down.
    #[test]
    fn bollinger_of_enormous_prices_reports_no_bands_instead_of_panicking() {
        let closes =
            [Decimal::new(1_000_000_000_000_000, 0), Decimal::new(2_000_000_000_000_000, 0)];
        assert_eq!(BollingerBands::compute(&closes, dec!(2)), None);

        let mut boll = BollingerBands::new(2, dec!(2));
        for close in closes {
            boll.push(close);
        }
        assert_eq!(boll.bands(), None, "the rolling window must not panic either");
    }

    /// The guard is about magnitude, not "big": `1e12` prices differ far enough
    /// to still produce bands, so an over-eager guard would silently disable the
    /// indicator for a high-priced venue.
    #[test]
    fn bollinger_of_large_but_representable_prices_still_yields_bands() {
        let closes = [Decimal::new(1_000_000_000_000, 0), Decimal::new(1_000_000_000_001, 0)];
        let bands = BollingerBands::compute(&closes, dec!(2)).expect("bands");
        assert!(bands.lower <= bands.middle, "{:?}", bands);
        assert!(bands.middle <= bands.upper, "{:?}", bands);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Bollinger bands are ordered `lower <= middle <= upper` for every
        /// non-negative multiplier, and the middle is the arithmetic mean.
        #[test]
        fn bollinger_bands_are_ordered(
            closes in prop::collection::vec(0i64..10_000, 2..40),
            milli_mult in 0u32..10_000,
        ) {
            let closes: Vec<Decimal> = closes.into_iter().map(Decimal::from).collect();
            let mult = Decimal::new(i64::from(milli_mult), 3);
            let bands = BollingerBands::compute(&closes, mult).expect("two or more closes");
            let count = Decimal::from(closes.len());
            let mean = closes.iter().copied().sum::<Decimal>() / count;
            prop_assert_eq!(bands.middle, mean, "the middle band is the mean");
            prop_assert!(bands.lower <= bands.middle, "{:?} lower>middle", bands);
            prop_assert!(bands.middle <= bands.upper, "{:?} middle>upper", bands);
        }

        /// The incremental window and the stateless helper agree once the
        /// rolling buffer holds at least `window` samples.
        #[test]
        fn bollinger_incremental_matches_stateless(
            window in 2usize..16,
            extra in 0usize..16,
            closes in prop::collection::vec(0i64..10_000, 2..32),
        ) {
            let closes: Vec<Decimal> = closes.into_iter().map(Decimal::from).collect();
            let mut boll = BollingerBands::new(window, dec!(2));
            for close in &closes {
                boll.push(*close);
            }
            for _ in 0..extra {
                boll.push(dec!(1));
            }
            let Some(actual) = boll.bands() else { return Ok(()); };
            let start = closes.len().saturating_sub(window - 1);
            let windowed: Vec<Decimal> = if start < closes.len() {
                closes[start..].to_vec()
            } else {
                vec![dec!(1); extra + 1]
            };
            if windowed.len() < window {
                return Ok(());
            }
            let windowed = windowed[..window].to_vec();
            let expected = BollingerBands::compute(&windowed, dec!(2)).expect("full window");
            prop_assert_eq!(actual.middle, expected.middle);
            prop_assert_eq!(actual.upper, expected.upper);
            prop_assert_eq!(actual.lower, expected.lower);
        }
    }
}
