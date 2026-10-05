//! Compact incremental indicators for worker strategies.
//!
//! The worker intentionally does not depend on sibling-repo domain crates,
//! so it carries its own minimal indicator set (EMA / RSI / Bollinger /
//! ATR / DEMA / Supertrend) mirroring the engine-side shared indicator
//! set. All math is `Decimal`.

use std::collections::VecDeque;

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

/// Supertrend trend direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    Up,
    Down,
    #[default]
    Flat,
}

/// Supertrend stop-and-reverse line.
#[derive(Debug, Clone)]
pub struct Supertrend {
    atr: Atr,
    multiplier: Decimal,
    prev_close: Option<Decimal>,
    prev_upper: Option<Decimal>,
    prev_lower: Option<Decimal>,
    direction: Direction,
    value: Option<Decimal>,
}

impl Supertrend {
    #[must_use]
    pub fn new(period: usize, multiplier: Decimal) -> Self {
        Self {
            atr: Atr::new(period),
            multiplier,
            prev_close: None,
            prev_upper: None,
            prev_lower: None,
            direction: Direction::Flat,
            value: None,
        }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal, close: Decimal) {
        self.atr.push(high, low, close);
        let Some(atr) = self.atr.value() else {
            self.prev_close = Some(close);
            return;
        };
        let hl2 = (high + low) / Decimal::from(2);
        let mut basic_upper = hl2 + atr * self.multiplier;
        let mut basic_lower = hl2 - atr * self.multiplier;

        if let (Some(pu), Some(pc)) = (self.prev_upper, self.prev_close) &&
            !(pu < pc || close < pu)
        {
            basic_upper = pu;
        }
        if let (Some(pl), Some(pc)) = (self.prev_lower, self.prev_close) &&
            !(pl > pc || close > pl)
        {
            basic_lower = pl;
        }

        if let (Some(fu), Some(fl)) = (self.prev_upper, self.prev_lower) {
            let prev_dir =
                if self.direction == Direction::Flat { Direction::Up } else { self.direction };
            self.direction = match prev_dir {
                Direction::Down => {
                    if close > fu {
                        Direction::Up
                    } else {
                        Direction::Down
                    }
                }
                _ => {
                    if close < fl {
                        Direction::Down
                    } else {
                        Direction::Up
                    }
                }
            };
        } else {
            self.direction = Direction::Up;
        }

        self.value = Some(match self.direction {
            Direction::Down => basic_upper,
            _ => basic_lower,
        });
        self.prev_upper = Some(basic_upper);
        self.prev_lower = Some(basic_lower);
        self.prev_close = Some(close);
    }

    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
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
    fn bollinger_stateless_matches_incremental() {
        let closes = [dec!(1), dec!(2), dec!(3)];
        let mut boll = BollingerBands::new(3, dec!(2));
        for c in closes {
            boll.push(c);
        }
        assert_eq!(boll.bands(), BollingerBands::compute(&closes, dec!(2)));
    }

    #[test]
    fn supertrend_flips_on_crash() {
        let mut st = Supertrend::new(3, dec!(2));
        for (h, l, c) in [(11, 9, 10), (12, 10, 11), (13, 11, 12), (14, 12, 13)] {
            st.push(Decimal::from(h), Decimal::from(l), Decimal::from(c));
        }
        assert_eq!(st.direction(), Direction::Up);
        st.push(dec!(8), dec!(2), dec!(3));
        assert_eq!(st.direction(), Direction::Down);
    }

    // -----------------------------------------------------------------------
    // decimal_sqrt
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Ema
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Rsi
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Bollinger bands
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Atr
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Dema
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Supertrend
    // -----------------------------------------------------------------------

    #[test]
    fn direction_defaults_to_flat() {
        assert_eq!(Direction::default(), Direction::Flat);
    }

    #[test]
    fn supertrend_is_flat_and_valueless_before_the_atr_warms() {
        let mut st = Supertrend::new(5, dec!(3));
        assert_eq!(st.direction(), Direction::Flat);
        assert_eq!(st.value(), None);
        for _ in 0..4 {
            st.push(dec!(10), dec!(9), dec!(9.5));
        }
        assert_eq!(st.value(), None, "the ATR has not filled its window yet");
    }

    #[test]
    fn supertrend_reports_the_lower_band_while_up_and_the_upper_while_down() {
        let mut st = Supertrend::new(2, dec!(2));
        // Warm the ATR, then climb so the direction latches Up.
        for (h, l, c) in [
            (dec!(11), dec!(9), dec!(10)),
            (dec!(12), dec!(10), dec!(11)),
            (dec!(13), dec!(11), dec!(12)),
            (dec!(14), dec!(12), dec!(13)),
        ] {
            st.push(h, l, c);
        }
        let up = st.direction();
        assert_eq!(up, Direction::Up);
        let up_value = st.value().expect("a band is published");
        assert!(up_value < dec!(13), "the Up band trails below price, got {up_value}");

        // A crash below the previous lower band flips the direction to Down,
        // which publishes the upper band instead.
        st.push(dec!(3), dec!(1), dec!(2));
        assert_eq!(st.direction(), Direction::Down);
        let down_value = st.value().expect("a band is published");
        assert!(down_value > dec!(2), "the Down band sits above price, got {down_value}");
    }

    #[test]
    fn supertrend_never_reverts_to_flat_once_publishing() {
        let mut st = Supertrend::new(3, dec!(2));
        for i in 0..40 {
            // Alternate a sharp rally and a sharp crash so the direction flips.
            let base = 100;
            let close = if i % 4 < 2 { Decimal::from(base + i) } else { Decimal::from(base - i) };
            st.push(close + dec!(1), close - dec!(1), close);
            if i >= 3 {
                assert_ne!(
                    st.direction(),
                    Direction::Flat,
                    "step {i}: a warmed Supertrend must commit to a direction"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

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

        /// A warmed Supertrend always commits to Up or Down and always publishes
        /// a band value alongside it.
        #[test]
        fn supertrend_always_publishes_a_direction_and_a_band(
            period in 1usize..12,
            bars in prop::collection::vec((0i64..200, 0i64..200, 0i64..200), 1..40),
        ) {
            let mut st = Supertrend::new(period, dec!(2));
            let mut warm = false;
            for (high, low, close) in bars {
                st.push(Decimal::from(high), Decimal::from(low), Decimal::from(close));
                if st.value().is_some() {
                    warm = true;
                    prop_assert_ne!(st.direction(), Direction::Flat);
                }
            }
            let _ = warm;
        }

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
