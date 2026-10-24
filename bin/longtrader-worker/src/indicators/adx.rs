//! Average Directional Index with +DI / -DI.

use rust_decimal::Decimal;

/// Average Directional Index with +DI / -DI.
#[derive(Debug, Clone)]
pub struct Adx {
    period: usize,
    prev_high: Option<Decimal>,
    prev_low: Option<Decimal>,
    prev_close: Option<Decimal>,
    smooth_plus_dm: Option<Decimal>,
    smooth_minus_dm: Option<Decimal>,
    smooth_tr: Option<Decimal>,
    samples: usize,
    sum_plus_dm: Decimal,
    sum_minus_dm: Decimal,
    sum_tr: Decimal,
}

impl Adx {
    #[must_use]
    pub fn new(period: usize) -> Self {
        let period = period.max(1);
        Self {
            period,
            prev_high: None,
            prev_low: None,
            prev_close: None,
            smooth_plus_dm: None,
            smooth_minus_dm: None,
            smooth_tr: None,
            samples: 0,
            sum_plus_dm: Decimal::ZERO,
            sum_minus_dm: Decimal::ZERO,
            sum_tr: Decimal::ZERO,
        }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal, close: Decimal) {
        let (Some(ph), Some(pl)) = (self.prev_high, self.prev_low) else {
            self.prev_high = Some(high);
            self.prev_low = Some(low);
            self.prev_close = Some(close);
            return;
        };
        let up = high - ph;
        let down = pl - low;
        let plus_dm = if up > down && up > Decimal::ZERO { up } else { Decimal::ZERO };
        let minus_dm = if down > up && down > Decimal::ZERO { down } else { Decimal::ZERO };
        let tr = (high - low)
            .max((high - self.prev_close.expect("prev_close set after first push")).abs())
            .max((low - self.prev_close.expect("prev_close set after first push")).abs());
        let n = Decimal::from(self.period);

        if let (Some(sp), Some(sm), Some(st)) =
            (self.smooth_plus_dm, self.smooth_minus_dm, self.smooth_tr)
        {
            self.smooth_plus_dm = Some(sp - sp / n + plus_dm);
            self.smooth_minus_dm = Some(sm - sm / n + minus_dm);
            self.smooth_tr = Some(st - st / n + tr);
        } else {
            self.sum_plus_dm += plus_dm;
            self.sum_minus_dm += minus_dm;
            self.sum_tr += tr;
            self.samples += 1;
            if self.samples >= self.period {
                self.smooth_plus_dm = Some(self.sum_plus_dm / n);
                self.smooth_minus_dm = Some(self.sum_minus_dm / n);
                self.smooth_tr = Some(self.sum_tr / n);
            }
        }
        self.prev_high = Some(high);
        self.prev_low = Some(low);
        self.prev_close = Some(close);
    }

    #[must_use]
    pub fn value(&self) -> Option<Decimal> {
        let tr = self.smooth_tr?;
        if tr.is_zero() {
            return Some(Decimal::ZERO);
        }
        let plus_di = self.smooth_plus_dm? / tr * Decimal::from(100);
        let minus_di = self.smooth_minus_dm? / tr * Decimal::from(100);
        let denom = plus_di + minus_di;
        if denom.is_zero() {
            return Some(Decimal::ZERO);
        }
        Some((plus_di - minus_di).abs() / denom * Decimal::from(100))
    }

    #[must_use]
    pub fn plus_di(&self) -> Option<Decimal> {
        let tr = self.smooth_tr?;
        if tr.is_zero() {
            return Some(Decimal::ZERO);
        }
        Some(self.smooth_plus_dm? / tr * Decimal::from(100))
    }

    #[must_use]
    pub fn minus_di(&self) -> Option<Decimal> {
        let tr = self.smooth_tr?;
        if tr.is_zero() {
            return Some(Decimal::ZERO);
        }
        Some(self.smooth_minus_dm? / tr * Decimal::from(100))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// ADX non-negative once warmed.
        #[test]
        fn adx_is_non_negative(
            period in 1usize..12,
            bars in prop::collection::vec((0i64..200, 0i64..200, 0i64..200), 2..40),
        ) {
            let mut adx = Adx::new(period);
            for (h, l, c) in bars {
                adx.push(Decimal::from(h), Decimal::from(l), Decimal::from(c));
                if let Some(value) = adx.value() {
                    prop_assert!(value >= Decimal::ZERO, "adx {value} was negative");
                }
            }
        }
    }
}
