//! Supertrend stop-and-reverse line.

use rust_decimal::Decimal;

use super::atr::Atr;

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
    fn supertrend_flips_on_crash() {
        let mut st = Supertrend::new(3, dec!(2));
        for (h, l, c) in [(11, 9, 10), (12, 10, 11), (13, 11, 12), (14, 12, 13)] {
            st.push(Decimal::from(h), Decimal::from(l), Decimal::from(c));
        }
        assert_eq!(st.direction(), Direction::Up);
        st.push(dec!(8), dec!(2), dec!(3));
        assert_eq!(st.direction(), Direction::Down);
    }

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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

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
    }
}
