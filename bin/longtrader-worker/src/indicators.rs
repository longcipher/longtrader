//! Compact incremental indicators for worker strategies.
//!
//! The worker intentionally does not depend on sibling-repo domain crates,
//! so it carries its own minimal indicator set (EMA / RSI / Bollinger /
//! ATR / DEMA / Supertrend) mirroring the engine-side shared indicator
//! set. All math is `Decimal`.

use std::collections::VecDeque;

use rust_decimal::Decimal;

/// Newton's-method square root on `Decimal`.
#[must_use]
pub fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let two = Decimal::from(2);
    let mut x = if value >= Decimal::ONE { value / two } else { Decimal::ONE };
    for _ in 0..64 {
        let next = (x + value / x) / two;
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
        Self { period, avg_gain: None, avg_loss: None, prev: None, samples: 0 }
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
    #[must_use]
    pub fn compute(closes: &[Decimal], mult: Decimal) -> Option<Bands> {
        let n = closes.len();
        if n < 2 {
            return None;
        }
        let count = Decimal::from(n);
        let mean = closes.iter().copied().sum::<Decimal>() / count;
        let variance = closes.iter().map(|&x| (x - mean) * (x - mean)).sum::<Decimal>() / count;
        let sd = decimal_sqrt(variance);
        Some(Bands { middle: mean, upper: mean + sd * mult, lower: mean - sd * mult })
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
        Self { rma_value: None, prev_close: None, period, samples: 0, sum: Decimal::ZERO }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal, close: Decimal) {
        let tr = match self.prev_close {
            None => high - low,
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
}
