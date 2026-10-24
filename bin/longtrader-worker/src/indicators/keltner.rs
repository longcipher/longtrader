//! Keltner channel: EMA ± ATR * multiplier.

use rust_decimal::Decimal;

use super::{atr::Atr, ema::Ema};

/// Keltner channel: EMA ± ATR * multiplier.
#[derive(Debug, Clone)]
pub struct KeltnerChannel {
    mid: Ema,
    atr: Atr,
    multiplier: Decimal,
}

impl KeltnerChannel {
    #[must_use]
    pub fn new(ema_window: usize, atr_period: usize, multiplier: Decimal) -> Self {
        Self { mid: Ema::new(ema_window), atr: Atr::new(atr_period), multiplier }
    }

    pub fn push(&mut self, high: Decimal, low: Decimal, close: Decimal) {
        self.mid.push(close);
        self.atr.push(high, low, close);
    }

    #[must_use]
    pub fn middle(&self) -> Option<Decimal> {
        self.mid.value()
    }

    #[must_use]
    pub fn upper(&self) -> Option<Decimal> {
        let mid = self.mid.value()?;
        let atr = self.atr.value()?;
        Some(mid + atr * self.multiplier)
    }

    #[must_use]
    pub fn lower(&self) -> Option<Decimal> {
        let mid = self.mid.value()?;
        let atr = self.atr.value()?;
        Some(mid - atr * self.multiplier)
    }
}
