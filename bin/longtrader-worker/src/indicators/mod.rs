//! Compact incremental indicators for worker strategies.
//!
//! The worker intentionally does not depend on sibling-repo domain crates,
//! so it carries its own minimal indicator set (EMA / RSI / Bollinger /
//! ATR / DEMA / Supertrend / Donchian / Keltner / ADX / Z-score) mirroring
//! the engine-side shared indicator set. All math is `Decimal`.
//!
//! Each indicator lives in its own submodule with its tests alongside it;
//! this module re-exports the full set so `crate::indicators::{Ema, ...}`
//! keeps working unchanged.

mod adx;
mod atr;
mod bollinger;
mod dema;
mod donchian;
mod ema;
mod keltner;
mod rsi;
mod sqrt;
mod supertrend;
mod zscore;

pub use adx::Adx;
pub use atr::Atr;
pub use bollinger::{Bands, BollingerBands};
pub use dema::Dema;
pub use donchian::DonchianChannel;
pub use ema::Ema;
pub use keltner::KeltnerChannel;
pub use rsi::Rsi;
pub use sqrt::decimal_sqrt;
pub use supertrend::{Direction, Supertrend};
pub use zscore::ZScore;
