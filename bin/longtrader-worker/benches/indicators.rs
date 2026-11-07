//! Benchmarks for the worker's incremental `Decimal` indicators, fed with a
//! deterministic synthetic candle series the way a strategy feeds them.

use divan::{Bencher, black_box};
use longtrader_worker::indicators::{
    Adx, Atr, BollingerBands, Dema, DonchianChannel, Ema, KeltnerChannel, Rsi, Supertrend, ZScore,
    decimal_sqrt,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn main() {
    divan::main();
}

/// Number of candles fed to each indicator per iteration.
const CANDLES: usize = 1_000;

#[derive(Clone, Copy)]
struct Candle {
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

/// A deterministic random walk around 64,000 with two-decimal prices.
fn candles(len: usize) -> Vec<Candle> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut close: i64 = 6_400_000;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let step = ((state >> 33) % 2_001) as i64 - 1_000;
            let spread = ((state >> 13) % 1_500) as i64 + 100;
            close = (close + step).max(100_000);
            Candle {
                high: Decimal::new(close + spread, 2),
                low: Decimal::new(close - spread, 2),
                close: Decimal::new(close, 2),
            }
        })
        .collect()
}

#[divan::bench]
fn ema(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut ema = Ema::new(21);
        for c in black_box(&series) {
            ema.push(c.close);
        }
        ema.value()
    });
}

#[divan::bench]
fn dema(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut dema = Dema::new(21);
        for c in black_box(&series) {
            dema.push(c.close);
        }
        dema.value()
    });
}

#[divan::bench]
fn rsi(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut rsi = Rsi::new(14);
        for c in black_box(&series) {
            rsi.push(c.close);
        }
        rsi.value()
    });
}

#[divan::bench]
fn atr(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut atr = Atr::new(14);
        for c in black_box(&series) {
            atr.push(c.high, c.low, c.close);
        }
        atr.value()
    });
}

#[divan::bench]
fn adx(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut adx = Adx::new(14);
        for c in black_box(&series) {
            adx.push(c.high, c.low, c.close);
        }
        adx.value()
    });
}

#[divan::bench]
fn supertrend(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut st = Supertrend::new(10, dec!(3));
        for c in black_box(&series) {
            st.push(c.high, c.low, c.close);
        }
        st
    });
}

#[divan::bench]
fn keltner_channel(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut kc = KeltnerChannel::new(20, 10, dec!(2));
        for c in black_box(&series) {
            kc.push(c.high, c.low, c.close);
        }
        (kc.upper(), kc.lower())
    });
}

#[divan::bench]
fn donchian_channel(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut dc = DonchianChannel::new(20);
        let mut acc = Decimal::ZERO;
        for c in black_box(&series) {
            dc.push(c.high, c.low);
            acc += dc.middle().unwrap_or_default();
        }
        acc
    });
}

#[divan::bench]
fn zscore(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut z = ZScore::new(50);
        let mut acc = Decimal::ZERO;
        for c in black_box(&series) {
            z.push(c.close);
            acc += z.value().unwrap_or_default();
        }
        acc
    });
}

/// Bollinger bands recomputed on every candle, as poll-based strategies do.
#[divan::bench]
fn bollinger_bands(bencher: Bencher<'_, '_>) {
    let series = candles(CANDLES);
    bencher.bench_local(|| {
        let mut bb = BollingerBands::new(20, dec!(2));
        let mut acc = Decimal::ZERO;
        for c in black_box(&series) {
            bb.push(c.close);
            if let Some(bands) = bb.bands() {
                acc += bands.upper - bands.lower;
            }
        }
        acc
    });
}

#[divan::bench(args = ["2", "64120.25", "0.00012345", "79228162514264337593543950"])]
fn sqrt(bencher: Bencher<'_, '_>, value: &str) {
    let value: Decimal = value.parse().expect("valid decimal");
    bencher.bench_local(|| decimal_sqrt(black_box(value)));
}
