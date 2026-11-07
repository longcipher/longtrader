//! Benchmarks for the contract hot paths: the Connect envelope framing, the
//! decimal wire codec, and protobuf encode/decode of high-traffic messages.

use buffa::{Message, MessageField};
use buffa_types::google::protobuf::Timestamp;
use divan::{Bencher, black_box};
use longtrader_contract::{
    envelope::{FLAG_MESSAGE, decode_envelope_result, encode_envelope},
    ext::{common_to_decimal, decimal_to_common},
    proto::longtrader::{common::v1 as common, market::v1 as market},
};
use rust_decimal::Decimal;

fn main() {
    divan::main();
}

const PAYLOAD_SIZES: &[usize] = &[64, 1024, 64 * 1024];
const BOOK_DEPTHS: &[usize] = &[20, 200];

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn decimal(mantissa: i64, scale: u32) -> common::Decimal {
    decimal_to_common(Decimal::new(mantissa, scale))
}

fn order_book(depth: usize) -> market::OrderBook {
    let level = |price: i64, amount: i64| market::PriceLevel {
        price: MessageField::some(decimal(price, 2)),
        amount: MessageField::some(decimal(amount, 6)),
        ..Default::default()
    };
    let depth = depth as i64;
    market::OrderBook {
        header: MessageField::some(common::EventHeader {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            sequence: 4_294_967_296,
            exchange_time_ns: 1_726_000_000_123_456_789,
            gateway_in_time_ns: 1_726_000_000_223_456_789,
            local_dispatch_time_ns: 1_726_000_000_323_456_789,
            ..Default::default()
        }),
        symbol: "ETH/USDT".to_string(),
        timestamp: MessageField::some(Timestamp {
            seconds: 1_726_000_001,
            nanos: 0,
            ..Default::default()
        }),
        bids: (0..depth).map(|i| level(3_450_000 - i * 50, 12_500_000 + i * 1_000)).collect(),
        asks: (0..depth).map(|i| level(3_450_050 + i * 50, 9_750_000 + i * 1_000)).collect(),
        ..Default::default()
    }
}

mod envelope {
    use super::{
        Bencher, FLAG_MESSAGE, PAYLOAD_SIZES, black_box, decode_envelope_result, encode_envelope,
        payload,
    };

    #[divan::bench(args = PAYLOAD_SIZES)]
    fn encode(bencher: Bencher<'_, '_>, len: usize) {
        let data = payload(len);
        bencher.bench_local(|| encode_envelope(FLAG_MESSAGE, black_box(&data)));
    }

    #[divan::bench(args = PAYLOAD_SIZES)]
    fn decode(bencher: Bencher<'_, '_>, len: usize) {
        let framed = encode_envelope(FLAG_MESSAGE, &payload(len));
        bencher.bench_local(|| {
            decode_envelope_result(black_box(&framed)).map(|(_, p, n)| (p.len(), n))
        });
    }

    /// Walk a buffer holding many small frames, as a streaming client does.
    #[divan::bench]
    fn decode_stream_of_256_frames(bencher: Bencher<'_, '_>) {
        let mut stream = Vec::new();
        for i in 0..256 {
            stream.extend_from_slice(&encode_envelope(FLAG_MESSAGE, &payload(32 + i % 64)));
        }
        bencher.bench_local(|| {
            let buf = black_box(&stream);
            let mut cursor = 0;
            let mut frames = 0usize;
            while cursor < buf.len() {
                let (_, _, consumed) =
                    decode_envelope_result(&buf[cursor..]).expect("valid stream");
                cursor += consumed;
                frames += 1;
            }
            frames
        });
    }
}

mod decimal_codec {
    use super::{Bencher, Decimal, black_box, common, common_to_decimal, decimal_to_common};

    const VALUES: &[&str] = &[
        "0",
        "64120.25",
        "-0.00012345",
        "79228162514264337593543950335",
        "0.0000000000000000000000000001",
    ];

    #[divan::bench(args = VALUES)]
    fn encode(bencher: Bencher<'_, '_>, value: &str) {
        let value: Decimal = value.parse().expect("valid decimal");
        bencher.bench_local(|| decimal_to_common(black_box(value)));
    }

    #[divan::bench(args = VALUES)]
    fn decode(bencher: Bencher<'_, '_>, value: &str) {
        let wire = common::Decimal { value: value.to_string(), ..Default::default() };
        bencher.bench_local(|| common_to_decimal(black_box(&wire)));
    }

    #[divan::bench]
    fn decode_rejects_malformed(bencher: Bencher<'_, '_>) {
        let wire = common::Decimal { value: "1_000.5e3".to_string(), ..Default::default() };
        bencher.bench_local(|| common_to_decimal(black_box(&wire)).is_err());
    }
}

mod order_book {
    use super::{
        BOOK_DEPTHS, Bencher, Decimal, FLAG_MESSAGE, Message, black_box, common_to_decimal,
        decode_envelope_result, encode_envelope, market, order_book,
    };

    #[divan::bench(args = BOOK_DEPTHS)]
    fn encode(bencher: Bencher<'_, '_>, depth: usize) {
        let book = order_book(depth);
        bencher.bench_local(|| black_box(&book).encode_to_vec());
    }

    #[divan::bench(args = BOOK_DEPTHS)]
    fn decode(bencher: Bencher<'_, '_>, depth: usize) {
        let bytes = order_book(depth).encode_to_vec();
        bencher.bench_local(|| {
            market::OrderBook::decode_from_slice(black_box(&bytes)).expect("valid order book")
        });
    }

    /// Decode a framed book and convert every level to domain decimals, the
    /// full path a strategy pays for each streamed book update.
    #[divan::bench(args = BOOK_DEPTHS)]
    fn decode_framed_to_decimals(bencher: Bencher<'_, '_>, depth: usize) {
        let framed = encode_envelope(FLAG_MESSAGE, &order_book(depth).encode_to_vec());
        bencher.bench_local(|| {
            let (_, body, _) = decode_envelope_result(black_box(&framed)).expect("valid frame");
            let book = market::OrderBook::decode_from_slice(body).expect("valid order book");
            book.bids
                .iter()
                .chain(book.asks.iter())
                .filter_map(|level| level.price.as_option())
                .map(|price| common_to_decimal(price).expect("valid price"))
                .fold(Decimal::ZERO, |acc, price| acc + price)
        });
    }
}
