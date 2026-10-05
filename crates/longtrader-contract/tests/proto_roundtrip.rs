//! Wire round-trip coverage for the highest-traffic contract messages.
//!
//! This guards the WIRE CONTRACT, not the code generator. Every message below
//! is encoded with the protobuf binary codec and decoded back, then compared
//! against the original with the generated `PartialEq` impls. A failure here
//! means a field number, wire type, presence rule, or enum encoding changed in
//! a way the rest of the workspace — and every SDK generated from these
//! `.proto` files — would silently misinterpret.
//!
//! Which trait to use: the generated types implement [`buffa::Message`], not
//! `prost::Message`. `connectrpc-build` generates against buffa, and there is no
//! prost dependency anywhere in this crate; `prost` was never an option here.

#![forbid(unsafe_code)]
// The whole file is about generated structs, whose field-by-field construction
// would otherwise drown the output in pedantic noise. This mirrors the
// allowance `src/lib.rs` already carries for the generated module itself.
#![allow(clippy::pedantic)]

use std::fmt::Debug;

use buffa::{EnumValue, Message, MessageField};
use buffa_types::google::protobuf::{Duration, Struct, Timestamp, Value, value::Kind};
use longtrader_contract::{
    ext::{common_to_decimal, decimal_to_common},
    proto::longtrader::{
        account::v1 as account, common::v1 as common, market::v1 as market, ops::v1 as ops,
        terminal::v1 as terminal_v1, trading::v1 as trading, worker::v1 as worker,
    },
};
use proptest::prelude::*;
use rust_decimal::Decimal;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Encode `message` with the protobuf binary codec and decode it back.
fn roundtrip<M: Message>(message: &M) -> M {
    let bytes = message.encode_to_vec();
    M::decode_from_slice(&bytes).expect("buffa-encoded bytes must decode")
}

/// Assert `message` survives an encode/decode cycle unchanged.
fn assert_wire_roundtrip<M: Message + Debug>(message: &M) {
    let bytes = message.encode_to_vec();
    let decoded = M::decode_from_slice(&bytes).expect("buffa-encoded bytes must decode");
    assert_eq!(&decoded, message, "wire round trip must be lossless");
}

// ---- Fully-populated leaf builders ----------------------------------------

/// A contract decimal for `mantissa * 10^-scale`, built the way a writer does.
fn decimal(mantissa: i64, scale: i32) -> common::Decimal {
    let scale = u32::try_from(scale).expect("these fixtures never use a negative scale");
    longtrader_contract::ext::decimal_to_common(rust_decimal::Decimal::new(mantissa, scale))
}

/// A wire decimal carrying an arbitrary payload, valid or not.
fn decimal_payload(text: &str) -> common::Decimal {
    common::Decimal { value: text.to_string(), ..Default::default() }
}

fn timestamp(seconds: i64, nanos: i32) -> Timestamp {
    Timestamp { seconds, nanos, ..Default::default() }
}

fn duration(seconds: i64, nanos: i32) -> Duration {
    Duration { seconds, nanos, ..Default::default() }
}

fn exchange_id(id: &str, label: &str) -> common::ExchangeId {
    common::ExchangeId { id: id.to_string(), label: label.to_string(), ..Default::default() }
}

fn event_header() -> common::EventHeader {
    common::EventHeader {
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        sequence: 4_294_967_296,
        exchange_time_ns: 1_726_000_000_123_456_789,
        gateway_in_time_ns: 1_726_000_000_223_456_789,
        local_dispatch_time_ns: 1_726_000_000_323_456_789,
        ..Default::default()
    }
}

fn price_level(price: i64, amount: i64) -> market::PriceLevel {
    market::PriceLevel {
        price: MessageField::some(decimal(price, 2)),
        amount: MessageField::some(decimal(amount, 6)),
        ..Default::default()
    }
}

// ---- google.protobuf.Struct helpers (ops examples) -------------------------

fn string_value(raw: &str) -> Value {
    Value { kind: Some(Kind::StringValue(raw.to_string())), ..Default::default() }
}

fn number_value(raw: f64) -> Value {
    Value { kind: Some(Kind::NumberValue(raw)), ..Default::default() }
}

fn bool_value(raw: bool) -> Value {
    Value { kind: Some(Kind::BoolValue(raw)), ..Default::default() }
}

/// Build a `Struct` by inserting into its `map<string, Value>` field.
fn proto_struct(entries: &[(&str, Value)]) -> Struct {
    let mut params = Struct::default();
    for (key, value) in entries {
        params.fields.insert((*key).to_string(), value.clone());
    }
    params
}

// ---------------------------------------------------------------------------
// common.v1 — the shared numeric / paging / tracing types
// ---------------------------------------------------------------------------

#[test]
fn decimal_round_trips_as_a_single_value() {
    // An ordinary price.
    assert_wire_roundtrip(&decimal(123_456_789, 8));
    // A 96-bit mantissa that an int64 could never have carried. Under the old
    // dual representation this was the case that forced the `raw_str` fallback;
    // now it is just a value like any other.
    assert_wire_roundtrip(&decimal_payload("79228162514264337593543950335"));
    assert_wire_roundtrip(&decimal_payload("-79228162514264337593543950335"));
    // The smallest and largest scales a decimal can hold.
    assert_wire_roundtrip(&decimal_payload("0.0000000000000000000000000001"));
}

/// The wire is lossless and the domain decoder is strict, and those are two
/// different claims. A payload the decoder refuses must still cross the byte
/// for byte, so a peer can see exactly what was sent instead of a mangled
/// version of it.
#[test]
fn a_payload_the_decoder_refuses_still_round_trips_byte_for_byte() {
    let refused: Vec<String> = [
        "1_000", // digit separator
        "1e3",   // exponent
        "+1",    // leading plus
        ".5",    // no integer part
        "1.",    // no fraction digits
        " 1",    // leading whitespace
        "",      // never populated
    ]
    .into_iter()
    .map(String::from)
    .chain(["9".repeat(40)]) // wider than a decimal
    .collect();
    for payload in refused {
        assert_wire_roundtrip(&decimal_payload(&payload));
        assert!(
            longtrader_contract::ext::common_to_decimal(&decimal_payload(&payload)).is_err(),
            "{payload:?} is outside the contract grammar and must not decode"
        );
    }
}

/// Presence, and only presence, reports "no value". A present message carrying a
/// zero is a value; an untouched message is the one thing that is not — and the
/// two must stay distinguishable on the wire.
#[test]
fn a_present_zero_is_distinguishable_from_an_untouched_message() {
    let zero = decimal_payload("0");
    let untouched = common::Decimal::default();
    assert_eq!(longtrader_contract::ext::common_to_decimal(&zero), Ok(Decimal::ZERO));
    assert!(longtrader_contract::ext::common_to_decimal(&untouched).is_err());

    assert_wire_roundtrip(&zero);
    assert_ne!(
        zero.encode_to_vec(),
        untouched.encode_to_vec(),
        "a present zero and an untouched message must not collapse into the same bytes"
    );
}

#[test]
fn exchange_id_round_trips() {
    assert_wire_roundtrip(&exchange_id("binance-futures", "Binance USD-M Futures"));
}

#[test]
fn pagination_round_trips() {
    assert_wire_roundtrip(&common::Pagination {
        limit: 1_000,
        since: 1_726_000_000_000,
        cursor: "eyJvIjoxLCJhIjoyfQ".to_string(),
        ..Default::default()
    });
}

#[test]
fn event_header_round_trips() {
    assert_wire_roundtrip(&event_header());
}

// ---------------------------------------------------------------------------
// market.v1 — the highest-volume data path
// ---------------------------------------------------------------------------

#[test]
fn ticker_round_trips() {
    let ticker = market::Ticker {
        header: MessageField::some(event_header()),
        symbol: "BTC/USDT".to_string(),
        timestamp: MessageField::some(timestamp(1_726_000_000, 250_000_000)),
        bid: MessageField::some(decimal(64_120_000, 2)),
        bid_volume: MessageField::some(decimal(1_500_000, 8)),
        ask: MessageField::some(decimal(64_120_500, 2)),
        ask_volume: MessageField::some(decimal(2_500_000, 8)),
        last: MessageField::some(decimal(64_120_250, 2)),
        high: MessageField::some(decimal(65_000_000, 2)),
        low: MessageField::some(decimal(63_000_000, 2)),
        open: MessageField::some(decimal(64_000_000, 2)),
        close: MessageField::some(decimal(64_120_250, 2)),
        base_volume: MessageField::some(decimal(18_250_500_000, 8)),
        quote_volume: MessageField::some(decimal(1_169_500_123, 2)),
        change: MessageField::some(decimal(120_250, 2)),
        percentage: MessageField::some(decimal(1_879, 4)),
        vwap: MessageField::some(decimal(64_090_000, 2)),
        average: MessageField::some(decimal(64_100_000, 2)),
        ..Default::default()
    };
    assert_wire_roundtrip(&ticker);
}

#[test]
fn price_level_round_trips() {
    assert_wire_roundtrip(&price_level(64_120_000, 325_000_000));
}

#[test]
fn order_book_round_trips() {
    // Two bid levels and two ask levels, so the repeated length-delimited
    // element encoding (varint length prefix per element) is exercised more than
    // once per side.
    let book = market::OrderBook {
        header: MessageField::some(event_header()),
        symbol: "ETH/USDT".to_string(),
        timestamp: MessageField::some(timestamp(1_726_000_001, 0)),
        bids: vec![price_level(3_450_000, 12_500_000), price_level(3_449_500, 8_250_000)],
        asks: vec![price_level(3_450_500, 9_750_000), price_level(3_451_000, 15_000_000)],
        ..Default::default()
    };
    assert_wire_roundtrip(&book);
}

#[test]
fn candle_round_trips() {
    let candle = market::Candle {
        timestamp_ms: 1_726_000_000_000,
        open: MessageField::some(decimal(64_000_000, 2)),
        high: MessageField::some(decimal(65_000_000, 2)),
        low: MessageField::some(decimal(63_000_000, 2)),
        close: MessageField::some(decimal(64_120_250, 2)),
        volume: MessageField::some(decimal(18_250_500_000, 8)),
        ..Default::default()
    };
    assert_wire_roundtrip(&candle);
}

#[test]
fn ohlcv_round_trips() {
    let ohlcv = market::OHLCV {
        header: MessageField::some(event_header()),
        timestamp: MessageField::some(timestamp(1_726_000_000, 999_000_000)),
        open: MessageField::some(decimal(64_000_000, 2)),
        high: MessageField::some(decimal(65_000_000, 2)),
        low: MessageField::some(decimal(63_000_000, 2)),
        close: MessageField::some(decimal(64_120_250, 2)),
        volume: MessageField::some(decimal(18_250_500_000, 8)),
        ..Default::default()
    };
    assert_wire_roundtrip(&ohlcv);
}

#[test]
fn funding_rate_round_trips() {
    // Every `optional` Decimal and the `optional ExchangeId` are populated:
    // proto3 `optional` fields carry explicit presence, so a dropped one here
    // would be a presence-tracking bug rather than a value bug.
    let rate = market::FundingRate {
        symbol: "BTC/USDT".to_string(),
        rate: MessageField::some(decimal(1, 4)),
        next_funding_time_ms: 1_726_012_800_000,
        mark_price: MessageField::some(decimal(64_118_400, 2)),
        funding_interval_hours: Some(8),
        open_interest: MessageField::some(decimal(18_250_500_000, 8)),
        volume_24h: MessageField::some(decimal(1_169_500_123, 2)),
        exchange_id: MessageField::some(exchange_id("binance-futures", "Binance USD-M")),
        ..Default::default()
    };
    assert_wire_roundtrip(&rate);
}

/// `funding_interval_hours` was a bare `uint32` whose documented "0 when
/// unknown" collided with a venue that genuinely settles every 0 hours.
/// Presence is the fix, so the three cases a reader must tell apart have to
/// survive the wire distinctly: absent, a real zero, and a real interval.
#[test]
fn a_funding_interval_of_zero_is_distinguishable_from_an_absent_one() {
    let base = market::FundingRate {
        symbol: "BTC/USDT".to_string(),
        rate: MessageField::some(decimal(1, 4)),
        next_funding_time_ms: 1_726_012_800_000,
        ..Default::default()
    };

    let absent = base.clone();
    let zero = market::FundingRate { funding_interval_hours: Some(0), ..base.clone() };
    let eight = market::FundingRate { funding_interval_hours: Some(8), ..base.clone() };

    let read_back = |wire: &market::FundingRate| {
        let bytes = wire.encode_to_vec();
        let decoded = market::FundingRate::decode_from_slice(&bytes).expect("the message decodes");
        decoded.funding_interval_hours
    };

    assert_eq!(read_back(&absent), None, "an unreported interval stays absent");
    assert_eq!(read_back(&zero), Some(0), "a reported zero must not collapse into absent");
    assert_eq!(read_back(&eight), Some(8));
    assert_ne!(
        absent.encode_to_vec(),
        zero.encode_to_vec(),
        "presence is the whole point: absent and zero must differ on the wire"
    );
}

// ---------------------------------------------------------------------------
// trading.v1 — the money path
// ---------------------------------------------------------------------------

#[test]
fn order_round_trips() {
    let mut order = trading::Order {
        id: "981_554_991_337".to_string(),
        client_order_id: "grid-42-000117".to_string(),
        symbol: "BTC/USDT".to_string(),
        r#type: EnumValue::Known(trading::OrderType::Limit),
        side: EnumValue::Known(trading::OrderSide::Buy),
        status: EnumValue::Known(trading::OrderStatus::Open),
        amount: MessageField::some(decimal(250, 6)),
        price: MessageField::some(decimal(64_100_000, 2)),
        filled: MessageField::some(decimal(120, 6)),
        remaining: MessageField::some(decimal(130, 6)),
        cost: MessageField::some(decimal(769_200_000, 8)),
        average: MessageField::some(decimal(64_100_000, 2)),
        fee: MessageField::some(decimal(384, 8)),
        fee_currency: "USDT".to_string(),
        time_in_force: EnumValue::Known(trading::TimeInForce::Gtc),
        timestamp: MessageField::some(timestamp(1_726_000_002, 500_000_000)),
        last_trade_timestamp: MessageField::some(timestamp(1_726_000_003, 750_000_000)),
        post_only: true,
        reduce_only: false,
        info: Default::default(),
        created_at: MessageField::some(timestamp(1_726_000_002, 0)),
        updated_at: MessageField::some(timestamp(1_726_000_003, 750_000_000)),
        take_profit: MessageField::some(decimal(66_000_000, 2)),
        stop_loss: MessageField::some(decimal(62_500_000, 2)),
        ..Default::default()
    };
    // `info` is `map<string, string>`; insert after construction so the test
    // never has to name buffa's doc-hidden `HashMap` alias.
    order.info.insert("venue".to_string(), "binance".to_string());
    order.info.insert("liquidity".to_string(), "maker".to_string());
    assert_wire_roundtrip(&order);
}

#[test]
fn position_round_trips() {
    let position = trading::Position {
        id: "pos-8831".to_string(),
        symbol: "BTC/USDT".to_string(),
        contracts: MessageField::some(decimal(1_250, 6)),
        contract_size: MessageField::some(decimal(1, 8)),
        side: EnumValue::Known(trading::OrderSide::Sell),
        entry_price: MessageField::some(decimal(64_100_000, 2)),
        mark_price: MessageField::some(decimal(64_118_400, 2)),
        unrealized_pnl: MessageField::some(decimal(23_000_000, 8)),
        order_id: Some("981_554_991_337".to_string()),
        current_price: MessageField::some(decimal(64_120_250, 2)),
        timestamp: MessageField::some(timestamp(1_726_000_004, 125_000_000)),
        take_profit: MessageField::some(decimal(63_000_000, 2)),
        stop_loss: MessageField::some(decimal(65_000_000, 2)),
        opened_at: MessageField::some(timestamp(1_726_000_000, 0)),
        ..Default::default()
    };
    assert_wire_roundtrip(&position);
}

#[test]
fn create_order_request_round_trips() {
    let mut inner = trading::OrderRequest {
        client_order_id: "grid-42-000118".to_string(),
        symbol: "BTC/USDT".to_string(),
        r#type: EnumValue::Known(trading::OrderType::Limit),
        side: EnumValue::Known(trading::OrderSide::Sell),
        amount: MessageField::some(decimal(250, 6)),
        price: MessageField::some(decimal(65_000_000, 2)),
        trigger_price: MessageField::some(decimal(64_900_000, 2)),
        time_in_force: EnumValue::Known(trading::TimeInForce::Gtd),
        post_only: true,
        reduce_only: true,
        params: Default::default(),
        take_profit: MessageField::some(decimal(66_000_000, 2)),
        stop_loss: MessageField::some(decimal(62_500_000, 2)),
        ..Default::default()
    };
    inner.params.insert("grid_level".to_string(), "7".to_string());
    inner.params.insert("strategy".to_string(), "grid_maker".to_string());

    let request = trading::CreateOrderRequest {
        exchange_id: MessageField::some(exchange_id("binance-futures", "Binance USD-M")),
        order: MessageField::some(inner),
        session_id: "sess_01HQ8Z3".to_string(),
        ..Default::default()
    };
    assert_wire_roundtrip(&request);
}

#[test]
fn trigger_order_round_trips() {
    let trigger = trading::TriggerOrder {
        id: "trg-5512".to_string(),
        client_order_id: "stop-42-000009".to_string(),
        symbol: "BTC/USDT".to_string(),
        side: EnumValue::Known(trading::OrderSide::Sell),
        trigger_price: MessageField::some(decimal(63_000_000, 2)),
        qty: MessageField::some(decimal(250, 6)),
        trigger_type: EnumValue::Known(trading::TriggerPriceType::Mark),
        order_price: MessageField::some(decimal(62_900_000, 2)),
        order_type: EnumValue::Known(trading::OrderType::Limit),
        reduce_only: true,
        status: EnumValue::Known(trading::TriggerOrderStatus::Triggered),
        created_at: MessageField::some(timestamp(1_726_000_005, 0)),
        order_id: Some("981_554_991_999".to_string()),
        triggered_at: MessageField::some(timestamp(1_726_003_600, 0)),
        ..Default::default()
    };
    assert_wire_roundtrip(&trigger);
}

// ---------------------------------------------------------------------------
// worker.v1 — session control plane
// ---------------------------------------------------------------------------

#[test]
fn kill_switch_policy_round_trips() {
    let policy = worker::KillSwitchPolicy {
        lease_timeout: MessageField::some(duration(45, 500_000_000)),
        scope: EnumValue::Known(worker::kill_switch_policy::Scope::AllOrders),
        ..Default::default()
    };
    assert_wire_roundtrip(&policy);
}

#[test]
fn attach_session_request_round_trips() {
    let request = worker::AttachSessionRequest {
        token: "ltd_live_9f2c41ab7d".to_string(),
        client_name: "python-sdk".to_string(),
        client_version: "0.2.0".to_string(),
        policy: MessageField::some(worker::KillSwitchPolicy {
            lease_timeout: MessageField::some(duration(45, 500_000_000)),
            scope: EnumValue::Known(worker::kill_switch_policy::Scope::SessionOrders),
            ..Default::default()
        }),
        session_id: "sess_01HQ8Z3".to_string(),
        ..Default::default()
    };
    assert_wire_roundtrip(&request);
}

// ---------------------------------------------------------------------------
// ops.v1 — the schema-driven venue-operations registry
// ---------------------------------------------------------------------------

#[test]
fn op_descriptor_round_trips() {
    let descriptor = ops::OpDescriptor {
        name: "transfer".to_string(),
        category: "wallet".to_string(),
        summary: "Move funds between accounts on one venue.".to_string(),
        mutating: true,
        params: vec![param_descriptor(), param_descriptor()],
        examples: vec![
            ops::Example {
                summary: "Spot to futures".to_string(),
                params: MessageField::some(proto_struct(&[
                    ("asset", string_value("USDT")),
                    ("amount", number_value(1_250.5)),
                    ("from", string_value("spot")),
                    ("to", string_value("futures")),
                ])),
                ..Default::default()
            },
            ops::Example {
                summary: "Internal transfer with a flag".to_string(),
                params: MessageField::some(proto_struct(&[
                    ("asset", string_value("BTC")),
                    ("amount", number_value(0.75)),
                    ("internal", bool_value(true)),
                ])),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert_wire_roundtrip(&descriptor);
}

#[test]
fn param_descriptor_round_trips() {
    assert_wire_roundtrip(&param_descriptor());
}

/// A `ParamDescriptor` with every field populated, including the
/// `repeated string enum_values` that only applies to `ParamType::Enum`.
fn param_descriptor() -> ops::ParamDescriptor {
    ops::ParamDescriptor {
        name: "asset".to_string(),
        r#type: EnumValue::Known(ops::ParamType::Enum),
        required: true,
        default: "USDT".to_string(),
        doc: "Settlement asset.".to_string(),
        enum_values: vec![
            "USDT".to_string(),
            "USDC".to_string(),
            "BTC".to_string(),
            "ETH".to_string(),
        ],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// terminal.v1 — schema-driven strategy configuration
// ---------------------------------------------------------------------------

#[test]
fn strategy_descriptor_round_trips() {
    let descriptor = terminal_v1::StrategyDescriptor {
        id: "grid_maker".to_string(),
        name: "Grid Maker".to_string(),
        note: "Layer resting orders from lower to upper bound.".to_string(),
        builtin: true,
        params: vec![param_slot(), param_slot()],
        kind: "maker".to_string(),
        ..Default::default()
    };
    assert_wire_roundtrip(&descriptor);
}

#[test]
fn param_slot_round_trips() {
    assert_wire_roundtrip(&param_slot());
}

/// A `ParamSlot` with all four proto3 `optional double` fields populated, so
/// explicit presence on optional scalars is exercised.
fn param_slot() -> terminal_v1::ParamSlot {
    terminal_v1::ParamSlot {
        label: "levels".to_string(),
        dimension: "decimal".to_string(),
        min: Some(1.0),
        max: Some(200.0),
        step: Some(0.5),
        default: Some(20.0),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// account.v1 — wallet state
// ---------------------------------------------------------------------------

#[test]
fn balance_round_trips() {
    let balance = account::Balance {
        currency: "USDT".to_string(),
        free: MessageField::some(decimal(12_500_000_000, 8)),
        used: MessageField::some(decimal(1_600_250_000, 8)),
        total: MessageField::some(decimal(14_100_250_000, 8)),
        ..Default::default()
    };
    assert_wire_roundtrip(&balance);
}

#[test]
fn ledger_entry_round_trips() {
    let entry = account::LedgerEntry {
        id: "led-20481".to_string(),
        currency: "USDT".to_string(),
        direction: "in".to_string(),
        r#type: "deposit".to_string(),
        amount: MessageField::some(decimal(25_000_000, 8)),
        timestamp: MessageField::some(timestamp(1_726_000_006, 42_000_000)),
        status: "ok".to_string(),
        ..Default::default()
    };
    assert_wire_roundtrip(&entry);
}

// ---------------------------------------------------------------------------
// Property test: the numeric contract's highest-risk payload
// ---------------------------------------------------------------------------

/// Strategy over the representable `rust_decimal` space: sign x 96-bit mantissa
/// (two i64 halves) x scale. Deliberately duplicated from the `arb_decimal`
/// helper in `src/ext.rs` rather than shared, so this integration test needs no
/// change to production or in-crate test code.
fn arb_decimal() -> impl Strategy<Value = Decimal> {
    (proptest::bool::ANY, proptest::num::i64::ANY, proptest::num::i64::ANY, 0u32..=28).prop_map(
        |(negative, hi, lo, scale)| {
            let bits = ((hi as u64 as u128) << 64) | (lo as u64 as u128);
            let magnitude = if negative { -(bits as i128) } else { bits as i128 };
            Decimal::try_from_i128_with_scale(magnitude, scale).unwrap_or(Decimal::ZERO)
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// End to end for the numeric contract: `rust_decimal` -> contract decimal
    /// -> protobuf bytes -> contract decimal -> `rust_decimal` must be the
    /// identity. This is the one payload where a silent precision loss would
    /// move real money, so it gets a property test rather than fixed vectors.
    #[test]
    fn decimal_survives_the_wire(value in arb_decimal()) {
        let wire = decimal_to_common(value);
        let decoded_wire = roundtrip(&wire);
        prop_assert!(decoded_wire == wire, "the wire form must be a fixed point");

        let decoded = common_to_decimal(&decoded_wire).expect("writer output always decodes");
        prop_assert_eq!(decoded, value, "the wire must preserve the decimal exactly");
    }
}
