//! Connect streaming envelope helpers, re-exported from the contract crate.
//!
//! The implementation (and its tests) lives in
//! [`longtrader_contract::envelope`]: the worker, the native/WASM proto
//! clients and the fuzz targets all frame the same five bytes, and a single
//! owner is what keeps them from drifting — the end-of-stream flag used to be
//! ignored by two hand-rolled copies of this decoder.
//!
//! `connectrpc` handles framing internally on the serving side; these helpers
//! exist so bare-protocol clients (and tests) can encode/decode without
//! re-implementing.

pub use longtrader_contract::envelope::{
    EnvelopeError, FLAG_EOS, FLAG_MESSAGE, decode_envelope, decode_envelope_result, encode_envelope,
};
