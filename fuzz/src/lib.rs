//! Fuzz-target crate for the longtrader workspace.
//!
//! This crate has no library of its own: `cargo fuzz` builds each file in
//! `fuzz_targets/` as a standalone binary that links `libfuzzer-sys`. The empty
//! library target exists only because Cargo requires the manifest to declare at
//! least one target.

#![forbid(unsafe_code)]