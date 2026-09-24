//! Shared constants and URL helpers for Connect-RPC clients.
//! Single owner is `crate::service_name`; these re-exports keep call sites short.

pub(crate) use crate::service_name::{
    MARKET_DATA as SERVICE_MARKET, RUNTIME as SERVICE_RUNTIME, STRATEGY as SERVICE_STRATEGY,
    TRADING as SERVICE_TRADING,
};

/// Trim trailing slashes from a base URL.
#[must_use]
pub(crate) fn trim_base_url(base_url: &str) -> String {
    base_url.trim_end_matches('/').to_string()
}

/// Build a Connect path `/{service}/{method}`.
#[must_use]
pub(crate) fn service_url(base_url: &str, service: &str, method: &str) -> String {
    format!("{base_url}/{service}/{method}")
}
