//! Shared constants and URL helpers for Connect-RPC clients.

pub(crate) const SERVICE_MARKET: &str = "longtrader.terminal.v1.MarketDataService";
pub(crate) const SERVICE_TRADING: &str = "longtrader.terminal.v1.TradingService";
pub(crate) const SERVICE_RUNTIME: &str = "longtrader.terminal.v1.RuntimeService";

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
