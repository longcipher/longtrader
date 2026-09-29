use std::path::Path;

use color_eyre::{Result, eyre::WrapErr};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub daemon_endpoint: String,
    /// Optional longtrader-api endpoint. When set, the remote backend takes
    /// precedence over `backend` for backwards compatibility.
    #[serde(default)]
    pub api_endpoint: Option<String>,
    /// Optional path to a file containing the terminal API token.
    #[serde(default)]
    pub api_token_file: Option<String>,
    /// Whether to cancel active orders when the session lease expires.
    #[serde(default)]
    pub kill_switch_on_disconnect: Option<bool>,
    /// Backend selection. `"api"` — the longtrader-api unified endpoint served
    /// by the terminal/daemon. `"terminal"` is selected implicitly when
    /// `api_endpoint` is set. Defaults to `"api"`.
    #[serde(default)]
    pub backend: Option<String>,
    /// Optional bind address for the control-plane RPC server
    /// (`WorkerSessionService` + unified trading/market proxies).
    #[serde(default)]
    pub listen_endpoint: Option<String>,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    #[serde(rename = "type")]
    pub strategy_type: String,
    #[serde(default)]
    pub params: StrategyParams,
}

/// The on-file shape of [`StrategyParams`], before the well-known keys are
/// merged back into the catch-all table.
#[derive(Deserialize)]
struct StrategyParamsWire {
    #[serde(default)]
    exchange_id: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    lower_price: Option<Decimal>,
    #[serde(default)]
    upper_price: Option<Decimal>,
    #[serde(default)]
    num_levels: Option<u32>,
    #[serde(default)]
    qty_per_level: Option<Decimal>,
    #[serde(flatten)]
    extra: toml::Table,
}

impl<'de> Deserialize<'de> for StrategyParams {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let wire = StrategyParamsWire::deserialize(de)?;
        let mut params = Self {
            exchange_id: wire.exchange_id,
            label: wire.label,
            symbol: wire.symbol,
            lower_price: wire.lower_price,
            upper_price: wire.upper_price,
            num_levels: wire.num_levels,
            qty_per_level: wire.qty_per_level,
            extra: wire.extra,
        };
        params.merge_known_keys();
        Ok(params)
    }
}

/// Render a decimal as a TOML string.
///
/// A string (not a float) is deliberate: a config that round-trips through
/// `f64` would lose precision on a price, and every strategy parses these back
/// as `Decimal`.
fn decimal_value(v: Decimal) -> toml::Value {
    toml::Value::String(v.normalize().to_string())
}

#[derive(Debug, Clone, Default)]
pub struct StrategyParams {
    pub exchange_id: Option<String>,
    pub label: Option<String>,
    pub symbol: Option<String>,
    pub lower_price: Option<Decimal>,
    pub upper_price: Option<Decimal>,
    pub num_levels: Option<u32>,
    pub qty_per_level: Option<Decimal>,
    /// Catch-all for strategy-specific parameters (strategy-specific keys). Each
    /// strategy deserializes its own config struct from this table. Also
    /// carries the well-known keys above, merged in by [`Deserialize`] — see
    /// [`StrategyParams::table`].
    pub extra: toml::Table,
}

impl StrategyParams {
    /// The full parameter table a strategy deserializes its config from.
    ///
    /// This is the *merged* view: the well-known keys serde consumed
    /// (`symbol`, `exchange_id`, `label`, the grid bounds) are written back
    /// into `extra` by [`Deserialize`], so a strategy's own config struct sees
    /// everything the file declared. Returning the catch-all alone silently
    /// dropped every well-known key, so the documented
    /// `[strategy.params] symbol = "..."` never reached a strategy and any
    /// strategy requiring a symbol failed at startup with "missing field".
    pub const fn table(&self) -> &toml::Table {
        &self.extra
    }

    /// Merge the well-known keys back into the catch-all table.
    fn merge_known_keys(&mut self) {
        // An explicit `extra` entry wins: it is the more specific source, and
        // serde cannot have produced a collision here from a real document.
        let put = |table: &mut toml::Table, key: &str, value: toml::Value| {
            table.entry(key).or_insert(value);
        };
        if let Some(v) = self.exchange_id.clone() {
            put(&mut self.extra, "exchange_id", toml::Value::String(v));
        }
        if let Some(v) = self.label.clone() {
            put(&mut self.extra, "label", toml::Value::String(v));
        }
        if let Some(v) = self.symbol.clone() {
            put(&mut self.extra, "symbol", toml::Value::String(v));
        }
        if let Some(v) = self.lower_price {
            put(&mut self.extra, "lower_price", decimal_value(v));
        }
        if let Some(v) = self.upper_price {
            put(&mut self.extra, "upper_price", decimal_value(v));
        }
        if let Some(v) = self.num_levels {
            put(&mut self.extra, "num_levels", toml::Value::Integer(i64::from(v)));
        }
        if let Some(v) = self.qty_per_level {
            put(&mut self.extra, "qty_per_level", decimal_value(v));
        }
    }

    /// Venue identifier string, defaulting to `"mock"`.
    pub fn venue(&self) -> &str {
        self.exchange_id.as_deref().unwrap_or("mock")
    }

    /// Optional sub-account label.
    pub fn venue_label(&self) -> &str {
        self.label.as_deref().unwrap_or_default()
    }
}

impl Config {
    /// Effective backend, honouring the legacy `api_endpoint` override.
    ///
    /// Resolves to `"terminal"` when `api_endpoint` is set, otherwise to the
    /// configured `backend` (defaulting to `"api"`). The previously documented
    /// `"daemon"` backend has no adapter implementation, so it is no longer a
    /// valid default — callers relying on it must configure a real backend.
    pub fn resolved_backend(&self) -> String {
        if self.api_endpoint.is_some() {
            return "terminal".to_string();
        }
        self.backend
            .clone()
            .filter(|b| !b.is_empty() && b != "daemon")
            .unwrap_or_else(|| "api".to_string())
    }

    /// Terminal API token loaded from `api_token_file` (empty when unset).
    /// Returns empty string if the file is missing or unreadable and emits a
    /// warning so badly mounted secrets are visible without leaking the token.
    pub fn api_token(&self) -> String {
        let Some(path) = self.api_token_file.as_deref() else {
            return String::new();
        };
        match std::fs::read_to_string(path) {
            Ok(s) => s.trim().to_string(),
            Err(err) => {
                tracing::warn!(path = %path, error = %err, "api_token_file unreadable, using empty token");
                String::new()
            }
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read config file: {}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .wrap_err_with(|| format!("failed to parse config file: {}", path.display()))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    /// Parse a config the way the README documents it.
    fn parse(params_toml: &str) -> StrategyParams {
        let doc = format!(
            "daemon_endpoint = \"http://127.0.0.1:8810\"\n[strategy]\ntype = \"simple_grid\"\n[strategy.params]\n{params_toml}\n"
        );
        let config: Config = toml::from_str(&doc).expect("config parses");
        config.strategy.params
    }

    /// The well-known keys serde consumed must be written back into the table a
    /// strategy deserializes from. Before this merge, `table()` returned only
    /// the catch-all, so the documented `[strategy.params] symbol = "..."` never
    /// reached a strategy and every symbol-requiring strategy failed at
    /// startup with "missing field `symbol`".
    #[test]
    fn well_known_keys_reach_the_strategy_table() {
        let params = parse(
            "symbol = \"BTCUSDT\"\nexchange_id = \"binance\"\nnum_levels = 7\nlower_price = \"90000\"\nqty_per_level = \"0.001\"\n",
        );
        let table = params.table();
        assert_eq!(table.get("symbol").and_then(toml::Value::as_str), Some("BTCUSDT"));
        assert_eq!(table.get("exchange_id").and_then(toml::Value::as_str), Some("binance"));
        assert_eq!(table.get("num_levels").and_then(toml::Value::as_integer), Some(7));
        // Decimals round-trip as strings, so a price never loses precision by
        // passing through `f64`.
        assert_eq!(table.get("lower_price").and_then(toml::Value::as_str), Some("90000"));
        assert_eq!(table.get("qty_per_level").and_then(toml::Value::as_str), Some("0.001"));
    }

    #[test]
    fn strategy_specific_keys_survive_the_merge() {
        let params = parse("symbol = \"BTCUSDT\"\nfunding_rate_threshold = \"0.0005\"\n");
        assert_eq!(
            params.table().get("funding_rate_threshold").and_then(toml::Value::as_str),
            Some("0.0005")
        );
    }

    /// The whole point of the merge: a strategy's own config struct must
    /// deserialize from the table the file produced.
    #[test]
    fn a_strategy_config_deserializes_from_the_merged_table() {
        #[derive(serde::Deserialize)]
        struct GridConfig {
            symbol: String,
            num_levels: u32,
            qty_per_level: Decimal,
        }
        let params = parse(
            "symbol = \"BTCUSDT\"\nexchange_id = \"mock\"\nnum_levels = 5\nqty_per_level = \"0.001\"\n",
        );
        let cfg: GridConfig = toml::Value::Table(params.table().clone())
            .try_into()
            .expect("the documented config must deserialize");
        assert_eq!(cfg.symbol, "BTCUSDT");
        assert_eq!(cfg.num_levels, 5);
        assert_eq!(cfg.qty_per_level, dec!(0.001));
    }

    /// An absent well-known key must stay absent rather than materialise as a
    /// zero: a strategy that treats `num_levels = 0` as "use the default" would
    /// otherwise silently get a different code path from "not specified".
    #[test]
    fn absent_well_known_keys_stay_absent() {
        let params = parse("symbol = \"BTCUSDT\"\n");
        assert!(!params.table().contains_key("num_levels"));
        assert!(!params.table().contains_key("lower_price"));
        assert!(!params.table().contains_key("label"));
    }

    /// The accessors keep working off the typed fields, and still default.
    #[test]
    fn venue_accessor_defaults_to_mock() {
        assert_eq!(parse("symbol = \"X\"\n").venue(), "mock");
        assert_eq!(parse("symbol = \"X\"\nexchange_id = \"htx\"\n").venue(), "htx");
    }
}
