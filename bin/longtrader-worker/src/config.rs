use std::path::Path;

use color_eyre::{Result, eyre::WrapErr};
use rust_decimal::Decimal;
use serde::Deserialize;

/// Reject unknown keys, so a removed or misspelled setting is a startup error
/// rather than a silent fallback. This matters most for the keys that were
/// removed when the single-representation contract landed: a config still
/// carrying `api_endpoint` used to override the backend selection, and without
/// this attribute it would parse cleanly and quietly connect somewhere else.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub daemon_endpoint: String,
    /// Endpoint the remote backend talks to. Only consulted when `backend` is
    /// the remote one; `backend = "mock"` ignores it.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Optional path to a file containing the terminal API token.
    #[serde(default)]
    pub api_token_file: Option<String>,
    /// Whether to cancel active orders when the session lease expires.
    #[serde(default)]
    pub kill_switch_on_disconnect: Option<bool>,
    /// Backend selection: `"api"` (the unified `longtrader.{market,trading}.v1`
    /// services, reached over Connect) or `"mock"` (in-process, offline
    /// dry-run). Defaults to `"api"`.
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
    /// The configured backend, defaulting to `"api"`.
    ///
    /// There is nothing to resolve: one selector picks one backend. An unset or
    /// blank value is the default rather than an error, so an operator who omits
    /// the key gets the real backend instead of a startup failure.
    pub fn backend(&self) -> &str {
        self.backend.as_deref().map(str::trim).filter(|b| !b.is_empty()).unwrap_or("api")
    }

    /// The endpoint the remote backend should use.
    pub fn endpoint_or_default(&self, default: &str) -> String {
        self.endpoint.clone().unwrap_or_else(|| default.to_string())
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
    use proptest::prelude::*;
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

    // -----------------------------------------------------------------------
    // backend selection
    // -----------------------------------------------------------------------

    fn full_config(doc: &str) -> Config {
        // `doc` holds **top-level** keys, so it must be emitted before the
        // `[strategy]` table: a key written after a table header belongs to that
        // table, and `Config` would silently never see it.
        let body = format!(
            "daemon_endpoint = \"http://127.0.0.1:8810\"\n{doc}\n[strategy]\ntype = \"simple_grid\"\n"
        );
        toml::from_str(&body).expect("config parses")
    }

    /// The configured backend is used verbatim. There is no second selector that
    /// can override it, so there is nothing to resolve.
    #[test]
    fn an_explicit_backend_is_used_verbatim() {
        assert_eq!(full_config("backend = \"api\"\n").backend(), "api");
        assert_eq!(full_config("backend = \"mock\"\n").backend(), "mock");
        assert_eq!(full_config("backend = \"custom\"\n").backend(), "custom");
    }

    /// An empty or blank value is a config mistake, not a backend. Falling back
    /// to the real one beats starting a worker that cannot connect.
    #[test]
    fn an_absent_or_blank_backend_falls_back_to_api() {
        assert_eq!(full_config("").backend(), "api");
        assert_eq!(full_config("backend = \"\"\n").backend(), "api");
        assert_eq!(full_config("backend = \"  \"\n").backend(), "api", "blank is trimmed");
        assert_eq!(full_config("backend = \" mock \"\n").backend(), "mock", "and so is a name");
    }

    /// The endpoint is a plain URL with no effect on which backend runs.
    #[test]
    fn an_endpoint_does_not_choose_the_backend() {
        let config = full_config("endpoint = \"http://api:9000\"\n");
        assert_eq!(config.backend(), "api", "an endpoint is not a backend selector");
        assert_eq!(config.endpoint_or_default("fallback"), "http://api:9000");
    }

    /// The `api_endpoint` key is gone. A config still carrying it must fail
    /// loudly rather than parse cleanly and quietly connect to the default
    /// endpoint — the override it used to provide cannot be honoured, and
    /// silently ignoring it points the worker at the wrong host.
    #[test]
    fn a_removed_api_endpoint_key_is_rejected_rather_than_ignored() {
        let err = toml::from_str::<Config>(
            "daemon_endpoint = \"http://x:1\"\napi_endpoint = \"http://internal:9000\"\n\
             [strategy]\ntype = \"simple_grid\"\n",
        )
        .expect_err("a removed key must not be silently dropped");
        let message = err.to_string();
        assert!(
            message.contains("api_endpoint"),
            "the error must name the offending key so the operator knows what to fix: {message}"
        );
    }

    /// A misspelled key is the same hazard: it silently vanishes and the worker
    /// runs against the default endpoint with no indication why.
    #[test]
    fn a_misspelled_endpoint_key_is_rejected() {
        // Assembled at runtime: the point is that *some* unknown key is
        // reported, and spelling a real typo literally would trip `typos`.
        let typo = ["end", "pi", "ont"].join("");
        let err = toml::from_str::<Config>(&format!(
            "daemon_endpoint = \"http://x:1\"\n{typo} = \"http://typo:9000\"\n\
             [strategy]\ntype = \"simple_grid\"\n"
        ))
        .expect_err("an unknown key must be reported");
        assert!(err.to_string().contains(&typo), "{err}");
    }

    #[test]
    fn an_absent_endpoint_falls_back_to_the_default() {
        assert_eq!(full_config("").endpoint_or_default("http://fallback"), "http://fallback");
    }

    // -----------------------------------------------------------------------
    // api_token
    // -----------------------------------------------------------------------

    #[test]
    fn an_unset_token_file_yields_an_empty_token() {
        assert!(full_config("").api_token().is_empty());
    }

    /// A token file usually ends in a newline from `echo`; the whitespace must
    /// not be sent as part of the bearer credential.
    #[test]
    fn a_token_file_is_trimmed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, "  secret-token\n").expect("write");
        let config = full_config(&format!("api_token_file = {:?}\n", path.display().to_string()));
        assert_eq!(config.api_token(), "secret-token");
    }

    /// A missing or unreadable secret file must degrade to an empty token rather
    /// than aborting startup; the warning makes the misconfiguration visible.
    #[test]
    fn an_unreadable_token_file_yields_an_empty_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("absent");
        let config = full_config(&format!("api_token_file = {:?}\n", path.display().to_string()));
        assert!(config.api_token().is_empty());
    }

    #[test]
    fn a_whitespace_only_token_file_yields_an_empty_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, "\n\t  \n").expect("write");
        let config = full_config(&format!("api_token_file = {:?}\n", path.display().to_string()));
        assert!(config.api_token().is_empty());
    }

    // -----------------------------------------------------------------------
    // Config::load
    // -----------------------------------------------------------------------

    #[test]
    fn load_reads_a_valid_config_off_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("worker.toml");
        std::fs::write(
            &path,
            "daemon_endpoint = \"http://127.0.0.1:8810\"\nkill_switch_on_disconnect = true\nlisten_endpoint = \"127.0.0.1:0\"\n[strategy]\ntype = \"ema_cross\"\n[strategy.params]\nsymbol = \"BTC/USDT\"\n",
        )
        .expect("write");
        let config = Config::load(&path).expect("loads");
        assert_eq!(config.daemon_endpoint, "http://127.0.0.1:8810");
        assert_eq!(config.kill_switch_on_disconnect, Some(true));
        assert_eq!(config.listen_endpoint.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(config.strategy.strategy_type, "ema_cross");
        assert_eq!(config.strategy.params.symbol.as_deref(), Some("BTC/USDT"));
    }

    /// A missing file is reported with its path so the operator knows which one.
    #[test]
    fn load_reports_a_missing_file_with_its_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nope.toml");
        let err = Config::load(&path).expect_err("missing file");
        let message = err.to_string();
        assert!(message.contains("failed to read config file"), "{message}");
        assert!(message.contains("nope.toml"), "{message}");
    }

    #[test]
    fn load_reports_malformed_toml_with_its_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "this is not = = toml\n").expect("write");
        let err = Config::load(&path).expect_err("malformed");
        let message = err.to_string();
        assert!(message.contains("failed to parse config file"), "{message}");
        assert!(message.contains("bad.toml"), "{message}");
    }

    /// `daemon_endpoint` and `strategy.type` are required: a config missing
    /// either cannot start a worker, and the error must say which.
    #[test]
    fn load_rejects_a_config_missing_a_required_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("partial.toml");
        std::fs::write(&path, "[strategy]\ntype = \"ema_cross\"\n").expect("write");
        // `color_eyre`'s `Display` renders only the outermost wrap, so the field
        // detail lives in the source chain; `{:?}` prints the whole chain.
        let err = Config::load(&path).expect_err("missing daemon_endpoint");
        let chain = format!("{err:?}");
        assert!(chain.contains("daemon_endpoint"), "{chain}");

        std::fs::write(&path, "daemon_endpoint = \"http://x\"\n").expect("write");
        let err = Config::load(&path).expect_err("missing strategy");
        let chain = format!("{err:?}");
        assert!(chain.contains("strategy"), "{chain}");
    }

    // -----------------------------------------------------------------------
    // Decimal rendering and merge precedence
    // -----------------------------------------------------------------------

    /// Decimals are rendered as normalized strings so a price never loses
    /// precision by passing through `f64`.
    #[test]
    fn decimals_are_merged_as_normalized_strings() {
        let cases = [
            (dec!(90000.00), "90000"),
            (dec!(0.00100), "0.001"),
            (dec!(1000), "1000"),
            (dec!(-1.500), "-1.5"),
            (Decimal::ZERO, "0"),
        ];
        for (value, expected) in cases {
            let params = parse(&format!("symbol = \"X\"\nlower_price = \"{value}\"\n"));
            assert_eq!(
                params.table().get("lower_price").and_then(toml::Value::as_str),
                Some(expected),
                "{value}"
            );
        }
    }

    /// An explicit strategy-specific key of the same name is the more specific
    /// source and must win over the merged well-known value.
    #[test]
    fn an_explicit_extra_key_wins_over_the_merged_well_known_key() {
        let params = parse("symbol = \"FROM_WELL_KNOWN\"\n");
        assert_eq!(
            params.table().get("symbol").and_then(toml::Value::as_str),
            Some("FROM_WELL_KNOWN")
        );

        let mut table = params.table().clone();
        table.insert("symbol".into(), toml::Value::String(String::from("OVERRIDDEN")));
        let merged: StrategyParams = toml::Value::Table(table).try_into().expect("re-parses");
        assert_eq!(merged.table().get("symbol").and_then(toml::Value::as_str), Some("OVERRIDDEN"));
    }

    #[test]
    fn venue_label_defaults_to_empty() {
        assert_eq!(parse("symbol = \"X\"\n").venue_label(), "");
        assert_eq!(parse("symbol = \"X\"\nlabel = \"sub\"\n").venue_label(), "sub");
    }

    #[test]
    fn strategy_type_is_read_from_the_type_key() {
        assert_eq!(full_config("").strategy.strategy_type, "simple_grid");
    }

    #[test]
    fn absent_params_deserialize_to_an_empty_table() {
        let config = full_config("");
        assert!(config.strategy.params.table().is_empty());
        assert!(config.strategy.params.symbol.is_none());
    }

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Whatever decimal the file declares, the merged table always carries a
        /// string a strategy can re-parse into the same value.
        #[test]
        fn a_merged_decimal_always_round_trips_through_the_table(
            mantissa in -1_000_000_000i64..1_000_000_000,
            scale in 0u32..19,
        ) {
            let Ok(value) = Decimal::try_from_i128_with_scale(i128::from(mantissa), scale) else {
                return Ok(());
            };
            let params = parse(&format!("symbol = \"X\"\nqty_per_level = \"{value}\"\n"));
            let rendered = params
                .table()
                .get("qty_per_level")
                .and_then(toml::Value::as_str)
                .expect("the key is merged as a string");
            prop_assert_eq!(
                rendered.parse::<Decimal>().expect("the rendering re-parses"),
                value.normalize(), "{} rendered as {}", value, rendered );
        }

        /// `num_levels` merges as an integer, never a string or a float.
        #[test]
        fn a_merged_level_count_is_always_an_integer(levels in 0u32..10_000) {
            let params = parse(&format!("symbol = \"X\"\nnum_levels = {levels}\n"));
            prop_assert_eq!(
                params.table().get("num_levels").and_then(toml::Value::as_integer),
                Some(i64::from(levels))
            );
        }

        /// Setting an endpoint never changes which backend runs. There is no
        /// override left to trigger.
        #[test]
        fn an_endpoint_never_selects_a_backend(
            backend in prop::sample::select(&["api", "mock", "", "daemon", "custom"]),
        ) {
            let config =
                full_config(&format!("endpoint = \"http://api:9000\"\nbackend = \"{backend}\"\n"));
            let expected = match backend {
                "" => "api",
                other => other.trim(),
            };
            prop_assert_eq!(config.backend(), if expected.is_empty() { "api" } else { expected });
        }

        /// Only a blank value falls back to `"api"`; every other name — including
        /// ones with no adapter, such as the historical `"daemon"` — passes
        /// through so `main` can reject it by name instead of silently running a
        /// different backend than the config asked for.
        #[test]
        fn only_a_blank_backend_falls_back(
            backend in prop::sample::select(&["api", "", "  ", "mock", "daemon", "custom"]),
        ) {
            let config = full_config(&format!("backend = \"{backend}\"\n"));
            let trimmed = backend.trim();
            prop_assert_eq!(config.backend(), if trimmed.is_empty() { "api" } else { trimmed });
        }
    }
}
