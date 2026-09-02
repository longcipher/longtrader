use std::path::Path;

use color_eyre::{
    Result,
    eyre::{WrapErr, eyre},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{adapters::exchange_id, strategies::simple_grid::SimpleGridConfig};

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
    /// Backend selection: `"api"` — the longtrader-api unified endpoint.
    /// Defaults to `"daemon"`.
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

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StrategyParams {
    #[serde(default)]
    pub exchange_id: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub lower_price: Option<Decimal>,
    #[serde(default)]
    pub upper_price: Option<Decimal>,
    #[serde(default)]
    pub num_levels: Option<u32>,
    #[serde(default)]
    pub qty_per_level: Option<Decimal>,
    /// Catch-all for strategy-specific parameters (strategy-specific keys). Each
    /// strategy deserializes its own config struct from this table.
    #[serde(flatten)]
    pub extra: toml::Table,
}

impl StrategyParams {
    /// Strategy-specific parameter table for strategy-specific parameters.
    pub const fn table(&self) -> &toml::Table {
        &self.extra
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
    pub fn resolved_backend(&self) -> String {
        if self.api_endpoint.is_some() {
            return "terminal".to_string();
        }
        self.backend.clone().unwrap_or_else(|| "daemon".to_string())
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

    /// Build the unified grid configuration from strategy params.
    pub fn grid_config(&self) -> Result<SimpleGridConfig> {
        let params = &self.strategy.params;
        let required = |value: Option<Decimal>, name: &str| -> Result<Decimal> {
            value.ok_or_else(|| eyre!("{name} is required"))
        };
        Ok(SimpleGridConfig {
            profit_spread_pct: None,
            state_file: None,
            exchange_id: exchange_id(
                params.exchange_id.as_deref().unwrap_or("mock"),
                params.label.as_deref().unwrap_or_default(),
            ),
            symbol: params.symbol.clone().ok_or_else(|| eyre!("symbol is required"))?,
            lower_price: required(params.lower_price, "lower_price")?,
            upper_price: required(params.upper_price, "upper_price")?,
            num_levels: params.num_levels.ok_or_else(|| eyre!("num_levels is required"))?,
            qty_per_level: required(params.qty_per_level, "qty_per_level")?,
        })
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
