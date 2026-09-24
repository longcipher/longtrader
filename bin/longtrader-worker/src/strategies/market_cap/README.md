# market_cap

Ranks assets by market cap and holds the top `top_n` (optionally excluding
non-stables when `include_stable = false`), rebalancing toward equal weight when
drift exceeds `band_pct`. Market caps are fetched as a point-in-time snapshot.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | quote symbol, e.g. `USDT` |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `top_n` | usize | `10` | number of assets to hold |
| `rebalance_interval_secs` | u64 | `3600` | rebalance cadence (seconds) |
| `band_pct` | Decimal | `0.05` | rebalance when drift exceeds this |
| `include_stable` | bool | `true` | include stablecoins in the universe |
| `stablecoins` | Vec<String> | `["USDT", "USDC"]` | stablecoin ids to exclude when `include_stable = false` |
| `quote_asset` | String | `"USDT"` | numéraire |
| `use_limit` | bool | `false` | place limit instead of market |
| `limit_offset` | Decimal | `0.001` | limit offset from price (fraction) |

## Example configuration

```toml
[strategy]
type = "market_cap"
[strategy.params]
symbol = "USDT"
top_n = 10
rebalance_interval_secs = 3600
band_pct = "0.05"
include_stable = true
```

## Capabilities

- `TradingGateway` (`get_account`, `create_order`)
- `MarketDataSource` (`get_market_cap`, `fetch_ticker`)

## Risk notes

- Market cap is a point-in-time snapshot → the strategy chases momentum and can buy tops.
- Rebalancing realizes taxable events.
- Requires a backend that serves `get_market_cap` (the open mock does not).
