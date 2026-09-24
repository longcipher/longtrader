# nav_recorder

Analytics strategy. Every `record_interval_secs` it records NAV (account equity

- position value) to a local JSON file (`persist_path`). No trades.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol (scope) |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `record_interval_secs` | u64 | `60` | record cadence (seconds) |
| `persist_path` | String | `"state/nav.json"` | local JSON path for the NAV series |

## Example configuration

```toml
[strategy]
type = "nav_recorder"
[strategy.params]
symbol = "BTC/USDT"
record_interval_secs = 60
persist_path = "state/nav.json"
```

## Capabilities

- `TradingGateway` (`get_account`, `get_positions`)

## Risk notes

- **Analytics only — opens no trades.** NAV uses account equity + position mark as a proxy (no realized/unrealized PnL split).
- Writes a local file; ensure `persist_path` is on durable storage if you need history.
- For real IRR, pair with your own dated cash-flow ledger.
