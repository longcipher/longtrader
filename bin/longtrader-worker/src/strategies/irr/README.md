# irr

Demo/analytics strategy. Every `compute_interval_secs` it prints an IRR-style
report using `get_account` + `get_positions` as a proxy for cash flows. It opens
no trades.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol (scope) |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `compute_interval_secs` | u64 | `60` | report cadence (seconds) |
| `baseline` | Decimal | `0` | starting NAV baseline |

## Example configuration

```toml
[strategy]
type = "irr"
[strategy.params]
symbol = "BTC/USDT"
compute_interval_secs = 60
```

## Capabilities

- `TradingGateway` (`get_account`, `get_positions`)

## Risk notes

- **Analytics/demo only — opens no trades.** The "IRR" uses account balance as NAV and a single `baseline`; it is not a true XIRR over dated cash flows.
- Baseline is in-memory → restart resets the series.
- For real performance accounting, use `nav_recorder` + your own dated ledger.
