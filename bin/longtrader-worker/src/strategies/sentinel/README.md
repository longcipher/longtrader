# sentinel

Risk sentinel. On each check it reads the account, computes drawdown from peak
equity, and if `max_drawdown_pct` is breached it hard-flattens: cancel **all**
orders on the symbol and close the position, then cools down for `cooldown_secs`.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol to guard |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `max_drawdown_pct` | Decimal | `0.20` | drawdown from peak equity that triggers a flatten |
| `check_interval_secs` | u64 | `30` | drawdown check cadence (seconds) |
| `cooldown_secs` | u64 | `300` | silence after a trigger before re-arming |

## Example configuration

```toml
[strategy]
type = "sentinel"
[strategy.params]
symbol = "BTC/USDT"
max_drawdown_pct = "0.20"
check_interval_secs = 30
cooldown_secs = 300
```

## Capabilities

- `TradingGateway` (`cancel_all_orders`, `close_position`, `create_order`)
- `MarketDataSource` (`get_account`)

## Risk notes

- `cancel_all_orders("")` wipes **every** order on the symbol (and, on some backends, the whole venue) — not just this strategy's.
- Closing the position realizes the loss it was meant to stop.
- Peak equity is in-memory → a restart resets the baseline.
- Uses account equity as a proxy for drawdown; no per-position stop logic.
