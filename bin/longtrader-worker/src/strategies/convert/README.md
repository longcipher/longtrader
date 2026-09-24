# convert

Margin sentinel variant. Runs a heartbeat; on `kill_switch_on_disconnect` (broker
disconnect) it liquidates the position to protect margin, then on reconnect
re-opens near the last entry. (Despite the name, it does not call
`VenueOpInvoker.wallet_convert`.)

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol to guard |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `max_drawdown_pct` | Decimal | `0.20` | drawdown that triggers flatten |
| `check_interval_secs` | u64 | `60` | check cadence (seconds) |
| `cooldown_secs` | u64 | `300` | silence after a trigger |

## Example configuration

```toml
[strategy]
type = "convert"
[strategy.params]
symbol = "BTC/USDT"
max_drawdown_pct = "0.20"
check_interval_secs = 60
cooldown_secs = 300
```

## Capabilities

- `TradingGateway` (`cancel_all_orders`, `close_position`, `create_order`, `get_account`)

## Risk notes

- This is a **disconnect sentinel**, not a conversion strategy — despite the name it does not call `VenueOpInvoker.wallet_convert`.
- `cancel_all_orders("")` wipes all orders on the symbol; closing realizes the loss.
- Peak equity is in-memory → restart resets the baseline.
- Re-opening after reconnect uses the last entry price, which may be stale.
