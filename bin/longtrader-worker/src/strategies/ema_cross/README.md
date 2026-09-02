> **English** | [中文](README.zh.md)

# ema_cross — EMA Golden/Death Cross

Native LongTrader strategy for fast/slow EMA crossover trend following.

## Logic

- Every `poll_secs` fetches the recent candle window via `MarketDataSource::get_candles`.
- Computes fast/slow EMAs over close prices.
- Fast crossing above slow → market buy `qty`; crossing below → market sell.
- Crossover is edge-triggered (in-memory): after restart, waits for the next fresh cross and does not replay old signals.

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `exchange_id` | `"mock"` | Venue identifier |
| `label` | `""` | Sub-account label |
| `symbol` | required | Trading pair |
| `timeframe` | `"5m"` | Candle interval |
| `poll_secs` | `30` | Poll interval |
| `fast_window` | `9` | Fast EMA window |
| `slow_window` | `21` | Slow EMA window |
| `qty` | `0.001` | Order quantity |

## Example

```toml
[strategy]
type = "ema_cross"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
timeframe = "5m"
poll_secs = 30
fast_window = 9
slow_window = 21
qty = "0.001"
```
