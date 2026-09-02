> **English** | [中文](README.zh.md)

# boll_grid — Bollinger Band Grid

Native LongTrader strategy that trades a grid inside Bollinger Bands.

## Logic

- Every `poll_secs` fetches `boll_window` candles and computes Bollinger Bands.
- When the latest close is inside the bands and bandwidth is sufficient: cancel all open orders, then place `grid_num` limit orders on each side of the close (step = bandwidth / (grid_num + 1)), discarding levels outside the bands.
- No grid is placed when price breaks outside the bands (strong trend) or bandwidth is too narrow (quiet market).

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `symbol` | required | Trading pair |
| `timeframe` | `"5m"` | Candle interval |
| `poll_secs` | `30` | Poll interval |
| `boll_window` | `21` | Bollinger window |
| `boll_mult` | `2` | Stddev multiplier |
| `grid_num` | `3` | Levels per side |
| `qty` | `0.001` | Quantity per level |
| `profit_spread_pct` | `0.0005` | Opposite-leg spread (reserved) |

## Example

```toml
[strategy]
type = "boll_grid"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
timeframe = "5m"
boll_window = 21
grid_num = 4
qty = "0.01"
```
