> **English** | [中文](README.zh.md)

# supertrend — Supertrend + DEMA Trend Following

Native LongTrader strategy with dual Supertrend + DEMA confirmation.

## Logic

- Every `poll_secs` fetches the candle window and rebuilds Supertrend and fast/slow DEMA series.
- Opens only when Supertrend direction and DEMA relative position agree: up → market buy, down → market sell.
- On signal reversal, closes existing directional position first (in-memory state machine: Flat / Long / Short).

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `symbol` | required | Trading pair |
| `timeframe` | `"5m"` | Candle interval |
| `poll_secs` | `30` | Poll interval |
| `atr_window` | `14` | ATR / Supertrend period |
| `atr_multiplier` | `3` | Supertrend bandwidth multiplier |
| `fast_dema_window` | `10` | Fast DEMA window |
| `slow_dema_window` | `21` | Slow DEMA window |
| `qty` | `0.001` | Order quantity |

## Example

```toml
[strategy]
type = "supertrend"

[strategy.params]
exchange_id = "binance"
symbol = "ETHUSDT"
timeframe = "15m"
atr_window = 21
atr_multiplier = "2.5"
qty = "0.5"
```

## Notes

Current version focuses on Supertrend + DEMA core signals. Linear-regression confirmation, ATR take-profit and other extensions can be composed via risk layer; position sizing is fixed `qty` with account-level leverage handled by risk controls.
