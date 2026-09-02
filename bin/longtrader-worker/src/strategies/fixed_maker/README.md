> **English** | [中文](README.zh.md)

# fixed_maker — Fixed-Spread Market Making

Native LongTrader strategy that quotes both sides around a fixed spread.

## Logic

- Every `poll_secs` fetches the latest ticker price.
- Quotes around mid: bid = mid × (1 − bid_spread), ask = mid × (1 + ask_spread).
- Each cycle cancels old quotes via `cancel_all_orders`, then places new two-sided limit orders.

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `symbol` | required | Trading pair |
| `timeframe` | `"5m"` | (reserved common field, unused) |
| `poll_secs` | `30` | Re-quote interval |
| `bid_spread` | `0.001` | Bid spread ratio |
| `ask_spread` | `0.001` | Ask spread ratio |
| `qty` | `0.001` | Quantity per side |

## Example

```toml
[strategy]
type = "fixed_maker"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
poll_secs = 10
bid_spread = "0.0005"
ask_spread = "0.0008"
qty = "0.01"
```

## Risk Note

Unconditional two-sided quoting has no inventory protection. In live trading, combine with the terminal kill-switch and position limits, or compose with inventory-aware strategies such as `rebalance`.
