> **English** | [中文](README.zh.md)

# dca_scheduler — DCA / Scheduled Accumulation

Native LongTrader strategy for dollar-cost averaging and periodic batch execution.

## Logic

- Submits one market order every `interval_secs`; the first order executes immediately on startup.
- `is_buy = true` for scheduled buying; `false` for scheduled selling (fee-asset restocking / batch distribution).

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `symbol` | required | Trading pair |
| `poll_secs` | `30` | (unused, kept for common-field compatibility) |
| `interval_secs` | `86400` | Interval in seconds |
| `is_buy` | `true` | Buy/sell direction |
| `qty` | `0.001` | Quantity per order |

## Example

```toml
[strategy]
type = "dca_scheduler"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
interval_secs = 3600
is_buy = true
qty = "0.002"
```

## Notes

Current version focuses on core DCA scheduling. Amount-based sizing and other extensions can be composed via upper-layer config and risk controls.
