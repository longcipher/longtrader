> **English** | [中文](README.zh.md)

# random_entry — Random Entry (Connectivity Test)

Native LongTrader strategy for reproducible random connectivity testing.

## Logic

- Every `interval_secs`, submits a random-direction market order with probability `entry_probability`.
- RNG is SplitMix64 seeded by `seed` → same config replays the same sequence for integration testing and regression.

Purpose: smoke-test connectivity, signing, rate limits, and risk chain. **Not a profitable strategy.**

## Params (`[strategy.params]`)

| Field | Default | Description |
|-------|---------|-------------|
| `symbol` | required | Trading pair |
| `interval_secs` | `60` | Decision interval |
| `entry_probability` | `0.05` | Trigger probability [0,1] |
| `qty` | `0.0001` | Order quantity |
| `seed` | golden-ratio constant | RNG seed |

## Example

```toml
[strategy]
type = "random_entry"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
interval_secs = 30
entry_probability = "0.5"
qty = "0.001"
seed = 42
```
