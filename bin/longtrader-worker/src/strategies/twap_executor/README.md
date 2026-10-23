# TWAP Executor

Time-Weighted Average Price order execution. Ported from Hummingbot's
[TWAP strategy](https://hummingbot.org/strategies/twap/).

## Summary

Splits a large order into `num_orders` equal child orders, placing one
every `duration_secs / num_orders` seconds. Minimises market impact
relative to a single large market order. Supports maker-mode (limit
orders with buffer) and taker-mode (aggressive limits).

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `total_amount` | — | Total base units to execute |
| `duration_secs` | 600 | Total execution horizon |
| `num_orders` | 10 | Number of child orders |
| `is_buy` | true | Buy or sell side |
| `is_maker` | true | Limit orders (maker) vs market (taker) |
| `limit_buffer` | 0.0005 | Price buffer for maker orders |

## Risk

TWAP does not guarantee execution if liquidity is thin at the
chosen interval. Maker mode may leave orders unfilled in fast
markets.
