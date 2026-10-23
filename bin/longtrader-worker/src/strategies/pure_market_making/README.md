# Pure Market Making (PMM)

Multi-level market making with optional inventory skew and hanging orders.
Ported from the Hummingbot [Pure Market Making](https://hummingbot.org/strategies/pure-market-making/)
strategy.

## Summary

Places `levels` bid and ask orders at geometrically or linearly increasing
distances from the mid-price. Each level's spread grows by `order_step_size`.
When inventory accumulates on one side, sizes are skewed to encourage
rebalancing.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `levels` | 3 | Number of bid/ask levels |
| `order_step_size` | 0.005 | Spread increment per level |
| `order_size_spread` | 0.001 | Additional spread widening per level |
| `order_amount` | 0.001 | Base order size |
| `inventory_skew` | 0.5 | Inventory→size adjustment factor |
| `price_band` | 0.1 | Requote when mid moves beyond this band |
| `hanging_orders` | true | Keep orders after counterpart fill |
| `hanging_cancel_ticks` | 5 | Cancel hanging after N ticks |

## Risk

Regular MM adverse-selection exposure. Inventory skew mitigates
directional drift but does not eliminate it. Price bands cap re-quote
churn during fast moves but may leave stale quotes in gap risk.
