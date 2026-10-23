# Perpetual Hedge

Maintains a delta-neutral position by continuously hedging a spot holding with an offsetting perpetual futures position. When the spot position changes, the strategy adjusts the perp hedge to keep the net delta near zero.

## How it works

1. Read the current spot position and perpetual position.
2. Compute the target perp quantity: `target = −spot_position`.
3. If the drift `|target − current_perp| / max(|spot|, |perp|)` exceeds the threshold, trade the difference on the perpetual venue.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Base symbol.                              |
| `spot_symbol`       | required    | Spot symbol on the primary venue.         |
| `perp_symbol`       | required    | Perp symbol on the hedge venue.           |
| `[primary]`         | required    | Spot venue.                               |
| `[hedge]`           | required    | Perpetual venue.                          |
| `threshold`         | `0.01`      | Rebalance drift threshold (fraction).     |
| `max_order_size`    | `1`         | Maximum single hedge order size.          |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/perpetual-market-making/>
