# Spot-Perpetual Arbitrage

Exploits the price difference (basis) between a spot market and its corresponding perpetual futures contract. When the basis is positive (perp trades at a premium), the strategy shorts the perp and goes long spot. When negative, it does the opposite.

## How it works

1. Compute the basis: `(perp_price − spot_price) / spot_price`.
2. If `basis > entry_threshold`: short perp, long spot.
3. If `basis < −entry_threshold`: long perp, short spot.
4. If `|basis| < exit_threshold` and a position is open: close.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Base symbol.                              |
| `spot_symbol`       | required    | Spot symbol on the primary venue.         |
| `perp_symbol`       | required    | Perp symbol on the hedge venue.           |
| `[primary]`         | required    | Spot venue.                               |
| `[hedge]`           | required    | Perpetual venue.                          |
| `entry_threshold`   | `0.01`      | Basis threshold to open.                  |
| `exit_threshold`    | `0.002`     | Basis threshold to close.                 |
| `qty`               | `1`         | Position size in base units.              |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/spot-perpetual-arbitrage/>
