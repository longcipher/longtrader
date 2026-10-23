# Grid Strike

A grid-based directional strategy with strike prices. Places a grid of orders between a lower and upper price band. Each filled order is reversed at a profit spread. Aims to accumulate base assets in a downtrend and distribute in an uptrend.

## How it works

1. Compute evenly spaced grid levels between `lower_price` and `upper_price`.
2. Place buy orders below the mid-price and sell orders above it.
3. When an order fills, reverse it at `fill_price · (1 ± profit_spread)`.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `lower_price`       | required    | Lower price of the grid.                  |
| `upper_price`       | required    | Upper price of the grid.                  |
| `levels`            | `5`         | Number of grid levels.                    |
| `qty`               | `1`         | Quantity per grid order.                  |
| `profit_spread`     | `0.01`      | Profit spread as a fraction.              |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/grid-strike/>
