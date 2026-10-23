# PMM Dynamic

An enhanced Pure Market Making strategy that dynamically adjusts bid/ask half-spreads based on real-time volatility. High volatility widens spreads to compensate for risk; low volatility narrows them to remain competitive.

## How it works

1. Estimate realised volatility from a rolling window of bar returns.
2. Compute the volatility ratio: `current_vol / base_vol`.
3. Scale the base spread: `dynamic_spread = base_spread · vol_ratio · inventory_skew_factor`.
4. Place bid at `mid · (1 − bid_spread)` and ask at `mid · (1 + ask_spread)`.
5. Re-quote on each tick.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `base_spread`       | `0.005`     | Base half-spread as fraction.             |
| `base_vol`          | `0.01`      | Reference volatility for scaling.         |
| `window`            | `20`        | Rolling window for realised volatility.   |
| `inventory_skew`    | `0.5`       | Inventory skew factor.                    |
| `qty`               | `1`         | Quote quantity per side.                  |
| `timeframe`         | `"5m"`      | Candle timeframe.                         |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/pure-market-making/>
