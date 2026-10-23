# Liquidity Mining (CLMM)

Manages concentrated liquidity market maker (CLMM) positions by adjusting the price range based on realised volatility. When volatility is high, the range widens to avoid going out of range. When volatility is low, the range narrows to maximise fee income.

## How it works

1. Estimate realised volatility from a rolling window of bar returns.
2. Compute the range half-width: `k · σ`, clamped to `[min_range, max_range]`.
3. Set the CLMM position range: `[mid · (1 − width), mid · (1 + width)]`.
4. Rebalance the position when the range changes significantly.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `range_k`           | `1`         | Range multiplier.                         |
| `window`            | `20`        | Rolling window for volatility.            |
| `min_range`         | `0.01`      | Minimum range width (fraction).           |
| `max_range`         | `0.5`       | Maximum range width (fraction).           |
| `timeframe`         | `"5m"`      | Candle timeframe.                         |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/liquidity-mining/>
