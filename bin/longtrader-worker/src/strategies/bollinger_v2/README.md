# Bollinger Bands v2

A directional mean-reversion strategy using Bollinger Bands. Goes long when the price touches the lower band (oversold) and short/sells when it touches the upper band (overbought). A squeeze filter avoids trading in low-volatility regimes.

## How it works

1. Compute Bollinger Bands over a rolling window of closes.
2. If `band_width / middle < squeeze_threshold`: no trade (squeeze).
3. If `close ≤ lower_band`: buy (mean reversion up).
4. If `close ≥ upper_band`: sell (mean reversion down).

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `window`            | `20`        | Bollinger window length.                  |
| `mult`              | `2`         | Standard deviation multiplier.            |
| `squeeze_threshold` | `0.01`      | Minimum band width fraction to trade.     |
| `qty`               | `1`         | Position size in base units.              |
| `timeframe`         | `"5m"`      | Candle timeframe.                         |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/bollinger-bands-v2/>
