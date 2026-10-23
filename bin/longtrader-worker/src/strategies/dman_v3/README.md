# DMAN v3 (Dual Moving Average Nested)

A directional strategy using nested moving averages across multiple timeframes. Computes fast and slow EMAs on the primary timeframe, plus a trend-filter EMA on a higher timeframe. A long signal requires the fast EMA to cross above the slow EMA while price is above the trend filter.

## How it works

1. Compute fast EMA, slow EMA, and trend-filter EMA over the close window.
2. If `ema_fast > ema_slow` and `close > ema_trend`: buy.
3. If `ema_fast < ema_slow` and `close < ema_trend`: sell.
4. Otherwise: no trade.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `fast_window`       | `9`         | Fast EMA window.                          |
| `slow_window`       | `21`        | Slow EMA window.                          |
| `trend_window`      | `50`        | Trend-filter EMA window.                  |
| `qty`               | `1`         | Position size in base units.              |
| `timeframe`         | `"5m"`      | Candle timeframe.                         |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/directional-macd-ema/>
