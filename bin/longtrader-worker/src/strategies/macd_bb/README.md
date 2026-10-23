# MACD + Bollinger Bands

A combined momentum and mean-reversion strategy. Uses MACD for momentum direction and Bollinger Bands for entry timing. A buy signal requires bullish MACD (line above signal) AND price near the lower Bollinger Band. A sell signal is the inverse.

## How it works

1. Compute the MACD line (fast EMA − slow EMA) and signal line (EMA of MACD).
2. Compute Bollinger Bands over the close window.
3. If `macd > signal` and `close ≤ lower_band`: buy.
4. If `macd < signal` and `close ≥ upper_band`: sell.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `macd_fast`         | `12`        | MACD fast EMA window.                     |
| `macd_slow`         | `26`        | MACD slow EMA window.                     |
| `macd_signal`       | `9`         | MACD signal EMA window.                   |
| `boll_window`       | `20`        | Bollinger window length.                  |
| `boll_mult`         | `2`         | Bollinger standard deviation multiplier.  |
| `qty`               | `1`         | Position size in base units.              |
| `timeframe`         | `"5m"`      | Candle timeframe.                         |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/macd-bollinger/>
