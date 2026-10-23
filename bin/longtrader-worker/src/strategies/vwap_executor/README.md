# VWAP Executor

Volume-Weighted Average Price execution. Ported from Hummingbot's
[VWAP strategy](https://hummingbot.org/strategies/vwap/).

## Summary

Reads the order book and allocates order sizes proportional to the
displayed volume at each price level. More volume at a level → more
size allocated there. Reduces slippage compared to TWAP by following
liquidity.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `total_amount` | — | Total quote units to execute |
| `price_spread` | 0.001 | Max distance from mid to consider |
| `volume_perc` | 0.001 | Fraction of book volume to take per level |
| `order_delay_secs` | 10 | Delay between orders |
| `is_buy` | true | Buy or sell side |

## Risk

Re-plan each tick may lead to over-execution if prices move
faster than the delay. Book depth can be spoofed.
