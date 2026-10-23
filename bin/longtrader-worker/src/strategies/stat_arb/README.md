# Statistical Arbitrage (Pairs Trading)

Identifies a pair of cointegrated assets and trades the spread between them. The spread is standardised into a z-score. When the z-score exceeds a threshold, the spread is opened (long the underperformer, short the outperformer). When it reverts, the spread is closed for a profit.

## How it works

1. Compute the spread: `price_A − hedge_ratio · price_B`.
2. Track the rolling mean and standard deviation of the spread.
3. Compute the z-score: `(spread − mean) / stdev`.
4. If `z > entry_threshold`: short spread (sell A, buy B).
5. If `z < −entry_threshold`: long spread (buy A, sell B).
6. If `|z| < exit_threshold` and a position is open: close.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Base symbol.                              |
| `symbol_a`          | required    | Lead asset symbol.                        |
| `symbol_b`          | required    | Lag asset symbol.                         |
| `exchange_id`       | `"mock"`    | Venue identifier.                         |
| `hedge_ratio`       | `1`         | Units of B per unit of A.                 |
| `window`            | `50`        | Rolling window for spread statistics.     |
| `entry_threshold`   | `2`         | Z-score entry threshold.                  |
| `exit_threshold`    | `0.5`       | Z-score exit threshold.                   |
| `qty`               | `1`         | Position size in units of A.              |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/statistical-arbitrage/>
