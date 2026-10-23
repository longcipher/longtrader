# Cross-Exchange Market Making (XEMM)

A cross-venue market making strategy that takes liquidity on one venue (the "taker") and makes on another venue (the "maker"). The strategy profits from the spread difference between venues.

## How it works

The strategy reads the best bid/ask from the taker venue and quotes a wider half-spread on the maker venue. When a maker-side fill occurs, it can be profitably hedged on the taker side since:

- **Maker buy** → hedge by selling on the taker (at a higher price).
- **Maker sell** → hedge by buying on the taker (at a lower price).

## Configuration

| Parameter              | Default     | Description                               |
| ---------------------- | ----------- | ----------------------------------------- |
| `symbol`               | required    | Trading symbol (same on both venues).     |
| `[primary]`            | required    | Maker venue: `[primary].exchange_id`.     |
| `[hedge]`              | required    | Taker venue: `[hedge].exchange_id`.       |
| `min_profitability`    | `0.0001`    | Minimum profitability fraction.           |
| `qty`                  | `0.001`     | Quote quantity per side.                  |
| `levels`               | `3`         | Number of maker levels per side.          |
| `step_size`            | `0.0005`    | Step size as fraction between levels.     |
| `poll_secs`            | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/cross-exchange-market-making/>
