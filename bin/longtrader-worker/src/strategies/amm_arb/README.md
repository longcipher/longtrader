# AMM Arbitrage

Exploits price discrepancies between a centralised exchange (CEX) and a decentralised AMM venue. When the spread exceeds the gas cost plus profit threshold, the strategy buys on the cheaper venue and sells on the more expensive one.

## How it works

1. Fetch the mid-price on both the CEX and the AMM.
2. Compute the spread: `|cex_price − amm_price| / min(cex_price, amm_price)`.
3. If `spread > threshold + gas_cost`, execute the arbitrage:
   - CEX cheaper → buy on CEX, sell on AMM.
   - AMM cheaper → buy on AMM, sell on CEX.

## Configuration

| Parameter           | Default     | Description                               |
| ------------------- | ----------- | ----------------------------------------- |
| `symbol`            | required    | Trading symbol.                           |
| `[primary]`         | required    | CEX venue: `[primary].exchange_id`.       |
| `[hedge]`           | required    | AMM venue: `[hedge].exchange_id`.         |
| `threshold`         | `0.005`     | Minimum spread fraction to trigger.       |
| `gas_cost`          | `0.001`     | Estimated gas cost as fraction.           |
| `max_qty`           | `1`         | Maximum position size per arb.            |
| `min_profit`        | `1`         | Minimum profit in quote units.            |
| `poll_secs`         | `30`        | Poll cadence in seconds.                  |

## Reference

<https://hummingbot.org/strategies/amm-arbitrage/>
