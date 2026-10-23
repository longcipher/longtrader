# basis_mm

Basis market maker — spot vs. perpetual mean reversion with passive making.

## Model

The basis `B = mark_price − spot_price` tends to revert around a rolling
mean. The strategy trades the z-score of the basis:

```
z = (B − mean(B)) / stdev(B)

if  z >  threshold  →  sell perp / buy spot
if  z < −threshold  →  buy perp / sell spot
if  mean reverts    →  close with profit
```

The quoting side **additionally** layers a tight passive-maker ladder around
the fair value derived from the basis forecast.

A rolling window (`window`) maintains mean and variance for the z-score
calculation. Inventory is hedged by trading spot and perp in opposite
directions.

## Parameters

| Key            | Default | Description                                   |
|----------------|---------|-----------------------------------------------|
| `perp_symbol`  | —       | Perpetual leg symbol (e.g. `BTC/USDT:USDT`).  |
| `window`       | 50      | Rolling window for basis statistics.          |
| `threshold`    | 1.0     | Z-score entry threshold.                      |
| `qty`          | 0.001   | Position size in base units per trade.        |
| `half_spread`  | 0.001   | Half-spread for the passive ladder.           |
| `maker_levels` | 3       | Passive-maker levels per side.                |
| `maker_qty`    | 0.001   | Quantity per maker level.                     |

## When to use

Cross-venue or cross-product mean reversion where the basis between spot
and perpetual futures is stationary — the classic crypto basis trade.
