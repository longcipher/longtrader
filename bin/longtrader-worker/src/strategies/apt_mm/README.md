# apt_mm

APT (Arbitrage Pricing Theory) market maker with lead-lag regression.

## Model

Models the lead–lag relationship between two correlated instruments —
typically futures and spot — using a rolling OLS regression:

```
R_futures = α + β · R_spot + ε
```

The residual `ε` is mean reverting around 0. When ε is large-positive the
futures is "overvalued" relative to the model:

```
if  ε >  threshold · stdev(ε)  →  sell futures / buy spot
if  ε < −threshold · stdev(ε)  →  buy futures / sell spot
```

A passive-maker ladder is quoted based on the model-implied fair value of
the futures. The rolling window (`window`) refreshes the OLS coefficients
each tick.

## Parameters

| Key            | Default | Description                                   |
|----------------|---------|-----------------------------------------------|
| `lead_symbol`  | —       | Lead symbol (e.g. perpetual futures).         |
| `lag_symbol`   | —       | Lag symbol (e.g. spot).                       |
| `window`       | 50      | Rolling regression window.                    |
| `threshold`    | 1.0     | Entry threshold as multiple of residual stdev.|
| `qty`          | 0.001   | Position size in base units.                  |
| `half_spread`  | 0.001   | Half-spread for the passive ladder.           |
| `levels`       | 3       | Maker levels per side.                        |
| `maker_qty`    | 0.001   | Quantity per maker level.                     |

## When to use

Pairs trading and relative-value market making between instruments with a
stable lead-lag relationship — futures vs. spot, or correlated ETF pairs.
