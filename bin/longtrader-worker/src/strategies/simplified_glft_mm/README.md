# simplified_glft_mm

Simplified GLFT market maker — calibration-free variant.

## Model

A lighter variant of the full GLFT model that skips order-arrival
calibration. The half-spread is taken directly from realised volatility:

```
half_spread = vol_to_spread · σ
```

The reservation price combines a **micro-price** (bid-ask weighted
midpoint) with an Avellaneda-style inventory skew:

```
mid_micro = (bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)
R        = mid_micro − q · γ · σ² · (T − t)
bid      = R − half_spread
ask      = R + half_spread
```

The `micro_weight` parameter blends between simple mid (`0`) and pure
micro-price (`1`). Calibration-free `vol_to_spread` replaces the full
arrival/decay model and is practically robust across a wide range of
assets; the inventory term keeps exposure bounded.

## Parameters

| Key             | Default | Description                                   |
|-----------------|---------|-----------------------------------------------|
| `vol_to_spread` | 0.5     | Spread multiplier on realised vol.            |
| `gamma`         | 0.1     | Risk-aversion γ (inventory penalty).          |
| `sigma`         | 1.0     | Volatility σ (fallback before EMA warms up).  |
| `time_horizon`  | 1.0     | Remaining horizon in matching units.          |
| `qty`           | 0.001   | Quote quantity per side.                      |
| `vol_window`    | 20      | Rolling window for realised vol estimation.   |
| `micro_weight`  | 0.5     | Micro-price blend: 0 = mid, 1 = micro-price.  |

## When to use

Most everyday market making — fast to calibrate (only `vol_to_spread`
needs tuning), robust across assets, and avoids over-fitting the arrival
model. The go-to strategy when full GLFT calibration is impractical.
