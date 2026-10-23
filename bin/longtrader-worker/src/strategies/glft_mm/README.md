# glft_mm

GLFT (Guéant–Lehalle–Fernandez–Tapia) optimal market maker.

## Model

The GLFT closed-form solution gives the optimal bid/ask half-spread as a
function of risk aversion, volatility, and market order arrival intensity:

```
δ_bid* = γ·σ²·(T − t) + (1/γ)·ln(1 + γ/k)
δ_ask* = γ·σ²·(T − t) − (1/γ)·ln(1 − γ/k)
```

where `γ` is risk aversion, `σ` is volatility, `k` is the order-arrival
decay, and `(T − t)` is the remaining time horizon. The inventory skew:

```
skew = γ · σ² · q
```

The reservation price is `R = mid − skew`; bid and ask are placed at
`R ∓ δ*`. This is the canonical optimal MM baseline that maximises
expected utility of terminal wealth.

The regularity condition `γ < k` must hold for the ask-side log to be
defined; the function returns `None` when violated.

## Parameters

| Key                | Default | Description                                   |
|--------------------|---------|-----------------------------------------------|
| `gamma`            | 0.1     | Risk-aversion coefficient γ.                  |
| `sigma`            | 1.0     | Volatility σ (fallback before EMA warms up).  |
| `arrival_intensity`| 10      | Order arrival intensity A.                    |
| `decay`            | 1.0     | Order arrival decay k.                        |
| `time_horizon`     | 1.0     | Remaining horizon in matching units.          |
| `qty`              | 0.001   | Quote quantity per side.                      |
| `vol_window`       | 20      | Rolling window for realised vol estimation.   |

## When to use

The theoretically-grounded baseline for any market making operation — ideal
when the order-arrival model is a reasonable approximation of real queue
dynamics, or as a benchmark against simpler heuristics.
