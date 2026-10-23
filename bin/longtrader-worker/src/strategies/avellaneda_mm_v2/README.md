# Avellaneda–Stoikov Market Making v2

Direct AS optimal spread with trade-intensity tracking. Ported from
Hummingbot's [Avellaneda-Stoikov strategy](https://hummingbot.org/strategies/avellaneda-market-making/).

## Summary

Computes the optimal bid/ask half-spread from the Avellaneda-Stoikov
closed form using the *observed* trade-arrival intensity λ instead of a
calibrated decay parameter. Higher trade flow → narrower spreads because
fills arrive faster. Adds a reservation-price inventory skew.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `gamma` | 0.1 | Risk-aversion γ |
| `sigma` | 1.0 | Volatility σ (fallback before EMA warms up) |
| `time_horizon` | 1.0 | Remaining horizon (matches σ units) |
| `qty` | 0.001 | Quote size per side |
| `intensity_window` | 20 | Rolling window for trade-intensity estimation |
| `vol_window` | 20 | Rolling window for realised vol estimation |
| `min_decay` | 0.1 | Floor for effective decay k_eff |

## Risk

Continuous re-quotation. Trade-intensity tracking adapts to market
conditions but lags regime changes during sudden volatility spikes.
