# Perpetual Market Making

MM on perpetual futures with funding-rate awareness. Ported from
Hummingbot's [Perpetual MM](https://hummingbot.org/strategies/perpetual-market-making/).

## Summary

Quotes two-sided on a perpetual contract, skewing inventory toward
flattening as funding accrues. When funding is positive, the strategy
leans short to capture the premium; when negative, leans long.
Monitors position limits and liquidation prices.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `perp_symbol` | — | Perpetual contract symbol |
| `half_spread` | 0.0005 | Base half-spread |
| `qty` | 0.001 | Quote size per side |
| `position_limit` | 1.0 | Max inventory before max skew |
| `funding_threshold` | 0.00001 | Min funding to trigger skew |
| `liq_buffer` | 0.1 | Fraction of margin to stay clear of liquidation |
| `vol_window` | 20 | Rolling window for vol estimation |

## Risk

Funding-rate capture is not guaranteed — rate can invert. Liquidation
risk exists if margin is thin relative to position.
