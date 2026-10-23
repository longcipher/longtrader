# bb_keltner_squeeze

Polls candles, constructs both Bollinger Bands (SMA ± 2σ) and Keltner Channel
(EMA ± 1.5 ATR), and detects the "squeeze" — when the Bollinger Bands contract
inside the Keltner Channel. A squeeze signals an imminent volatility expansion.

On squeeze release, the direction is determined by where price breaks: above
the upper Bollinger → long, below the lower Bollinger → short. This is a
volatility breakout strategy popularized by John Carter's TTM Squeeze.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `poll_secs` | u64 | `60` | poll cadence (seconds) |
| `window` | usize | `20` | look-back period for both bands |
| `bb_mult` | Decimal | `"2.0"` | Bollinger standard-deviation multiplier |
| `kc_mult` | Decimal | `"1.5"` | Keltner ATR multiplier |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "bb_keltner_squeeze"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
window = 20
bb_mult = "2.0"
kc_mult = "1.5"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- A squeeze can persist for a long time without releasing; capital is idle.
- False releases are common in choppy markets — combine with a trend filter.
