# mean_reversion

Polls candles, computes the rolling Z-score of returns over the look-back
window, and trades extreme deviations:

- Z-score > +threshold  → price overextended → short.
- Z-score < -threshold  → price oversold → long.

The position is closed when the Z-score reverts to within `exit_band`, which
should be smaller than the entry threshold so the strategy does not flip
immediately after entry.

A classic statistical arbitrage primitive that works best when markets are
range-bound or oscillating around a well-defined fair value.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `period` | usize | `20` | rolling window |
| `entry_threshold` | Decimal | `"2.0"` | Z-score level for an entry |
| `exit_band` | Decimal | `"0.5"` | Z-score level to exit |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "mean_reversion"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 20
entry_threshold = "2.0"
exit_band = "0.5"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- This is a "catching falling knives" strategy. In a strong, persistent trend
  it can fall repeatedly into deep Z-score territory. Combine with a
  trend filter (e.g. ADX) to suppress entries against the macro trend.
