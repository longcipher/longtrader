# darvas_box

Polls candles and detects a "Darvas Box" — a short-term congestion zone where a
stock makes a new N-day high, then trades within a tight rectangle (the box)
for at least three bars. A breakout above the box top triggers a long entry,
with a stop at the box bottom. The mirror condition on the downside triggers
a short.

Originally devised by Nicolas Darvas in the 1950s for equities, it translates
well to trending crypto markets where a pause at a new high frequently precedes
a further leg up.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `period` | usize | `20` | look-back window to establish "new high" |
| `box_bars` | usize | `3` | minimum bars inside the box to confirm |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "darvas_box"
[strategy.params]
symbol = "ETH/USDT"
exchange_id = "mock"
period = 20
box_bars = 3
qty = "0.05"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Requires a prolonged trending market to generate 52-week / N-day highs.
  In range-bound markets this strategy stays idle for weeks on end.
