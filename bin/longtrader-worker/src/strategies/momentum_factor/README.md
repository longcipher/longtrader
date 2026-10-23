# momentum_factor

Polls candles and builds a composite momentum score over a rolling window:

- return over `period` bars (raw momentum),
- RSI smoothed (pace of gains vs losses).
The score drives long when both components are above their respective
thresholds, short when both are below. Exits trigger when momentum fades
below the neutral zone.

This strategy fuses trend-continuation momentum with velocity, an approach
that tends to trail the start of a move but stay through its middle.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `period` | usize | `14` | look-back window |
| `return_threshold` | Decimal | `"0.02"` | minimum return to trigger momentum |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "momentum_factor"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 14
return_threshold = "0.02"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Momentum is inherently lagging: enters late and exits very late. In
  consolidating markets it can produce several successive small losses.
- Lower thresholds increase sensitivity but also the false-breakout rate.
