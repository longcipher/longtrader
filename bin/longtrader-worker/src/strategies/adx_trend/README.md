# adx_trend

Polls candles, computes the Average Directional Index (ADX) and the directional
indicators +DI / -DI. Enters long when ADX > threshold (strong trend) and +DI
crosses above -DI; enters short when ADX > threshold and -DI crosses above +DI.
Exits when ADX falls below the threshold (trend weakening).

This is the classic Wilder trend-strength filter: avoid trading when the market
is directionless and only take signals when the ADX confirms a strong trend.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `poll_secs` | u64 | `60` | poll cadence (seconds) |
| `period` | usize | `14` | ADX smoothing period |
| `threshold` | Decimal | `"25"` | ADX threshold for trending market |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "adx_trend"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 14
threshold = "25"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- ADX is a lagging indicator; the trend is well-established before it crosses
  the threshold.
- A high ADX alone says nothing about direction — the DI cross is essential.
