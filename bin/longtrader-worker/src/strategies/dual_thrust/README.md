# dual_thrust

Polls candles and channels the N-day range into an upper and lower "thrust"
band using the classic ratio parameters K1 / K2. Breach of the upper band opens
long; breach of the lower band opens short. Exiting is via a stop based on
the opposite band, so the strategy serves as a short-term opening-range
breakout with built-in asymmetric通道.

Originally popularized as a day-trading system on futures, it adapts cleanly to
perpetual-crypto markets where morning ranges mark meaningful support and
resistance.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"1h"` | candle timeframe |
| `period` | usize | `5` | look-back window for the high/low range |
| `k1` | Decimal | `"0.5"` | upper-band ratio (Open + K1 * range) |
| `k2` | Decimal | `"0.5"` | lower-band ratio (Open - K2 * range) |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "dual_thrust"
[strategy.params]
symbol = "ETH/USDT"
exchange_id = "mock"
period = 5
k1 = "0.5"
k2 = "0.5"
qty = "0.01"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Breakouts from short opening ranges are easily faked out in low-volume
  sessions — the K ratios can be tightened to reduce the false-signal rate
  but at the cost of missing genuine breakouts.
