# ichimoku

Polls candles and computes the Ichimoku Cloud (Tenkan-sen, Kijun-sen, Senkou
Span A, Senkou Span B, Chikou Span). Enters long when Tenkan crosses above
Kijun while price is above the Cloud and the Cloud is bullish (Senkou A > B).
Enters short on the mirror. Exits when Tenkan recrosses Kijun or price re-enters
the Cloud.

A complete trend-following system from Japan that simultaneously defines
support/resistance, trend direction, momentum, and entry/exit timing.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"1h"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"4h"` | candle timeframe (Ichimoku designed for daily/4h) |
| `poll_secs` | u64 | `60` | poll cadence (seconds) |
| `tenkan_period` | usize | `9` | Tenkan-sen look-back |
| `kijun_period` | usize | `26` | Kijun-sen / Senkou A base |
| `senkou_b_period` | usize | `52` | Senkou Span B look-back |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "ichimoku"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
tenkan_period = 9
kijun_period = 26
senkou_b_period = 52
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Ichimoku is inherently lagging — Span A/B are displaced +26 bars, the cloud
  reflects conditions a month past on weekly frames.
- Whipsaws around the cloud edge are common; Tenkan/Kijun cross confirms momentum.
