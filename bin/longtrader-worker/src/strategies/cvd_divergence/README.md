# cvd_divergence

Polls candles, computes the cumulative volume delta (CVD) approximation over
the rolling window, and detects between-price-and-volume divergence:
when price pushes higher but CVD starts to fade (bearish divergence), or
price pushes lower while CVD holds up (bullish divergence).

This serves as a regime-aware mean-reversion overlay that reacts when momentum
is confirmed by volume. A typical use is fading moves where CVD inks in the
opposite direction.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"15m"`),
`poll_secs` (default `60`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"15m"` | candle timeframe |
| `period` | usize | `20` | rolling window |
| `z_threshold` | Decimal | `"1.5"` | Z-score threshold to flag divergence |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "cvd_divergence"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 20
z_threshold = "1.5"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- CVD is a proxy; futures open-interest / CVD data from a dedicated feed is far more accurate.
- Divergence is not timing — the move can continue long after the divergence appears.
