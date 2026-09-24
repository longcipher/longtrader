# boll_grid

Polls candles, computes Bollinger bands over a close-price window, and — when the
latest close sits inside sufficiently wide bands — cancels the previous ladder and
re-quotes `grid_num` buy limits below and `grid_num` sell limits above the close.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `boll_window` | usize | `21` | candles in the Bollinger window |
| `boll_mult` | Decimal | `2` | Bollinger band multiplier (k) |
| `grid_num` | u32 | `3` | buy/sell limit levels per side |
| `qty` | Decimal | `0.001` | quantity per leg |
| `profit_spread_pct` | Decimal | `0.0005` | reverse-leg spread as a fraction of price |

## Example configuration

```toml
[strategy]
type = "boll_grid"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
timeframe = "5m"
boll_window = 21
boll_mult = 2
grid_num = 3
qty = "0.001"
profit_spread_pct = "0.0005"
```

## Capabilities

- `TradingGateway` (`cancel_all_orders`, `create_order`)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Cancels **all** orders on the symbol every cycle, so only this ladder survives.
- Bollinger bands need warmup and quotes appear only inside wide bands — may stay idle.
- Grids accumulate inventory in one-sided trends; limit/market fills slip.
