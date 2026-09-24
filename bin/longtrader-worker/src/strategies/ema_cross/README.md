# ema_cross

Polls candles, computes fast/slow EMAs, and submits a market order on a fresh
fast/slow cross. Cross state is edge-triggered in-memory; after a restart it waits
for the next fresh cross instead of replaying the last one.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `fast_window` | usize | `9` | fast EMA period |
| `slow_window` | usize | `21` | slow EMA period |
| `qty` | Decimal | `0.001` | market order quantity |

## Example configuration

```toml
[strategy]
type = "ema_cross"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
fast_window = 9
slow_window = 21
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Market orders slip versus the signal price.
- Cross state is in-memory → a restart may trade on the first post-restart cross.
- Lagging indicator → whipsaws in choppy regimes.
