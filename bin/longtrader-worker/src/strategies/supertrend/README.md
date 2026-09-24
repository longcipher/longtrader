# supertrend

Polls candles, computes the Supertrend (ATR + multiplier), and on a fresh
direction flip (down→up or up→down) submits a market order to flip exposure to
long/short. Direction is edge-triggered in-memory; after a restart it waits for
the next fresh flip rather than replaying the last one.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `atr_period` | usize | `10` | ATR lookback |
| `multiplier` | Decimal | `3.0` | Supertrend ATR multiplier |
| `qty` | Decimal | `0.001` | order quantity |
| `use_limit` | bool | `false` | place limit instead of market (offset = `limit_offset`) |
| `limit_offset` | Decimal | `0.001` | limit offset from price (fraction) |

## Example configuration

```toml
[strategy]
type = "supertrend"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
atr_period = 10
multiplier = "3.0"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order`)
- `MarketDataSource` (`get_candles`)

## Risk notes

- Market orders slip versus the signal price; limit mode can miss fills.
- Direction is in-memory → restart may flip on the first post-restart signal.
- Lagging trend filter → whipsaws in choppy regimes.
