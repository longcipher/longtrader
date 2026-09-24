# random_entry

**Research/demo strategy — not profitable.** On each cycle it flips a biased coin
(`p` probability long) and submits a market order of `qty`, optionally seeded for
reproducibility. Use it to load-test the worker, not to trade.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `qty` | Decimal | `0.001` | order quantity |
| `seed` | u64 | `42` | RNG seed (reproducible runs) |
| `p` | Decimal | `0.1` | probability of a long entry per cycle |

## Example configuration

```toml
[strategy]
type = "random_entry"
[strategy.params]
symbol = "BTC/USDT"
qty = "0.001"
seed = 42
p = "0.1"
```

## Capabilities

- `TradingGateway` (`create_order` / market order)
- `MarketDataSource` (`get_candles`)

## Risk notes

- **Not a trading strategy** — entries are random and expected PnL is ~0 minus fees.
- Market orders slip; runs can accumulate fees/inventory.
- Use only on the mock venue for load/integration testing.
