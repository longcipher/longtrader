# rebalance

Reads the account snapshot, computes each asset's value against live prices, and
rebalances toward `targets` weights (as fractions that should sum to 1). Trades
are planned in quote value and only executed for assets whose weight drift
exceeds `band_pct` (default 5%).

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `targets` | BTreeMap<String, Decimal> | required | target weights per asset, e.g. `{ "BTC": "0.5", "ETH": "0.5" }` |
| `band_pct` | Decimal | `0.05` | rebalance only when `|actual − target|` weight exceeds this |
| `quote_asset` | String | `"USDT"` | numéraire used for valuation |
| `include_quote_asset` | bool | `true` | include the quote asset in the weight budget |
| `rebalance_interval_secs` | u64 | `30` | poll cadence (seconds) |
| `refetch_prices` | bool | `true` | refresh prices each cycle (vs cache) |
| `use_limit` | bool | `false` | place limit instead of market |
| `limit_offset` | Decimal | `0.001` | limit offset from price (fraction) |

## Example configuration

```toml
[strategy]
type = "rebalance"
[strategy.params]
targets = { "BTC": "0.5", "ETH": "0.5" }
band_pct = "0.05"
quote_asset = "USDT"
rebalance_interval_secs = 30
use_limit = false
```

## Capabilities

- `TradingGateway` (`get_account`, `create_order`)
- `MarketDataSource` (`fetch_ticker` / `get_prices`)

## Risk notes

- Rebalancing **realizes taxable events** on every rebalance.
- Uses live prices; quotes can be stale between polls.
- `band_pct` gates trades — small bands rebalance more often (more fees/tax).
- With `include_quote_asset = true` the quote asset is part of the budget, so cash is also rebalanced.
