# simple_grid

Price-addressed grid quoting with fill-flip rebalancing and an error kill-switch.

The strategy maintains a static ladder of limit orders between `lower_price` and
`upper_price`. When the venue reports a level as gone (filled or cancelled), the
opposite leg is placed one grid step away, so a filled buy re-quotes as a sell
one step higher — the classic grid inventory cycle.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label` (default `""`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | Trading symbol |
| `lower_price` | Decimal | required | Lower band bound |
| `upper_price` | Decimal | required | Upper band bound |
| `num_levels` | u32 | required | Grid step divisor (step = span / num_levels) |
| `qty_per_level` | Decimal | required | Order size per rung |
| `profit_spread_pct` | Decimal | None (unset) | Reverse-leg spread as a fraction of the fill price; `None` uses one grid step |
| `state_file` | String | None (unset) | Optional path for restart recovery |

## Restart recovery

With `state_file` set, the ladder is written after every seed and after the
kill-switch clears it. On the first `rebalance` after a restart the persisted
ladder is resumed instead of seeding a second one on top of the orders the
previous process left resting. A missing file is a normal first start; a
**corrupt or unreadable file stops the grid** rather than silently
double-quoting.

## Example config

```toml
[strategy]
type = "simple_grid"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
lower_price = "90000"
upper_price = "110000"
num_levels = 20
qty_per_level = "0.001"
state_file = "/var/lib/longtrader/simple-grid-state.json"
```

## Required capabilities

- `TradingGateway` (`create_order` / `cancel_order` / `fetch_open_orders`)
- `MarketDataSource` (`fetch_ticker`)

## Price resolution

`current_price` reads the ticker's `last`, then `close`, then falls back to the
bid-ask midpoint. A rung that is absent, undecodable, or not strictly positive
is skipped; the ladder only fails when every rung is unusable.

## Risk notes

- The grid has no stop-loss: a trend through the whole band accumulates
  one-sided inventory. Size `qty_per_level` for that outcome.
- Five consecutive placement errors trip the kill-switch: every tracked order
  is cancelled and the ladder is cleared.
- Restored levels outside the current band are dropped with a warning; they
  can be neither re-quoted nor flipped.
