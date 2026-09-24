# hedge_grid

Runs a grid on the primary venue (limit buys below / sells above the mid) and, on
each detected fill imbalance (resting bid/ask count mismatch), sends an opposite
market hedge order on the hedge venue to keep net delta ~zero.

## Parameters

Inherited from `CrossVenueParams`: `primary` (required), `hedge` (required),
`symbol` (required), `poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary` | VenueRef | required | quoting venue (`{ exchange_id = "mock" }`) |
| `hedge` | VenueRef | required | hedging venue (`{ exchange_id = "mock" }`) |
| `symbol` | String | required | symbol (same on both venues) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `lower_price` | Decimal | required | grid lower bound |
| `upper_price` | Decimal | required | grid upper bound |
| `num_levels` | u32 | required | number of grid steps |
| `qty_per_level` | Decimal | required | order qty per level (and per hedge) |

## Example configuration

```toml
[strategy]
type = "hedge_grid"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "mock" }
hedge = { exchange_id = "mock" }
lower_price = "90000"
upper_price = "110000"
num_levels = 10
qty_per_level = "0.001"
```

## Capabilities

- `TradingGateway` (`fetch_open_orders`, `create_order`)
- `MarketDataSource` (`fetch_ticker`)

## Risk notes

- Hedge is a market order → slippage.
- Hedge size equals `qty_per_level` regardless of actual fill size, so net delta is approximate.
- Fill detection is by bid/ask count imbalance (not real size).
- Requires two live venues (the mock stubs only one).
