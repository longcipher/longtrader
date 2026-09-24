# cross_fixed_maker

Quotes a bid/ask pair on the primary venue around the hedge venue's mid. When a
primary leg is missing from open orders (filled), a market hedge order on the
hedge venue flattens the fill, keeping inventory flat.

## Parameters

Inherited from `CrossVenueParams`: `primary` (required), `hedge` (required),
`symbol` (required), `poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary` | VenueRef | required | quoting venue (`{ exchange_id = "binance" }`) |
| `hedge` | VenueRef | required | hedging venue (`{ exchange_id = "mock" }`) |
| `symbol` | String | required | trading symbol |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `spread` | Decimal | `0.002` | bid/ask spread from fair value |
| `qty` | Decimal | `0.001` | order quantity (and hedge qty) |

## Example configuration

```toml
[strategy]
type = "cross_fixed_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`fetch_open_orders`, `create_order`)
- `MarketDataSource` (`fetch_ticker`)

## Risk notes

- Hedge is a market order → slippage, and hedge volume equals `qty` regardless of actual fill size (over/under-hedge).
- Fill detection is by "missing open order" (bid/ask count imbalance) → fragile if the venue drops orders.
- Requires two live venues.
