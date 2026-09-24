# cross_maker

Fair value is taken from the hedge venue's ticker last price. Each cycle the
strategy quotes a bid/ask on the primary venue, sizing each side with an
Avellaneda-style inventory skew so the side that reduces inventory quotes larger.

## Parameters

Inherited from `CrossVenueParams`: `primary` (required), `hedge` (required),
`symbol` (required), `poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary` | VenueRef | required | quoting venue (`{ exchange_id = "binance" }`) |
| `hedge` | VenueRef | required | fair-value venue (`{ exchange_id = "mock" }`) |
| `symbol` | String | required | trading symbol (same on both venues) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `spread` | Decimal | `0.002` | bid/ask spread from fair value (fraction) |
| `inventory_cap` | Decimal | required | inventory cap in base units; skew saturates at ±cap |
| `qty` | Decimal | `0.001` | base order quantity |

## Example configuration

```toml
[strategy]
type = "cross_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
inventory_cap = "0.5"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order`)
- `MarketDataSource` (`fetch_ticker`)

## Risk notes

- Skew relies on an in-memory signed inventory estimate that resets on restart → skew inaccurate after restart.
- Fair value from a single hedge-venue ticker last → stale if the hedge venue lags.
- Requires two live venues.
