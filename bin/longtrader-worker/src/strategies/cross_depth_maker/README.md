# cross_depth_maker

Fair value is taken from the hedge venue's level-1 order-book mid
`(best_bid + best_ask) / 2`. Each cycle the strategy quotes a bid/ask pair on the
primary venue at `± spread` from that mid.

## Parameters

Inherited from `CrossVenueParams`: `primary` (required), `hedge` (required),
`symbol` (required), `poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary` | VenueRef | required | quoting venue (`{ exchange_id = "binance" }`) |
| `hedge` | VenueRef | required | fair-value venue (`{ exchange_id = "mock" }`) |
| `symbol` | String | required | trading symbol |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `spread` | Decimal | `0.002` | bid/ask spread from book-mid (fraction) |
| `qty` | Decimal | `0.001` | quantity per side |

## Example configuration

```toml
[strategy]
type = "cross_depth_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`create_order`)
- `MarketDataSource` (`fetch_order_book`)

## Risk notes

- Fair value from a single level-1 book → a thin book gives a misleading mid.
- No inventory skew (flat sizing) → inventory accumulation.
- Requires two live venues with an order-book feed (the mock stubs only a ticker).
