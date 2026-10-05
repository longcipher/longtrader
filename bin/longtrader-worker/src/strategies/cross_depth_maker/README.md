# cross_depth_maker

Fair value is the hedge venue's level-1 book mid — the mean of the **first bid
level and the first ask level as the venue returned them** (nothing is sorted, so
the hedge venue must return each side best-first).

Each cycle the strategy quotes a bid/ask pair on the primary venue at
`mid * (1 - spread)` and `mid * (1 + spread)`.

`spread` is a **fraction of the mid, not a price offset**. Worked example: a mid
of `100` with `spread = "0.002"` quotes `99.8` / `100.2` — the quotes sit `0.2`
away from the mid, not `0.002`. The distance therefore scales with the price, so
the same config on an instrument at `10 000` quotes `9 980` / `10 020`.

## Parameters

Inherited from `CrossVenueParams`: `primary` (required), `hedge` (required),
`symbol` (required), `poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary` | VenueRef | required | quoting venue (`{ exchange_id = "binance" }`) |
| `hedge` | VenueRef | required | fair-value venue (`{ exchange_id = "mock" }`) |
| `symbol` | String | required | trading symbol |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `spread` | Decimal | `0.002` | half-spread as a fraction of the mid (`0.002` = 0.2%) |
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
- A refused quote fails the cycle, so the pair is never half-quoted; one bad leg
  means no quote at all until the next poll.
- Requires two live venues with an order-book feed (the mock stubs only a ticker).
