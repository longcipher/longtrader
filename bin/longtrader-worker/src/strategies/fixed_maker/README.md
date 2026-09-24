# fixed_maker

Polls the ticker, re-centres on the mid price, and each cycle cancels all orders
and re-quotes a bid at `mid * (1 - bid_spread)` and an ask at `mid * (1 + ask_spread)`.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `bid_spread` | Decimal | `0.001` | bid offset from mid (fraction) |
| `ask_spread` | Decimal | `0.001` | ask offset from mid (fraction) |
| `qty` | Decimal | `0.001` | quantity per side |

## Example configuration

```toml
[strategy]
type = "fixed_maker"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
bid_spread = "0.001"
ask_spread = "0.001"
qty = "0.001"
```

## Capabilities

- `TradingGateway` (`cancel_all_orders`, `create_order`)
- `MarketDataSource` (`fetch_ticker`)

## Risk notes

- Cancels all orders every poll — wipes unrelated resting orders on the symbol.
- No inventory skew → adverse selection in trends (quotes get hit on the losing side).
- Market/limit fills slip.
