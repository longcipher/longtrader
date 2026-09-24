# balance_align

Tracks a target margin ratio; when the account's margin ratio drifts below
`target_ratio` it tops up buying power by transferring `rebalance_amount` via
`VenueOpInvoker.account_transfer`.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol (scope) |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `target_ratio` | Decimal | `0.5` | desired margin ratio |
| `check_interval_secs` | u64 | `60` | check cadence (seconds) |
| `currency` | String | `"USDT"` | transfer currency |
| `rebalance_amount` | Decimal | `100` | quote amount to transfer per top-up |

## Example configuration

```toml
[strategy]
type = "balance_align"
[strategy.params]
symbol = "BTC/USDT"
target_ratio = "0.5"
check_interval_secs = 60
currency = "USDT"
rebalance_amount = "100"
```

## Capabilities

- `TradingGateway` (`get_account`, `create_order`)
- `MarketDataSource` (`fetch_ticker`)
- `VenueOpInvoker` (`account_transfer`)

## Risk notes

- Requires `VenueOpInvoker.account_transfer`, which the open mock only **stubs** (logs a dry-run warning, performs no real transfer).
- Transfers may be unsupported or have limits on the real backend.
- Margin ratio is read from the account snapshot; cadence lag can miss fast moves.
