# deposit_transfer

Spot/funding sweeper. When `get_deposits` reports new deposits above
`deposit_threshold`, it transfers `rebalance_amount` from the source account to
`target_account` via `VenueOpInvoker.account_transfer`.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol (scope) |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `check_interval_secs` | u64 | `60` | deposit check cadence (seconds) |
| `deposit_threshold` | Decimal | `0` | minimum deposit size to act on |
| `currency` | String | `"USDT"` | deposit/transfer currency |
| `target_account` | String | `"spot"` | destination account |
| `rebalance_amount` | Decimal | `100` | amount to transfer per sweep |

## Example configuration

```toml
[strategy]
type = "deposit_transfer"
[strategy.params]
symbol = "BTC/USDT"
deposit_threshold = "0"
currency = "USDT"
target_account = "spot"
rebalance_amount = "100"
```

## Capabilities

- `TradingGateway` (`get_account`, `create_order`)
- `MarketDataSource` (`fetch_ticker`)
- `VenueOpInvoker` (`account_transfer`, `get_deposits`)

## Risk notes

- Requires `VenueOpInvoker.account_transfer` / `get_deposits`, which the open mock only **stubs** (dry-run warning, no real transfer).
- Deposit detection is naive (relies on the backend's deposit feed).
- Transfer may be unsupported or limited on the real backend.
