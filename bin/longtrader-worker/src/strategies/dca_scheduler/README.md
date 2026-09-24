# dca_scheduler

On a fixed cron schedule (default daily midnight UTC) buys `buy_amount` of each
symbol in `symbols` with a market (or limit) order. Cadence is time-based, not
price-based.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | quote symbol for the DCA buys |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `schedule_cron` | String | `"0 0 * * *"` | cron expression (min hour day month weekday), UTC |
| `buy_amount` | Decimal | `100` | quote amount to spend per symbol per run |
| `symbols` | Vec<String> | required | base assets to dollar-cost-average |
| `use_limit` | bool | `false` | place limit instead of market |
| `limit_offset` | Decimal | `0.001` | limit offset from price (fraction) |

## Example configuration

```toml
[strategy]
type = "dca_scheduler"
[strategy.params]
symbol = "USDT"
schedule_cron = "0 0 * * *"
buy_amount = "100"
symbols = ["BTC", "ETH"]
```

## Capabilities

- `TradingGateway` (`create_order`)
- `MarketDataSource` (`fetch_ticker`)

## Risk notes

- Fixed cadence ignores price levels → buys into local tops.
- Market orders slip; limit mode can miss fills and skip the run.
- cron is evaluated in UTC.
