# premium_monitor

Monitor-only strategy. Polls funding rates; when a funding rate's absolute value
exceeds `threshold` it logs/prints an alert. No trades are opened.

## Parameters

Inherited from `CommonParams`: `exchange_id` (default `"mock"`), `label`
(default `""`), `symbol` (required), `timeframe` (default `"5m"`),
`poll_secs` (default `30`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | String | required | trading symbol |
| `timeframe` | String | `"5m"` | candle timeframe (passed through) |
| `poll_secs` | u64 | `30` | poll cadence (seconds) |
| `funding_interval_secs` | u64 | `3600` | funding-rate poll cadence (seconds) |
| `threshold` | Decimal | `0.001` | alert when `|funding_rate|` exceeds this |

## Example configuration

```toml
[strategy]
type = "premium_monitor"
[strategy.params]
symbol = "BTC/USDT"
funding_interval_secs = 3600
threshold = "0.001"
```

## Capabilities

- `MarketDataSource` (`fetch_funding_rate`)
- `TradingGateway` (`get_positions`)

## Risk notes

- **Monitor only — opens no trades.** Funding rates are venue/symbol specific; the threshold is an absolute rate, not annualized.
- Useful as a signal feed; pair with a trading strategy if you want to act on it.
