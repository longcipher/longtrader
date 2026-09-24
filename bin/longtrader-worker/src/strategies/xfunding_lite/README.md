# xfunding_lite

Funding-rate capture. Polls funding rates; when `|funding_rate|` exceeds
`threshold` it opens a position directionally (short when funding is positive,
long when negative) sized by `qty`, with a `backoff_secs` cooldown after each
fill to avoid over-trading.

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
| `threshold` | Decimal | `0.0005` | act when `|funding_rate|` exceeds this |
| `qty` | Decimal | `0.001` | position size per entry |
| `backoff_secs` | u64 | `60` | cooldown after a fill |

## Example configuration

```toml
[strategy]
type = "xfunding_lite"
[strategy.params]
symbol = "BTC/USDT"
funding_interval_secs = 3600
threshold = "0.0005"
qty = "0.001"
backoff_secs = 60
```

## Capabilities

- `MarketDataSource` (`fetch_funding_rate`)
- `TradingGateway` (`create_order`, `get_positions`)

## Risk notes

- Funding rates are venue/symbol specific; the threshold is an absolute rate.
- Funding can stay one-sided for long stretches → directional exposure carries price risk, not just funding capture.
- May open many small positions; size `qty` conservatively.
