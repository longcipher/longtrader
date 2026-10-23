# DCA Executor

Dollar-Cost Averaging position execution. Ported from Hummingbot's
[DCA strategy](https://hummingbot.org/strategies/dca/).

## Summary

Places multiple limit orders at progressively better prices. Each
level has its own amount and target price. When the market reaches a
level, the order triggers. Supports stop-loss, take-profit, and
trailing-stop barriers on the average entry.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `prices` | — | Target price per level |
| `amounts_quote` | — | Quote amount per level |
| `is_buy` | true | Buy or sell side |
| `is_maker` | true | Limit orders (maker) |
| `stop_loss` | 0.05 | Max loss fraction before close |
| `take_profit` | 0 | Profit fraction to trigger close |
| `trailing_activation` | 0 | Peak PnL to activate trailing stop |
| `trailing_delta` | 0 | Trailing distance |

## Risk

Stop-loss triggers on adverse moves. Trailing stops lock in gains
but may close too early in a trend.
