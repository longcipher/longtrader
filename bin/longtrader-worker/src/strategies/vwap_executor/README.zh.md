# VWAP Executor

成交量加权平均价执行。移植自 Hummingbot 的
[VWAP 策略](https://hummingbot.org/strategies/vwap/)。

## 摘要

读取订单簿，并按每个价位上展示的成交量成比例分配订单数量。
某个价位成交量越大，分配到那里的数量越多。相比 TWAP 通过跟随流动性来降低滑点。

`plan_vwap_orders` 只考虑距中间价 `price_spread` 以内的档位，并在每档取
`volume_perc` 比例的挂单量。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `total_amount` | Decimal | 必填 | 要执行的总量（报价单位） |
| `price_spread` | Decimal | `0.001` | 考虑的距中间价最大距离 |
| `volume_perc` | Decimal | `0.001` | 每档吃掉的盘口量比例 |
| `order_delay_secs` | u64 | `10` | 订单之间的延迟（秒） |
| `is_buy` | bool | `true` | 买方向 / 卖方向 |

## 示例配置

```toml
[strategy]
type = "vwap_executor"
[strategy.params]
symbol = "BTC/USDT"
total_amount = "1000"
price_spread = "0.001"
volume_perc = "0.001"
order_delay_secs = 10
is_buy = true
```

## 风险

每 tick 重新规划，若价格变动快于延迟可能导致超量执行。盘口深度可能被幌单欺骗。
