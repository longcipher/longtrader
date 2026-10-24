# DCA Executor

定投式建仓执行器。移植自 Hummingbot 的
[DCA 策略](https://hummingbot.org/strategies/dca/)。

## 摘要

在逐步更优的价格上分批挂限价单。每一档都有自己的数量与目标价。当市场到达某一档时该档触发。
在均价上支持止损、止盈与移动止损屏障。

内部状态机 `DcaState` 取 `Active` / `Complete` / `StopLoss` / `TakeProfit` /
`TrailingStop`：任一站外价位被触及即平掉整个定投组合，而不是只撤一档。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `prices` | Vec<Decimal> | 必填 | 每档目标价，如 `["99", "98", "97"]` |
| `amounts_quote` | Vec<Decimal> | 必填 | 每档报价金额 |
| `is_buy` | bool | `true` | 买方向 / 卖方向 |
| `is_maker` | bool | `true` | 限价单（maker） |
| `stop_loss` | Decimal | `0.05` | 触发平仓的最大亏损比例 |
| `take_profit` | Decimal | `0` | 触发平仓的利润比例 |
| `trailing_activation` | Decimal | `0` | 激活移动止损的峰值盈利 |
| `trailing_delta` | Decimal | `0` | 移动止损距离 |

`prices` 与 `amounts_quote` 必须等长。

## 示例配置

```toml
[strategy]
type = "dca_executor"
[strategy.params]
symbol = "BTC/USDT"
prices = ["99", "98", "97"]
amounts_quote = ["100", "100", "100"]
is_buy = true
is_maker = true
stop_loss = "0.05"
take_profit = "0"
trailing_activation = "0"
trailing_delta = "0"
```

## 风险

止损在不利波动时触发。移动止损能锁定盈利，但在趋势中可能过早离场。
