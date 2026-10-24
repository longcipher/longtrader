# TWAP Executor

时间加权平均价订单执行。移植自 Hummingbot 的
[TWAP 策略](https://hummingbot.org/strategies/twap/)。

## 摘要

把一笔大单拆成 `num_orders` 笔等量子单，每 `duration_secs / num_orders` 秒下一笔。
相比单笔大市价单可最小化市场冲击。支持 maker 模式（带缓冲的限价单）与
taker 模式（激进限价单）。

每笔子单数量恒为 `total_amount / N`；`TwapStep` 顺序推进，因此 TWAP 完成前
策略不会重复下同一笔。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `total_amount` | Decimal | 必填 | 要执行的总量（基准单位） |
| `duration_secs` | u64 | `600` | 总执行时长（秒） |
| `num_orders` | u32 | `10` | 子单笔数 |
| `is_buy` | bool | `true` | 买方向 / 卖方向 |
| `is_maker` | bool | `true` | 限价单（maker）或市价单（taker） |
| `limit_buffer` | Decimal | `0.0005` | maker 限价单相对中间价的缓冲 |

## 示例配置

```toml
[strategy]
type = "twap_executor"
[strategy.params]
symbol = "BTC/USDT"
total_amount = "1"
duration_secs = 600
num_orders = 10
is_buy = true
is_maker = true
limit_buffer = "0.0005"
```

## 风险

若所选间隔上流动性稀薄，TWAP 不保证成交。快市中 maker 模式可能留下未成交单。
