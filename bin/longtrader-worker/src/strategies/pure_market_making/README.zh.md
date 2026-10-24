# Pure Market Making（PMM）

多层做市，带可选的库存偏移与挂单保留。移植自 Hummingbot 的
[Pure Market Making](https://hummingbot.org/strategies/pure-market-making/) 策略。

## 摘要

在中间价两侧按几何或线性递增的距离挂 `levels` 档买单与卖单。每档价差按
`order_step_size` 递增。当库存在一侧累积时，调整各档数量以鼓励再平衡。

```text
level_i 价差 = order_step_size · (i + 1) · (1 + order_size_spread)
bid_qty_i   = base_qty · (1 + inventory_skew · q)
ask_qty_i   = base_qty · (1 − inventory_skew · q)
```

其中 `q` 是带符号库存：库存为正时买单放大、卖单缩小，从而把持仓推向中性。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `levels` | u32 | `3` | 买卖各挂几档 |
| `order_step_size` | Decimal | `0.005` | 每档价差增量 |
| `order_size_spread` | Decimal | `0.001` | 深档额外放宽的价差 |
| `order_amount` | Decimal | `0.001` | 基础订单数量 |
| `inventory_skew` | Decimal | `0.5` | 库存→数量调整因子 |
| `price_band` | Decimal | `0.1` | 中间价超出该带时重新报价 |
| `hanging_orders` | bool | `true` | 对手方成交后是否保留挂单 |
| `hanging_cancel_ticks` | u32 | `5` | 挂单保留多少 tick 后撤销 |

## 示例配置

```toml
[strategy]
type = "pure_market_making"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
levels = 3
order_step_size = "0.005"
order_size_spread = "0.001"
order_amount = "0.001"
inventory_skew = "0.5"
price_band = "0.1"
hanging_orders = true
hanging_cancel_ticks = 5
```

## 风险

常规做市面临逆向选择。库存偏移能减轻方向性漂移，但不能消除。
价格带能抑制快市中过度重报价，但在跳空风险下可能留下过期报价。
