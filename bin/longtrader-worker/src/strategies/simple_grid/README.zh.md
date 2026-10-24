# simple_grid

价格寻址的网格报价策略：成交翻转再平衡 + 错误熔断。

策略在 `lower_price` 与 `upper_price` 之间维护一挂静态限价单阶梯。当场所
报告某一档已消失（成交或撤销）时，会在高/低一个网格步长的位置挂出反向腿——
成交的买单会上移一步重新挂为卖单，即经典的网格库存循环。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `lower_price` | Decimal | 必填 | 网格下界 |
| `upper_price` | Decimal | 必填 | 网格上界 |
| `num_levels` | u32 | 必填 | 网格步长分母（步长 = 区间 / num_levels） |
| `qty_per_level` | Decimal | 必填 | 每档下单数量 |
| `profit_spread_pct` | Decimal | 不设 | 反向腿价差占成交价的比例；不设则使用一个网格步长 |
| `state_file` | String | 不设 | 可选的重启恢复状态文件路径 |

## 重启恢复

设置 `state_file` 后，阶梯会在每次播种和熔断清仓后落盘。重启后的第一次
`rebalance` 会恢复已持久化的阶梯，而不是在上一进程遗留挂单之上再播种一套。
文件不存在属于正常首次启动；**文件损坏或不可读会停止网格**，而不是静默地
双重报价。

## 示例配置

```toml
[strategy]
type = "simple_grid"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
lower_price = "90000"
upper_price = "110000"
num_levels = 20
qty_per_level = "0.001"
state_file = "/var/lib/longtrader/simple-grid-state.json"
```

## 依赖能力

- `TradingGateway`（`create_order` / `cancel_order` / `fetch_open_orders`）
- `MarketDataSource`（`fetch_ticker`）

## 价格解析

`current_price` 依次读取 ticker 的 `last`、`close`，最后回退到买卖中间价。
某一档缺失、无法解码或非正数时会被跳过；只有全部不可用时阶梯才报错。

## 风险提示

- 网格没有止损：价格单向穿过整个区间会累积单边库存，请按此结果设定
  `qty_per_level`。
- 连续五次下单失败会触发熔断：撤销全部已追踪订单并清空阶梯。
- 恢复出的、落在当前区间之外的档位会被丢弃并记警告——它们既无法重新报价
  也无法翻转。
