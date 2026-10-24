# queue_mm

面向大跳价资产的基于队列的做市商。

## 模型

当跳价（tick）相对于价差很宽时（大跳价regime），队列首位主导价格形成。策略跟踪两个信号：

```text
pressure = log(Σ bid_qty / Σ ask_qty)
impulse  = Σ(buy_volume) − Σ(sell_volume)     （带符号，滚动窗口）
signal   = w₁ · pressure + w₂ · normalise(impulse)
fair    += α · signal · mid
```

盘口偏宽（买盘深度更大）或买向冲击强时公允价值上移；卖价收紧以抢在预期变动前成交，
买价放宽以避开逆向选择。价差随 `|signal|` 线性放宽（参数 `spread_widen`）。

策略不会把单挂到距中间价超过 `max_depth` 个 tick 的位置；激进报价只会停在盘口顶部几档。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `half_spread` | Decimal | `0.0005` | 基础半价差（比例） |
| `qty` | Decimal | `0.001` | 报价基准数量 |
| `depth` | u32 | `5` | 计算盘口压力的档数 |
| `impulse_window` | usize | `20` | 成交冲击滚动窗口（tick 数） |
| `w_pressure` | Decimal | `0.5` | 盘口压力信号权重 w₁ |
| `w_impulse` | Decimal | `0.5` | 成交冲击信号权重 w₂ |
| `alpha` | Decimal | `0.5` | 激进度因子 α |
| `spread_widen` | Decimal | `0.001` | 每单位不利信号的半价差放宽量 |

> 注：英文 `README.md` 中 `alpha` 记为 `0.001`；以 `queue_mm/mod.rs` 为准的默认值是 `0.5`。

## 示例配置

```toml
[strategy]
type = "queue_mm"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
half_spread = "0.0005"
qty = "0.001"
depth = 5
impulse_window = 20
w_pressure = "0.5"
w_impulse = "0.5"
alpha = "0.5"
spread_widen = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 限价单）
- `MarketDataSource`（`fetch_order_book`）

## 风险提示

- 依赖盘口快照与成交流，幌单与刷量会直接污染信号。
- 大跳价资产本身流动性差，`qty` 相对盘口可能过大。
- 无库存控制，需外部风控配合。

## 适用场景

大跳价资产（低价股、部分外汇对、小市值代币）——在这些市场中队列位置与成交流
比中间价本身更能驱动短期价格形成。
