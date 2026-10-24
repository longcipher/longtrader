# simplified_glft_mm

简化的 GLFT 做市商——免标定变体。

## 模型

完整 GLFT 模型的轻量变体，跳过订单到达标定。半价差直接由已实现波动率给出：

```text
half_spread = vol_to_spread · σ
```

预留价把**微观价格**（买卖量加权中点）与 Avellaneda 式库存偏移结合：

```text
mid_micro = (bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)
R         = mid_micro − q · γ · σ² · (T − t)
bid       = R − half_spread
ask       = R + half_spread
```

`micro_weight` 在简单中间价（`0`）与纯微观价格（`1`）之间插值。
免标定的 `vol_to_spread` 取代了完整的到达/衰减模型，在很宽范围的标的上都稳健；
库存项则把敞口控制在有界范围内。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `vol_to_spread` | Decimal | `0.5` | 已实现波动率上的价差倍数 |
| `gamma` | Decimal | `0.1` | 风险厌恶 γ（库存惩罚） |
| `sigma` | Decimal | `1.0` | 波动率 σ（EMA 预热前的回退值） |
| `time_horizon` | Decimal | `1.0` | 剩余 horizon（同单位） |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `vol_window` | usize | `20` | 已实现波动率估计的滚动窗口 |
| `micro_weight` | Decimal | `0.5` | 微观价格插值：0 = 中间价，1 = 微观价格 |

## 示例配置

```toml
[strategy]
type = "simplified_glft_mm"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
vol_to_spread = "0.5"
gamma = "0.1"
sigma = "1.0"
time_horizon = "1.0"
qty = "0.001"
vol_window = 20
micro_weight = "0.5"
```

## 依赖能力

- `TradingGateway`（`create_order` / 限价单）
- `MarketDataSource`（`fetch_order_book`）

## 风险提示

- 微观价格依赖盘口深度，盘口被幌单污染时预留价失真。
- 波动率 EMA 预热完成前使用回退值 `sigma`，可能明显偏离实际。
- 无仓位上限/止损，敞口靠库存项渐近收敛。

## 适用场景

绝大多数日常做市——标定快（只需调 `vol_to_spread`）、跨标的稳健、避免对到达模型过拟合。
当完整 GLFT 标定不切实际时的首选策略。
