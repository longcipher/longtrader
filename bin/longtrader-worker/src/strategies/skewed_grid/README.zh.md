# skewed_grid

带 Avellaneda–Stoikov 库存风险偏移的网格做市商。

## 模型

每个 tick，网格重新锚定到**预留价**：

```text
R = S − q·γ·σ²·(T − t)
```

其中 `q` 是带符号库存，`γ` 是风险厌恶系数，`σ` 是已实现波动率，`(T − t)` 是剩余 horizon。
随后在 `R` 下方挂 `num_levels` 档买单、上方挂 `num_levels` 档卖单，档间距 `grid_tick`。

随着库存增长，整条阶梯平移：卖侧收紧（鼓励卖出）、买侧放宽（抑制买入）——
这正是把库存限制在有界范围内的经典 AS 控制。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `grid_tick` | Decimal | 必填 | 网格间距（价格单位，如 `1`） |
| `num_levels` | u32 | `5` | 预留价每侧的档数 |
| `qty_per_level` | Decimal | 必填 | 每档订单数量 |
| `gamma` | Decimal | `0.1` | 风险厌恶系数 γ |
| `sigma` | Decimal | `1.0` | 波动率 σ（EMA 预热前的回退值） |
| `time_horizon` | Decimal | `1.0` | 剩余 horizon（与 σ 同单位） |
| `vol_window` | usize | `20` | 由中间价收益率估计已实现波动率的滚动窗口 |

`time_horizon → 0` 时偏移衰减为零，网格退化为普通网格。

## 示例配置

```toml
[strategy]
type = "skewed_grid"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
grid_tick = "1"
num_levels = 5
qty_per_level = "0.001"
gamma = "0.1"
sigma = "1.0"
time_horizon = "1.0"
vol_window = 20
```

## 依赖能力

- `TradingGateway`（`fetch_open_orders`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 无止损/仓位上限；偏移只渐近收敛，极端单边行情仍可能大幅敞口。
- `gamma` 与 `sigma` 单位需与 `time_horizon` 匹配，否则偏移量级失真。
- 每 tick 全量重报价，撤单/挂单频率高，手续费敏感。

## 适用场景

对库存敏感、要求价差随当前持仓自适应调整的做市——现货-永续基差、衍生品对冲，
以及任何平坦网格无法控制的单向风险。
