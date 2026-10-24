# glft_mm

GLFT（Guéant–Lehalle–Fernandez–Tapia）最优做市。

## 模型

GLFT 闭式解给出最优买/卖半价差，作为风险厌恶、波动率与市价单到达强度的函数：

```text
δ_bid* = γ·σ²·(T − t) + (1/γ)·ln(1 + γ/k)
δ_ask* = γ·σ²·(T − t) − (1/γ)·ln(1 − γ/k)
```

其中 `γ` 是风险厌恶，`σ` 是波动率，`k` 是订单到达衰减，`(T − t)` 是剩余时间 horizon。
库存偏移为：

```text
skew = γ · σ² · q
```

预留价为 `R = mid − skew`，买卖报价挂在 `R ∓ δ*`。这是最大化终端财富期望效用的
经典最优做市基线。

正则条件 `γ < k` 必须成立，否则卖侧对数无定义；此时 `optimal_spreads` 返回 `None`，
策略该轮不报价。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `gamma` | Decimal | `0.1` | 风险厌恶系数 γ |
| `sigma` | Decimal | `1.0` | 波动率 σ（EMA 预热前的回退值） |
| `arrival_intensity` | Decimal | `10` | 订单到达强度 A |
| `decay` | Decimal | `1.0` | 订单到达衰减 k |
| `time_horizon` | Decimal | `1.0` | 剩余 horizon（与 σ 同单位） |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `vol_window` | usize | `20` | 已实现波动率估计的滚动窗口 |

## 示例配置

```toml
[strategy]
type = "glft_mm"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
gamma = "0.1"
sigma = "1.0"
arrival_intensity = "10"
decay = "1.0"
time_horizon = "1.0"
qty = "0.001"
vol_window = 20
```

## 依赖能力

- `TradingGateway`（`create_order` / 限价单）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 需满足 `γ < k`，否则整轮跳过（参数配置错误会被静默跳过，需自行校验）。
- 到达模型是近似；真实队列动态偏离模型时最优性不成立。
- 无止损/仓位上限，需靠外部风控（如 `sentinel`）兜底。

## 适用场景

任何做市 operation 的理论基线——当订单到达模型能合理近似真实队列动态时尤为合适，
或作为对比简单启发式的基准。
