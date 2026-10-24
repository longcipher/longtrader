# Avellaneda–Stoikov Market Making v2

带成交强度跟踪的 AS 最优价差。移植自 Hummingbot 的
[Avellaneda-Stoikov 策略](https://hummingbot.org/strategies/avellaneda-market-making/)。

## 摘要

用 Avellaneda–Stoikov 闭式解计算最优买/卖半价差，其中到达强度 λ 取**实测**成交到达强度，
而非标定好的衰减参数。成交越活跃 → 价差越窄，因为成交来得更快。并叠加预留价的库存偏移。

```text
δ*  = γ·σ²·(T − t) + (1/γ) · ln(1 + γ/k_eff)
R   = S − q · γ · σ² ·(T − t)
```

其中 `k_eff` 由实测市价单流（成交强度）估计出的有效衰减参数。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `gamma` | Decimal | `0.1` | 风险厌恶系数 γ |
| `sigma` | Decimal | `1.0` | 波动率 σ（EMA 预热前的回退值） |
| `time_horizon` | Decimal | `1.0` | 剩余时间 horizon（与 σ 同单位） |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `intensity_window` | usize | `20` | 成交强度估计的滚动窗口 |
| `vol_window` | usize | `20` | 已实现波动率估计的滚动窗口 |
| `min_decay` | Decimal | `0.1` | 有效衰减 `k_eff` 的下限 |

## 示例配置

```toml
[strategy]
type = "avellaneda_mm_v2"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
gamma = "0.1"
sigma = "1.0"
time_horizon = "1.0"
qty = "0.001"
intensity_window = 20
vol_window = 20
min_decay = "0.1"
```

## 风险

持续重新报价。成交强度跟踪能适应市场状态，但在波动率突然飙升时会滞后于regime切换。
