> [English](README.md) | **中文**

# random_entry — 随机测试单

LongTrader 原生策略，基于可复现随机数源的连通性测试策略。

## 逻辑

- 每 `interval_secs` 秒以 `entry_probability` 的概率提交一笔随机方向的
  市价单。
- 随机源为 SplitMix64，种子由 `seed` 固定 → 同配置重放序列一致，
  便于联调与回归。

用途：连通性、签名、限频、风控链路冒烟测试。**非盈利策略**。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `symbol` | 必填 | 交易对 |
| `interval_secs` | `60` | 决策间隔 |
| `entry_probability` | `0.05` | 每次触发概率 [0,1] |
| `qty` | `0.0001` | 下单数量 |
| `seed` | 黄金比例常数 | RNG 种子 |

## 配置示例

```toml
[strategy]
type = "random_entry"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
interval_secs = 30
entry_probability = "0.5"
qty = "0.001"
seed = 42
```
