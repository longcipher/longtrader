> [English](README.md) | **中文**

# supertrend — Supertrend + DEMA 趋势跟踪

LongTrader 原生策略，基于 Supertrend + DEMA 双重确认的趋势跟踪实现。

## 逻辑

- 每 `poll_secs` 秒拉取 K 线窗口，重建 Supertrend 与快/慢 DEMA 序列。
- Supertrend 方向与 DEMA 相对位置**同时一致**才开仓：向上 → 市价买入，
  向下 → 市价卖出。
- 信号反转时先平掉现有方向仓位（内存相位机：Flat / Long / Short）。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `symbol` | 必填 | 交易对 |
| `timeframe` | `"5m"` | K 线周期 |
| `poll_secs` | `30` | 轮询间隔 |
| `atr_window` | `14` | ATR / Supertrend 周期 |
| `atr_multiplier` | `3` | Supertrend 带宽乘数 |
| `fast_dema_window` | `10` | 快 DEMA 窗口 |
| `slow_dema_window` | `21` | 慢 DEMA 窗口 |
| `qty` | `0.001` | 下单数量 |

## 配置示例

```toml
[strategy]
type = "supertrend"

[strategy.params]
exchange_id = "binance"
symbol = "ETHUSDT"
timeframe = "15m"
atr_window = 21
atr_multiplier = "2.5"
qty = "0.5"
```

## 说明

当前版本聚焦 Supertrend + DEMA 核心信号，线性回归确认、ATR 止盈等
扩展能力可通过风控层与策略组合实现；仓位规模基于固定 `qty`，
账户级杠杆 sizing 由风控层负责。
