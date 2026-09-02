> [English](README.md) | **中文**

# dca_scheduler — 定投 / 定期补货

LongTrader 原生策略，提供定投与定期分批执行的调度能力。

## 逻辑

- 每 `interval_secs` 提交一笔市价单；启动时立即执行第一笔。
- `is_buy = true` 为定投买入；`false` 为定期卖出（手续费资产补货 /
  分批派发场景）。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `symbol` | 必填 | 交易对 |
| `poll_secs` | `30` | （未使用，保留公共字段兼容） |
| `interval_secs` | `86400` | 下单间隔秒数 |
| `is_buy` | `true` | 买/卖方向 |
| `qty` | `0.001` | 每笔数量 |

## 配置示例

```toml
[strategy]
type = "dca_scheduler"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
interval_secs = 3600
is_buy = true
qty = "0.002"
```

## 说明

当前版本聚焦核心定投调度能力，按金额（amount）换算数量等扩展能力
可通过上层配置与风控层组合实现。
