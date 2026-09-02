> [English](README.md) | **中文**

# fixed_maker — 固定价差双边做市

LongTrader 原生策略，基于固定价差的双边做市实现。

## 逻辑

- 每 `poll_secs` 秒拉取 ticker 取最新价。
- 以最新价为中枢：bid = mid × (1 − bid_spread)，ask = mid × (1 + ask_spread)。
- 每轮先 `cancel_all_orders` 清旧报价，再挂新双边限价单。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `symbol` | 必填 | 交易对 |
| `timeframe` | `"5m"` | （保留公共字段，未使用） |
| `poll_secs` | `30` | 重新报价间隔 |
| `bid_spread` | `0.001` | 买单价差比例 |
| `ask_spread` | `0.001` | 卖单价差比例 |
| `qty` | `0.001` | 单边数量 |

## 配置示例

```toml
[strategy]
type = "fixed_maker"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
poll_secs = 10
bid_spread = "0.0005"
ask_spread = "0.0008"
qty = "0.01"
```

## 风险提示

无条件双边报价没有库存保护，实盘请配合终端侧 kill-switch 与仓位上限
使用；如需库存保护请结合风控策略或 `rebalance` 等仓位管理策略组合使用。
