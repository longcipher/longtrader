> [English](README.md) | **中文**

# boll_grid — 布林带网格

LongTrader 原生策略，基于布林带（Bollinger Bands）通道的网格交易实现。

## 逻辑

- 每 `poll_secs` 秒拉取 `boll_window` 根 K 线，计算布林带。
- 最新收盘价在带内且带宽足够时：先 `cancel_all_orders` 清掉旧网格，再在
  收盘价上下各挂 `grid_num` 档限价单（步长 = 带宽 / (grid_num + 1)），
  越出带外的档位自动丢弃。
- 收盘价越出带外（强趋势）或带宽过窄（静默市）时不挂网格。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `symbol` | 必填 | 交易对 |
| `timeframe` | `"5m"` | K 线周期 |
| `poll_secs` | `30` | 轮询间隔 |
| `boll_window` | `21` | 布林窗口 |
| `boll_mult` | `2` | 标准差乘数 |
| `grid_num` | `3` | 单边档位数 |
| `qty` | `0.001` | 每档数量 |
| `profit_spread_pct` | `0.0005` | 反手腿价差比例（预留） |

## 配置示例

```toml
[strategy]
type = "boll_grid"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
timeframe = "5m"
boll_window = 21
grid_num = 4
qty = "0.01"
```
