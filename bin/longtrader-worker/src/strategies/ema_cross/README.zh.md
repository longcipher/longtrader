> [English](README.md) | **中文**

# ema_cross — EMA 金叉/死叉

LongTrader 原生策略，基于快慢 EMA 金叉/死叉的趋势跟踪实现。

## 逻辑

- 每 `poll_secs` 秒通过 `MarketDataSource::get_candles` 拉取最近 K 线窗口。
- 对收盘价序列计算快/慢 EMA。
- 快线上穿慢线 → 市价买入 `qty`；下穿 → 市价卖出。
- 交叉状态为边沿触发（内存态）：重启后等待下一次新鲜交叉，不会重放旧信号。

## 参数（`[strategy.params]`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `exchange_id` | `"mock"` | 交易所标识 |
| `label` | `""` | 子账户标签 |
| `symbol` | 必填 | 交易对 |
| `timeframe` | `"5m"` | K 线周期 |
| `poll_secs` | `30` | 轮询间隔 |
| `fast_window` | `9` | 快 EMA 窗口 |
| `slow_window` | `21` | 慢 EMA 窗口 |
| `qty` | `0.001` | 下单数量 |

## 配置示例

```toml
[strategy]
type = "ema_cross"

[strategy.params]
exchange_id = "binance"
symbol = "BTCUSDT"
timeframe = "5m"
poll_secs = 30
fast_window = 9
slow_window = 21
qty = "0.001"
```
