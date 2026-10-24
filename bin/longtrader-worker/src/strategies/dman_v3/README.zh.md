# DMAN v3（Dual Moving Average Nested）

用跨周期嵌套均线做的方向性策略。在主周期上计算快/慢 EMA，并在更高周期上计算趋势过滤 EMA。
做多信号要求快 EMA 上穿慢 EMA，同时价格位于趋势过滤之上。

## 工作原理

1. 计算快 EMA、慢 EMA 与趋势过滤 EMA。
2. 若 `ema_fast > ema_slow` 且 `close > ema_trend`：买入。
3. 若 `ema_fast < ema_slow` 且 `close < ema_trend`：卖出。
4. 其他情况：不交易。

`dman_signal` 返回 `Buy` / `Sell` / `NoTrade` 三态，只在条件成立的那一根 K 线上给一次信号。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `fast_window` | usize | `9` | 快 EMA 窗口 |
| `slow_window` | usize | `21` | 慢 EMA 窗口 |
| `trend_window` | usize | `50` | 趋势过滤 EMA 窗口（更高周期） |
| `qty` | Decimal | `1` | 持仓规模（基准单位） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "dman_v3"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
fast_window = 9
slow_window = 21
trend_window = 50
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 三重均线全部滞后，趋势反转时离场偏晚。
- 趋势过滤 EMA 需足量 K 线预热，冷启动阶段信号不可靠。

## 参考

<https://hummingbot.org/strategies/directional-macd-ema/>（DMAN 系列）
