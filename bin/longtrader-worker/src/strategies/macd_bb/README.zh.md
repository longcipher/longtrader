# MACD + Bollinger Bands

动量与均值回归的组合策略。用 MACD 判断动量方向，用布林带把握入场时机。
买入信号要求 MACD 看多（线在信号线之上）**且**价格接近布林下轨；卖出信号相反。

## 工作原理

1. 计算 MACD 线（快 EMA − 慢 EMA）与信号线（MACD 的 EMA）。
2. 计算布林带。
3. 若 `macd > signal` 且 `close ≤ lower_band`：买入。
4. 若 `macd < signal` 且 `close ≥ upper_band`：卖出。

两个条件必须同时成立，因此信号频率低于单独使用任一指标。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `macd_fast` | usize | `12` | MACD 快 EMA 窗口 |
| `macd_slow` | usize | `26` | MACD 慢 EMA 窗口 |
| `macd_signal` | usize | `9` | MACD 信号 EMA 窗口 |
| `boll_window` | usize | `20` | 布林带窗口长度 |
| `boll_mult` | Decimal | `2` | 布林带标准差倍数 |
| `qty` | Decimal | `1` | 持仓规模（基准单位） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "macd_bb"
[strategy.params]
symbol = "BTC/USDT"
macd_fast = 12
macd_slow = 26
macd_signal = 9
boll_window = 20
boll_mult = "2"
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 双重条件导致信号稀疏，可能长期空仓。
- 无离场/止损逻辑，需外部风控配合。
- 指标组合在趋势行情中会持续给出逆势入场。

## 参考

<https://hummingbot.org/strategies/macd-bollinger/>
