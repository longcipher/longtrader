# premium_monitor

仅监控策略。轮询资金费率；当资金费率绝对值超过 `threshold` 时记录/打印告警。不开仓。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `funding_interval_secs` | u64 | `3600` | 资金费率轮询间隔（秒） |
| `threshold` | Decimal | `0.001` | `|资金费率|` 超过该值时告警 |

## 示例配置

```toml
[strategy]
type = "premium_monitor"
[strategy.params]
symbol = "BTC/USDT"
funding_interval_secs = 3600
threshold = "0.001"
```

## 依赖能力

- `MarketDataSource`（`fetch_funding_rate`）
- `TradingGateway`（`get_positions`）

## 风险提示

- **仅监控——不开仓。** 资金费率随场所/标的而异；阈值为绝对费率，非年化。
- 可作为信号源；若要据此行动，请配合交易策略。
