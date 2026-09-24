# xfunding_lite

资金费率捕获。轮询资金费率；当 `|资金费率|` 超过 `threshold` 时按方向开仓（资金费率为正做空、为负做多），以 `qty` 定量，每次成交后冷却 `backoff_secs` 避免过度交易。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `funding_interval_secs` | u64 | `3600` | 资金费率轮询间隔（秒） |
| `threshold` | Decimal | `0.0005` | `|资金费率|` 超过该值时行动 |
| `qty` | Decimal | `0.001` | 每次开仓数量 |
| `backoff_secs` | u64 | `60` | 成交后冷却时间 |

## 示例配置

```toml
[strategy]
type = "xfunding_lite"
[strategy.params]
symbol = "BTC/USDT"
funding_interval_secs = 3600
threshold = "0.0005"
qty = "0.001"
backoff_secs = 60
```

## 依赖能力

- `MarketDataSource`（`fetch_funding_rate`）
- `TradingGateway`（`create_order`、`get_positions`）

## 风险提示

- 资金费率随场所/标的而异；阈值为绝对费率。
- 资金费率可能长期单边 → 方向性敞口承担价格风险，不只是费率捕获。
- 可能开多个小仓；`qty` 需保守设置。
