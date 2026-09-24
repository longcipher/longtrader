# market_cap

按市值对资产排序，持有前 `top_n` 名（当 `include_stable = false` 时排除非稳定币），在漂移超过 `band_pct` 时向等权再平衡。市值取即时快照。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 报价标的，如 `USDT` |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `top_n` | usize | `10` | 持有资产数量 |
| `rebalance_interval_secs` | u64 | `3600` | 再平衡间隔（秒） |
| `band_pct` | Decimal | `0.05` | 漂移超过该值时再平衡 |
| `include_stable` | bool | `true` | 将稳定币纳入选股池 |
| `stablecoins` | Vec<String> | `["USDT", "USDC"]` | `include_stable = false` 时排除的稳定币 id |
| `quote_asset` | String | `"USDT"` | 记账单位 |
| `use_limit` | bool | `false` | 以限价代替市价 |
| `limit_offset` | Decimal | `0.001` | 限价相对价格的偏移（比例） |

## 示例配置

```toml
[strategy]
type = "market_cap"
[strategy.params]
symbol = "USDT"
top_n = 10
rebalance_interval_secs = 3600
band_pct = "0.05"
include_stable = true
```

## 依赖能力

- `TradingGateway`（`get_account`、`create_order`）
- `MarketDataSource`（`get_market_cap`、`fetch_ticker`）

## 风险提示

- 市值为即时快照 → 策略追逐动量，可能买在高点。
- 再平衡实现应税事件。
- 需要后端提供 `get_market_cap`（开源 mock 不提供）。
