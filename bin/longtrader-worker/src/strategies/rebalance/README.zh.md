# rebalance

读取账户快照，按实时价格计算各资产市值，并向 `targets` 权重（应累加到 1 的比例）再平衡。交易以报价价值规划，仅当某资产权重漂移超过 `band_pct`（默认 5%）时才执行。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `targets` | BTreeMap<String, Decimal> | 必填 | 各资产目标权重，如 `{ "BTC": "0.5", "ETH": "0.5" }` |
| `band_pct` | Decimal | `0.05` | 仅当 `|实际 − 目标|` 权重超过该值时再平衡 |
| `quote_asset` | String | `"USDT"` | 用于估值的记账单位 |
| `include_quote_asset` | bool | `true` | 将报价资产纳入权重预算 |
| `rebalance_interval_secs` | u64 | `30` | 轮询间隔（秒） |
| `refetch_prices` | bool | `true` | 每周期刷新价格（vs 缓存） |
| `use_limit` | bool | `false` | 以限价代替市价 |
| `limit_offset` | Decimal | `0.001` | 限价相对价格的偏移（比例） |

## 示例配置

```toml
[strategy]
type = "rebalance"
[strategy.params]
targets = { "BTC": "0.5", "ETH": "0.5" }
band_pct = "0.05"
quote_asset = "USDT"
rebalance_interval_secs = 30
use_limit = false
```

## 依赖能力

- `TradingGateway`（`get_account`、`create_order`）
- `MarketDataSource`（`fetch_ticker` / `get_prices`）

## 风险提示

- 再平衡每次都会**实现应税事件**。
- 使用实时价格；轮询间隔内报价可能过时。
- `band_pct` 控制交易频率，过小会频繁再平衡（更多手续费/税）。
- `include_quote_asset = true` 时现金也参与预算，即现金也会被再平衡。
