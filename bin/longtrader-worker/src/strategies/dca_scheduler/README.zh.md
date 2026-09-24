# dca_scheduler

按固定 cron 计划（默认每日 UTC 零点）以市价（或限价）单为每个 `symbols` 中的标的买入 `buy_amount`。节奏由时间决定，与价格无关。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | DCA 买入的报价标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `schedule_cron` | String | `"0 0 * * *"` | cron 表达式（分 时 日 月 周），UTC |
| `buy_amount` | Decimal | `100` | 每次每标的花费的报价金额 |
| `symbols` | Vec<String> | 必填 | 定投的基准资产 |
| `use_limit` | bool | `false` | 以限价代替市价 |
| `limit_offset` | Decimal | `0.001` | 限价相对价格的偏移（比例） |

## 示例配置

```toml
[strategy]
type = "dca_scheduler"
[strategy.params]
symbol = "USDT"
schedule_cron = "0 0 * * *"
buy_amount = "100"
symbols = ["BTC", "ETH"]
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 固定节奏忽略价格水平 → 可能在局部高点买入。
- 市价单滑点；限价模式可能漏单并跳过该次执行。
- cron 以 UTC 计算。
