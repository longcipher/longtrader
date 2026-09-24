# nav_recorder

分析策略。每 `record_interval_secs` 将 NAV（账户权益 + 仓位市值）记录到本地 JSON 文件（`persist_path`）。不开仓。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的（作用域） |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `record_interval_secs` | u64 | `60` | 记录间隔（秒） |
| `persist_path` | String | `"state/nav.json"` | 保存 NAV 序列的本地 JSON 路径 |

## 示例配置

```toml
[strategy]
type = "nav_recorder"
[strategy.params]
symbol = "BTC/USDT"
record_interval_secs = 60
persist_path = "state/nav.json"
```

## 依赖能力

- `TradingGateway`（`get_account`、`get_positions`）

## 风险提示

- **仅分析——不开仓。** NAV 以账户权益 + 仓位市值作为代理（不区分已实现/未实现盈亏）。
- 写入本地文件；若需要历史，请确保 `persist_path` 位于持久存储。
- 真实 IRR 需配合你自己的带日期现金流账本。
