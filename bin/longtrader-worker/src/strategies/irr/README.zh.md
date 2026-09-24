# irr

演示/分析策略。每 `compute_interval_secs` 以 `get_account` + `get_positions` 作为现金流代理，打印类 IRR 报告。不开仓。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的（作用域） |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `compute_interval_secs` | u64 | `60` | 报告间隔（秒） |
| `baseline` | Decimal | `0` | 起始 NAV 基线 |

## 示例配置

```toml
[strategy]
type = "irr"
[strategy.params]
symbol = "BTC/USDT"
compute_interval_secs = 60
```

## 依赖能力

- `TradingGateway`（`get_account`、`get_positions`）

## 风险提示

- **仅分析/演示——不开仓。** "IRR" 以账户余额作为 NAV、单一 `baseline`，并非基于带日期现金流的真实 XIRR。
- 基线在内存中，重启重置序列。
- 真实绩效核算请用 `nav_recorder` + 你自己的带日期账本。
