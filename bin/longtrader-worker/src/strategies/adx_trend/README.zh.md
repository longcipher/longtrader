# adx_trend

轮询 K 线，计算平均趋向指数（ADX）与方向指标 +DI / -DI。当 ADX > 阈值（强趋势）且
+DI 上穿 -DI 时做多；当 ADX > 阈值且 -DI 上穿 +DI 时做空。ADX 跌破阈值（趋势转弱）时离场。

这是经典的 Wilder 趋势强度过滤器：市场无方向时空仓，只在 ADX 确认强趋势时才出手。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `poll_secs` | u64 | `60` | 轮询间隔（秒） |
| `period` | usize | `14` | ADX 平滑周期 |
| `threshold` | Decimal | `"25"` | 判定趋势市的 ADX 阈值 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "adx_trend"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 14
threshold = "25"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- ADX 是滞后指标，等它上穿阈值时趋势往往已经走了一段。
- ADX 高只说明趋势强，不含方向信息——必须配合 DI 交叉使用。
