# Grid Strike

带执行价的网格方向性策略。在上下价格带之间挂一整套网格单。每张成交单按 `profit_spread`
反向平掉。目标是在下跌中累积基准资产、在上涨中派发。

## 工作原理

1. 在 `lower_price` 与 `upper_price` 之间均匀划分网格档位。
2. 中间价下方挂买单，上方挂卖单。
3. 某张单成交后，按 `fill_price · (1 ± profit_spread)` 反向挂单。

档位为 `levels + 1` 等分：`step = (upper − lower) / (levels + 1)`，价格取
`lower + step · i`（`i = 1..=levels`），因此不会正好落在边界上。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `lower_price` | Decimal | 必填 | 网格下界 |
| `upper_price` | Decimal | 必填 | 网格上界 |
| `levels` | u32 | `5` | 网格档数 |
| `qty` | Decimal | `1` | 每张网格单数量 |
| `profit_spread` | Decimal | `0.01` | 反向价差（比例） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "grid_strike"
[strategy.params]
symbol = "BTC/USDT"
lower_price = "90000"
upper_price = "110000"
levels = 5
qty = "1"
profit_spread = "0.01"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 方向性持仓：价格单边突破网格边界后敞口无上限。
- 无止损；跌破下界时持续接刀。
- 每档成交都要付一次手续费，`profit_spread` 必须覆盖双边费用。

## 参考

<https://hummingbot.org/strategies/grid-strike/>
