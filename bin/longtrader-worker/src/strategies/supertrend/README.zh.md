# supertrend

轮询 K 线，计算 Supertrend（ATR + 倍数）；当方向出现新翻转（下→上或上→下）时下一笔市价单将敞口翻为多/空。方向为内存中的边沿触发；重启后等待下一个新翻转而非重放。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `atr_period` | usize | `10` | ATR 回看周期 |
| `multiplier` | Decimal | `3.0` | Supertrend ATR 倍数 |
| `qty` | Decimal | `0.001` | 下单数量 |
| `use_limit` | bool | `false` | 以限价代替市价（偏移 = `limit_offset`） |
| `limit_offset` | Decimal | `0.001` | 限价相对价格的偏移（比例） |

## 示例配置

```toml
[strategy]
type = "supertrend"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
atr_period = 10
multiplier = "3.0"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 市价单相对信号价存在滑点；限价模式可能漏单。
- 方向在内存中，重启后可能在重启后首次信号即翻转。
- 滞后趋势过滤器，震荡行情中易反复打脸。
