# ema_cross

轮询 K 线，计算快/慢 EMA，在快慢线出现新交叉时下一笔市价单。交叉状态为内存中的边沿触发；重启后等待下一个新交叉，而非重放上一次。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `fast_window` | usize | `9` | 快线 EMA 周期 |
| `slow_window` | usize | `21` | 慢线 EMA 周期 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "ema_cross"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
fast_window = 9
slow_window = 21
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 市价单相对信号价存在滑点。
- 交叉状态在内存中，重启后可能在重启后首次交叉即交易。
- 滞后指标，震荡行情中易反复打脸。
