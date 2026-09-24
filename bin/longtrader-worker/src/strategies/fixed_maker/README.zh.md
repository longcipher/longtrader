# fixed_maker

轮询行情，以中间价为中心，每个周期撤销全部订单，并重新挂买价 `mid * (1 - bid_spread)`、卖价 `mid * (1 + ask_spread)` 的限价单。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `bid_spread` | Decimal | `0.001` | 买价相对中间价偏移（比例） |
| `ask_spread` | Decimal | `0.001` | 卖价相对中间价偏移（比例） |
| `qty` | Decimal | `0.001` | 每侧数量 |

## 示例配置

```toml
[strategy]
type = "fixed_maker"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
bid_spread = "0.001"
ask_spread = "0.001"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`cancel_all_orders`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 每个周期撤销全部订单，会清掉该标的上无关的挂单。
- 无库存倾斜 → 趋势中逆向选择（挂单在亏损侧被吃）。
- 市价/限价成交存在滑点。
