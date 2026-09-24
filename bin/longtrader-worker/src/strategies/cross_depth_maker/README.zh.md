# cross_depth_maker

公允价值取自对冲场所的一档订单簿中间价 `(best_bid + best_ask) / 2`。每个周期在主场所以该中间价 `± spread` 挂买卖报价对。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 报价场所（`{ exchange_id = "binance" }`） |
| `hedge` | VenueRef | 必填 | 公允价值场所（`{ exchange_id = "mock" }`） |
| `symbol` | String | 必填 | 交易标的 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `spread` | Decimal | `0.002` | 相对簿中间价的买卖价差（比例） |
| `qty` | Decimal | `0.001` | 每侧数量 |

## 示例配置

```toml
[strategy]
type = "cross_depth_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_order_book`）

## 风险提示

- 公允价值来自单一一档订单簿，盘口薄时中间价失真。
- 无库存倾斜（等量大小时）→ 库存累积。
- 需要两个带订单簿行情的实时场所（mock 仅提供 ticker）。
