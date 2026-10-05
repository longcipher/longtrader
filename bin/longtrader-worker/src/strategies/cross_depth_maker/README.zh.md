# cross_depth_maker

公允价值取自对冲场所的一档订单簿中间价，即**场所返回的首档买价与首档卖价的均值**（不做任何排序，因此对冲场所必须按最优价在前返回每一侧）。

每个周期在主场所按 `mid * (1 - spread)` 与 `mid * (1 + spread)` 挂出买卖报价对。

`spread` 是**中间价的比例，而不是价格偏移量**。举例：中间价为 `100`、`spread = "0.002"` 时，报价为 `99.8` / `100.2`——距中间价 `0.2`，而不是 `0.002`。因此价差距离随价格放大：同一配置作用于价格 `10 000` 的标的时，报价为 `9 980` / `10 020`。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 报价场所（`{ exchange_id = "binance" }`） |
| `hedge` | VenueRef | 必填 | 公允价值场所（`{ exchange_id = "mock" }`） |
| `symbol` | String | 必填 | 交易标的 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `spread` | Decimal | `0.002` | 半价差占中间价的比例（`0.002` = 0.2%） |
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
- 报价被拒会使该周期失败，因此不会只挂出单边；但一条腿出错就意味着直到下一轮之前完全不报价。
- 需要两个带订单簿行情的实时场所（mock 仅提供 ticker）。
