# cross_maker

公允价值取自对冲场所的最新成交价。每个周期在主场所挂买卖报价，并以 Avellaneda 式库存倾斜对两侧分别定价，使降低库存的一侧报价更大。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 报价场所（`{ exchange_id = "binance" }`） |
| `hedge` | VenueRef | 必填 | 公允价值场所（`{ exchange_id = "mock" }`） |
| `symbol` | String | 必填 | 交易标的（两场所相同） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `spread` | Decimal | `0.002` | 相对公允价值的买卖价差（比例） |
| `inventory_cap` | Decimal | 必填 | 库存上限（基准资产单位）；倾斜在 ±cap 处饱和 |
| `qty` | Decimal | `0.001` | 基础下单数量 |

## 示例配置

```toml
[strategy]
type = "cross_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
inventory_cap = "0.5"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 倾斜依赖内存中的带符号库存估计，重启后归零 → 重启后倾斜不准确。
- 公允价值来自单一对冲场所的最新价，若对冲场所滞后则失真。
- 需要两个实时场所。
