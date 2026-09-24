# hedge_grid

在主场所运行网格（中间价下方挂买限价、上方挂卖限价）；每当检测到成交失衡（买卖挂单数量不匹配）时，在对冲场所下一笔反向市价对冲单，使净 delta 接近零。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 报价场所（`{ exchange_id = "mock" }`） |
| `hedge` | VenueRef | 必填 | 对冲场所（`{ exchange_id = "mock" }`） |
| `symbol` | String | 必填 | 交易标的（两场所相同） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `lower_price` | Decimal | 必填 | 网格下界 |
| `upper_price` | Decimal | 必填 | 网格上界 |
| `num_levels` | u32 | 必填 | 网格步数 |
| `qty_per_level` | Decimal | 必填 | 每层数量（亦为对冲数量） |

## 示例配置

```toml
[strategy]
type = "hedge_grid"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "mock" }
hedge = { exchange_id = "mock" }
lower_price = "90000"
upper_price = "110000"
num_levels = 10
qty_per_level = "0.001"
```

## 依赖能力

- `TradingGateway`（`fetch_open_orders`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 对冲为市价单 → 滑点。
- 对冲量恒为 `qty_per_level`，与实际成交大小无关，净 delta 为近似值。
- 成交检测基于买卖挂单数量失衡（非真实成交量）。
- 需要两个实时场所（mock 仅提供一个）。
