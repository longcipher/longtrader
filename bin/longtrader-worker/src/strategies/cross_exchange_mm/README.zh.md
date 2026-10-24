# Cross-Exchange Market Making（XEMM）

跨场所做市：在一个场所（taker）吃流动性，在另一个场所（maker）挂单做市，
赚取两个场所之间的价差。

## 工作原理

策略读取 taker 场所的最优买/卖价，并在 maker 场所挂出更宽的半价差。由于

- **maker 买入成交** → 可在 taker 侧以更高价卖出对冲；
- **maker 卖出成交** → 可在 taker 侧以更低价买入对冲；

因此 maker 侧的每次成交都能在对冲后获利。报价公式：

```text
maker_bid = taker_bid · (1 − min_profitability)
maker_ask = taker_ask · (1 + min_profitability)
```

当 taker 场所自身价差小于 `min_profitability` 时无利可图，`plan_xemm_quotes`
返回 `skip = true`，该轮不报价。更深档位按 `step_size` 比例逐层外移。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、
`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | maker 场所：`primary.exchange_id` |
| `hedge` | VenueRef | 必填 | taker 场所：`hedge.exchange_id` |
| `symbol` | String | 必填 | 交易标的（两场所相同） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `min_profitability` | Decimal | `0.0001` | 报价所需的最小利润率 |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `levels` | u32 | `3` | 每侧 maker 档数 |
| `step_size` | Decimal | `0.0005` | 档位间的比例步长 |

## 示例配置

```toml
[strategy]
type = "cross_exchange_mm"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "okx" }
min_profitability = "0.0001"
qty = "0.001"
levels = 3
step_size = "0.0005"
```

## 依赖能力

- `TradingGateway`（`create_order`、`fetch_open_orders`）
- `MarketDataSource`（`fetch_order_book`，两侧）

## 风险提示

- 对冲腿依赖 taker 场所仍有深度，价差可能在成交瞬间消失。
- 双边均需实时订单簿；开源 mock 仅提供 ticker。
- maker 成交与 taker 对冲非原子，存在单边敞口窗口。

## 参考

<https://hummingbot.org/strategies/cross-exchange-market-making/>
