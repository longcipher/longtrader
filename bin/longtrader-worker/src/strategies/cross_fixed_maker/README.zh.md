# cross_fixed_maker

以对冲场所中间价为中心在主场所挂买卖报价对。当主场所某条腿（已成交）从挂单中消失时，在对冲场所下一笔市价对冲单平掉该成交，使库存保持中性。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 报价场所（`{ exchange_id = "binance" }`） |
| `hedge` | VenueRef | 必填 | 对冲场所（`{ exchange_id = "mock" }`） |
| `symbol` | String | 必填 | 交易标的 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `spread` | Decimal | `0.002` | 相对公允价值的买卖价差 |
| `qty` | Decimal | `0.001` | 下单数量（亦为对冲数量） |

## 示例配置

```toml
[strategy]
type = "cross_fixed_maker"
[strategy.params]
symbol = "BTC/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "mock" }
spread = "0.002"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`fetch_open_orders`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 对冲为市价单 → 滑点；且对冲量恒为 `qty`，与实际成交大小无关（可能过度/不足对冲）。
- 成交检测依赖"挂单缺失"（买卖数量失衡）→ 若场所丢单则判断脆弱。
- 需要两个实时场所。
