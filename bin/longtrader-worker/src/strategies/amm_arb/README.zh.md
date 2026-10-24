# AMM 套利

利用中心化交易所（CEX）与去中心化 AMM 场所之间的价差。当价差超过 gas 成本加利润阈值时，
策略在较便宜的场所买入、在较贵的场所卖出。

## 工作原理

1. 分别取 CEX 与 AMM 的中间价。
2. 计算价差：`|cex_price − amm_price| / min(cex_price, amm_price)`。
3. 若 `spread > threshold + gas_cost`，执行套利：
   - CEX 更便宜 → 在 CEX 买入，在 AMM 卖出。
   - AMM 更便宜 → 在 AMM 买入，在 CEX 卖出。

价差按两边价格的较小值归一化，因此两边报价都必须是正数；任一价格 ≤ 0 时
`detect_arb` 返回 `None`（不开仓）。下单量另由 `arb_qty` 按价差、可用深度、
`max_qty` 与 `min_profit` 计算，保证名义利润覆盖固定成本。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、
`poll_secs`（默认 `30`）。注意 `CrossVenueParams` 不含 `timeframe`。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | CEX 场所：`primary.exchange_id` |
| `hedge` | VenueRef | 必填 | AMM 场所：`hedge.exchange_id` |
| `symbol` | String | 必填 | 交易标的 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `threshold` | Decimal | `0.005` | 触发套利的最小价差比例 |
| `gas_cost` | Decimal | `0.001` | 预估 gas 成本（占成交额比例） |
| `max_qty` | Decimal | `1` | 单次套利最大持仓量（基准单位） |
| `min_profit` | Decimal | `1` | 覆盖固定成本所需的最小利润（报价单位） |

## 示例配置

```toml
[strategy]
type = "amm_arb"
[strategy.params]
symbol = "ETH/USDT"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "uniswap" }
threshold = "0.005"
gas_cost = "0.001"
max_qty = "1"
min_profit = "1"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker` / `get_candles`）

## 风险提示

- gas 成本是静态估值；链上拥堵时实际成本可能远超 `gas_cost`。
- 双腿（CEX 与 AMM）非原子成交：一条腿成交、另一条腿失败会留下单边敞口。
- AMM 价格随池子深度滑移，价差在真实成交时会缩小。
- 需要带行情与下单能力的 AMM 后端；开源 mock 仅提供单一 CEX。

## 参考

<https://hummingbot.org/strategies/amm-arbitrage/>
