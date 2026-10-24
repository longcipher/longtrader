# Spot-Perpetual Arbitrage

利用现货市场与对应永续合约之间的价差（基差）。基差为正（永续溢价）时做空永续、做多现货；
为负时反向操作。

## 工作原理

1. 计算基差：`(perp_price − spot_price) / spot_price`。
2. 若 `basis > entry_threshold`：做空永续、做多现货。
3. 若 `basis < −entry_threshold`：做多永续、做空现货。
4. 若 `|basis| < exit_threshold` 且已有持仓：平仓。

`compute_basis` 在现货价或永续价 ≤ 0 时返回 `None`。
`BasisArbSignal` 四态：`ShortPerpLongSpot` / `LongPerpShortSpot` / `Close` / `NoTrade`。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、
`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 现货场所 |
| `hedge` | VenueRef | 必填 | 永续场所 |
| `spot_symbol` | String | 必填 | 现货场所上的现货标的 |
| `perp_symbol` | String | 必填 | 永续场所上的永续标的 |
| `entry_threshold` | Decimal | `0.01` | 开仓的基差阈值 |
| `exit_threshold` | Decimal | `0.002` | 平仓的基差阈值 |
| `qty` | Decimal | `1` | 持仓规模（基准单位） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "spot_perp_arb"
[strategy.params]
symbol = "BTC"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "binance" }
spot_symbol = "BTC/USDT"
perp_symbol = "BTC/USDT:USDT"
entry_threshold = "0.01"
exit_threshold = "0.002"
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker`，现货与永续两侧）

## 风险提示

- 两腿分开成交：基差可能在成交窗口内收敛，利润被侵蚀。
- 现货腿在现货场所、永续腿在永续场所，资金与保证金相互独立，存在一边被强平的风险。
- 需要两个支持相应产品的实时场所。

## 参考

<https://hummingbot.org/strategies/spot-perpetual-arbitrage/>
