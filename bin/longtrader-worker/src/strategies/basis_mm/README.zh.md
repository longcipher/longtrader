# basis_mm

基差做市——现货对永续的均值回归，叠加被动做市。

## 模型

基差 `B = mark_price − spot_price` 倾向于围绕滚动均值回归。策略交易基差的 z-score：

```text
z = (B − mean(B)) / stdev(B)

if  z >  threshold  →  卖永续 / 买现货
if  z < −threshold  →  买永续 / 卖现货
if  均值回归        →  带利润平仓
```

报价侧**额外**在基差预测得出的公允价值周围叠一层紧密的被动做市阶梯。

滚动窗口 `window` 维护 z-score 计算所需的均值与方差。库存通过对现货与永续做反向交易来对冲。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `perp_symbol` | String | 必填 | 永续腿标的（如 `BTC/USDT:USDT`） |
| `window` | usize | `50` | 基差统计的滚动窗口 |
| `threshold` | Decimal | `1.0` | z-score 入场阈值 |
| `qty` | Decimal | `0.001` | 每笔基差交易的持仓规模（基准单位） |
| `half_spread` | Decimal | `0.001` | 被动阶梯的半价差 |
| `maker_levels` | u32 | `3` | 每侧被动做市档数 |
| `maker_qty` | Decimal | `0.001` | 每档数量 |

## 示例配置

```toml
[strategy]
type = "basis_mm"
[strategy.params]
symbol = "BTC/USDT"
perp_symbol = "BTC/USDT:USDT"
window = 50
threshold = "1.0"
qty = "0.001"
half_spread = "0.001"
maker_levels = 3
maker_qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`get_candles`，需覆盖现货与永续两个标的）

## 风险提示

- 基差并非总是平稳：资金费率与宏观冲击可令基差长期偏离滚动均值。
- z-score 窗口在regime切换时引入滞后，可能过早逆势入场。
- 现货与永续双腿分开成交，非原子对冲。
- 做市阶梯占用保证金，且不自动带止损。

## 适用场景

现货与永续基差平稳的跨场所/跨产品均值回归——经典的加密基差交易。
