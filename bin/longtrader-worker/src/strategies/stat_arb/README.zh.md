# Statistical Arbitrage（Pairs Trading）

找出一对协整资产并交易它们之间的价差。价差被标准化为 z-score。
当 z-score 超过阈值时建仓（做多表现差的一方、做空表现强的一方）；
价差回归时平仓获利。

## 工作原理

1. 计算价差：`price_A − hedge_ratio · price_B`。
2. 跟踪价差的滚动均值与标准差。
3. 计算 z-score：`(spread − mean) / stdev`。
4. 若 `z > entry_threshold`：做空价差（卖 A、买 B）。
5. 若 `z < −entry_threshold`：做多价差（买 A、卖 B）。
6. 若 `|z| < exit_threshold` 且已有持仓：平仓。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol_a` | String | 必填 | 领先资产 A 的标的 |
| `symbol_b` | String | 必填 | 滞后资产 B 的标的 |
| `hedge_ratio` | Decimal | `1` | 每单位 A 对应的 B 数量 |
| `window` | usize | `50` | 价差统计的滚动窗口 |
| `entry_threshold` | Decimal | `2` | z-score 入场阈值 |
| `exit_threshold` | Decimal | `0.5` | z-score 离场阈值 |
| `qty` | Decimal | `1` | 持仓规模（以 A 为单位） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "stat_arb"
[strategy.params]
symbol = "BTC/USDT"
symbol_a = "BTC/USDT"
symbol_b = "ETH/USDT"
hedge_ratio = "1"
window = 50
entry_threshold = "2"
exit_threshold = "0.5"
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`fetch_ticker`，需覆盖 A 与 B）

## 风险提示

- 协整关系会破裂；`hedge_ratio` 为静态值，未做滚动估计。
- 两腿分开成交，存在单边敞口窗口。
- 无止损，价差可持续偏离。

## 参考

<https://hummingbot.org/strategies/statistical-arbitrage/>
