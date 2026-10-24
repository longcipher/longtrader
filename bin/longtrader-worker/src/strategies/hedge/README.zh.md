# Perpetual Hedge

用反向永续头寸持续对冲现货持仓，维持 delta 中性。现货持仓变化时，策略调整永续对冲量，
使净 delta 保持接近零。

## 工作原理

1. 读取当前现货持仓与永续持仓。
2. 计算目标永续数量：`target = −spot_position`。
3. 若漂移 `|target − current_perp| / max(|spot|, |perp|)` 超过阈值，在永续场所成交差额。

```text
target_perp_qty = −spot_position
trade_qty       = target_perp_qty − current_perp_position
```

任一侧持仓为零时 `compute_hedge_trade` 返回 `None`（无需对冲）。单笔对冲量上限为
`max_order_size`，因此大漂移会被拆成多轮执行。

## 参数

继承自 `CrossVenueParams`：`primary`（必填）、`hedge`（必填）、`symbol`（必填）、
`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `primary` | VenueRef | 必填 | 现货场所 |
| `hedge` | VenueRef | 必填 | 永续场所 |
| `spot_symbol` | String | 必填 | 主场所上的现货标的 |
| `perp_symbol` | String | 必填 | 对冲场所上的永续标的 |
| `symbol` | String | 必填 | 交易标的 |
| `threshold` | Decimal | `0.01` | 触发再平衡的漂移阈值（比例） |
| `max_order_size` | Decimal | `1` | 单笔对冲单最大数量 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "hedge"
[strategy.params]
symbol = "BTC"
primary = { exchange_id = "binance" }
hedge = { exchange_id = "binance" }
spot_symbol = "BTC/USDT"
perp_symbol = "BTC/USDT:USDT"
threshold = "0.01"
max_order_size = "1"
```

## 依赖能力

- `TradingGateway`（`get_positions`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）

## 风险提示

- 只对冲 delta，不隔离资金费率与基差风险。
- 永续持仓状态在内存中，重启后需重新对齐。
- 阈值过小会因手续费侵蚀收益；过大则delta裸露时间变长。

## 参考

<https://hummingbot.org/strategies/perpetual-market-making/>（hedge 模式）
