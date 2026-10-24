# Perpetual Market Making

带资金费率感知的永续做市。移植自 Hummingbot 的
[Perpetual MM](https://hummingbot.org/strategies/perpetual-market-making/)。

## 摘要

在永续合约上双边报价，随资金费累计把库存偏向平仓。资金费为正时策略偏空以收取溢价；
为负时偏多。同时监控仓位上限与强平价。

`plan_perp_quotes` 中库存偏移按 `position_limit` 归一化并 clamp 到 `[−1, 1]`；
当资金费率在 `funding_threshold` 以内时 `skip_signal = true`，即不因资金费产生方向偏移。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `perp_symbol` | String | 必填 | 永续合约标的 |
| `half_spread` | Decimal | `0.0005` | 基础半价差 |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `position_limit` | Decimal | `1` | 达到最大偏移前的最大库存 |
| `funding_threshold` | Decimal | `0.00001` | 触发偏移的最小资金费率 |
| `liq_buffer` | Decimal | `0.1` | 距强平价保留的保证金比例 |
| `vol_window` | usize | `20` | 波动率估计的滚动窗口 |

## 示例配置

```toml
[strategy]
type = "perpetual_mm"
[strategy.params]
symbol = "BTC/USDT:USDT"
perp_symbol = "BTC/USDT:USDT"
exchange_id = "binance"
half_spread = "0.0005"
qty = "0.001"
position_limit = "1"
funding_threshold = "0.00001"
liq_buffer = "0.1"
vol_window = 20
```

## 风险

资金费收益不保证——费率可能反向。若保证金相对仓位过薄则存在强平风险。
