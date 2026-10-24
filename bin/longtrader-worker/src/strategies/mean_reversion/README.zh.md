# mean_reversion

轮询 K 线，计算回看窗口内收益率的滚动 Z-score，并交易极端偏离：

- Z-score > +threshold → 价格过度延伸 → 做空。
- Z-score < −threshold → 价格超卖 → 做多。

当 Z-score 回归到 `exit_band` 以内时平仓；该值应小于入场阈值，
以免策略在入场后立刻反手。

这是一个经典的统计套利原语，在市场震荡、或围绕明确公允价值摆动时最有效。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `period` | usize | `20` | 滚动窗口 |
| `entry_threshold` | Decimal | `"2.0"` | 入场 Z-score 水平 |
| `exit_band` | Decimal | `"0.5"` | 离场 Z-score 水平 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "mean_reversion"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 20
entry_threshold = "2.0"
exit_band = "0.5"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 这是"接飞刀"策略。强趋势中会反复进入极端 Z-score 区域。建议叠加趋势过滤（如 ADX）
  以抑制逆宏观趋势的入场。
- 无止损，理论上亏损无上限。
