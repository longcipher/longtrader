# darvas_box

轮询 K 线并检测"达瓦斯箱体"——一个短期盘整区：价格创出 N 日新高，随后在至少三根 K 线内
在一个窄幅矩形（箱体）中运行。向上突破箱顶触发做多，止损放在箱底。镜像条件在下方触发做空。

该形态由 Nicolas Darvas 于 1950 年代为股票提出，在趋势性的加密市场中同样适用：
新高处的休整经常预示着下一段上涨。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `period` | usize | `20` | 判定"新高"的回看窗口 |
| `box_bars` | usize | `3` | 确认箱体所需的最少 K 线数 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "darvas_box"
[strategy.params]
symbol = "ETH/USDT"
exchange_id = "mock"
period = 20
box_bars = 3
qty = "0.05"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 需要持续性趋势市才能产生 52 周 / N 日新高。震荡市中该策略可能数周空仓。
- 止损为箱底，箱体较宽时单笔风险放大。
