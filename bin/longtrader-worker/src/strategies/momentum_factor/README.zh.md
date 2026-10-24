# momentum_factor

轮询 K 线，在滚动窗口上构建复合动量评分：

- `period` 根 K 线的收益率（原始动量），
- 平滑 RSI（涨跌速度）。

当两个分量同时高于各自阈值时做多，同时低于时做空。动量衰减进入中性区时离场。

该策略把趋势延续动量与速度融合，通常会晚于行情起点入场，但能留在中段。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `period` | usize | `14` | 回看窗口 |
| `return_threshold` | Decimal | `"0.02"` | 触发动性的最小收益率 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "momentum_factor"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 14
return_threshold = "0.02"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 动量天然滞后：入场晚、离场很晚。盘整市中可能连续多笔小亏。
- 降低阈值会提高敏感度，同时提高假突破率。
