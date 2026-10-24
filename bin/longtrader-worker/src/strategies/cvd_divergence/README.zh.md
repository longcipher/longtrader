# cvd_divergence

轮询 K 线，在滚动窗口上近似计算累计成交量差（CVD），并检测价格与成交量之间的背离：
价格创新高但 CVD 开始衰减（看跌背离），或价格创新低而 CVD 稳企（看跌转看涨背离）。

它作为一个regime感知的均值回归覆盖层，只在动量被成交量确认时反应。
典型用法是在 CVD 与价格反向的位置做反转。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"15m"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"15m"` | K 线周期 |
| `period` | usize | `20` | 滚动窗口 |
| `z_threshold` | Decimal | `"1.5"` | 标记背离的 z-score 阈值 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "cvd_divergence"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
period = 20
z_threshold = "1.5"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- CVD 只是代理指标；专用数据源的期货持仓量 / CVD 要准确得多。
- 背离不是时机——背离出现后行情可能还会走很久。
