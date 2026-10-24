# Bollinger Bands v2

用布林带做的方向性均值回归策略。价格触及下轨（超卖）时做多，触及上轨（超买）时做空。
挤压过滤器避免在低波动率regime下交易。

## 工作原理

1. 在收盘价滚动窗口上计算布林带。
2. 若 `band_width / middle < squeeze_threshold`：不交易（挤压）。
3. 若 `close ≤ lower_band`：买入（向上均值回归）。
4. 若 `close ≥ upper_band`：卖出（向下均值回归）。

信号为边沿触发：`BollingerV2Signal` 每次只在条件新成立时给一次 `Buy` / `Sell`，
否则为 `NoTrade`，因此不会每轮重复下单。入场/离场均用市价单。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `window` | usize | `20` | 布林带窗口长度 |
| `mult` | Decimal | `2` | 标准差倍数 |
| `squeeze_threshold` | Decimal | `0.01` | 允许交易的最小带宽比例 |
| `qty` | Decimal | `1` | 持仓规模（基准单位） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "bollinger_v2"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
window = 20
mult = "2"
squeeze_threshold = "0.01"
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 逆势策略：强趋势中会连续"接飞刀"。
- 无止损/止盈逻辑，亏损敞口无上限。
- 挤压阈值过滤掉窄幅行情，但也可能过滤掉突破前的低波动起点。

## 参考

<https://hummingbot.org/strategies/bollinger-bands-v2/>
