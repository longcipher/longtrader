# bb_keltner_squeeze

轮询 K 线，同时构造布林带（SMA ± 2σ）与肯特纳通道（EMA ± 1.5 ATR），并检测"挤压"——
即布林带收缩进入肯特纳通道内部。挤压预示波动率即将扩张。

挤压释放时，方向由价格突破位置决定：突破布林上轨 → 做多，跌破布林下轨 → 做空。
这是由 John Carter 的 TTM Squeeze 推广开来的波动率突破策略。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `poll_secs` | u64 | `60` | 轮询间隔（秒） |
| `window` | usize | `20` | 两条通道的回看周期 |
| `bb_mult` | Decimal | `"2.0"` | 布林带标准差倍数 |
| `kc_mult` | Decimal | `"1.5"` | 肯特纳 ATR 倍数 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "bb_keltner_squeeze"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
window = 20
bb_mult = "2.0"
kc_mult = "1.5"
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 挤压可能长期不释放，资金空置。
- 震荡市中假释放很常见——建议叠加趋势过滤。
