# ichimoku

轮询 K 线并计算一目均衡表（转换线、基准线、先行带 A、先行带 B、迟行线）。
当转换线上穿基准线、价格位于云之上且云看涨（先行带 A > B）时做多。镜像条件做空。
转换线再度下穿基准线、或价格重新进入云中时离场。

这是一套来自日本的完整趋势跟踪体系，同时给出支撑/阻力、趋势方向、动量与进出场时机。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"4h"` | K 线周期（一目为日线/4h 设计） |
| `poll_secs` | u64 | `60` | 轮询间隔（秒） |
| `tenkan_period` | usize | `9` | 转换线回看周期 |
| `kijun_period` | usize | `26` | 基准线 / 先行带 A 基准 |
| `senkou_b_period` | usize | `52` | 先行带 B 回看周期 |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "ichimoku"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
tenkan_period = 9
kijun_period = 26
senkou_b_period = 52
qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 一目本身滞后——先行带 A/B 前移 26 根，在周线级别上云反映的是一个月前的状态。
- 云边缘反复打脸很常见；转换线/基准线交叉用于确认动量。
- `senkou_b_period = 52` 需要 52 + 26 根 K 线预热，冷启动阶段无信号。
