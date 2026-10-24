# dual_thrust

轮询 K 线，用经典的比例参数 K1 / K2 把 N 日区间划分为上/下两条"推力"通道。
突破上轨开多，突破下轨开空。离场以对侧轨道为止损，因此它是一个内置非对称通道的
短期开盘区间突破系统。

该系统最初作为期货日内交易体系流行，在永续加密市场中同样适用：
早盘区间往往标定了有效的支撑与阻力。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"1h"`）、`poll_secs`（默认 `60`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"1h"` | K 线周期 |
| `period` | usize | `5` | 计算高/低区间的回看窗口 |
| `k1` | Decimal | `"0.5"` | 上轨比例（Open + K1 * range） |
| `k2` | Decimal | `"0.5"` | 下轨比例（Open - K2 * range） |
| `qty` | Decimal | `0.001` | 市价单数量 |

## 示例配置

```toml
[strategy]
type = "dual_thrust"
[strategy.params]
symbol = "ETH/USDT"
exchange_id = "mock"
period = 5
k1 = "0.5"
k2 = "0.5"
qty = "0.01"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 低成交量时段中，短开盘区间的突破极易被假突破打脸——收紧 K 比例能降低假信号率，
  但也会漏掉真突破。
- 开盘价取当周期首根 K 线开盘价，跨日重启后通道会重算。
