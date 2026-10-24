# PMM Dynamic

纯做市的增强版：根据实时波动率动态调整买卖半价差。高波动率时放宽价差以补偿风险；
低波动率时收窄以保持竞争力。

## 工作原理

1. 从滚动窗口的 bar 收益率估计已实现波动率。
2. 计算波动率比值：`current_vol / base_vol`。
3. 缩放基础价差：`dynamic_spread = base_spread · vol_ratio · inventory_skew_factor`。
4. 买价挂 `mid · (1 − bid_spread)`，卖价挂 `mid · (1 + ask_spread)`。
5. 每 tick 重新报价。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `base_spread` | Decimal | `0.005` | 基础半价差（比例） |
| `base_vol` | Decimal | `0.01` | 缩放参考波动率 |
| `window` | usize | `20` | 已实现波动率滚动窗口 |
| `inventory_skew` | Decimal | `0.5` | 库存偏移因子 |
| `qty` | Decimal | `1` | 每侧报价数量 |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "pmm_dynamic"
[strategy.params]
symbol = "BTC/USDT"
base_spread = "0.005"
base_vol = "0.01"
window = 20
inventory_skew = "0.5"
qty = "1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 限价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 波动率突升时价差会大幅放宽，可能长时间不成交。
- 无价格带/挂单保留逻辑，快市中报价容易过时。
- 波动率估计基于 bar 收益率，窗口内样本 < 2 时无信号。

## 参考

<https://hummingbot.org/strategies/pure-market-making/>（dynamic 模式）
