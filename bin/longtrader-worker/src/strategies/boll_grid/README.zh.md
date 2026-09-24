# boll_grid

轮询 K 线，在收盘价窗口上计算布林带；当最新收盘价落在足够宽的带内时，撤销原有阶梯并在收盘价下方挂 `grid_num` 个买限价、上方挂 `grid_num` 个卖限价。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `boll_window` | usize | `21` | 布林带窗口 K 线数 |
| `boll_mult` | Decimal | `2` | 布林带倍数（k） |
| `grid_num` | u32 | `3` | 每侧挂单层数 |
| `qty` | Decimal | `0.001` | 每层数量 |
| `profit_spread_pct` | Decimal | `0.0005` | 反向腿价差（占价格比例） |

## 示例配置

```toml
[strategy]
type = "boll_grid"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
timeframe = "5m"
boll_window = 21
boll_mult = 2
grid_num = 3
qty = "0.001"
profit_spread_pct = "0.0005"
```

## 依赖能力

- `TradingGateway`（`cancel_all_orders`、`create_order`）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 每个周期撤销该标的上的**全部**订单，仅保留本策略挂单。
- 布林带需要预热，且只有价格落在宽带内才挂单，可能长时间空闲。
- 网格在单边行情中累积库存；限价/市价成交存在滑点。
