# random_entry

**研究/演示策略——不盈利。** 每个周期抛一枚有偏硬币（以 `p` 概率做多）并下 `qty` 市价单，可用 `seed` 复现。仅用于压测 worker，而非实盘。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `qty` | Decimal | `0.001` | 下单数量 |
| `seed` | u64 | `42` | 随机数种子（可复现） |
| `p` | Decimal | `0.1` | 每周期做多概率 |

## 示例配置

```toml
[strategy]
type = "random_entry"
[strategy.params]
symbol = "BTC/USDT"
qty = "0.001"
seed = 42
p = "0.1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 市价单）
- `MarketDataSource`（`get_candles`）

## 风险提示

- **非交易策略**——入场随机，期望 PnL 约为 0 减手续费。
- 市价单滑点；运行会累积手续费/库存。
- 仅用于 mock 场所的负载/集成测试。
