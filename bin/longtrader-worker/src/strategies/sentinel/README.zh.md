# sentinel

风险哨兵。每次检查读取账户，从峰值权益计算回撤；若突破 `max_drawdown_pct`，则硬平：撤销该标的上的**全部**订单并平掉仓位，然后冷却 `cooldown_secs`。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 要守护的交易标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `max_drawdown_pct` | Decimal | `0.20` | 触发平仓的峰值权益回撤 |
| `check_interval_secs` | u64 | `30` | 回撤检查间隔（秒） |
| `cooldown_secs` | u64 | `300` | 触发后静默再武装的冷却时间 |

## 示例配置

```toml
[strategy]
type = "sentinel"
[strategy.params]
symbol = "BTC/USDT"
max_drawdown_pct = "0.20"
check_interval_secs = 30
cooldown_secs = 300
```

## 依赖能力

- `TradingGateway`（`cancel_all_orders`、`close_position`、`create_order`）
- `MarketDataSource`（`get_account`）

## 风险提示

- `cancel_all_orders("")` 会清掉该标的上的**所有**订单（在某些后端甚至是整个场所），而非仅本策略的。
- 平掉仓位即实现它本意要止损的亏损。
- 峰值权益在内存中，重启会重置基线。
- 以账户权益作为回撤代理，无逐仓止损逻辑。
