# convert

保证金哨兵变体。运行心跳；当检测到 `kill_switch_on_disconnect`（券商断开）时平仓以保护保证金，重连后在上次入场价附近重新开仓。（尽管名字如此，它并不调用 `VenueOpInvoker.wallet_convert`。）

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 要守护的交易标的 |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `max_drawdown_pct` | Decimal | `0.20` | 触发平仓的回撤 |
| `check_interval_secs` | u64 | `60` | 检查间隔（秒） |
| `cooldown_secs` | u64 | `300` | 触发后静默时间 |

## 示例配置

```toml
[strategy]
type = "convert"
[strategy.params]
symbol = "BTC/USDT"
max_drawdown_pct = "0.20"
check_interval_secs = 60
cooldown_secs = 300
```

## 依赖能力

- `TradingGateway`（`cancel_all_orders`、`close_position`、`create_order`、`get_account`）

## 风险提示

- 本策略是**断连哨兵**，并非兑换策略——尽管名字如此，它并不调用 `VenueOpInvoker.wallet_convert`。
- `cancel_all_orders("")` 清掉该标的全部订单；平仓实现亏损。
- 峰值权益在内存中，重启重置基线。
- 重连后按上次入场价重新开仓，可能过时。
