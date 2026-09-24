# deposit_transfer

现货/资金清扫器。当 `get_deposits` 报告超过 `deposit_threshold` 的新充值，即通过 `VenueOpInvoker.account_transfer` 从源账户向 `target_account` 划转 `rebalance_amount`。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的（作用域） |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `check_interval_secs` | u64 | `60` | 充值检查间隔（秒） |
| `deposit_threshold` | Decimal | `0` | 触发动作的最小充值额 |
| `currency` | String | `"USDT"` | 充值/划转币种 |
| `target_account` | String | `"spot"` | 目标账户 |
| `rebalance_amount` | Decimal | `100` | 每次清扫划转金额 |

## 示例配置

```toml
[strategy]
type = "deposit_transfer"
[strategy.params]
symbol = "BTC/USDT"
deposit_threshold = "0"
currency = "USDT"
target_account = "spot"
rebalance_amount = "100"
```

## 依赖能力

- `TradingGateway`（`get_account`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）
- `VenueOpInvoker`（`account_transfer`、`get_deposits`）

## 风险提示

- 依赖 `VenueOpInvoker.account_transfer` / `get_deposits`，开源 mock 仅**桩实现**（dry-run 警告，无真实划转）。
- 充值检测较朴素（依赖后端的充值推送）。
- 真实后端可能不支持或限制划转。
