# balance_align

跟踪目标保证金率；当账户保证金率低于 `target_ratio` 时，通过 `VenueOpInvoker.account_transfer` 划转 `rebalance_amount` 补充购买力。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `symbol` | String | 必填 | 交易标的（作用域） |
| `timeframe` | String | `"5m"` | K 线周期（透传） |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |
| `target_ratio` | Decimal | `0.5` | 期望保证金率 |
| `check_interval_secs` | u64 | `60` | 检查间隔（秒） |
| `currency` | String | `"USDT"` | 划转币种 |
| `rebalance_amount` | Decimal | `100` | 每次补足的报价金额 |

## 示例配置

```toml
[strategy]
type = "balance_align"
[strategy.params]
symbol = "BTC/USDT"
target_ratio = "0.5"
check_interval_secs = 60
currency = "USDT"
rebalance_amount = "100"
```

## 依赖能力

- `TradingGateway`（`get_account`、`create_order`）
- `MarketDataSource`（`fetch_ticker`）
- `VenueOpInvoker`（`account_transfer`）

## 风险提示

- 依赖 `VenueOpInvoker.account_transfer`，开源 mock 仅**桩实现**（记录 dry-run 警告，不执行真实划转）。
- 真实后端可能不支持或限制划转。
- 保证金率取自账户快照，节奏滞后可能错过快速变动。
