# Liquidity Mining（CLMM）

通过调整价格区间来管理集中流动性做市（CLMM）头寸。波动率高时区间放宽以避免出圈；
波动率低时区间收窄以最大化手续费收入。

## 工作原理

1. 从滚动窗口的 bar 收益率估计已实现波动率。
2. 计算区间半宽：`k · σ · √T`，并 clamp 到 `[min_range, max_range]`。
3. 设定 CLMM 头寸区间：`[mid · (1 − width), mid · (1 + width)]`。
4. 区间变化显著时再平衡头寸。

波动率估计器 `VolEstimator` 按 bar 收益率年化；窗口内样本少于 2 个时返回 `None`。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `range_k` | Decimal | `1` | 区间倍数 k |
| `window` | usize | `20` | 波动率滚动窗口 |
| `min_range` | Decimal | `0.01` | 最小区间宽度（比例） |
| `max_range` | Decimal | `0.5` | 最大区间宽度（比例） |
| `symbol` | String | 必填 | 交易标的 |
| `exchange_id` | String | `"mock"` | 场所标识 |
| `timeframe` | String | `"5m"` | K 线周期 |
| `poll_secs` | u64 | `30` | 轮询间隔（秒） |

## 示例配置

```toml
[strategy]
type = "liquidity_mining"
[strategy.params]
symbol = "ETH/USDT"
range_k = "1"
window = 20
min_range = "0.01"
max_range = "0.5"
```

## 依赖能力

- `TradingGateway`（`create_order`，取决于 CLMM 适配）
- `MarketDataSource`（`get_candles`）

## 风险提示

- 区间收窄提升资金效率，但出圈风险与无常损失同步放大。
- 每次再平衡都是一次链上/场内操作，费用与滑点会侵蚀挖矿收益。
- 需要支持 CLMM 区间持仓的场所后端；开源 mock 不提供。

## 参考

<https://hummingbot.org/strategies/liquidity-mining/>
