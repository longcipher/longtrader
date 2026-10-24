# apt_mm

APT（套利定价理论）做市商，带领先—滞后回归。

## 模型

用滚动 OLS 回归刻画两个相关标的（典型为期指与现货）之间的领先—滞后关系：

```text
R_futures = α + β · R_spot + ε
```

残差 `ε` 围绕 0 均值回归。当 `ε` 显著为正时，期指相对模型"高估"：

```text
if  ε >  threshold · stdev(ε)  →  卖期指 / 买现货
if  ε < −threshold · stdev(ε)  →  买期指 / 卖现货
```

同时基于模型隐含的期指公允价值挂出被动做市阶梯。滚动窗口 `window` 每次 tick 刷新
OLS 系数。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `lead_symbol` | String | 必填 | 领先标的（如永续合约） |
| `lag_symbol` | String | 必填 | 滞后标的（如现货） |
| `window` | usize | `50` | 滚动回归窗口 |
| `threshold` | Decimal | `1.0` | 以残差标准差倍数表示的入场阈值 |
| `qty` | Decimal | `0.001` | 持仓规模（基准单位） |
| `half_spread` | Decimal | `0.001` | 被动阶梯的半价差 |
| `levels` | u32 | `3` | 每侧做市档数 |
| `maker_qty` | Decimal | `0.001` | 每档数量 |

## 示例配置

```toml
[strategy]
type = "apt_mm"
[strategy.params]
symbol = "BTC/USDT"
lead_symbol = "BTC/USDT:USDT"
lag_symbol = "BTC/USDT"
window = 50
threshold = "1.0"
qty = "0.001"
half_spread = "0.001"
levels = 3
maker_qty = "0.001"
```

## 依赖能力

- `TradingGateway`（`create_order`）
- `MarketDataSource`（`get_candles`，需同时覆盖 `lead_symbol` 与 `lag_symbol`）

## 风险提示

- 领先—滞后关系会漂移；回归系数是滚动估计，结构性断点时信号失真。
- 残差阈值按历史 `stdev(ε)` 归一化，波动率突变时阈值随之漂移。
- 双腿非原子成交：回归信号触发时两条腿分开成交，存在执行价差风险。
- 要求两个标的都有可靠的 K 线数据。

## 适用场景

在领先—滞后关系稳定的标的上做配对交易与相对价值做市——期指对现货、或相关 ETF 对。
