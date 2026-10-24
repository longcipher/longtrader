# obi_mm

订单簿失衡（OBI）做市商。

## 模型

从盘口顶部买卖挂单量的相对压力预测短期价格方向：

```text
OBI    = (bid_size − ask_size) / (bid_size + ask_size)    ∈ [−1, 1]
fair   = mid + α · OBI · mid
bid    = fair · (1 − half_spread)
ask    = fair · (1 + half_spread)
```

`α` 是信号权重（`signal_weight`）——策略按失衡倾斜的激进程度。
OBI 为正（买盘深度更大）表示上行压力，因此公允价值上移、买价收紧以抢在上涨前成交，
而卖价则从回归中获利。

当 OBI 信号强且与某一侧一致时，该"受青睐侧"获得报价数量放大（`size_skew`，`1.0` = 不放大）。

## 参数

继承自 `CommonParams`：`exchange_id`（默认 `"mock"`）、`label`（默认 `""`）、`symbol`（必填）、
`timeframe`（默认 `"5m"`）、`poll_secs`（默认 `30`）。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `half_spread` | Decimal | `0.0005` | 半价差（占公允价值比例） |
| `signal_weight` | Decimal | `0.5` | 每单位 OBI 对公允价值的倾斜量 α |
| `qty` | Decimal | `0.001` | 每侧报价数量 |
| `depth` | u32 | `5` | 累计 OBI 所用的盘口档数 |
| `size_skew` | Decimal | `1` | 受青睐侧的报价数量倍数 |

> 注：英文 `README.md` 中写的是 `size_boost`（`2.0`）与 `boost_threshold`（`0.3`），
> 且 `signal_weight` 记为 `0.001`。以 `obi_mm/mod.rs` 为准的实际字段是 `size_skew`
> （默认 `1`），且不存在 `boost_threshold`；`signal_weight` 的默认值为 `0.5`。

## 示例配置

```toml
[strategy]
type = "obi_mm"
[strategy.params]
symbol = "BTC/USDT"
exchange_id = "mock"
half_spread = "0.0005"
signal_weight = "0.5"
qty = "0.001"
depth = 5
size_skew = "1"
```

## 依赖能力

- `TradingGateway`（`create_order` / 限价单）
- `MarketDataSource`（`fetch_order_book`）

## 风险提示

- OBI 是瞬时快照，极易被大单撤单与幌单操纵。
- `signal_weight` 过大时公允价值偏离中间价过多，报价可能自成交或偏离市场。
- 无库存控制，单边行情下敞口累积。

## 适用场景

订单流失衡对短期方向有预测力的高流动、小跳价市场——主流加密现货与永续。
