> [English](README.md) | **中文**

# rebalance — 组合再平衡

按 `targets` 权重估值各 symbol，权重漂移超 `band_pct` 即市价调仓回目标。

参数：`targets`（BTreeMap）、`quote_asset`、`band_pct`。
