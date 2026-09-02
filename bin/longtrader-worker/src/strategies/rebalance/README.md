> **English** | [中文](README.zh.md)

# rebalance — Portfolio Rebalancing

Values each symbol by `targets` weights; when weight drift exceeds `band_pct`, market-orders back to target.

Params: `targets` (BTreeMap), `quote_asset`, `band_pct`.
