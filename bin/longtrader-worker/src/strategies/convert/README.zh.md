> [English](README.md) | **中文**

# convert — 定期资产转换

按 `interval_secs` 周期调用 `wallet.convert`，把 `from_asset` 超过 `min_amount` 的部分换成 `to_asset`。经通用 VenueOpInvoker 通道，适配任何暴露该操作的交易所。
