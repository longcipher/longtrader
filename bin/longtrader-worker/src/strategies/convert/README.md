> **English** | [中文](README.zh.md)

# convert — Periodic Asset Conversion

Periodically calls `wallet.convert` every `interval_secs` to convert the amount of `from_asset` exceeding `min_amount` into `to_asset`. Routed through the generic VenueOpInvoker so any venue exposing the operation is supported.
