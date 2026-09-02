> **English** | [中文](README.zh.md)

# balance_align — Cross-Venue Balance Alignment

Compares free balance of `asset` in `sync_state` snapshots between primary/hedge venues. When deviation from `primary_share` target exceeds `threshold`, rebalances via `WalletGateway::transfer`.

Params: `primary`/`hedge` venue refs, `symbol`, `asset`, `primary_share`, `threshold`.
