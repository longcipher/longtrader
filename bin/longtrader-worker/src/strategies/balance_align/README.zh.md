> [English](README.md) | **中文**

# balance_align — 跨所余额对齐

对比 primary/hedge 两所 `sync_state` 快照中 `asset` 的自由余额，偏离 `primary_share` 目标比例超过 `threshold` 时经 `WalletGateway::transfer` 划转对齐。

参数：`primary`/`hedge` venue 引用、`symbol`、`asset`、`primary_share`、`threshold`。
