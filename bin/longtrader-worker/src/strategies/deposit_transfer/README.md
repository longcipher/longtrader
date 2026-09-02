> **English** | [中文](README.zh.md)

# deposit_transfer — Deposit Sweep

Polls the deposit ledger and transfers newly completed deposits via `WalletGateway::transfer` to `dest_label`. In-memory dedup ensures each deposit is transferred once; `ignore_below` filters dust.
