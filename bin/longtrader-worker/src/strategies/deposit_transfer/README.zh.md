> [English](README.md) | **中文**

# deposit_transfer — 充值扫转

轮询充值账本，新到账且完成的充值经 `WalletGateway::transfer` 划转到 `dest_label`；内存去重保证每笔只转一次，`ignore_below` 过滤灰尘。
