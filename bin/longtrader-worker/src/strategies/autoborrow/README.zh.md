> [English](README.md) | **中文**

# autoborrow — 自动借入维持余额

通过 `VenueOpInvoker` 调用 `account.balance` / `margin.borrow` / `margin.repay`：自由余额低于 `min_balance` 时自动借入补足，超过 `repay_above` 时归还。htx/bitget 已有 ops 表；binance/okx 待补。

参数：`asset`、`min_balance`、`repay_above`、公共字段。
