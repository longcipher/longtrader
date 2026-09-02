> **English** | [中文](README.zh.md)

# autoborrow — Auto Borrow to Maintain Balance

Calls `account.balance` / `margin.borrow` / `margin.repay` via `VenueOpInvoker`: automatically borrows to top up when free balance falls below `min_balance` and repays when it exceeds `repay_above`.

Ops table is ready for htx/bitget; binance/okx support is planned.
Params: `asset`, `min_balance`, `repay_above`, plus common fields.
