# ports

The hexagonal seam a strategy programs against, mirroring
`sdks/python/longtrader_sdk/ports.py` and `bin/longtrader-worker/src/ports.rs`
1:1.

- `TradingPort` — the 13-method order/account/position surface (the Rust
  `TradingGateway`), ending in `SyncState` for the recovery snapshot.
- `MarketPort` — the 5-method market data surface (the Rust
  `MarketDataSource`).
- `SessionTradingPort` / `SessionMarketPort` adapt a `*session.Session` to
  both, so a strategy written against the ports runs unchanged against any
  transport.
- `OverflowPolicy` (`DropOldest` / `Coalesce` / `Block`), `DefaultOverflow` and
  `OverflowPolicyForChannel` mirror the worker's `ports::OVERFLOW_*` constants:
  an order book is coalesced, a ticker may drop, and private order / balance /
  position streams never drop.
- `IsSequenceGap(prev, next)` reports a dropped-event discontinuity, which
  forces a snapshot resync (order book) or a `ReconcileState` (private
  streams).
