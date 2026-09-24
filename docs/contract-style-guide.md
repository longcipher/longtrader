# Contract Style Guide (ADR)

This document is the **single source of truth for LongTrader contract conventions**.
Every `.proto` under `proto/longtrader/**` (buf v2, `longtrader.*.v1`) MUST follow
it. CI runs `buf lint` + `buf breaking`; new code that drifts from these rules fails
the build before merge.

## 1. Enums — single authority, no duplication

- enums live once, in the package that owns the concept:
  - `trading.v1` owns `OrderSide`, `OrderType`, `OrderStatus`, `TimeInForce`,
    `PositionSide`, `CloseReason`.
  - `market.v1` owns `TradeSide` (taker-perspective public-trade side) and
    `StreamChannel`.
- **Do not** define parallel enums for the same concept in another package
  (e.g. a second `PositionSide` or a `LegSide` that repeats `OrderSide`). Reuse the
  existing enum via its fully-qualified name.
- Every enum starts at `UNSPECIFIED = 0` so absent values decode safely.
- Numbers are stable; **never** renumber an existing value (that is a wire-breaking
  change — see §5).

## 2. Timestamps

Two representations intentionally coexist; pick by context:

| Use | Type | Example |
|---|---|---|
| Snapshot / entity fields | `google.protobuf.Timestamp` | `Order.timestamp`, `Ticker.timestamp`, `Position.timestamp` |
| High-frequency event time | `int64 …_ms` (Unix ms) | `Candle.timestamp_ms`, `Order.created_at_ms`, `Position.opened_at_ms` |

Rule: snapshot/entity messages use `google.protobuf.Timestamp`; per-event market
data (candles, ticks at scale) uses `int64 *_ms`. Do not mix the two within one
message family without a comment explaining why.

## 3. Pagination

Every list / history RPC takes `common.v1.Pagination` and echoes
`common.v1.Page` on the response:

```proto
message GetOrderHistoryRequest {
  string venue = 1;
  common.v1.Pagination pagination = 2;   // cursor-based, not bare `uint32 limit`
}
message GetOrderHistoryResponse {
  repeated Order orders = 1;
  common.v1.Page page = 2;               // next_cursor + total
}
```

- `common.v1.Pagination { uint64 limit; uint64 since; string cursor; }` — `limit`
  caps page size; `since` is an optional inclusive lower bound in **Unix
  milliseconds** (time-windowed paging); `cursor` is the opaque continuation token
  from a previous `Page.next_cursor`. Prefer `cursor` over `since` for stable
  paging across writes.
- `common.v1.Page { string next_cursor; uint32 total; }` — `next_cursor` is empty
  on the last page; `total` is best-effort and may be `0` when the backend does
  not compute a count.

**Bare `uint32 limit` is legacy** and is being retired. `GetOrderHistoryRequest`
and `GetClosedPositionsRequest` (terminal.v1) were migrated to `Pagination` in the
v1 breaking release (see §6). The unified `trading.v1` `GetOrderHistoryRequest` /
`GetClosedPositionsRequest` and the market.v1 / strategy_cfg `limit` fields remain
on `uint32 limit` because they are forwarded to the out-of-repo backend and are
scheduled for a later phase.

## 4. Money & quantity

All monetary/decimal values use `common.v1.Decimal` (string-backed, arbitrary
precision). Never use `double`/`float` for prices, amounts, balances, or PnL —
binary floats cannot represent decimal currency exactly.

## 5. Error model

Every RPC surfaces failures through the Connect error trail; structured detail is
`longtrader.common.v1.ErrorDetail` (`reason`, `domain`, `retryable`,
`retry_after`, `kill_switch_recommended`, `native_exchange_code`,
`correlation_id`). See `docs/bare-protocol-guide.md` §7. Do not invent per-RPC
error messages that bypass `ErrorDetail`.

## 6. Breaking-change migration (staged)

The following existing inconsistencies are **known** and tracked here; they are
deliberately **not** changed in a feature branch because each is wire-breaking and
requires coordinated regeneration of all SDKs (`crates/longtrader-contract`,
`sdks/python`, `sdks/typescript`, `sdks/go`) plus a versioned release.

| Item | Today | Target | Status |
|---|---|---|---|
| `terminal.v1.GetOrderHistoryRequest.limit` | `uint32 limit` | `common.v1.Pagination pagination` + `Page` echoed | **DONE** (v1 breaking release) |
| `terminal.v1.GetClosedPositionsRequest.limit` | `uint32 limit` | `common.v1.Pagination pagination` + `Page` echoed | **DONE** (v1 breaking release) |
| `PositionSide` duplication | defined in `trading`/`stream`/`terminal` | single authority `trading.v1.PositionSide` | **DONE** — `stream.v1`/`terminal.v1` now reuse it (wire-compatible: identical `LONG=1`/`SHORT=2`) |
| `CloseReason` duplication + `stream.v1` value reorder | 3 defs; `stream.v1` renumbered `1=TAKE_PROFIT` | single authority `trading.v1.CloseReason` | **DONE** — also fixed a silent cross-package corruption bug (`stream.v1` `1` was `TAKE_PROFIT`, not `MANUAL`) |
| `trading.v1.GetOrderHistoryRequest.limit` / `GetClosedPositionsRequest.limit` | `uint32 limit` | `common.v1.Pagination pagination` + `Page` echoed | **DONE** (v1 breaking release) |
| remaining `uint32`/`int64 limit` (market.v1 `GetCandles`/`FetchOrderBook`; strategy_cfg `ListNotifications`/`ListEpisodes`; terminal.v1 `GetCandles`/`SearchSymbols`) | `limit` | `common.v1.Pagination pagination` + `Page` echoed | **DONE** (v1 breaking release) |
| `OrderStatus` (`trading` `OPEN`/`CLOSED` vs `stream`/`terminal` `PENDING`/`FILLED`) | different lifecycle models per surface | keep distinct | NONE — the surfaces model different lifecycles; documented, not a defect |
| entity lifecycle timestamps (`Order`/`Position`/`ClosedPosition`/`HedgeUnit`/`Episode`/`NotificationRecord`/`EpisodeFill`/`SessionInfo`/`Opportunity` `*_at_ms`) | `int64` ms | `google.protobuf.Timestamp` | **DONE** — promoted; `Candle`/`stream` `timestamp_ms` and `heartbeat_interval_ms` kept as integers (event-time / duration, per §2) |
| `TradeSide` vs `OrderSide` | taker vs owner perspective | keep distinct | NONE — perspectives differ; documented |

Completed migrations shipped in this v1 **breaking** release (verified with
`buf lint` clean, `cargo check` green, `buf breaking` enumerating only the
intended surface, and SDKs regenerated):

- Enum consolidation: `stream.v1`/`terminal.v1` `PositionSide`/`CloseReason`
  now reuse `trading.v1` (and the `stream.v1.CloseReason` value reorder that
  caused silent cross-package corruption was fixed).
- Pagination: every list/history request (`terminal.v1` + unified
  `trading.v1` `GetOrderHistory`/`GetClosedPositions`; `market.v1`
  `GetCandles`/`FetchOrderBook`; `strategy_cfg` `ListNotifications`/`ListEpisodes`;
  `terminal.v1` `GetCandles`/`SearchSymbols`) now takes `common.v1.Pagination`
  and echoes `common.v1.Page` on the response. The in-repo worker `MockAdapter`
  and CLI/client were updated to build/consume `Pagination`; the out-of-repo
  backends that receive these requests must be released in lockstep.
- Timestamps: all entity lifecycle `*_at_ms` fields now use
  `google.protobuf.Timestamp`. `Candle.timestamp_ms` / stream `timestamp_ms`
  (event-time, high-frequency) and `heartbeat_interval_ms` (a duration) are
  intentionally kept as integer milliseconds per §2.

No further phased wire-breaking items remain in this contract; `OrderStatus`
and `TradeSide`/`OrderSide` are kept distinct by design (§1), not defects.

## 7. Lint policy (intentional exceptions)

`proto/buf.yaml` runs `buf lint` with the `STANDARD` set. Two classes of
exceptions are intentional and documented here so reviewers don't "fix" them by
re-introducing churn:

- **`RPC_REQUEST_RESPONSE_UNIQUE` is disabled.** `HedgeUnitService` returns the
  `HedgeUnit` entity from `GetHedgeUnit`/`CreateHedgeUnit`/`UpdateHedgeUnit`/
  `SetHedgeUnitStatus`, and `StrategyService` returns `StrategyStatus` from
  `StartStrategy`/`PauseStrategy`/`ResumeStrategy`/`StopStrategy`/
  `GetStrategyStatus`. This is the resource-as-response pattern (like a REST
  resource body returned from several CRUD ops). Wrapping each RPC in a
  per-RPC response message would be wire-compatible but would break the worker
  and every SDK client for no behavioral gain, so the rule is excepted.
- **`RPC_REQUEST_STANDARD_NAME` / `RPC_RESPONSE_STANDARD_NAME` /
  `PACKAGE_VERSION_SUFFIX`** are disabled: the project uses request/response
  names that don't end in `Request`/`Response` and a `v1`-suffixed package, by
  deliberate convention.
