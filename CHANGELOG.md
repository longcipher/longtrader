# Changelog

All notable changes to the longtrader contract and its language SDKs are
documented here. The contract is language-neutral protobuf under `proto/`; the
Rust crates and the Python/TypeScript SDKs are generated from it, and the Go
SDK is a hand-written codec pinned to the same wire format.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/). While
the crates are pre-1.0, a wire-breaking contract change increments the minor
version (0.x.0).

## [Unreleased]

- **Every venue capability is now implemented on the open backends.** The four
  extended ports — `FundingRateSource`, `TriggerOrderGateway`,
  `VenueOpInvoker`, `WalletGateway` — returned `Unsupported` on both
  `RemoteAdapter` and `TerminalAdapter`, so `xfunding_lite`,
  `premium_monitor`, `autoborrow`, `convert`, `deposit_transfer` and
  `balance_align` could not run on a real backend. All 8 methods are now
  implemented on both adapters, and the worker re-exports them through the
  control plane so a remote strategy reaches them over the same RPCs it uses
  for orders:
  - `FetchFundingRate` / `FetchFundingRateHistory` on `market.v1`;
  - `CreateTriggerOrder` / `CancelTriggerOrder` / `ListTriggerOrders`,
    `FetchLedgerEntries` and `Transfer` on `trading.v1`;
  - the pre-existing `ops.v1.VenueOpService` is now mounted, so discovery
    (`ListVenueOps`, `DescribeVenueOp`) and dynamic invocation work.
  The startup bail that refused these strategies outright is gone. A backend
  that genuinely cannot serve a capability answers `unimplemented` — a
  permanent, non-retryable answer — instead of failing mid-tick.

- **Contract (all additive, `buf breaking` clean).** 12 new RPCs and their
  messages, so the capabilities are part of the wire contract rather than an
  adapter convention:
  - `market.v1`: `FundingRate`, `FundingRatePoint`, `FetchFundingRate`,
    `FetchFundingRateHistory`;
  - `trading.v1`: `TriggerOrderStatus`, `TriggerPriceType`, `TriggerOrder`,
    `TriggerOrderRequest`, `CreateTriggerOrder`, `CancelTriggerOrder`,
    `ListTriggerOrders`, `FetchLedgerEntries` (reusing the previously dead
    `account.v1.LedgerEntry`), `Transfer`;
  - `terminal.v1`: the same funding / trigger / ledger / transfer surface in
    its string-decimal style, so the terminal backend is not a second-class
    citizen.
  `TriggerOrder` is deliberately separate from `OrderRequest.trigger_price`:
  the latter places a stop on the main book through the strategy's own
  session, while a trigger order rests on the venue's matching engine and
  fires even if the strategy process is dead — the only reliable stop backstop.

- **`longtrader-proto` client** gained the matching `TerminalClient` methods
  for every new RPC, plus the `ops.v1` surface, with the unified-surface
  variants (`fetch_funding_rate`, `create_trigger_order_unified`,
  `transfer_unified`, ...) used by `RemoteAdapter`.

- `trading.v1.CreateOrderRequest.session_id` and
  `CreateOrdersRequest.session_id` (additive, `buf breaking` clean). A
  non-empty value binds the order to the session that placed it: the host
  rejects a submission from a session that has not reached `ACTIVE` with
  `failed_precondition` / `SYNC_IN_PROGRESS`, and records the resulting order
  id so `KillSwitchPolicy.SCOPE_SESSION_ORDERS` and
  `StopStrategy.cancel_open_orders` cancel exactly that session's orders. An
  empty value marks an unscoped operator call (the CLI), which is neither gated
  nor tracked.
- Python and TypeScript SDKs now cover all 9 `worker.v1.WorkerSessionService`
  RPCs (added `RegisterStrategy`, `StrategyStatus`, `StopStrategy`,
  `ReportLog`, `StreamStrategyEvents`) plus the full `trading.v1` (12) and
  `market.v1` (5) surfaces, including the Connect 5-byte envelope codec for
  streaming and `resume_token` replay with sequence-gap signalling.
- `SessionTradingPort` / `SessionMarketPort` adapters in both SDKs, making the
  `TradingPort` / `MarketPort` seam usable rather than declarative.
- SDK test suites and `just sdk-test` / `just sdk-lint`, wired into `just ci`.
  The TypeScript SDK gained a `test` script and a `tsconfig.build.json` so the
  examples no longer ship inside the published tarball.
- Python SDK: `py.typed` marker, `session_id` reconnect support,
  `capabilities` and `snapshot_sequence` exposure, and a local pre-ACTIVE
  trade gate that fails fast before the round trip.
- **Go SDK is now a real client, not a scaffold.** `sdks/go` implements the same
  `Session` lifecycle as Python and TypeScript (`Attach` with `session_id`
  reconnect, `KeepAlive`, `ReconcileState`, `SetKillSwitchPolicy`,
  `RegisterStrategy`, `StrategyStatus`, `StopStrategy`, `ReportLog`,
  `StreamEvents`), the full `trading.v1` (12) and `market.v1` (5) surfaces, the
  two terminal lifecycle states, the local pre-ACTIVE gate, and the lease
  watchdog. It adds `contract/`, a hand-written, **dependency-free** protobuf
  codec for `common.v1`/`worker.v1`/`trading.v1`/`market.v1`/`account.v1`, so
  `go.mod` has no `require` directives and the module builds, vets and tests in
  a fresh checkout with no codegen step.
  - `ports/` is completed to parity: `OverflowPolicy` with `DefaultOverflow`
    and `OverflowPolicyForChannel`, `IsSequenceGap`, the 13-method
    `TradingPort` and 5-method `MarketPort`, and working `SessionTradingPort` /
    `SessionMarketPort` adapters.
  - `examples/grid_strategy.go` is runnable end to end and mirrors
    `sdks/python/examples/grid_strategy.py`: attach → register → heartbeat +
    lease watchdog → reconcile → loop { fetch mid, cancel stale rungs, batch
    place } → `StopStrategy(cancel_open_orders=true)`.

### Fixed

- **Documented config keys never reached any strategy.** `StrategyParams::table()`
  returned only the `extra` catch-all, but serde had already consumed the
  well-known keys, so `[strategy.params] symbol = "..."` (the format in
  `README.md`) never arrived and every symbol-requiring strategy failed at
  startup with `missing field \`symbol\``. The well-known keys are now merged
  back into the table, so the documented config actually works.
- **Every capability error was reported as `internal`.** A rejected argument, a
  missing object and a permanently unsupported capability all collapsed into
  one server-fault code, so a client could not tell "fix your request" from
  "this backend cannot do that" and retried permanent answers forever. Port
  errors now map onto the matching Connect codes, and an upstream RPC code is
  preserved rather than flattened.
- **The mock venue reported one contract's funding rate for every symbol, and
  invented settlement history from wall time.** A carry strategy would size a
  real position off another contract's number, and a backtest against the mock
  would not reproduce. Funding is now symbol-scoped and history is
  deterministic on the settlement grid.
- **A cancelled or fired trigger order could be "cancelled" again.** The mock
  now rejects a second cancel, because reporting success would tell a strategy
  its backstop is gone when it is actually live on the venue.
- **Transfers were not idempotent and did not appear on the ledger.** A retried
  transfer moved the funds twice. Transfers now honour
  `client_transfer_id` and write a signed ledger row, so a strategy that scans
  the ledger sees its own capital movements. The two strategies that transfer
  (`deposit_transfer`, `balance_align`) pass a deterministic idempotency key.

- **`buf generate` produced no Go output at all.** Go has no default in buf
  managed mode, so `protoc-gen-go` failed on the missing `go_package` and
  aborted the whole run, silently discarding the Go stubs. `proto/buf.gen.yaml`
  now carries a `managed.override` with
  `go_package_prefix: github.com/longcipher/longtrader/sdks/go/gen`, so no
  `go_package` option is needed in the language-neutral `.proto` files.
- **Go SDK heartbeat had a dead lease check.** `StartHeartbeat` skipped its
  lease evaluation whenever `KeepAlive` failed, so the guard could never fire
  in the only situation it exists for. The lease is now evaluated on every
  tick, including after a failed heartbeat; on expiry the session moves to
  `KILL_SWITCH_TRIPPED` and asks the host to cancel its orders.
- **Go SDK authenticated nothing.** The token was sent only as an
  `Authorization` header, but the host reads it from the
  `AttachSessionRequest.token` body field. The body field is now authoritative
  and the header is still set for intermediaries.
- **Go SDK stream decoding was endian-sensitive.** Envelope lengths are written
  big-endian per the Connect spec, and a single-message reply (such as
  `ReportLog`) is decoded from the first data frame rather than from the whole
  body, which also carries a trailing end-of-stream JSON frame.
- **Session order gate was not enforced.** `TradingProxy` forwarded every
  order straight to the gateway, so the documented `SYNC_IN_PROGRESS`
  rejection did not exist; a strategy could trade on stale state after a
  reconnect. The gate now lives in `SessionManager::authorize_order_submission`
  and is enforced host-side.
- **Kill-switch `SESSION_ORDERS` scope was a no-op.** Session order ids were
  only ever recorded from tests, so neither the lease-expiry kill-switch nor
  `StopStrategy(cancel_open_orders)` cancelled anything. Orders are now
  attributed on submission.
- **Python SDK could not execute any RPC.** The generated stubs import each
  other by absolute proto package path (`from longtrader.account.v1 import ...`)
  but are installed under `longtrader_sdk/proto/`, so `Session.attach()` — the
  first call in every example — raised `ModuleNotFoundError`. Resolved by a
  meta-path finder in `longtrader_sdk/proto/__init__.py` that does not shadow a
  genuine `longtrader` package.
- `TerminalAdapter` no longer drops `trigger_price` and `reduce_only` when
  placing orders (a `reduce_only` close could open opposite exposure, and a
  conditional order was sent to the venue with no trigger). A conditional order
  with no trigger is now rejected instead of being sent as a plain order.
- Unknown timeframes are rejected instead of silently degrading to `M1`, and
  malformed OHLCV fields propagate as errors instead of becoming `0`.
- `StopStrategy` logic is now shared between the RPC and embedders
  (`SessionManager::stop_session`) rather than duplicated.
- SDK decimals populate `unscaled`/`scale` as well as `raw_str`; writing only
  the string form arrives as zero on the wire.

## [0.2.0] - 2026-09-24

### ⚠️ Breaking changes

This release ships the remaining contract-breaking migrations decided in
`docs/contract-style-guide.md` (ADR). **All of these are wire-breaking**: bump
the SDK major/minor and coordinate the out-of-repo backends that receive these
requests before rolling out.

#### 1. Pagination: `limit` → `common.v1.Pagination` (+ `Page` echoed)

Every list/history request now takes `common.v1.Pagination` instead of a bare
`limit`, and every matching response now echoes `common.v1.Page`.

| Request (was `uint32`/`int64 limit`) | New field |
| --- | --- |
| `trading.v1.GetOrderHistoryRequest` | `pagination` (`common.v1.Pagination`) |
| `trading.v1.GetClosedPositionsRequest` | `pagination` |
| `terminal.v1.GetOrderHistoryRequest` | `pagination` |
| `terminal.v1.GetClosedPositionsRequest` | `pagination` |
| `market.v1.GetCandlesRequest` | `pagination` |
| `market.v1.FetchOrderBookRequest` | `pagination` |
| `strategy_cfg.v1.ListNotificationsRequest` | `pagination` (was `int64 limit`) |
| `strategy_cfg.v1.ListEpisodesRequest` | `pagination` (was `int64 limit`) |
| `terminal.v1.GetCandlesRequest` | `pagination` |
| `terminal.v1.SearchSymbolsRequest` | `pagination` |

Responses gaining `page` (`common.v1.Page`): `GetOrderHistoryResponse`,
`GetClosedPositionsResponse`, `GetCandlesResponse` (both services),
`FetchOrderBookResponse`, `ListNotificationsResponse`, `ListEpisodesResponse`,
`SearchSymbolsResponse`.

- Migration: set `pagination.limit` (and optionally `since` / `cursor`) instead of
  `limit`. The in-repo worker `MockAdapter` and CLI client already do this.
- The out-of-repo backends that receive these requests must parse `pagination`
  and echo `page`; release them in lockstep with this client/SDK bump.

#### 2. Timestamps: `*_at_ms` (`int64` ms) → `google.protobuf.Timestamp`

All entity lifecycle timestamps are now `google.protobuf.Timestamp` (the field
name drops the `_ms` suffix). Affected messages (in every service where they
appear — `trading.v1`, `terminal.v1` `messages`, `stream.v1`):

- `Order.created_at` / `Order.updated_at`
- `Position.opened_at`
- `ClosedPosition.opened_at` / `ClosedPosition.closed_at`
- `HedgeUnit.opened_at` / `HedgeUnit.updated_at`
- `Episode.opened_at` / `Episode.closed_at`
- `EpisodeFill.timestamp`
- `NotificationRecord.timestamp`
- `SessionInfo.expires_at`
- `Opportunity.updated_at`

**Intentionally kept as integer milliseconds** (event-time / duration, per
`docs/contract-style-guide.md` §2): `Candle.timestamp_ms`, every `stream.v1`
snapshot `timestamp_ms`, and `heartbeat_interval_ms`.

#### 3. Enum consolidation (carried in this release)

- `stream.v1.PositionSide` / `CloseReason` and `terminal.v1.PositionSide` /
  `CloseReason` are removed; reuse `trading.v1.PositionSide` / `CloseReason`.
- `stream.v1.CloseReason` value reorder (the `CANCELED`/`REJECTED` corruption
  that silently decoded cross-package incorrectly) is fixed.

### SDK impact

- **Rust** (`longtrader-proto`, `longtrader-cli`, `longtrader-worker`): bumped to
  `0.2.0`; builders/consumers updated.
- **TypeScript** / **Python**: regenerate with `just sdk-generate` (or
  `bash scripts/gen-proto.sh`); consumers must build `Pagination` and read
  `Timestamp`.
- **Go**: `sdks/go` tracks this release by hand — its `contract/` package is a
  dependency-free protobuf codec whose field numbers mirror `proto/`, so no
  regeneration is needed. `sdks/go/gen/` (optional, gitignored) is populated by
  `buf generate --template proto/buf.gen.yaml` and is imported by nothing.

### Notes / non-changes

- `OrderStatus` (`trading` `OPEN`/`CLOSED` vs `stream`/`terminal`
  `PENDING`/`FILLED`) and `TradeSide` vs `OrderSide` remain distinct by design
  (different lifecycle models / perspectives) — not defects.
- `buf breaking --against main` reports exactly the surface enumerated above and
  no unintended breaks.

## [0.1.0] - initial

- Baseline contract: duplicated `PositionSide`/`CloseReason` across
  `stream.v1`/`terminal.v1`, bare `limit` pagination, and `int64 *_ms` lifecycle
  timestamps (the state this release migrates away from).
