# LongTrader SDKs

Tier-2 thin wrappers over the generated `longtrader` Connect stubs. Hand-written
code stays minimal — session handling plus overflow policies — while all trading
semantics live in the contract (`proto/`, buf-managed).

## Real layout (per language)

The published SDKs do **not** all mirror one strict 6-directory tree at the SDK
root. Python and TypeScript bury their real code under a package directory
(`longtrader_sdk/` and `src/`), with thin **placeholder redirect directories** at
the SDK root. The Go SDK is a generated-stub **scaffold** (its `Session` API is
not yet implemented). The canonical *concepts* map 1:1; the *directory names*
differ by language.

| Canonical concept | Rust | Python | TypeScript | Go |
|---|---|---|---|---|
| `contract/` (generated, never hand-edited) | `crates/longtrader-contract` | `longtrader_sdk/proto/` | `src/gen/` | `gen/` |
| `session/` | `bin/longtrader-worker/src/session/` | `longtrader_sdk/session.py` | `src/session.ts` | `session/` |
| `ports/` | `src/ports.rs` | `longtrader_sdk/ports.py` | `src/ports.ts` | `ports/` |
| `adapters/` | `src/adapters/{remote,mock}.rs` | Connect-over-httpx in `session.py` | Connect-over-undici in `session.ts` | Connect-over-http in `session/` |
| `strategies/` | `src/strategies/` | `examples/grid_strategy.py` | `examples/grid_strategy.ts` | `strategies/` (planned) |
| `examples/` | `examples/` / `docs/` | `sdks/python/examples/` | `sdks/typescript/examples/` | `sdks/go/examples/` |

> **Layout reality check.** If you `ls` a Python/TypeScript SDK root you may see
> near-empty placeholder dirs (e.g. `contract/`, `session/`, `ports/`) that exist
> only to mirror the canonical map. The *real* code lives in `longtrader_sdk/`
> (Python) or `src/` (TypeScript). Don't edit the placeholder dirs — edit the
> package directory. The Go SDK currently has only generated `gen/` plus a
> `session/` scaffold stub.

## Concept parity

Concept names match across wrappers (`Session`, `TradingPort`/`TradingGateway`,
`MarketPort`/`MarketDataSource`, `sync_state()`/`syncState()`/`SyncState()`,
`OverflowPolicy`). The one place casing diverges by design is the
`OverflowPolicy` enum:

| Policy | Python | TypeScript | Go |
|---|---|---|---|
| drop oldest | `DROP_OLDEST` | `DROP_OLDEST` | `DropOldest` |
| coalesce | `COALESCE` | `COALESCE` | `Coalesce` |
| block | `BLOCK` | `BLOCK` | `Block` |

Each language maps the same three behaviors — `DropOldest` preserves newest,
`Coalesce` last-writer-wins per key, `Block` applies backpressure — but the
*encoding* is **not** identical: Python/TypeScript use UPPER-CASE string values,
Go uses the `OverflowPolicy` constants/ints. Wire values are the generated proto
enum; the SDK wrapper exposes the idiomatic per-language form above.

## Layout verification (worker)

- `crates/longtrader-contract` — single source of truth (`proto/`), no hand-edits.
- `bin/longtrader-worker/src/session/` — state machine ATTACHED→SYNCING→ACTIVE→KILL_SWITCH_TRIPPED,
  ReconcileState atomic snapshot (`snapshot_sequence`), lease 3× heartbeat, KillSwitch scope routing.
- `bin/longtrader-worker/src/ports.rs` — sequence gap detection
  (`is_sequence_gap`/`has_sequence_gap`), `OverflowPolicy` constants per channel
  (ticker DropOldest, book Coalesce, orders Block).
- `bin/longtrader-worker/src/envelope.rs` — 5-byte `encode_envelope`/`decode_envelope`
  (`flags 0x00/0x02`, `Content-Type: application/connect+proto`).
- `bin/longtrader-worker/src/state_store.rs` — JSON file state store (atomic save/load).
- Strategies use only ports; adapters implement ports for each backend.

## Dependency policy (open-source ready)

Published worker/SDK closures contain only public registry crates + in-repo
crates. `hpx-transport` and sibling-repo domain crates are forbidden on this
surface (`bin/longtrader-worker/Cargo.toml` verified).
