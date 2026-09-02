# LongTrader SDKs — Canonical Layout (aligned)

This directory holds Tier-2 thin wrappers over the generated `longtrader` Connect stubs.
Hand-written code stays minimal — session handling plus overflow policies — while
all trading semantics live in the contract (`proto/`, buf-managed).

## Canonical structure (design doc §6.8) — already aligned across Rust/Python/TypeScript/Go

Every language artifact mirrors one canonical layout, so review transfers 1:1:

```
<impl>/
  contract/        # generated stubs ONLY (buf output, never hand-edited)
                   #   Rust: `crates/longtrader-contract` / `longtrader_contract::proto`
                   #   Python: `sdks/python/longtrader_sdk/proto/`
                   #   TypeScript: `sdks/typescript/src/gen/`
                   #   Go: `sdks/go/gen/` (paths=source_relative)
  session/         # AttachSession / KeepAlive / lease / kill-switch helper
                   #   Rust: `bin/longtrader-worker/src/session/`
                   #   Python: `sdks/python/longtrader_sdk/session.py`
                   #   TypeScript: `sdks/typescript/src/session.ts`
                   #   Go: `sdks/go/session/`
  ports/           # TradingPort, MarketPort, StatePort + DTO re-exports
                   #   Rust: `bin/longtrader-worker/src/ports.rs`
                   #   Python: `sdks/python/longtrader_sdk/ports.py`
                   #   TypeScript: `sdks/typescript/src/ports.ts`
                   #   Go: `sdks/go/ports/`
  adapters/        # daemon | terminal | mock backends
                   #   Rust: `bin/longtrader-worker/src/adapters/`
                   #   Python/TypeScript/Go: thin Connect-over-http inside `session.*` (mock planned)
  strategies/      # portable strategies (the same grid in every language)
                   #   Rust: `bin/longtrader-worker/src/strategies/`
                   #   Python: `sdks/python/examples/grid_strategy.py`
                   #   TypeScript: `sdks/typescript/examples/grid_strategy.ts`
                   #   Go: `sdks/go/strategies/`
  examples/        # minimal runnable snippets, one per docs chapter
                   #   Rust: `examples/` / `docs/bare-protocol-guide.md`
                   #   Python/TypeScript/Go: `sdks/{python,typescript,go}/examples/`
```

Concept names are identical across languages — `Session`, `TradingPort`, `MarketPort`,
`sync_state()` / `syncState()` / `SyncState()`, `OverflowPolicy::{DropOldest, Coalesce, Block}` —
differing only in idiomatic casing. Generated code is quarantined under `contract/`.

## Layout verification

- `crates/longtrader-contract` — single source of truth (`proto/`), no hand-edits.
- `bin/longtrader-worker/src/session/` — state machine ATTACHED→SYNCING→ACTIVE→KILL_SWITCH_TRIPPED,
  ReconcileState atomic snapshot (`snapshot_sequence`), lease 3x heartbeat, KillSwitch Scope routing.
- `bin/longtrader-worker/src/ports.rs` — Decimal v2 dual helpers (`try_fast_path` / `fallback`),
  sequence gap detection (`is_sequence_gap`/`has_sequence_gap`), OverflowPolicy constants per channel (ticker DropOldest, book Coalesce, orders Block).
- `bin/longtrader-worker/src/envelope.rs` — 5-byte `encode_envelope`/`decode_envelope` (`flags 0x00/0x02`, `Content-Type: application/connect+proto`).
- `bin/longtrader-worker/src/overflow.rs` / `session/proxy.rs` — per-session independent `mpsc` with policy mapping.
- Strategies use only ports; adapters implement ports for each backend.
- Python, TypeScript & Go SDKs follow the same 6-dir canonical layout (see their own README.md; `sdks/go/gen` via `paths=source_relative`, local regeneration `buf generate`).

## Dependency policy (open-source ready)

Published worker/SDK closures contain only public registry crates + in-repo crates.
`hpx-transport` and sibling-repo domain crates are forbidden on this surface
(`bin/longtrader-worker/Cargo.toml` verified).
