# LongTrader Agent Instructions

## Non-negotiables

- Surgical edits only: every changed line must trace to the request. No drive-by refactors, no reformatting untouched code, no deleting pre-existing dead code (mention it instead).
- Fix root causes. Never silence a warning, weaken a check, or patch a symptom to make a gate pass.
- Never report completion while any `just` gate below fails. Report the failure verbatim and stop.
- Stop and ask when a request is ambiguous, conflicts with existing conventions, or requires credentials, publishing, or destructive operations.
- When a correction reveals a missing rule, add one specific rule here ("always X for Y", never "be careful with Y"); delete rules that never catch real errors.

## Scope

- LongTrader is a contract-first trading platform. `proto/longtrader/**` (buf v2) is the single source of truth for the wire contract: the `account`, `backtest`, `common`, `data`, `market`, `ops`, `stream`, `terminal`, `trading` and `worker` services.
- `crates/longtrader-contract` holds the generated protobuf types and ConnectRPC traits (`connectrpc-build` plus `buffa`); `crates/longtrader-proto` is the client (feature `client-native` uses `hpx`, feature `client-wasm` uses `gloo-net`); `crates/longtrader-catalog` is the schema-driven descriptor catalog the terminal renders.
- `bin/longtrader-worker` is the strategy host (ports, adapters, session control plane, envelope, strategies); `bin/longtrader-cli` is the Terminal API client (binary name `longtrader`).
- `sdks/python`, `sdks/typescript` and `sdks/go` mirror the same canonical per-language layout (`contract/`, `session/`, `ports/`, `adapters/`, `strategies/`). The Go SDK is hand-written and stdlib-only with its own protobuf codec; the other SDKs wrap generated stubs.
- `fuzz/` is a separate cargo workspace (sanitizers, own release profile). `docs/` holds the architecture, strategy-author and contract-style guides.
- English only in code, comments, docs, and commit messages. The Chinese strategy READMEs (`README.zh.md`) and the bilingual top-level README are the only exceptions.

## Execution

- Parallelize aggressively: dispatch subagents for independent research, exploration, and implementation.
- NEVER run two `cargo` commands concurrently, and never overlap file edits with a running `cargo` command (`target/` file lock): finish edits, wait, then run.
- NEVER use `git worktree`. Edit the current branch in place.
- TDD for behavior changes: failing crate-local test first, smallest change to pass, then run `just mutation` and fix surviving mutants.

## Verification loop

After each feature or bug fix, in order; any failure means the task is not done:

```bash
just format
just lint
just test
just mutation
```

`just lint` is exactly the tool set CI installs: `typos`, `rumdl`, `cargo sort -w -g -c`, `cargo +nightly fmt --check`, `cargo +nightly clippy --all -- -D warnings`, `cargo shear`, `cargo workspace-inheritance-check` and `scripts/sync-contract-proto.sh --check`. `just lint` also runs `just proto-lint`, so `buf` must be on PATH.

## Workspace rules

- Edit `proto/longtrader/**` only, then run `just proto-sync`: the vendored copy at `crates/longtrader-contract/proto/longtrader/` must stay byte-identical, because published crates compile from the vendored copy. `just lint` fails on drift.
- `just proto-lint` (`buf lint`) and `just proto-breaking` (`buf breaking --against <git ref>`) guard the wire format. A deliberate break needs the `contract-breaking` PR label so CI reports it without failing.
- The contract style guide (`docs/contract-style-guide.md`) is binding: one authority per enum, RFC-3339 timestamps, money as `rust_decimal`, one error model. Read it before adding a field.
- Never hand-edit generated code in `sdks/*/src/gen`, `sdks/python/longtrader_sdk/proto` or `sdks/go/gen`. Regenerate with `just sdk-generate`.
- Never hand-type dependency versions. Add per-crate deps with `cargo add <crate> -p <crate>`. Shared deps go into root `[workspace.dependencies]` (numeric versions only, no default features) and are referenced with `dep = { workspace = true }`.
- Sub-crates inherit `version`/`edition`/`license`/`authors`/`repository` via `workspace = true` and must set `[lints] workspace = true`. `cargo workspace-inheritance-check` enforces this in `just lint` and `just check`.
- There is no `rust-toolchain.toml`; CI installs a nightly toolchain, so use `cargo +nightly fmt` and `cargo +nightly clippy` exactly as the Justfile does.
- `.cargo/config.toml` sets `rustc-wrapper = "kache"`, which breaks clippy's lints and `--fix`: prefix clippy with `RUSTC_WRAPPER=` (the Justfile does).
- When a native link fails with undefined symbols that survive `cargo clean -p <crate>`, always suspect a poisoned kache `build_script_run` entry: confirm the dep's static lib in `target/debug/build/` is truncated (`ar t`), match it by sha256 to a blob under the kache store, evict that entry, then rebuild.
- Clippy is `pedantic` plus a curated nursery set; `unwrap_used` and `print_stdout`/`print_stderr` are denied, `expect_used` is allowed, `allow_attributes` is allowed at priority -10.
- `cargo shear` is told to ignore the crates that only exist for the generated contract (`[package.metadata.cargo-shear] ignored` in `longtrader-contract` and `longtrader-proto`).

## Dependency choices

- Errors: `thiserror` in `crates/`, `color-eyre` in `bin/`. Never `anyhow`.
- Observability: `tracing` only (`tracing-subscriber` with `env-filter` in the binaries). Never `log`.
- HTTP: `hpx` with rustls. Never `reqwest`.
- Async: `tokio`. There is no `parking_lot`, no `dashmap` and no `scc` in this tree.
- Money and quantity: `rust_decimal` (plus `rust_decimal_macros` in the worker). `f64` is limited to the float strategy-parameter descriptors in `longtrader-catalog`; never use it for money or quantity.
- Contract plumbing: `buffa`/`buffa-types`/`connectrpc`; the Connect envelope is framed by hand (5-byte prefix) - see `docs/bare-protocol-guide.md`.
- Secrets: tokens are loaded from `api_token_file` only and compared with constant-time `subtle`. Never log a token and never commit one.

## Code conventions

- Hexagonal layering is strict: a strategy depends only on `ports::{TradingGateway, MarketDataSource, FundingRateSource, VenueOpInvoker, WalletGateway}`; transport selection lives in `adapters/` (`remote` = ConnectRPC over `hpx`, `mock` = offline dry-run).
- `bin/longtrader-worker/src/strategies/registry.rs` is the single owner of strategy construction. Adding a strategy means one registry entry plus its own `strategies/<name>/` directory with a `README.md` and `README.zh.md`; do not dispatch on the strategy name anywhere else.
- The session control plane is lease-based with an atomic `ReconcileState` and a per-session `KillSwitchPolicy` (`session`/`all`/`none`). Keep that state machine atomic.
- `longtrader-worker` has `hotpath` and `hotpath-alloc` feature flags for profiling; they are off by default and must stay zero-cost when disabled.
- The CLI writes pipeable JSON to stdout with errors and hints on stderr and a non-zero exit code on failure. Keep that split.
- Property tests (`proptest`) are colocated in the crate they exercise and run in the ordinary `cargo test` path. Fuzz targets cover only the two places that parse bytes they did not produce: the Connect envelope decoder (`envelope`) and the dual-representation decimal codec (`decimal_codec`).
