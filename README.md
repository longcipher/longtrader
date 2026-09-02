# LongTrader

> **English** | [中文](README.zh.md)

Local strategy host for portable trading strategies over a contract-first ConnectRPC API. Strategies depend only on generated protobuf ports; the daemon/terminal backend is pluggable, and the same strategy ships in Rust, Python, TypeScript and Go.

> **Status:** open-source preview. The contract under `proto/` is the single source of truth (`buf` managed); all language SDKs are thin wrappers over generated stubs. Built from the ground up as an original LongTrader implementation.

## Features

- **Contract-first** — `proto/longtrader/**` defines trading, market, worker, data and backtest services. Breaking changes are checked by `buf breaking` in CI.
- **Portable strategies** — 20+ native strategies under `bin/longtrader-worker/src/strategies/` depend only on `TradingGateway`/`MarketDataSource` traits (`ports.rs`). No hidden domain coupling.
- **Session control plane** — deterministic `ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED` lifecycle, atomic `ReconcileState` snapshot, 3× heartbeat lease, per-session `KillSwitchPolicy` (session/all/none).
- **Thin SDKs** — `sdks/{python,typescript,go}` mirror the same 6-dir canonical layout (`contract/session/ports/adapters/strategies/examples`). Generated code is never hand-edited.
- **Worker hardening** — `tracing` only, `eyre`/`thiserror` error split, `hpx`+`rustls`, `tokio` async, strict `clippy::pedantic`/`nursery` lints.

## Architecture

```
proto/                        # buf v2, single source of truth (longtrader.*.v1)
crates/longtrader-contract/   # generated types + ConnectRPC traits (build.rs → buffa/connectrpc)
bin/longtrader-worker/        # strategy host: ports, session, adapters, envelope, strategies
sdks/{python,typescript,go}/  # thin wrappers over generated stubs (contract/session/ports/...)
```

Canonical layout per language:

| Canonical dir | Rust | Python | TypeScript | Go |
|---|---|---|---|---|
| `contract/` | `crates/longtrader-contract` | `sdks/python/longtrader_sdk/proto/` | `sdks/typescript/src/gen/` | `sdks/go/gen/` |
| `session/` | `bin/longtrader-worker/src/session/` | `longtrader_sdk/session.py` | `src/session.ts` | `session/` |
| `ports/` | `src/ports.rs` | `longtrader_sdk/ports.py` | `src/ports.ts` | `ports/` |
| `adapters/` | `src/adapters/{remote,mock}.rs` | Connect-over-httpx in `session.py` | Connect-over-undici in `session.ts` | Connect-over-http in `session/` |
| `strategies/` | `src/strategies/` | `examples/grid_strategy.py` | `examples/grid_strategy.ts` | `strategies/` |
| `examples/` | `examples/` / `docs/` | `sdks/python/examples/` | `sdks/typescript/examples/` | `sdks/go/examples/` |

## Quick Start

Prerequisites: Rust stable + nightly (`rustfmt`/`clippy`), `just`, `buf` (for contract lint/generation).

```bash
just setup          # installs cargo-mutants, cargo-shear, cargo-sort, typos, rumdl
just check          # cargo check --all-targets --all-features
just lint           # typos + rumdl + cargo sort/fmt + clippy -D warnings + shear
just test           # cargo test --all-features (unit + proptest)
just build          # cargo build --workspace

# Contract
just proto-lint     # buf lint on proto/
just proto-breaking # breaking check vs main
just sdk-generate   # local stub generation (offline-safe via scripts/gen-proto.sh)

# Run worker (example)
cargo run -p longtrader-worker -- --config config.toml
```

Minimal `config.toml`:

```toml
daemon_endpoint = "http://127.0.0.1:8080"
api_endpoint = "http://127.0.0.1:7888"      # when set, backend = "terminal"
api_token_file = "/run/secrets/longtrader_token"
listen_endpoint = "127.0.0.1:9000"          # WorkerSessionService + proxies

[strategy]
type = "simple_grid"
[strategy.params]
symbol = "BTCUSDT"
exchange_id = "mock"
lower_price = "90000"
upper_price = "110000"
num_levels = 10
qty_per_level = "0.001"
```

Token is loaded from `api_token_file` only (trimmed, never logged); `Authorization: Bearer <token>` is sent over `hpx` with `rustls`.

## SDKs

```bash
# Python
just sdk-generate
pip install -e sdks/python
python sdks/python/examples/grid_strategy.py --help

# TypeScript
just sdk-generate
cd sdks/typescript && npm install && npm run build
npx tsx examples/grid_strategy.ts --help

# Go (generated stubs via buf, paths=source_relative)
just sdk-generate
go run ./sdks/go/examples/grid_strategy --help
```

Minimal use (same lifecycle in every language — Attach → heartbeat → ReconcileState gate to ACTIVE):

```python
from longtrader_sdk import Session
s = Session.attach("http://127.0.0.1:8080", token="YOUR_TERMINAL_TOKEN")
s.start_heartbeat()
snap = s.reconcile_state()
print(s.session_id, s.state, snap.snapshot_sequence)
s.close()
```

```ts
import { Session } from "@longtrader/sdk";
const s = await Session.attach("http://127.0.0.1:8080", "YOUR_TERMINAL_TOKEN");
s.startHeartbeat();
const snap = await s.reconcileState();
console.log(s.sessionId, s.state, snap.snapshotSequence);
s.stop();
```

## Strategies

Each strategy under `bin/longtrader-worker/src/strategies/<name>/` ships a `README.md` and is selected via `strategy.type`:

| Category | Strategies |
|---|---|
| Grid / Market Making | `simple_grid`, `boll_grid`, `hedge_grid`, `fixed_maker`, `cross_maker`, `cross_depth_maker`, `cross_fixed_maker` |
| Trend Following | `ema_cross`, `supertrend` |
| Portfolio & Rebalancing | `rebalance`, `market_cap`, `dca_scheduler` |
| Risk & Operations | `sentinel`, `autoborrow`, `balance_align`, `convert`, `deposit_transfer` |
| Signals & Analytics | `irr`, `nav_recorder`, `premium_monitor`, `random_entry`, `xfunding_lite` |

All strategies are original LongTrader implementations and use only `ports::{TradingGateway, MarketDataSource}`; transport selection lives in `adapters/`.

## Development

```bash
just format     # rumdl fmt + cargo sort + cargo +nightly fmt
just fix        # rumdl --fix + clippy --fix
just mutation   # cargo mutants (focused on library crates)
just ci         # lint + test + build (mirrors CI)
```

Workspace rules: root `[workspace.dependencies]` carries numeric versions only (no default features); sub-crates use `workspace = true`. Prefer `hpx` over `reqwest`, `tokio` for async, `tracing` for observability.

## Security

- No secrets are committed. Tokens are file-loaded (`api_token_file`) and compared with constant-time `subtle`.
- `proto/longtrader/exchange/v1/exchange_daemon.proto` was removed in this open-source release (legacy private daemon API). Use `longtrader.market/trading/stream/ops.v1` instead.
- Report vulnerabilities via GitHub Security Advisories; do not open public issues for sensitive reports.

## License

Apache-2.0 — see `LICENSE`. SDK packages (`sdks/python`, `sdks/typescript`) are also Apache-2.0 unless noted otherwise.

## Acknowledgements

Built with `buffa`/`connectrpc`, `hpx`, `tokio`, `rust_decimal`, `buf`.
