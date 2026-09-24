# Getting Started

## 1. Install prerequisites

- **Rust** stable + nightly (`rustfmt`/`clippy`)
- **[just](https://github.com/casey/just)** — task runner
- **[buf](https://buf.build)** — contract lint / generation (v2)

```bash
just setup    # installs cargo-mutants, cargo-shear, cargo-sort, typos, rumdl
just check    # cargo check --all-targets --all-features
just lint     # typos + rumdl + cargo sort/fmt + clippy -D warnings + shear
just test     # cargo test --all-features (unit + proptest)
just build    # cargo build --workspace
```

## 2. Generate SDKs (offline-safe)

```bash
just sdk-generate      # regenerates Python + TypeScript stubs via scripts/gen-proto.sh
```

> The Go SDK (`sdks/go`) is a **generated-stub scaffold** — `scripts/gen-proto.sh`
> currently generates Python + TypeScript only; its `gen/` is produced by `buf`
> directly. Its `Session` API is not yet implemented.

## 3. Configure `config.toml`

```toml
backend = "terminal"                          # mock | terminal | api (unified)
api_endpoint = "http://127.0.0.1:8810"      # embedded longtrader-terminal; standalone longtrader-api serve uses :8080
api_token_file = "/run/secrets/longtrader_token"
listen_endpoint = "127.0.0.1:9000"          # WorkerSessionService + proxies (optional)

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

The token is loaded from `api_token_file` only (trimmed, never logged) and sent
as `Authorization: Bearer <token>` over `hpx` with `rustls`.

## 4. Run a worker strategy (against the mock venue)

```bash
just sdk-generate
cargo run -p longtrader-worker -- --config config.toml
```

## 5. Drive it from the CLI

```bash
cargo run -p longtrader-cli -- --help
longtrader health                    # health check
longtrader symbols --venue mock      # list tradeable symbols
longtrader candles BTCUSDT --timeframe M1 --limit 100
longtrader buy BTCUSDT 0.001 --price 95000      # limit (price given)
longtrader sell BTCUSDT 0.001                   # market (no --price)
longtrader stream --topics MARKET_LITE,TRADING  # live updates (Ctrl+C to stop)
```

Global flags: `--endpoint`, `--token` (or `$LONGTRADER_TOKEN`), `--venue`,
`--format` (`table`|`json`). `buy`/`sell` infer limit vs market by the presence
of `--price`.

## 6. Drive it from an SDK (same lifecycle in every language)

```python
# Python (longtrader-sdk)
from longtrader_sdk import Session
s = Session.attach("http://127.0.0.1:8080", token="YOUR_TERMINAL_TOKEN")
s.start_heartbeat()
snap = s.reconcile_state()
print(s.session_id, s.state, snap.snapshot_sequence)
s.close()
```

```ts
// TypeScript (@longcipher/longtrader-sdk)
import { Session } from "@longcipher/longtrader-sdk";
const s = await Session.attach("http://127.0.0.1:8080", "YOUR_TERMINAL_TOKEN");
s.startHeartbeat();
const snap = await s.reconcileState();
console.log(s.sessionId, s.state, snap.snapshotSequence);
s.stop();
```

Attach → heartbeat → reconcile → gate to `ACTIVE`. This is the contract every
language implements identically. The Go SDK does not yet expose these methods.
