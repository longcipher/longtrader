# FAQ

## Which venues are supported?

LongTrader ships a **mock** venue and a pluggable backend layer
(`adapters/`): the `MockAdapter` runs offline, and `RemoteAdapter` targets a
private terminal/daemon. Concrete venue credentials are **not** part of the
public contract — you bring your own backend. See the [Architecture](architecture.md)
adapters seam.

### How are tokens handled?

Tokens load from `api_token_file` only (trimmed, never logged) and are compared
with a **constant-time** `subtle` compare. The CLI accepts `--token` or the
`$LONGTRADER_TOKEN` environment variable. Send them as
`Authorization: Bearer <token>` (see the [Bare Protocol Guide](bare-protocol-guide.md) §4).

### Is it production-ready?

This is an **open-source preview**. The contract is stable at `longtrader.*.v1`
and guarded by `buf breaking` in CI; the Python and TypeScript SDKs are complete
and runnable. Treat the worker/backends as preview-grade until you've validated
your own venue integration.

### How do I add a strategy?

Append a `StrategyDescriptor` to `registry()` in
`bin/longtrader-worker/src/strategies/mod.rs`, implement the `Strategy` trait
against the ports, and ship a `README.md`. Details in the
[Strategy Author Guide](strategy-author-guide.md).

### What languages are supported?

Rust, Python, and TypeScript today (complete SDKs). The **Go** SDK
(`sdks/go`) is a generated-stub **scaffold** — its `Session` API is not yet
implemented.

### How is the contract versioned / are breaking changes caught?

The contract is `proto/` managed with **buf v2**. `buf breaking` runs in CI on
every PR against `main`; any incompatible change fails the build before it ships.

### Can I run without a real exchange?

Yes — use the **mock** venue (`exchange_id = "mock"`) with `MockAdapter`, or run
a worker against `config.toml` with `api_endpoint` unset so the backend defaults
to mock. No credentials required.

### What is the session lifecycle?

`ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED`, plus `GRACEFUL_SHUTDOWN`
on explicit stop. Orders before `ACTIVE` are rejected `SYNC_IN_PROGRESS`; lease
expiry (3× heartbeat) trips the kill-switch. See [Architecture](architecture.md).

### How does the lease / kill-switch work?

`KeepAlive` every `heartbeat_interval_ms` feeds a watchdog; `lease_timeout`
defaults to 3× that. On expiry while `ACTIVE`, the configured `KillSwitchPolicy`
scope (`SESSION_ORDERS` / `ALL_ORDERS` / `NONE`) executes.

### How do I decode streaming responses by hand?

Use the 5-byte envelope (`flags` + `u32` length + payload) with
`application/connect+proto`. Full walk-through and hex examples in the
[Bare Protocol Guide](bare-protocol-guide.md) §5.

### Where do I report a vulnerability?

Via GitHub Security Advisories — do not open public issues for sensitive reports
(see `SECURITY.md`).
