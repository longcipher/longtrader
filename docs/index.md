# LongTrader

**A local strategy host for portable trading strategies over a contract-first
ConnectRPC API.** Write a strategy once against two small ports
(`TradingGateway`, `MarketDataSource`) and run it in Rust, Python, TypeScript,
or Go against the same generated, versioned contract. Your backend — mock,
private terminal, or your own daemon — is a detail you swap in the `adapters/`
layer without touching strategy code. The contract under `proto/` (buf v2,
`longtrader.*.v1`) is the single source of truth; SDKs are thin, never
hand-edited wrappers over generated stubs.

> **Start here**
>
> - [Getting Started](getting-started.md) — install, generate SDKs, run your first strategy.
> - [Architecture](architecture.md) — layers, the control-plane state machine, envelope framing.
> - [Bare Protocol Guide](bare-protocol-guide.md) — ConnectRPC wire format for clients without an SDK.
> - [Strategy Author Guide](strategy-author-guide.md) — build and register a portable strategy.
> - [Contract Style Guide](contract-style-guide.md) — enum/timestamp/pagination/money/error conventions (ADR).
> - [FAQ](faq.md) — venues, tokens, versioning, production readiness.

## Packages

| Language | Package | Registry |
|---|---|---|
| Rust (contract) | [`longtrader-contract`](https://crates.io/crates/longtrader-contract) | [![crates.io](https://img.shields.io/crates/v/longtrader-contract.svg)](https://crates.io/crates/longtrader-contract) |
| Rust (protocol) | [`longtrader-proto`](https://crates.io/crates/longtrader-proto) | [![crates.io](https://img.shields.io/crates/v/longtrader-proto.svg)](https://crates.io/crates/longtrader-proto) |
| Rust (CLI) | [`longtrader-cli`](https://crates.io/crates/longtrader-cli) | [![crates.io](https://img.shields.io/crates/v/longtrader-cli.svg)](https://crates.io/crates/longtrader-cli) |
| Rust (worker) | [`longtrader-worker`](https://crates.io/crates/longtrader-worker) | [![crates.io](https://img.shields.io/crates/v/longtrader-worker.svg)](https://crates.io/crates/longtrader-worker) |
| Python | [`longtrader-sdk`](https://pypi.org/project/longtrader-sdk/) | [![PyPI](https://img.shields.io/pypi/v/longtrader-sdk.svg)](https://pypi.org/project/longtrader-sdk/) |
| TypeScript | [`@longcipher/longtrader-sdk`](https://www.npmjs.com/package/@longcipher/longtrader-sdk) | [![npm](https://img.shields.io/npm/v/@longcipher/longtrader-sdk.svg)](https://www.npmjs.com/package/@longcipher/longtrader-sdk) |
| Go | [`sdks/go`](https://pkg.go.dev/github.com/longcipher/longtrader/sdks/go) | [![Go Reference](https://pkg.go.dev/badge/github.com/longcipher/longtrader/sdks/go.svg)](https://pkg.go.dev/github.com/longcipher/longtrader/sdks/go) |

> The Go SDK is a generated-stub scaffold; the `Session` API is available in
> Rust, Python, and TypeScript today. See [sdks/go/README.md](../sdks/go/README.md).
