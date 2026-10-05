# Default recipe to display help
default:
  @just --list

# Format all code
format:
  rumdl fmt .
  cargo sort -w -g
  cargo +nightly fmt --all

# Auto-fix linting issues
fix:
  rumdl check --fix .
  RUSTC_WRAPPER= cargo +nightly clippy --fix --all --allow-dirty

# Run all lints
lint: proto-lint
  typos
  rumdl check .
  cargo sort -w -g -c
  cargo +nightly fmt --all -- --check
  RUSTC_WRAPPER= cargo +nightly clippy --all -- -D warnings
  cargo shear
  cargo workspace-inheritance-check
  scripts/sync-contract-proto.sh --check

# Run tests
test:
  cargo test --all-features

# Run mutation tests with cargo-mutants
mutation:
  cargo mutants

# Run tests with coverage.
#
# The generated protobuf code is excluded: `build.rs` emits ~29k lines of it into
# the target dir, it is never exercised directly, and leaving it in swamps the
# denominator until the real coverage number is unreadable. This needs
# `cargo-tarpaulin >= 0.37` — an older vendored `llvm-profparser` cannot read the
# indexed-profile format the current toolchain's LLVM emits, and fails with
# "consistency check for reading counts failed".
# `--out Stdout` keeps the report in the terminal instead of writing a Cobertura
# file into the working directory. `.gitignore` covers `coverage.xml`, not
# `cobertura.xml`, so the default dropped an untracked ~900 KB artifact in the
# repo root that `git add -A` would have swept in.
test-coverage:
  cargo tarpaulin --all-features --workspace --timeout 300 --exclude-files '*/out/*' --out Stdout

# ---------------------------------------------------------------------------
# Fuzzing
#
# Targets live in `fuzz/fuzz_targets/` and cover the two places where this
# workspace parses bytes it did not produce: the Connect streaming envelope
# decoder (`envelope`) and the contract decimal codec (`decimal_codec`), which
# decodes the dual-representation numeric field carried by nearly every message.
#
# `fuzz/` is its own workspace (it needs sanitizers and a release profile), so
# `cargo` must be invoked through `cargo fuzz` from the repository root.
# ---------------------------------------------------------------------------

# Build every fuzz target without running it.
fuzz-build:
  cargo fuzz build --fuzz-dir fuzz

# Fuzz one target. Override the budget with `FUZZ_SECONDS=300 just fuzz envelope`.
fuzz target:
  cargo fuzz run --fuzz-dir fuzz {{target}} -- -max_total_time=${FUZZ_SECONDS:-60}

# Run every target briefly as a smoke test. This is the form CI should use; a long
# fuzzing session is a local activity, not a gate.
#
# fuzz-smoke: short bounded fuzz run over every target
fuzz-smoke: fuzz-build
  #!/usr/bin/env bash
  set -euo pipefail
  for target in envelope decimal_codec; do
    echo "::group::fuzz $target"
    cargo fuzz run --fuzz-dir fuzz "$target" -- -runs=20000 || true
    echo "::endgroup::"
  done

# Build entire workspace
build:
  cargo build --workspace

# Check all targets compile
check:
  cargo check --all-targets --all-features
  cargo workspace-inheritance-check

# Publish all crates to crates.io (dry run)
publish-check:
  cargo publish --workspace --dry-run --allow-dirty --registry crates-io

# Publish all Rust crates to crates.io in dependency order.
#
# We publish per-crate (not `cargo publish --workspace`) because path-dependency
# siblings must already be resolvable from the registry when the next crate is
# verified. `--config source.crates-io.replace-with=crates-io` disables any local
# crates.io mirror so the just-published sibling is found immediately on the real
# crates.io index (the default `cargo publish --workspace` flow breaks behind a
# mirror that lags the upload).
publish-rs:
  cargo publish --registry crates-io  -p longtrader-contract
  sleep 20
  cargo publish --registry crates-io  -p longtrader-proto
  sleep 20
  cargo publish --registry crates-io  -p longtrader-cli
  sleep 20
  cargo publish --registry crates-io  -p longtrader-worker

# Build sdist+wheel and upload to PyPI
publish-py: sdk-generate
  cd sdks/python && rm -rf dist && \
    pip install --quiet build twine && \
    python -m build && \
    twine upload dist/*

# Build and publish the TypeScript SDK to npm (npmjs.com)
publish-ts: sdk-generate
  cd sdks/typescript && npm install && npm run build && \
    npm publish --access public --registry https://registry.npmjs.org/

# Publish every language package (Rust -> PyPI -> npm)
publish-all: publish-rs publish-py publish-ts

# Check for Chinese characters
check-cn:
  rg --line-number --column "\p{Han}"

# Rust gate, mirroring the `rust` CI job: lint, test and build the workspace.
#
# `lint` folds in `proto-lint`, so this needs `buf` on PATH (the `rust` CI job
# installs it). The SDK gates live in their own recipes because each needs a
# different toolchain (Go, Python+grpcio-tools, Node) plus generated stubs.
# Keep them out of `ci` so the Rust job does not have to provision Node and
# Python just to re-run checks that the `go`, `python` and `typescript` jobs
# already run with the right toolchain.
ci: lint test build

# Full local gate: the Rust checks plus every SDK. Needs the Go, Python and
# Node toolchains and generated stubs (`just sdk-generate`) on PATH.
ci-all: ci sdk-lint sdk-test

# ============================================================
# Maintenance & Tools
# ============================================================

# Clean build artifacts
clean:
  cargo clean

# Install all required development tools
setup:
  cargo install cargo-mutants
  cargo install cargo-shear
  cargo install cargo-sort
  cargo install typos-cli
  cargo install rumdl

# Generate documentation for the workspace
docs:
  cargo doc --no-deps --open

# ---------------------------------------------------------------------------
# Proto contract & SDK toolchain (buf-managed; see proto/buf.yaml)
# ---------------------------------------------------------------------------

# Copy the canonical proto/ tree into the longtrader-contract vendored
# fallback (published crates compile from that copy).
proto-sync:
  scripts/sync-contract-proto.sh

# Lint the language-neutral contract tree under proto/
proto-lint:
  cd proto && buf lint

# Detect wire-breaking contract changes against a git ref.
#
# `ref` defaults to the local `main`. CI passes the PR base as
# `refs/remotes/origin/<base>`; a release job can pass a published tag
# (e.g. `refs/tags/v0.2.0`) to gate the release. A bare `just proto-breaking`
# is the local pre-push check.
#
# The `--against` URL is resolved relative to the recipe's working directory,
# which is `proto/` after the `cd`. A bare `.git#...` therefore points at
# `proto/.git`, which does not exist, and the gate fails with "does not appear
# to be a git repository" instead of reporting a real break. Use `../.git` and
# keep `subdir=proto` so the comparison still runs against the `proto/` subtree.
proto-breaking ref='refs/heads/main':
  cd proto && buf breaking --against "../.git#ref={{ref}},subdir=proto"

# Generate SDK stubs locally (Python -> sdks/python, TypeScript ->
# sdks/typescript) without network buf plugins; unavailable toolchains are
# skipped with a notice.
sdk-generate:
  bash scripts/gen-proto.sh

# Python SDK contract-conformance tests (offline; no server required)
sdk-test-py:
  cd sdks/python && python3 -m pytest tests -q

# TypeScript SDK tests via the Node built-in runner (offline)
sdk-test-ts:
  cd sdks/typescript && npm test

# Type-check the TypeScript SDK and vet the Go SDK without emitting
#
# Both SDKs are checked in place: the Go SDK is hand-written and stdlib-only,
# while the TypeScript SDK imports generated stubs from src/gen (gitignored).
# Type-checking without those stubs yields dozens of misleading "cannot find
# module/name" errors, so assert the prerequisites first and fail with an
# actionable message instead.
sdk-lint: sdk-lint-go
  bash scripts/check-sdk-deps.sh typescript
  cd sdks/typescript && npx tsc --noEmit -p tsconfig.json

# Go SDK lint: gofmt cleanliness plus `go vet` (stdlib only, no codegen)
sdk-lint-go:
  cd sdks/go && sh -c 'unformatted=$(gofmt -l .); [ -z "$unformatted" ] || { echo "not gofmt-ed:"; echo "$unformatted"; exit 1; }'
  cd sdks/go && go vet ./...

# All SDK tests
sdk-test: sdk-test-py sdk-test-ts sdk-test-go

# Go SDK tests (offline; the suite drives httptest servers only)
sdk-test-go:
  cd sdks/go && go test ./...
