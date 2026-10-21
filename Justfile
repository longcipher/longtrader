# Default recipe to display help
default:
  @just --list

# Format all code: markdown, Cargo.toml ordering, rustfmt
format:
  rumdl fmt .
  cargo sort -w -g
  cargo +nightly fmt --all

# Auto-fix what `format` cannot: markdown lint and clippy
fix:
  rumdl check --fix .
  RUSTC_WRAPPER= cargo +nightly clippy --fix --all --allow-dirty

# Run all lints (the CI gate; includes the buf contract lint)
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

# Run tests with coverage
#
# The generated protobuf code is excluded: `build.rs` emits ~29k lines of it into
# the target dir, it is never exercised directly, and leaving it in swamps the
# denominator until the real coverage number is unreadable. This needs
# `cargo-tarpaulin >= 0.37` - an older vendored `llvm-profparser` cannot read the
# indexed-profile format the current toolchain's LLVM emits, and fails with
# "consistency check for reading counts failed".
# `--out Stdout` keeps the report in the terminal instead of writing a Cobertura
# file into the working directory. `.gitignore` covers `coverage.xml`, not
# `cobertura.xml`, so the default dropped an untracked ~900 KB artifact in the
# repo root that `git add -A` would have swept in.
test-coverage:
  cargo tarpaulin --all-features --workspace --timeout 300 --exclude-files '*/out/*' --out Stdout

# Build every fuzz target without running it
fuzz-build:
  cargo fuzz build --fuzz-dir fuzz --target "$(rustc -vV | sed -n 's/^host: //p')"

# Fuzz one target. Override the budget with `FUZZ_SECONDS=300 just fuzz envelope`.
fuzz target:
  cargo fuzz run --fuzz-dir fuzz --target "$(rustc -vV | sed -n 's/^host: //p')" {{target}} -- -max_total_time=${FUZZ_SECONDS:-60}

# Run every target briefly as a smoke test (this is the form CI uses)
fuzz-smoke: fuzz-build
  #!/usr/bin/env bash
  set -euo pipefail
  fuzz_target="$(rustc -vV | sed -n 's/^host: //p')"
  for target in envelope decimal_codec; do
    echo "::group::fuzz $target"
    cargo fuzz run --fuzz-dir fuzz "$target" --target "$fuzz_target" -- -runs=20000 || true
    echo "::endgroup::"
  done

# Build entire workspace
build:
  cargo build --workspace

# Check all targets compile (and that workspace inheritance is complete)
check:
  cargo check --all-targets --all-features
  cargo workspace-inheritance-check

# Publish all crates to crates.io (dry run)
publish-check:
  cargo publish --workspace --dry-run --allow-dirty --registry crates-io

# Publish all Rust crates to crates.io in dependency order
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

# Publish every language package (irreversible; prompts only on an interactive terminal)
publish:
  #!/usr/bin/env bash
  set -euo pipefail
  if [ -t 0 ]; then
    read -r -p "Type 'publish' to continue: " confirm
    if [ "$confirm" != "publish" ]; then
      echo "aborted"
      exit 1
    fi
  fi
  just publish-all

# Check for Chinese characters (bilingual READMEs and the CJK width fixtures are excluded)
check-cn:
  ! rg --line-number --column "\p{Han}" --glob '!**/README.zh.md' --glob '!README.md' --glob '!bin/longtrader-cli/src/output.rs'

# Full CI check (mirrors the `rust` CI job)
#
# `lint` folds in `proto-lint`, so this needs `buf` on PATH (the `rust` CI job
# installs it). The SDK gates live in their own recipes because each needs a
# different toolchain (Go, Python+grpcio-tools, Node) plus generated stubs.
# Keep them out of `ci` so the Rust job does not have to provision Node and
# Python just to re-run checks that the `go`, `python` and `typescript` jobs
# already run with the right toolchain.
ci: lint test build

# Full local gate: Rust checks plus every SDK lint and test
#
# Needs the Go, Python and Node toolchains and generated stubs
# (`just sdk-generate`) on PATH.
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
  cargo install cargo-workspace-inheritance-check
  cargo install typos-cli
  cargo install rumdl
  cargo install cargo-tarpaulin
  cargo install cargo-audit
  cargo install cargo-outdated
  cargo install cargo-nextest
  rustup component add miri --toolchain nightly

# Generate documentation for the workspace
docs:
  cargo doc --no-deps --open

# Run security audit
audit:
  cargo audit

# Check for outdated dependencies
outdated:
  cargo outdated

# Run clippy with the cargo lint group on top of the workspace lints
clippy-all:
  RUSTC_WRAPPER= cargo +nightly clippy --all --all-targets --all-features -- -W clippy::cargo

# Run tests with nextest
test-nextest:
  cargo nextest run --all-features

# Run miri on unsafe code
miri:
  cargo +nightly miri test --all-features

# Check for unused dependencies
unused-deps:
  cargo shear --check

# Update all dependencies
update-deps:
  cargo update

# Full check (lint + test + build + audit)
full-check: lint test build audit

# ============================================================
# Proto contract & SDK toolchain (buf-managed; see proto/buf.yaml)
# ============================================================

# Copy the canonical proto/ tree into the longtrader-contract vendored
# fallback (published crates compile from that copy).
proto-sync:
  scripts/sync-contract-proto.sh

# Lint the language-neutral contract tree under proto/
proto-lint:
  cd proto && buf lint

# Detect wire-breaking contract changes against a git ref
#
# `ref` defaults to the local `main`. CI passes the PR base as
# `refs/remotes/origin/<base>`; a release job can pass a published tag
# (e.g. `refs/tags/v0.2.0`) to gate the release. A bare `just proto-breaking`
# is the local pre-push check.
# The `--against` URL is resolved relative to the recipe's working directory,
# which is `proto/` after the `cd`. A bare `.git#...` therefore points at
# `proto/.git`, which does not exist, and the gate fails with "does not appear
# to be a git repository" instead of reporting a real break. Use `../.git` and
# keep `subdir=proto` so the comparison still runs against the `proto/` subtree.
proto-breaking ref='refs/heads/main':
  cd proto && buf breaking --against "../.git#ref={{ref}},subdir=proto"

# Generate SDK stubs locally (offline-safe, no network buf plugins)
#
# Generates TypeScript stubs (needs Node deps with @bufbuild/protoc-gen-es
# installed under sdks/typescript) and Python stubs (needs python3 +
# grpcio-tools). Steps whose toolchain is unavailable are skipped with a
# notice; the script never fails the whole run because of one missing toolchain.
sdk-generate:
  bash scripts/gen-proto.sh

# Python SDK contract-conformance tests (offline; no server required)
sdk-test-py:
  cd sdks/python && python3 -m pytest tests -q

# TypeScript SDK tests via the Node built-in runner (offline)
sdk-test-ts:
  cd sdks/typescript && npm test

# Python SDK format + lint check (ruff), mirroring the `python` CI job
sdk-lint-py:
  ruff format --check sdks/python
  ruff check --select E,F sdks/python

# TypeScript SDK format check (prettier) plus type-check
#
# The generated stubs under `src/gen/` are gitignored, so the dependency check
# runs first and fails with an actionable message instead of a wall of
# "cannot find module" errors from `tsc`.
sdk-lint-ts:
  bash scripts/check-sdk-deps.sh typescript
  cd sdks/typescript && npx prettier --check "src/**/*.ts" "examples/**/*.ts" "tests/**/*.ts"
  cd sdks/typescript && npx tsc --noEmit -p tsconfig.json

# Go SDK lint: gofmt cleanliness plus `go vet` (stdlib only, no codegen)
sdk-lint-go:
  cd sdks/go && sh -c 'unformatted=$(gofmt -l .); [ -z "$unformatted" ] || { echo "not gofmt-ed:"; echo "$unformatted"; exit 1; }'
  cd sdks/go && go vet ./...

# Build the Go SDK, mirroring the `go` CI job
sdk-build-go:
  cd sdks/go && go build ./...

# Lint every SDK (Go, Python, TypeScript)
sdk-lint: sdk-lint-go sdk-lint-py sdk-lint-ts

# All SDK tests
sdk-test: sdk-test-py sdk-test-ts sdk-test-go

# Go SDK tests (offline; the suite drives httptest servers only)
sdk-test-go:
  cd sdks/go && go test ./...

# Go SDK tests with the race detector, mirroring the `go` CI job
sdk-test-go-race:
  cd sdks/go && go test -race ./...
