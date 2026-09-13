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
lint:
  typos
  rumdl check .
  cargo sort -w -g -c
  cargo +nightly fmt --all -- --check
  RUSTC_WRAPPER= cargo +nightly clippy --all -- -D warnings
  cargo shear
  cargo workspace-inheritance-check

# Run tests
test:
  cargo test --all-features

# Run mutation tests with cargo-mutants
mutation:
  cargo mutants

# Run tests with coverage
test-coverage:
  cargo tarpaulin --all-features --workspace --timeout 300

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

# Full CI check
ci: lint test build

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

# Lint the language-neutral contract tree under proto/
proto-lint:
  cd proto && buf lint

# Detect wire-breaking contract changes against the main branch
proto-breaking:
  cd proto && buf breaking --against '.git#branch=main,subdir=proto'

# Generate SDK stubs locally (Python -> sdks/python, TypeScript ->
# sdks/typescript) without network buf plugins; unavailable toolchains are
# skipped with a notice.
sdk-generate:
  bash scripts/gen-proto.sh

# Placeholder: SDK test suite (no-op until the Tier-2 wrappers gain tests)
sdk-test:
  @echo "sdk-test: no-op placeholder; SDK tests arrive with Tier-2 hardening"
