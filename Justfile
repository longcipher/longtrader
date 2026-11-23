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
  cargo workspace-inheritance-check
  scripts/sync-contract-proto.sh --check
  just check-mutation-scope

# Run the prek commit hooks on every tracked file (requires prek)
hooks:
  prek run --all-files

# Run tests
test:
  cargo test --all-features

# Run the divan benchmark suites (contract codecs + worker indicators)
bench:
  RUSTC_WRAPPER= cargo bench -p longtrader-contract -p longtrader-worker

# Mutation testing is intentionally absent from the default loop and from CI.
#
# Whole-workspace mutation runs are PROHIBITED here: a `cargo mutants` run over
# the whole workspace takes hours and rarely finishes, so it is never a gate and
# never a completion check. Only the lines the current change touched are
# mutated, within the /pb-build budget: at most 3 files, 60 mutants, 300 s.
#
#   just mutation                            # every changed .rs file
#   just mutation crates/core/src/parser.rs  # one specific file
#
# The budget is verified before the run starts, so an over-large change fails
# immediately instead of grinding for hours. A surviving mutant is a test
# failure: strengthen the assertion, or record an exemption in
# `.cargo/mutants.toml` under `exclude_re` with a one-line reason.
#
# Mutate the lines this change touched, within the /pb-build budget.
mutation *args:
  #!/usr/bin/env bash
  set -euo pipefail
  max_files=3
  max_mutants=60
  max_seconds=300

  base=""
  upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)
  for ref in "$upstream" origin/main origin/master main master; do
    [ -n "$ref" ] || continue
    if git rev-parse --verify --quiet "$ref" >/dev/null 2>&1; then
      base=$(git merge-base HEAD "$ref" 2>/dev/null || true)
      [ -n "$base" ] && break
    fi
  done
  [ -n "$base" ] || base=HEAD

  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  if [ "$#" -gt 0 ]; then
    printf '%s\n' "$@" | sed '/^[[:space:]]*$/d' > "$work/files"
  else
    git diff --name-only --diff-filter=ACMR "$base" -- '*.rs' | sort -u > "$work/files"
  fi
  file_count=$(grep -c . "$work/files" || true)
  if [ "$file_count" -eq 0 ]; then
    echo "error: no changed Rust files against $base; there is nothing to mutate." >&2
    echo "       Whole-workspace mutation is prohibited: it takes hours and rarely finishes." >&2
    exit 2
  fi
  if [ "$file_count" -gt "$max_files" ]; then
    echo "error: $file_count files exceed the ${max_files}-file mutation budget." >&2
    echo "       Mutate fewer files at a time; never raise the budget, never run the workspace." >&2
    exit 3
  fi

  : > "$work/diff"
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    git diff --no-ext-diff --no-textconv "$base" -- "$file" >> "$work/diff" || true
  done < "$work/files"
  if ! grep -q '^diff --git ' "$work/diff"; then
    echo "error: the diff against $base is empty; there is nothing to mutate." >&2
    exit 2
  fi
  RUSTC_WRAPPER= cargo mutants --workspace --in-diff "$work/diff" --list --no-times > "$work/list" 2>&1 \
    || { cat "$work/list" >&2; echo "error: cargo-mutants could not list the mutants." >&2; exit 1; }
  mutant_count=$(grep -Ec ':[0-9]+:[0-9]+:' "$work/list" || true)
  if [ "$mutant_count" -gt "$max_mutants" ]; then
    echo "error: $mutant_count mutants exceed the ${max_mutants}-mutant budget." >&2
    echo "       Narrow the change; never raise the budget, never fall back to a workspace run." >&2
    exit 3
  fi
  if [ "$mutant_count" -eq 0 ]; then
    echo "ok: no mutants in the changed lines."
    exit 0
  fi

  echo "mutating $mutant_count mutant(s) in $file_count file(s) (budget: $max_mutants mutants / ${max_seconds}s)"
  if command -v timeout >/dev/null 2>&1; then
    timeout "$max_seconds" env RUSTC_WRAPPER= cargo mutants --workspace --in-diff "$work/diff" --timeout-multiplier 2 --no-times
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$max_seconds" env RUSTC_WRAPPER= cargo mutants --workspace --in-diff "$work/diff" --timeout-multiplier 2 --no-times
  else
    echo "warning: neither 'timeout' nor 'gtimeout' is available; the ${max_seconds}s wall-clock cap is not enforced." >&2
    echo "         Install coreutils ('brew install coreutils') to get 'gtimeout'." >&2
    env RUSTC_WRAPPER= cargo mutants --workspace --in-diff "$work/diff" --timeout-multiplier 2 --no-times
  fi

# Fail if an unbounded mutation command reappears in a build or CI file.
# Whole-workspace mutation runs take hours and rarely finish, so they are
# prohibited: the only permitted forms are a diff-scoped (`--in-diff`), a
# file-scoped (`--file`), or a branch-scoped (`--since`) run.
#
# Check that no build or CI file regains an unbounded mutation command.
check-mutation-scope:
  #!/usr/bin/env bash
  set -euo pipefail
  status=0
  for file in Justfile justfile Makefile makefile .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -f "$file" ] || continue
    offenders=$(grep -nE 'cargo[[:space:]]+mutants' "$file" \
      | grep -vE '^[0-9]+:[[:space:]]*#' \
      | grep -vE -- '--in-diff|--file|--list|--since|-f[[:space:]]' \
      || true)
    if [ -n "$offenders" ]; then
      echo "error: $file runs an unbounded mutation command:" >&2
      printf '%s\n' "$offenders" >&2
      echo "       Whole-workspace mutation runs are prohibited. Use the diff-scoped 'just mutation'." >&2
      status=1
    fi
  done
  exit "$status"

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

# Publish every Rust crate to crates.io, in dependency order.
#
# We publish per-crate (not `cargo publish --workspace`) because path-dependency
# siblings must already be resolvable from the registry when the next crate is
# verified. The order below is the crate dependency order:
# contract -> catalog -> proto -> cli -> worker (worker depends on all three
# libraries, so it goes last). `sleep 20` lets the index propagate between
# publishes. `--registry crates-io` disables any local crates.io mirror so the
# just-published sibling is found immediately on the real index (a mirror that
# lags the upload otherwise breaks the next publish).
publish-rs:
  cargo publish --registry crates-io -p longtrader-contract
  sleep 20
  cargo publish --registry crates-io -p longtrader-catalog
  sleep 20
  cargo publish --registry crates-io -p longtrader-proto
  sleep 20
  cargo publish --registry crates-io -p longtrader-cli
  sleep 20
  cargo publish --registry crates-io -p longtrader-worker

# Build sdist+wheel and upload to PyPI.
#
# `twine upload` authenticates from `TWINE_USERNAME`/`TWINE_PASSWORD`; in CI set
# the password to a PyPI API token and the username to `__token__`. `sdk-generate`
# regenerates the Python protobuf stubs first (they are gitignored).
publish-py: sdk-generate
  cd sdks/python && rm -rf dist && \
    pip install --quiet build twine && \
    python -m build && \
    twine upload dist/*

# Build and publish the TypeScript SDK to npm (npmjs.com).
#
# Authenticates from `~/.npmrc` (the release job writes the
# `//registry.npmjs.org/:_authToken` entry from `NPM_TOKEN`). `sdk-generate`
# regenerates the TS stubs first; `prepublishOnly` runs `npm run build`.
publish-ts: sdk-generate
  cd sdks/typescript && npm install && npm run build && \
    npm publish --access public --registry https://registry.npmjs.org/

# Go SDK release gate.
#
# Go modules are consumed directly from the git tag (module path
# github.com/longcipher/longtrader/sdks/go): there is no registry to upload to,
# so "releasing" Go means the vX.Y.Z tag exists and the module still builds,
# vets and tests cleanly at that commit. The release job runs this; there is no
# Go upload step.
publish-go:
  cd sdks/go && go build ./...
  cd sdks/go && go vet ./...
  cd sdks/go && go test ./...

# Fail unless the Cargo workspace carries the release version. The version
# defaults to the current tag (`v` stripped); pass one explicitly for a local
# dry run. Run by the release gate so a mismatched tag never ships.
#
# Only the Rust workspace is enforced: the tag IS the Rust version. The Python
# (`sdks/python/pyproject.toml`) and TypeScript (`sdks/typescript/package.json`)
# SDKs are versioned independently and published only when their own version is
# new to the registry — bump them only when the SDK or the contract actually
# changed. Their versions are printed here for visibility, never compared.
check-release-versions version='':
  #!/usr/bin/env bash
  set -euo pipefail
  if [ -z "{{version}}" ]; then
    version="$(git describe --tags --abbrev=0 | sed 's/^v//')"
  else
    version="{{version}}"
  fi
  echo "release version: $version"
  cargo_v="$(awk '$0=="[workspace.package]"{s=1;next} s&&$1=="version"{gsub(/"/,"",$3);print $3;exit} s&&/^\[/{exit}' Cargo.toml)"
  if [ "$cargo_v" != "$version" ]; then
    echo "error: Cargo.toml version ($cargo_v) != release version ($version)" >&2
    exit 1
  fi
  py_v="$(grep -m1 '^version' sdks/python/pyproject.toml | sed -E 's/.*"([0-9.]+)".*/\1/')"
  ts_v="$(grep -m1 '"version"' sdks/typescript/package.json | sed -E 's/.*"([0-9.]+)".*/\1/')"
  echo "info: python SDK version ($py_v) and typescript SDK version ($ts_v) are independent of the tag"
  echo "ok: Cargo.toml version matches $version"

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
