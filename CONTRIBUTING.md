# Contributing to LongTrader

## Quick start

```bash
just setup        # typos, rumdl, cargo-shear, cargo-sort, cargo-mutants
just check        # cargo check --all-targets --all-features
just lint         # typos + rumdl + cargo sort/fmt + clippy -D warnings + shear
just test         # cargo test --all-features
just build
```

Contract changes:

```bash
just proto-lint       # buf lint
just proto-breaking   # buf breaking vs main
just sdk-generate     # regenerates sdks/python, sdks/typescript, sdks/go stubs
```

## Workspace rules

- Root `[workspace.dependencies]` uses numeric versions only, no default features. Sub-crates use `workspace = true` for `version` / `edition` / shared deps. Add deps with `cargo add <crate> --workspace` / `cargo add <crate> -p <crate> --workspace`.
- Prefer `hpx` (rustls) over `reqwest`, `tokio` for async, `tracing` for logs, `scc`/`ArcSwap` for concurrent state.
- Error handling: `thiserror` in libraries, `eyre` in binaries.
- Run `cargo check` / `cargo test` sequentially (cargo file locks). Do not parallelize `cargo` invocations.

## Style

- `just format` before PR: `rumdl fmt` + `cargo sort -w -g` + `cargo +nightly fmt --all`.
- Clippy is `pedantic` + `nursery` with `-D warnings`. Fix warnings rather than allowing them.
- No `anyhow` / `log` / `reqwest` / `dashmap` on new code; use `eyre` / `tracing` / `hpx` / `scc`.

## Tests

- Co-locate unit tests with `#[cfg(test)]`.
- Use `proptest` for invariants inside the normal `cargo test` flow.
- Run `just mutation` and fix surviving mutants.
- Do not add `cargo-fuzz` / Criterion unless the crate parses hostile input or has a measured hot path.

## Commits & PRs

Use conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`). `git-cliff` generates the changelog from them. Keep PRs focused; do not mix unrelated changes.

## License

By contributing, you agree that your contributions are licensed under Apache-2.0 (see `LICENSE`).
