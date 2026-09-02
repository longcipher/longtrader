# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| `main`  | yes       |
| tagged releases | yes (latest minor) |

## Reporting a Vulnerability

Do not open a public issue for security reports.

- GitHub Security Advisories: <https://github.com/longcipher/longtrader/security/advisories/new>
- Alternatively, email the maintainers listed in `CODEOWNERS` / `Cargo.toml` authors.

Include: affected version/commit, reproduction steps, impact, and whether the issue leaks secrets (tokens, private keys). We aim to acknowledge within 48h and to ship a fix or mitigation within 14 days.

## Handling of Secrets

- Never commit `api_token`, `api_key`, `private_key`, or `.env` files.
- `bin/longtrader-worker` loads the terminal token from `api_token_file` only (trimmed, never logged) and compares it with constant-time `subtle`. Set the file mode to `0600` and mount it via your secrets manager.
- `proto/longtrader/exchange/v1/exchange_daemon.proto` has been removed from this open-source release. Do not reintroduce venue credential fields into the public contract.

## Scope

This policy covers `proto/`, `crates/longtrader-contract`, `bin/longtrader-worker`, and `sdks/*`. The `exchange_daemon` legacy surface and sibling private repos are out of scope for this repo.
