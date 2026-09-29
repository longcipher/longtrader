#!/usr/bin/env bash
# Verify an SDK has the prerequisites its lint/test recipes need.
#
# The TypeScript SDK type-checks against generated stubs under
# `src/gen/` (gitignored, produced by `just sdk-generate`) and resolves
# `@bufbuild/protobuf` / `undici` / `@types/node` from `node_modules`.
# Running `tsc` without them does not fail cleanly: it reports every missing
# import plus every Node global (`console`, `process`, `setTimeout`, ...) as a
# type error, which reads like broken source rather than a missing setup step.
#
# This script turns that into one actionable message.
#
# Usage:
#   scripts/check-sdk-deps.sh typescript
set -euo pipefail

SDK="${1:-}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS_DIR="${ROOT}/sdks/typescript"

die() {
  echo "error: $1" >&2
  exit 1
}

case "${SDK}" in
  typescript)
    command -v node >/dev/null 2>&1 || die "node not found; install Node.js 20+ to lint the TypeScript SDK"
    command -v npm >/dev/null 2>&1 || die "npm not found; install Node.js 20+ to lint the TypeScript SDK"

    if [ ! -d "${TS_DIR}/node_modules" ]; then
      die "sdks/typescript/node_modules missing; run 'cd sdks/typescript && npm ci'"
    fi

    # The stubs are gitignored, so a fresh checkout must generate them first.
    if [ ! -d "${TS_DIR}/src/gen" ] ||
      [ -z "$(find "${TS_DIR}/src/gen" -name '*_pb.d.ts' -print -quit 2>/dev/null)" ]; then
      die "sdks/typescript/src/gen missing or empty; run 'just sdk-generate' (needs buf + @bufbuild/protoc-gen-es) to type-check the SDK"
    fi
    ;;
  *)
    die "unknown SDK '${SDK}' (expected: typescript)"
    ;;
esac
