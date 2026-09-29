#!/usr/bin/env bash
# Keep the vendored contract proto tree in lockstep with the canonical one.
#
# Canonical (authoring) tree:        proto/longtrader/**
# Vendored (published-crate) copy:   crates/longtrader-contract/proto/longtrader/**
#
# `crates/longtrader-contract/build.rs` prefers `<workspace>/proto` and only
# falls back to the vendored copy when the crate is packaged (crates.io / git
# checkout without the workspace root). The two trees MUST be identical or a
# published crate silently generates different wire types than local builds
# (e.g. the `stream.v1.CloseReason` renumber and the optional-`order_id`
# bracket fields have already drifted apart once).
#
# Usage:
#   scripts/sync-contract-proto.sh          # copy canonical -> vendored
#   scripts/sync-contract-proto.sh --check  # exit 1 if they differ (CI gate)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SRC="${ROOT}/proto/longtrader"
DST="${ROOT}/crates/longtrader-contract/proto/longtrader"

if [ ! -d "${SRC}" ]; then
  echo "error: canonical proto tree not found: ${SRC}" >&2
  exit 1
fi
mkdir -p "${DST}"

if [ "${1:-}" = "--check" ]; then
  if diff -rq "${SRC}" "${DST}" >/dev/null; then
    echo "contract proto trees are in sync"
    exit 0
  fi
  echo "error: contract proto trees have drifted:" >&2
  echo "  canonical: ${SRC}" >&2
  echo "  vendored:  ${DST}" >&2
  echo "Run \`scripts/sync-contract-proto.sh\` and commit the result." >&2
  diff -rq "${SRC}" "${DST}" >&2 || true
  exit 1
fi

rsync -a --delete "${SRC}/" "${DST}/"
diff -rq "${SRC}" "${DST}" >/dev/null
echo "synced ${DST} from ${SRC}"
