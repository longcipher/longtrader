# contract

A hand-written, **dependency-free** protobuf codec for the longtrader contract
(`proto/longtrader/**`): wire primitives (varint, length-delimited, fixed32/64,
proto3 default omission, unknown-field skipping) plus typed messages for
`common.v1`, `worker.v1`, `trading.v1`, `market.v1` and `account.v1`.

This is what lets `sdks/go` build and test in a fresh checkout, with no
`protoc-gen-go`, no `google.golang.org/protobuf` dependency, and no
`buf generate` step. It is the authoritative Go reader/writer for the contract
in this repository: field numbers and types mirror the `.proto` files exactly.

`../gen/` holds optional `buf generate` output. It is gitignored and nothing
imports it.

## Why the decimal is hand-rolled

`common.v1.Decimal` is `{int64 unscaled = 1, int32 scale = 2, string
raw_str = 3}` and a **writer must populate all three**. The host's fast-path
decoder trusts the numeric pair, so a value sent as a bare `raw_str` arrives
as zero. `ParseDecimal` therefore derives `unscaled` and `scale` from the
literal and fills `raw_str` as well, and it errors on an int64 overflow rather
than wrapping the mantissa.

Readers must accept either representation (`Decimal.Float64` and
`Decimal.String` do), because producers choose: the Rust host uses the numeric
pair whenever the mantissa fits, and the mock venue never sets `raw_str`.
`Decimal.IsZero` follows the same precedence — `raw_str` first, the numeric
pair as fallback — so that a self-inconsistent message cannot report "zero" and
"5" from two different accessors.

## Contract coverage

This package encodes the services a strategy session actually calls:
`worker.v1` (session lifecycle, heartbeat, reconcile, strategy registration,
event stream, log), `trading.v1` orders and positions, `market.v1` market data,
plus the shared `common.v1` and `account.v1` types.

The Rust host exposes more ports than that — **funding, trigger orders, ledger
and transfer are not encoded here**. `../ports` deliberately mirrors the
narrower surface of the Python SDK, which is also limited to worker / trading /
market data, so the omission is a scope decision on record rather than an
oversight. Anything reaching for those messages through this package will not
find them; they need a `funding` / `trigger` / `ledger` / `transfer` service
added here (and the corresponding methods on `session.Session`) first.
