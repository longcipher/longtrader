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

`common.v1.Decimal` is one field: `string value = 1`, the number in base 10.
There is deliberately no numeric fast path beside it — a second representation
is what made "which one is authoritative?" a question every reader had to
answer, and it is why the retired int64 mantissa had to under-power the host
decimal's own 96-bit coefficient.

`ParseDecimal` validates a literal against the grammar the message documents and
carries it through verbatim. It rejects everything the host would reject: a
blank payload, a digit separator, an exponent, a leading `+`, a bare `.5` or
`1.`, surrounding whitespace, more than 28 fractional digits, and a coefficient
that does not fit 96 bits. Verbatim matters — the host's `1.100` and `1.1` are
different decimals, so re-rendering the text would change what the number means.

Reading is validated by the same rules. `Decimal.Validate` says why a payload
is unusable, and `Float64`, `Rat` and `IsZero` all reject a payload they cannot
read instead of answering `0`, because presence lives on the containing field: a
blank payload means the writer never populated it, which is a contract violation
and not a price of zero. There is no precedence left to get wrong, since there is
only one field to read.

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
