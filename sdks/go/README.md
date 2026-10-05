# longtrader-sdk (Go)

Tier-2 thin wrapper over the longtrader Connect contract. The SDK is
**dependency-free** — the whole module is stdlib only, so `go get` pulls in
nothing and `go build` works in a fresh checkout.

Everything a strategy needs is implemented:

| Area | Go |
|---|---|
| `contract/` | hand-written protobuf codec for `common.v1`, `worker.v1`, `trading.v1`, `market.v1`, `account.v1` (no `protoc-gen-go` needed) |
| `session/` | `Session` lifecycle over `net/http`: attach, heartbeat, lease watchdog, reconcile, register, stream, log, and the 12 trading / 5 market RPCs |
| `ports/` | `TradingPort`, `MarketPort`, `OverflowPolicy`, `IsSequenceGap`, plus `SessionTradingPort` / `SessionMarketPort` adapters |
| `examples/` | `grid_strategy.go` — a runnable two-sided grid driven entirely through the SDK |
| `gen/` | optional `buf generate` output, gitignored; nothing depends on it |

## Installation

```bash
go get github.com/longcipher/longtrader/sdks/go
```

**Requirements:** Go >= 1.23. No other modules — `go.mod` has no `require`
directives.

## Module path

`github.com/longcipher/longtrader/sdks/go`

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/longcipher/longtrader/sdks/go/contract"
	"github.com/longcipher/longtrader/sdks/go/session"
)

func main() {
	ctx := context.Background()
	s, err := session.Attach(ctx, "http://127.0.0.1:9000", os.Getenv("LONGTRADER_TOKEN"),
		&contract.KillSwitchPolicy{Scope: contract.KillSwitchScopeSessionOrders}, "")
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	id, _ := s.RegisterStrategy(ctx, "my_grid", map[string]string{"symbol": "BTC/USDT"})
	fmt.Println("strategy", id, "state", s.State())

	// Two independent lease tiers: the heartbeat feeds the host's watchdog, and
	// the local watchdog stops this process trading even when the host is gone.
	// Either may be started first, and Started says whether it actually ran.
	s.StartHeartbeat(ctx)
	watchdog := s.SpawnLeaseWatchdog(ctx, 0)
	if !watchdog.Started {
		log.Print("lease watchdog did not start; the heartbeat still guards the lease")
	}

	// The ATTACHED -> SYNCING -> ACTIVE gate. Nothing may be submitted before
	// this returns; the SDK also refuses locally, with SYNC_IN_PROGRESS.
	if _, err := s.ReconcileState(ctx); err != nil {
		log.Fatal(err)
	}

	price := "64000.25"
	order, err := s.CreateOrder(ctx, session.OrderSpec{
		Symbol:      "BTC/USDT",
		Amount:      "0.001",
		Price:       &price,
		Side:        contract.OrderSideBuy,
		Type:        contract.OrderTypeLimit,
		TimeInForce: contract.TimeInForceGTC,
		PostOnly:    true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("placed", order.ID)

	// Let the host cancel exactly this session's resting orders.
	if _, err := s.StopStrategy(ctx, true); err != nil {
		log.Print(err)
	}
}
```

## Runnable example

```bash
go build -o /tmp/lt-grid ./examples
/tmp/lt-grid --help
/tmp/lt-grid --base-url http://127.0.0.1:9000 --token "$LONGTRADER_TOKEN" --iterations 3
```

The example is the lifecycle reference: attach → register → heartbeat +
watchdog → reconcile → loop { fetch mid, cancel stale rungs, batch place } →
`StopStrategy(cancelOpenOrders=true)`.

## Decimal handling

`common.v1.Decimal` is one field: `string value`, the number in base 10. There
is no numeric fast path beside it and no precedence rule, because a second
representation is what made "which one is authoritative?" a question every
reader had to answer.

```go
d, err := contract.ParseDecimal("1.25")          // d.Value == "1.25"
d, err := contract.ParseDecimal("0.001")         // d.Value == "0.001"
v, err := contract.Decimal{Value: "64000.25"}.Float64()
r, err := contract.Decimal{Value: "1.100"}.Rat() // exact; Float64 rounds
```

`ParseDecimal` is the validated constructor and is strict on purpose: a blank
payload, a digit separator (`1_000`), an exponent (`1e3`), a leading `+`, a
bare `.5` or `1.`, surrounding whitespace, more than 28 fractional digits, and
any value whose coefficient does not fit 96 bits are all errors — each one the
host would reject too. The payload is carried **verbatim**, trailing zeros
included, because the host's `1.100` and `1.1` are different decimals.

Reading obeys the same rules: `Validate` reports why a payload is unusable, and
`Float64`, `Rat` and `IsZero` all reject rather than answer `0`. That is what
makes a writer that never populated a field visible instead of silent —
presence lives on the containing field, so a blank payload is a contract
violation and never a zero.

`Rat` exists because the contract's range (a 96-bit coefficient over 28 decimal
places) is exact in decimal and only rounded in binary. Use it where the number
has to be exact and `Float64` where a rounded price is good enough.

## Lifecycle and the lease

`Session.State()` is the local view: `DISCONNECTED`, `ATTACHED`, `SYNCING`,
`ACTIVE`, and the two terminal states `KILL_SWITCH_TRIPPED` and
`GRACEFUL_SHUTDOWN` (exported as `session.TerminalStates`, checked with
`session.IsTerminal`).

- Order submission is refused locally in every non-`ACTIVE` state with a
  `*ConnectError` carrying `session.SyncInProgress` — the same reason the host
  would return, without the round trip.
- `ReconcileState` moves the local view to `SYNCING` **before** the RPC and to
  `ACTIVE` after, so a concurrent reader can never trade against a snapshot
  that has not arrived.
- `StartHeartbeat` sends `KeepAlive` at the negotiated interval and evaluates
  the lease on **every** tick, including after a failed heartbeat. On expiry it
  sets `KILL_SWITCH_TRIPPED` and asks the host to cancel this session's orders.
- `SpawnLeaseWatchdog` is the standalone tier (the Python SDK runs it as a
  second, independent thread) and can be used with or without the heartbeat.
  Either order works and neither suppresses the other; `Stop`/`Close` reaps
  both. Each call reports whether it started, and `LeaseWatchdog.Started` is
  the signal that distinguishes "finished" from "never ran" — `Done` is closed
  in both cases.

## Streaming

`StreamEvents` returns a channel of `session.Event`. A `Gap` item marks a
sequence discontinuity — the host's ring buffer overflowed, so re-run
`ReconcileState` rather than assuming continuity. Connect reports a *rejected*
subscription as HTTP 200 whose only frame is an end-of-stream error, so that
arrives as a final item with `Err` set; without it, a refusal and a clean end of
stream would be indistinguishable:

```go
for ev := range events {
	if ev.Err != nil {
		return fmt.Errorf("event stream: %w", ev.Err)
	}
	if ev.Gap {
		_, _ = s.ReconcileState(ctx)
		continue
	}
	handle(ev.Message)
}
```

`StreamMarketData` follows the same shape with `session.MarketDataEvent`
(`Event` / `Err`).

## Wire notes

- Unary calls: `POST {base}/{service}/{Method}` with
  `Content-Type: application/proto`; errors arrive as non-200 JSON bodies and
  are surfaced as `*ConnectError`.
- Streaming uses `application/connect+proto` with 5-byte envelopes
  (`[flags:1][u32 big-endian length:4][payload:N]`, flag `0x02` =
  end-of-stream). `session.Envelope`, `session.DecodeEnvelope`,
  `session.FirstMessagePayload` and `session.StreamDecoder` are exported; a
  single-message reply (such as `ReportLog`) is one data frame followed by an
  end-of-stream JSON frame, and the data frame is the one that is decoded.
  The announced frame length is untrusted, so it is bounded by
  `session.MaxFrameLength` (64 MiB) and an over-long claim is rejected with
  `ErrFrameTooLarge` rather than buffered.
- Connect reports streaming *errors* as HTTP 200 plus an end-of-stream frame
  holding the JSON error, so the status code alone never tells the whole story;
  a 200 with an error end-of-stream is surfaced as a `*ConnectError`.
- Only genuine server-streaming RPCs are sent through the client without a
  `Timeout`. Every finite-body call, including the Connect-framed `ReportLog`,
  is bounded by `unaryTimeout` (10s), so a wedged host cannot block a caller
  indefinitely.
- Auth is the terminal API token, sent as `AttachSessionRequest.token` (the
  field the host reads) and as an `Authorization: Bearer` header for
  intermediaries.
- `Attach` takes an optional `sessionID` to resume a previous session after a
  network drop, which keeps the kill-switch's tracked-order set intact.
- `OverflowPolicy` governs event-queue behavior under slow consumers
  (`DropOldest` preserves newest, `Coalesce` is last-writer-wins, `Block`
  applies backpressure); sequence gaps require snapshot resync/reconcile.

## Development

```bash
cd sdks/go
gofmt -l .        # must be empty
go vet ./...
go build ./...
go test ./...
go test -race ./...
```

All offline: the suite drives `net/http/httptest` servers and a hand-written
protobuf codec, and needs no worker process and no codegen step.

## Optional generated stubs

`buf generate --template proto/buf.gen.yaml` writes `protoc-gen-go` output to
`gen/`. It is gitignored and **nothing in the SDK imports it** — `contract/`
encodes the same wire format by hand so the module stays dependency-free. The
`managed.override.go_package_prefix` entry in `proto/buf.gen.yaml` is what
makes that generation work at all: Go has no default in buf managed mode, so
without it `protoc-gen-go` fails and aborts the whole `buf generate` run.

## License

Apache-2.0 — see the repository `LICENSE`.
