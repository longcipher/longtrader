# examples

Runnable snippets, one per docs chapter.

- `grid_strategy.go` — a two-sided limit grid driven entirely through the SDK:
  attach → `RegisterStrategy` → heartbeat + lease watchdog → `ReconcileState`
  → loop { fetch mid, cancel stale rungs, batch place } →
  `StopStrategy(cancelOpenOrders=true)`.

```bash
cd sdks/go
go run ./examples --help
go run ./examples --base-url http://127.0.0.1:9000 --token "$LONGTRADER_TOKEN" --iterations 3
```

`grid_strategy_test.go` runs the whole lifecycle against an offline
`httptest` stand-in for the worker, so the example is executable without a
running daemon.

See `../../docs/bare-protocol-guide.md` for the raw protocol the SDK wraps.
