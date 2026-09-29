# adapters

Thin by design: Connect-over-http lives inside `../session` (see
`../session/envelope.go` for the framing), so there is nothing to add here
until a second backend needs one.

`SessionTradingPort` / `SessionMarketPort` in `../ports` are the adapter seam a
strategy sees; a mock or in-process adapter would implement the same two
interfaces.

Mirrors `bin/longtrader-worker/src/adapters/` (daemon | terminal | mock).
