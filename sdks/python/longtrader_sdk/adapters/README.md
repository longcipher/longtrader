# adapters

Thin by design: Connect-over-httpx inside `session.py`; mock adapter planned.
Mirrors `bin/longtrader-worker/src/adapters/` (daemon | terminal | mock).
Concept names: `Session`, `TradingPort`, `sync_state`, `OverflowPolicy` match
TypeScript/Go/Rust 1:1 (design doc §6.8).
