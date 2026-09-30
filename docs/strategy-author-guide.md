# Strategy Author Guide

Write a *portable* strategy: depend only on the ports, never on a backend.

## 1. Implement against the ports

A strategy consumes `TradingGateway` and `MarketDataSource` (and, where noted,
the extended `FundingRateSource` / `VenueOpInvoker` / `WalletGateway` caps).
See `bin/longtrader-worker/src/ports.rs`. Examples:

```rust
use longtrader_worker::ports::{MarketDataSource, TradingGateway};

async fn tick(market: &dyn MarketDataSource, gw: &dyn TradingGateway) {
    let t = market.fetch_ticker(market::FetchTickerRequest { .. }).await.unwrap();
    let _ = gw.create_order(gw.create_limit(...)).await;
}
```

Never import an exchange SDK or a transport type — those live behind the
`adapters/` layer.

## 2. The `StrategyContext`

Strategies are built from a `StrategyContext` (config + the ports sourced from a
single backend adapter) — see `bin/longtrader-worker/src/strategies/mod.rs`:

```rust
pub struct StrategyContext {
    pub config: Config,
    pub gateway: Arc<dyn TradingGateway>,
    pub market: Arc<dyn MarketDataSource>,
    pub funding: Arc<dyn FundingRateSource>,
    pub triggers: Arc<dyn TriggerOrderGateway>,
    pub ops: Arc<dyn VenueOpInvoker>,
    pub wallet: Arc<dyn WalletGateway>,
}
```

All six ports are implemented by every in-tree backend (`mock` / `MockAdapter`,
and `api` / `terminal` / `RemoteAdapter` — the two remote backends converged
onto the same canonical services and now differ only in endpoint and
credentials), so any strategy can run against any of them. A backend that
genuinely cannot serve one — a venue with no funding feed, say — returns
`PortError::Unsupported`, which the control
plane surfaces to a remote strategy as the Connect code `unimplemented`. Treat
that as permanent: it is the answer "this venue has no such endpoint", not a
transient failure, so retrying cannot help.

Note that `PortError` distinguishes `NotFound` ("this venue has no funding for
*this* contract") from `Unsupported` ("this venue has no funding at all"). The
difference is actionable — one is a wrong instrument, the other a wrong
backend — so do not collapse them.

**Silent-default rule.** These ports exist because a plausible-looking wrong
value is worse than an error. Concretely, an adapter must never:

- default a missing or unparsable decimal to `0`;
- default an `optional` the venue left unset to a zero measurement (an absent
  `mark_price` means "unreported", not "mark is zero");
- default an unknown enum discriminant to its first variant (defaulting a
  conditional order's side to `Buy` would silently invert a stop);
- infer an unsigned ledger row's sign from its magnitude — take the sign from
  `direction`, so a withdrawal cannot read as a deposit;
- report an unmodelled capability as an empty success. Empty and
  "unavailable" are different answers, and a strategy will act on the
  difference.

## 3. How `build_strategy` resolves `strategy.type`

`build_strategy(ctx)` looks the configured `strategy.type` up in the
**registry** (`registry()`), a `&'static [StrategyDescriptor]` of
`{ name, factory }`. The factory is a pure function of `StrategyContext`. To add
a strategy:

1. Add `pub mod your_strategy;` to `strategies/mod.rs`.
2. Append one `StrategyDescriptor { name: "your_strategy", factory: |ctx| … }`
   entry to `registry()`.
3. Implement `Strategy` (`async fn run(&self) -> Result<()>`).

No string `match` to edit — open/closed.

## 4. Parameter parsing

Parameters come from the `[strategy.params]` TOML table. Define a `Config`
struct derived from `serde::Deserialize` and parse via `from_params` (which calls
`params_from_table`):

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct MyConfig {
    pub symbol: String,
    #[serde(default = "default_exchange")]   // "mock" if omitted
    pub exchange_id: String,
    #[serde(default)]                         // optional field
    pub label: String,
}
impl MyConfig {
    pub fn from_params(t: &toml::Table) -> Result<Self> {
        params_from_table(t)
    }
}
```

Use `#[serde(default)]` for optional fields and `#[serde(default = "fn")]` for
typed defaults so configs stay forgiving.

## 5. Persisting state — `StateStore`

For crash recovery, persist working state as JSON via `StateStore`
(`bin/longtrader-worker/src/state_store.rs`). It saves atomically
(write-to-temp + `fsync` + rename) and loads `Ok(None)` when absent:

```rust
let store = StateStore::new("/var/lib/longtrader/worker/state.json");
store.save(&serde_json::json!({ "levels": [1,2,3] }))?;
let loaded = store.load()?;   // Option<Value>
```

## 6. Ship a `README.md`

Every strategy under `bin/longtrader-worker/src/strategies/<name>/` ships its own
`README.md` (and `README.zh.md`) documenting parameters, risk notes, and an
example `config.toml`. The top-level README links to them.

## 7. Tiny grid example (pseudocode)

```
cfg   = MyConfig::from_params(params)         # [strategy.params]
mid   = market.fetch_ticker(symbol).last       # MarketDataSource
for i in 1..=levels:
    gw.create_order(limit(symbol, BUY,  mid*(1-step*i), qty))
    gw.create_order(limit(symbol, SELL, mid*(1+step*i), qty))
loop every refresh_secs:
    mid = market.fetch_ticker(symbol).last
    refresh_grid(mid)                          # cancel stale rungs, replace
```

A complete, runnable twin lives at
`sdks/python/examples/grid_strategy.py` and
`sdks/typescript/examples/grid_strategy.ts`.
