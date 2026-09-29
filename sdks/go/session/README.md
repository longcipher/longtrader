# session

`Session` — the single hand-written entry point of the Go SDK, mirroring
`sdks/python/longtrader_sdk/session.py` 1:1.

```text
AttachSession -> KeepAlive (heartbeat) -> ReconcileState -> trade
```

- `Attach(ctx, baseURL, token, policy, sessionID)` validates the terminal API
  token and negotiates the lease; `sessionID` resumes a previous session after
  a network drop.
- Lifecycle: `DISCONNECTED`, `ATTACHED`, `SYNCING`, `ACTIVE`, plus the terminal
  `KILL_SWITCH_TRIPPED` / `GRACEFUL_SHUTDOWN` (`TerminalStates`,
  `IsTerminal`).
- Order submission is refused locally in every non-`ACTIVE` state with a
  `*ConnectError` carrying `SyncInProgress` (`SYNC_IN_PROGRESS`).
- `StartHeartbeat` and `SpawnLeaseWatchdog` are two independent lease tiers
  (the Python SDK runs them as two independent threads). Either can start first
  and neither suppresses the other; both stop on `Stop`/`Close` or on context
  cancellation. Each reports whether it started, and `LeaseWatchdog.Started` is
  the signal separating "finished" from "never ran".
- Every RPC that takes `...Option` honours it, including the four order methods
  (`CreateOrder`, `CreateOrders`, `CancelOrder`, `CancelAllOrders`) — with
  `WithExchangeID` the order travels to the same backend the strategy priced it
  from, which is not the host's default.
- Streaming replies carry their errors in a 200: Connect delivers a rejection as
  an end-of-stream error frame. `StreamEvents` reports that as a final
  `Event{Err: ...}` and `StreamMarketData` as a final
  `MarketDataEvent{Err: ...}`, so a refused subscription is never mistaken for a
  clean end of stream.
- `envelope.go` holds the exported Connect framing helpers (`Envelope`,
  `DecodeEnvelope`, `FirstMessagePayload`, `StreamDecoder`). The announced
  frame length is untrusted and bounded by `MaxFrameLength`; a larger claim is
  rejected with `ErrFrameTooLarge` instead of being buffered.
