# Bare Protocol Guide — speaking LongTrader ConnectRPC without an SDK

This guide is for clients that do **not** use a generated SDK (Python
`longtrader-sdk`, TypeScript `@longcipher/longtrader-sdk`, or the Go scaffold).
It documents the exact wire contract so you can implement a client by hand with
any HTTP/1.1 or HTTP/2 stack. The contract under `proto/` (buf v2,
`longtrader.*.v1`) is the single source of truth.

> Everything here is also implemented for you in
> `bin/longtrader-worker/src/envelope.rs` (streaming framing) and
> `bin/longtrader-worker/src/session/mod.rs` (lease / kill-switch), and mirrored
> in the SDK session modules.

## 1. Transport & content types

| Call kind | Method | Request body | Response | Content-Type |
|---|---|---|---|---|
| Unary | `POST` | raw protobuf bytes | raw protobuf bytes (HTTP 200) | `application/proto` |
| Server-stream | `POST` | raw protobuf bytes | 5-byte envelope stream (§5) | `application/connect+proto` |

The base URL is the worker/terminal endpoint, e.g. `http://127.0.0.1:8080`.
Use TLS in production (the server transport is `hpx` + `rustls`).

## 2. Endpoint path pattern

Every RPC is reachable at:

```
POST /longtrader.<package>.v1.<Service>/<Method>
```

Concrete examples (package `longtrader.worker.v1`):

```
POST /longtrader.worker.v1.WorkerSessionService/AttachSession
POST /longtrader.worker.v1.WorkerSessionService/KeepAlive
POST /longtrader.worker.v1.WorkerSessionService/ReconcileState
POST /longtrader.worker.v1.WorkerSessionService/SetKillSwitchPolicy
POST /longtrader.worker.v1.WorkerSessionService/StreamStrategyEvents
```

And the trading / market surfaces:

```
POST /longtrader.trading.v1.TradingService/CreateOrders
POST /longtrader.trading.v1.TradingService/CancelOrder
POST /longtrader.market.v1.MarketDataService/FetchTicker
POST /longtrader.market.v1.MarketDataService/StreamMarketData
```

The full service set (from `proto/`): `trading.v1`, `market.v1`,
`worker.v1`, `stream.v1`, `data.v1`, `backtest.v1`, `ops.v1`,
`common.v1`.

## 3. Unary request / response shape

1. Serialize the request message to protobuf (no wrapper message).
2. `POST` to the path in §2 with header `content-type: application/proto`.
3. On success the server returns **HTTP 200** with the serialized protobuf
   response as the body.
4. On failure the server returns a **non-200** status with a Connect error
   JSON body — see §7.

Minimal Python sketch:

```python
import httpx, my_proto_stubs as pb

url = "http://127.0.0.1:8080/longtrader.worker.v1.WorkerSessionService/AttachSession"
req = pb.AttachSessionRequest(token="YOUR_TERMINAL_TOKEN",
                              client_name="bare-client", client_version="0.1.0")
resp = httpx.post(url, content=req.SerializeToString(),
                 headers={"content-type": "application/proto",
                          "authorization": "Bearer YOUR_TERMINAL_TOKEN"})
if resp.status_code != 200:
    raise RuntimeError(resp.text)          # Connect error JSON (§7)
out = pb.AttachSessionResponse()
out.ParseFromString(resp.content)
print(out.session_id, out.heartbeat_interval_ms)
```

## 4. Authentication

Send the terminal API token on every call:

```
Authorization: Bearer <token>
```

- The token is loaded by the worker from `api_token_file` only (trimmed, never
  logged) and compared with a **constant-time** `subtle` compare.
- The CLI reads the same token from the `--token` flag or the
  `$LONGTRADER_TOKEN` environment variable fallback.

## 5. Streaming envelope (5-byte framing)

Server-streaming RPCs (`StreamStrategyEvents`, `StreamMarketData`) emit a body
that is a **concatenation of envelopes**, each exactly:

```
byte 0      : flags
bytes 1..5  : u32 big-endian payload length (N)
bytes 5..5+N: payload
```

| flags | meaning | payload |
|---|---|---|
| `0x00` | message data | a serialized protobuf message |
| `0x02` | end-of-stream | a JSON object (e.g. `{"error":null}`) |

Hex walk-through — a single message envelope whose payload is the 11-byte ASCII
string `hello proto`:

```
00 00 00 00 0b 68 65 6c 6c 6f 20 70 72 6f 74 6f
│  │        │  └──────────── 11-byte payload ("hello proto") ────────────┘
│  └────────┴── u32 big-endian length = 0x0000000B = 11
└───────────── flags = 0x00 (message)
```

End-of-stream envelope whose payload is the 14-byte JSON `{"error":null}`
(`0x0E = 14`):

```
02 00 00 00 0e 7b 22 65 72 72 6f 72 22 3a 6e 75 6c 6c 7d
```

Envelopes are concatenated with no separator; decode front-to-back, advancing
by `5 + N` bytes each time (the worker's `decode_envelope` is the reference).

## 6. Resume token & replay cursor

Streams are resumable:

- `StreamMarketDataRequest.resume_token` (field 3) and
  `StreamStrategyEventsRequest.resume_token` (field 2) accept an opaque token
  from a previous stream's last event; empty starts fresh. The token is echoed
  on each event as `MarketDataEvent.resume_token` (field 6).
- For the session control plane, `ReconcileState` returns
  `ReconcileStateResponse` stamped with `snapshot_sequence` (a `uint64`
  watermark) plus `snapshot_time` and the atomic
  `balances` / `positions` / `open_orders` sets. Buffered deltas arriving after
  `snapshot_sequence` are replayed to converge — this avoids multi-RPC
  watermark races during recovery.
- Every streamed event carries an `EventHeader.sequence`. If
  `next != prev + 1` (a **gap**), treat it as a resync trigger: re-fetch a
  snapshot (orderbook) or re-run `ReconcileState` (private streams).

## 7. Decoding the error trail

A non-200 unary response carries a Connect error JSON body:

```json
{
  "code": "invalid_argument",
  "message": "insufficient balance for BTCUSDT",
  "details": [
    {
      "@type": "type.googleapis.com/longtrader.common.v1.ErrorDetail",
      "reason": "INSUFFICIENT_BALANCE",
      "domain": "longtrader.worker",
      "retryable": false,
      "kill_switch_recommended": false,
      "native_exchange_code": "BALANCE_TOO_LOW",
      "correlation_id": "cli-9f2a-..."
    }
  ]
}
```

`details` is a `repeated google.protobuf.Any`. The structured entry is
`longtrader.common.v1.ErrorDetail` (see `proto/longtrader/common/v1/errors.proto`):

| field | type | meaning |
|---|---|---|
| `reason` | string | stable machine code, e.g. `INSUFFICIENT_BALANCE` |
| `domain` | string | producing component, e.g. `longtrader.worker` |
| `retryable` | bool | whether a retry is safe |
| `retry_after` | Duration | backoff hint when `retryable` |
| `kill_switch_recommended` | bool | true for `AUTH_EXPIRED` / `SESSION_REVOKED` — cancel open orders before exit |
| `native_exchange_code` | string | verbatim venue error code/text for triage |
| `correlation_id` | string | echoes `client_order_id` / request id / trace id |

## 8. Minimal session lifecycle (no SDK)

```
AttachSession  -> { session_id, heartbeat_interval_ms }
KeepAlive       (every heartbeat_interval_ms; feeds the lease watchdog)
ReconcileState  -> { snapshot_sequence, balances, positions, open_orders }   # gate to ACTIVE
... trade via TradingService / MarketDataService ...
SetKillSwitchPolicy (optional; scope SESSION_ORDERS | ALL_ORDERS | NONE)
```

Orders submitted **before** `ReconcileState` succeeds are rejected with
`SYNC_IN_PROGRESS`. If the lease (default `3 × heartbeat_interval_ms`) expires
while `ACTIVE`, the configured `KillSwitchPolicy` scope is executed
(`KILL_SWITCH_TRIPPED`).

## Reference

- `proto/longtrader/worker/v1/worker.proto` — session RPCs & `KillSwitchPolicy`
- `proto/longtrader/common/v1/errors.proto` — `ErrorDetail`
- `bin/longtrader-worker/src/envelope.rs` — 5-byte `encode_envelope`/`decode_envelope`
- `bin/longtrader-worker/src/session/mod.rs` — lease, kill-switch, state machine
