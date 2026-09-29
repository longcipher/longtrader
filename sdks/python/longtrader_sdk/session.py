"""Session: the single hand-written entry point of the Python SDK.

Mirrors the worker session semantics over bare Connect unary calls:
attach -> heartbeat -> reconcile -> trade. Generated stubs are imported
lazily so this module imports cleanly even before ``just sdk-generate``
has produced ``longtrader_sdk/proto``.

Wire format (see ../../docs/bare-protocol-guide.md):
  POST {base_url}/longtrader.worker.v1.WorkerSessionService/{Method}
  Content-Type: application/proto (unary) or application/connect+proto (streaming)
  Body: raw protobuf request; 200 body is the raw protobuf response.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional, Type

import httpx

# Imported eagerly (not via `longtrader_sdk`) so `session.py` stays importable
# as a standalone module in tests and tooling.
__version__ = "0.2.0"

SERVICE = "longtrader.worker.v1.WorkerSessionService"
TRADING_SERVICE = "longtrader.trading.v1.TradingService"
MARKET_SERVICE = "longtrader.market.v1.MarketDataService"

# Server-enforced lifecycle states (proto/longtrader/worker/v1/worker.proto).
DISCONNECTED = "DISCONNECTED"
ATTACHED = "ATTACHED"
SYNCING = "SYNCING"
ACTIVE = "ACTIVE"
KILL_SWITCH_TRIPPED = "KILL_SWITCH_TRIPPED"
GRACEFUL_SHUTDOWN = "GRACEFUL_SHUTDOWN"

#: States from which no further transition is possible.
TERMINAL_STATES = frozenset({KILL_SWITCH_TRIPPED, GRACEFUL_SHUTDOWN})

#: Stable reason the host returns when a session-scoped order is submitted
#: before the session reaches ACTIVE (see `SessionManager::authorize_order_submission`).
SYNC_IN_PROGRESS = "SYNC_IN_PROGRESS"


class ConnectError(Exception):
    """A Connect error reply: non-200 unary response with a JSON body."""

    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(f"{code}: {message} (http {status})")
        self.status = status
        self.code = code
        self.message = message
        self.details = details


def _pb() -> Any:
    # Lazy import: generated code exists only after `just sdk-generate`.
    from .proto.longtrader.worker.v1 import worker_pb2

    return worker_pb2


def _trading_pb() -> Any:
    from .proto.longtrader.trading.v1 import trading_pb2

    return trading_pb2


def _market_pb() -> Any:
    from .proto.longtrader.market.v1 import market_pb2

    return market_pb2


# -- Connect envelope framing -------------------------------------------------
#
# Server- and client-streaming RPCs use `application/connect+proto` with a
# 5-byte frame per message: [flags:1][u32 big-endian length:4][payload:N].
# flags 0x00 = message, 0x02 = end-of-stream (JSON). See
# docs/bare-protocol-guide.md and `bin/longtrader-worker/src/envelope.rs`.
FLAG_MESSAGE = 0x00
FLAG_END_OF_STREAM = 0x02


def _envelope(payload: bytes, flags: int = FLAG_MESSAGE) -> bytes:
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


def _decode_envelope(buf: bytes) -> bytes:
    """Return the payload of a single message frame, or raise ``ValueError``."""
    if len(buf) < 5:
        raise ValueError(f"truncated connect envelope: {len(buf)} bytes")
    if buf[0] & 0x02:
        # End-of-stream frame: its payload is a JSON end-stream message, which
        # carries no protobuf for the caller.
        raise ValueError("unexpected end-of-stream frame")
    return buf[5:]


def _first_message_payload(buf: bytes) -> bytes:
    """Return the first non-end-of-stream frame payload in ``buf``.

    A streaming reply that carries a single response message (client-streaming
    RPCs such as ``ReportLog``) is one message frame followed by an
    end-of-stream frame whose payload is JSON. Naively decoding the whole body
    as protobuf fails, so the data frame is located first.
    """
    pos = 0
    while pos + 5 <= len(buf):
        flags = buf[pos]
        length = int.from_bytes(buf[pos + 1 : pos + 5], "big")
        end = pos + 5 + length
        if end > len(buf):
            break
        if not flags & 0x02:
            return bytes(buf[pos + 5 : end])
        pos = end
    raise ValueError("no message frame in connect response")


def _iter_events(chunks, resp_type, event_attr: str | None = "event"):
    """Decode a Connect protobuf stream into messages, yielding ``None`` on gap.

    Frames may straddle chunk boundaries, so a running buffer is kept and the
    reader blocks until each frame is complete. A ``None`` yield marks a
    dropped sequence, which callers must treat as "resync required".
    """
    buf = bytearray()
    prev: int | None = None
    for chunk in chunks:
        buf.extend(chunk)
        while len(buf) >= 5:
            length = int.from_bytes(buf[1:5], "big")
            if len(buf) < 5 + length:
                break
            flags, payload = buf[0], bytes(buf[5 : 5 + length])
            del buf[: 5 + length]
            if flags & 0x02:
                return
            msg = resp_type()
            msg.ParseFromString(payload)
            if event_attr is not None and prev is not None:
                header = getattr(msg, "header", None)
                seq = getattr(header, "sequence", 0) if header is not None else 0
                if seq and prev and seq != prev + 1:
                    yield None
            if event_attr is not None:
                header = getattr(msg, "header", None)
                seq = getattr(header, "sequence", 0) if header is not None else 0
                if seq:
                    prev = seq
            yield msg


def _iter_stream_events(chunks, pb, event_attr: str | None = None):
    """Stream strategy events (gap-aware) from a `StreamStrategyEvents` reply."""
    return _iter_events(chunks, pb.StrategyEvent, event_attr="event")


def _log_event(pb, session_id: str, level: int, message: str, fields: dict | None):
    ev = pb.LogEvent(session_id=session_id, level=level, message=message)
    now = time.time()
    ev.timestamp.seconds = int(now)
    ev.timestamp.nanos = int((now - int(now)) * 1_000_000_000)
    for key, value in (fields or {}).items():
        ev.fields[key] = str(value)
    return ev


def _to_decimal(value: Any) -> Any:
    """Build a ``common.v1.Decimal`` from a Decimal / int / float / str.

    The contract carries decimals as ``{unscaled, scale, raw_str}``. Writing
    ``raw_str`` alone leaves ``unscaled``/``scale`` at zero, and the host's
    decoder trusts the numeric pair, so a value passed as a bare string would
    silently become zero on the wire. All three fields are therefore populated
    from a single exact ``Decimal``.
    """
    from decimal import Decimal, InvalidOperation

    from .proto.longtrader.common.v1 import types_pb2 as common_pb

    if isinstance(value, Decimal):
        dec = value
    elif isinstance(value, float):
        dec = Decimal(repr(value))
    elif isinstance(value, int):
        dec = Decimal(value)
    else:
        try:
            dec = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"cannot interpret {value!r} as a decimal") from exc
    if not dec.is_finite():
        raise ValueError(f"decimal must be finite, got {value!r}")
    sign, digits, exponent = dec.as_tuple()
    out = common_pb.Decimal()
    out.unscaled = int("".join(map(str, digits))) * (-1 if sign else 1)
    out.scale = -exponent
    out.raw_str = format(dec, "f")
    return out


def _exchange_id(exchange_id: str) -> Any:
    """Build the contract ``ExchangeId``; empty leaves the host default."""
    from .proto.longtrader.common.v1 import types_pb2 as common_pb

    if not exchange_id:
        return None
    return common_pb.ExchangeId(id=exchange_id)


def _pagination(limit: int = 0, since: int = 0, cursor: str = "") -> Any:
    """Build a ``common.v1.Pagination``; returns ``None`` when unconstrained."""
    from .proto.longtrader.common.v1 import types_pb2 as common_pb

    if not limit and not since and not cursor:
        return None
    return common_pb.Pagination(limit=limit, since=since, cursor=cursor)


def _order_request(
    pb,
    *,
    symbol: str,
    amount: Any,
    price: Any,
    side: str,
    order_type: str,
    time_in_force: str,
    client_order_id: str,
    post_only: bool,
    reduce_only: bool,
) -> Any:
    """Build a contract ``OrderRequest`` from string-named spec values."""
    req = pb.OrderRequest(
        client_order_id=client_order_id,
        symbol=symbol,
        type=_enum(pb, "OrderType", order_type, "OrderType"),
        side=_enum(pb, "OrderSide", side, "OrderSide"),
        amount=_to_decimal(amount),
        time_in_force=_enum(pb, "TimeInForce", time_in_force, "TimeInForce"),
        post_only=post_only,
        reduce_only=reduce_only,
    )
    if price is not None:
        req.price.CopyFrom(_to_decimal(price))
    return req


def _enum(module: Any, enum_name: str, value: str, kind: str) -> int:
    """Resolve ``"LIMIT"`` / ``"limit"`` against a generated proto enum.

    ``grpcio-tools`` exposes values two ways: as module-level constants
    (``ORDER_TYPE_LIMIT``) and, from protobuf 5, as attributes on the enum
    type itself. Either is accepted so the helper works across toolchain
    versions. A ``ValueError`` naming the valid options beats an
    ``AttributeError`` when a caller passes a typo.
    """
    # The contract's constant prefix is the SCREAMING_SNAKE form of the enum
    # type name (`OrderType` -> `ORDER_TYPE_`), not of the human label.
    prefix = _screaming_snake(enum_name) + "_"
    key = value.strip().upper()
    if not key.startswith(prefix):
        key = f"{prefix}{key}"

    wrapper = getattr(module, enum_name, None)
    # `values_by_name` is the portable spelling across protobuf runtimes and is
    # authoritative, so it is consulted before any `getattr` fallback.
    values_by_name = getattr(
        getattr(wrapper, "DESCRIPTOR", None), "values_by_name", None
    )
    if values_by_name:
        resolved = values_by_name.get(key)
        if resolved is not None:
            return int(resolved.number)
        options = sorted(n for n in values_by_name if n != prefix)
        raise ValueError(f"unknown {kind} {value!r}; expected one of {options}")

    for holder in (module, wrapper):
        if holder is not None and hasattr(holder, key):
            return int(getattr(holder, key))
    options = sorted(
        n
        for n in dir(module)
        if n.startswith(prefix) and n != prefix and isinstance(getattr(module, n), int)
    )
    raise ValueError(f"unknown {kind} {value!r}; expected one of {options}")


def _is_loopback(base_url: str) -> bool:
    """True when the URL targets the local machine.

    Used to default `trust_env` off for local endpoints: honouring an ambient
    `all_proxy` on a loopback URL breaks the connection outright.
    """
    from urllib.parse import urlparse

    host = (urlparse(base_url).hostname or "").strip("[]")
    return host in {"", "localhost", "::1"} or host.startswith("127.")


def _screaming_snake(name: str) -> str:
    """``OrderType`` -> ``ORDER_TYPE``; ``TimeInForce`` -> ``TIME_IN_FORCE``."""
    out: list[str] = []
    for index, char in enumerate(name):
        if char.isupper() and index > 0 and not name[index - 1].isupper():
            out.append("_")
        out.append(char.upper())
    return "".join(out)


def _stream_channel(pb, channel: str) -> int:
    return _enum(pb, "StreamChannel", channel, "StreamChannel")


def _error_from_response(resp: httpx.Response) -> ConnectError:
    try:
        body = resp.json()
        return ConnectError(
            resp.status_code,
            str(body.get("code", "unknown")),
            str(body.get("message", "")),
            body.get("details"),
        )
    except ValueError:
        return ConnectError(resp.status_code, "unknown", resp.text[:200])


class Session:
    """One attached strategy session against the worker control plane."""

    def __init__(
        self, base_url: str, timeout: float = 10.0, trust_env: bool | None = None
    ):
        self._base_url = base_url.rstrip("/")
        if trust_env is None:
            # LongTrader is local-first: a strategy host on loopback must not be
            # routed through an ambient HTTP(S)/SOCKS proxy just because one is
            # exported in the environment. Remote endpoints still work -- set
            # `trust_env=True` when the terminal genuinely sits behind a proxy.
            trust_env = not _is_loopback(self._base_url)
        self._http = httpx.Client(timeout=timeout, trust_env=trust_env)
        self._state = DISCONNECTED
        self.session_id = ""
        self.heartbeat_interval_ms = 0
        #: Capabilities the host advertised at attach (reconcile, kill_switch,
        #: event_replay, ...). Callers can feature-detect instead of guessing.
        self.capabilities: list[str] = []
        #: Watermark of the last `reconcile_state`, used to discard replayed
        #: deltas the snapshot already covers.
        self.snapshot_sequence = 0
        self._stop = threading.Event()
        self._watchdog_stop = threading.Event()
        self._heartbeat: Optional[threading.Thread] = None
        self._watchdog: Optional[threading.Thread] = None
        self._last_keepalive_ok = time.monotonic()

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def attach(
        cls,
        base_url: str,
        token: str,
        policy: Any = None,
        session_id: str = "",
        **kwargs: Any,
    ) -> "Session":
        """Validate the terminal API token and negotiate lease parameters.

        ``policy`` is an optional generated ``KillSwitchPolicy`` message;
        omit it to accept server defaults (scope SESSION_ORDERS,
        lease_timeout = 3x negotiated heartbeat interval).

        ``session_id`` resumes a previous session after a network drop: the
        host reuses it (refreshing the lease and re-admitting a non-terminal
        state) instead of issuing a new one, which is what keeps the
        kill-switch's tracked-order set intact across a reconnect. Passing the
        id of a session the host has already killed raises rather than
        silently creating a fresh session.
        """
        pb = _pb()
        req = pb.AttachSessionRequest(
            token=token,
            client_name="longtrader-sdk-python",
            client_version=__version__,
            session_id=session_id,
        )
        if policy is not None:
            req.policy.CopyFrom(policy)
        session = cls(base_url, **kwargs)
        resp = session._unary(
            "AttachSession", req.SerializeToString(), pb.AttachSessionResponse
        )
        session.session_id = resp.session_id
        # A resumed session keeps its negotiated lease rather than resetting to
        # 0, which would stall the heartbeat and watchdog.
        session.heartbeat_interval_ms = (
            resp.heartbeat_interval_ms or session.heartbeat_interval_ms
        )
        session.capabilities = list(resp.capabilities)
        session._last_keepalive_ok = time.monotonic()
        session._state = ATTACHED
        return session

    def keep_alive(self) -> Any:
        """Feed the lease watchdog; call at the negotiated interval."""
        pb = _pb()
        req = pb.KeepAliveRequest(
            session_id=self.session_id, client_time_ns=time.time_ns()
        )
        resp = self._unary("KeepAlive", req.SerializeToString(), pb.KeepAliveResponse)
        self._last_keepalive_ok = time.monotonic()
        return resp

    def reconcile_state(self) -> Any:
        """Fetch the authoritative atomic snapshot.

        Returns balances/positions/open_orders stamped with
        ``snapshot_sequence`` (the stream watermark at snapshot time).
        A successful reconcile is the local gate out of syncing; orders
        submitted before ACTIVE are rejected with reason SYNC_IN_PROGRESS.
        """
        pb = _pb()
        # The host drives ATTACHED -> SYNCING inside this call, so the local
        # view is updated first: a concurrent reader then sees SYNCING and
        # refuses to trade rather than racing ahead of the snapshot.
        self._state = SYNCING
        req = pb.ReconcileStateRequest(session_id=self.session_id)
        resp = self._unary(
            "ReconcileState", req.SerializeToString(), pb.ReconcileStateResponse
        )
        self._state = ACTIVE
        # Remember the watermark so a resumed event stream can drop the
        # deltas the snapshot already contains.
        self.snapshot_sequence = int(resp.snapshot_sequence)
        return resp

    @property
    def can_trade(self) -> bool:
        """True once the session has reconciled and the host considers it ACTIVE."""
        return self._state == ACTIVE

    def set_kill_switch_policy(self, policy: Any) -> None:
        """Parameterize cancel-on-disconnect behavior for this session."""
        pb = _pb()
        req = pb.SetKillSwitchPolicyRequest(session_id=self.session_id)
        req.policy.CopyFrom(policy)
        self._unary(
            "SetKillSwitchPolicy",
            req.SerializeToString(),
            pb.SetKillSwitchPolicyResponse,
        )

    @property
    def state(self) -> str:
        """Local view of the lifecycle state (ATTACHED/SYNCING/ACTIVE/...)."""
        return self._state

    # -- heartbeat ---------------------------------------------------------

    def start_heartbeat(self) -> None:
        """Spawn a daemon thread sending KeepAlive at the negotiated interval."""
        if self._heartbeat is not None:
            return
        interval = max(self.heartbeat_interval_ms / 1000.0, 0.5)

        def loop() -> None:
            while not self._stop.wait(interval):
                try:
                    self.keep_alive()
                except Exception:  # noqa: BLE001 - transient; retried next tick
                    pass

        self._stop.clear()
        self._heartbeat = threading.Thread(
            target=loop, name="longtrader-heartbeat", daemon=True
        )
        self._heartbeat.start()

    def stop(self) -> None:
        """Cancel the background heartbeat / watchdog threads."""
        self._stop.set()
        self._watchdog_stop.set()
        cur = threading.current_thread()
        if self._heartbeat is not None and cur is not self._heartbeat:
            self._heartbeat.join(timeout=2.0)
            self._heartbeat = None
        elif cur is self._heartbeat:
            self._heartbeat = None
        if self._watchdog is not None and cur is not self._watchdog:
            self._watchdog.join(timeout=2.0)
            self._watchdog = None
        elif cur is self._watchdog:
            self._watchdog = None

    def spawn_lease_watchdog(
        self, lease_timeout_ms: int | None = None
    ) -> threading.Thread:
        """Strategy-side lease timeout simulation.

        Spawns a timer that checks every `heartbeat_interval` whether the lease
        has timed out; on timeout it triggers cancel. Mirrors the Rust
        `spawn_strategy_lease_guard` helper (the L_session tier of the
        session/daemon/exchange watchdog). Wakes every `heartbeat_interval_ms`
        and checks elapsed time since the last `KeepAlive`; on `lease_timeout`
        (default 3x heartbeat) it stops trading locally and asks the host to
        cancel this session's orders.
        """
        lease_ms = (
            lease_timeout_ms
            if lease_timeout_ms is not None
            else max(self.heartbeat_interval_ms * 3, 1500)
        )
        interval = max(self.heartbeat_interval_ms / 1000.0, 0.5)
        self._watchdog_stop.clear()

        def watchdog() -> None:
            while not self._watchdog_stop.wait(interval):
                elapsed_ms = (time.monotonic() - self._last_keepalive_ok) * 1000
                if elapsed_ms > lease_ms:
                    # Stop trading immediately: the host may already have
                    # tripped its own kill-switch on lease expiry, and a
                    # strategy that keeps trading past its lease is exactly
                    # the failure this tier exists to prevent.
                    self._state = KILL_SWITCH_TRIPPED
                    # Then ask the host to cancel this session's orders. The
                    # RPC usually fails here (the lease is already gone, which
                    # is why we tripped), so it is best-effort; the host's
                    # watchdog is the authority for the actual cancellation.
                    try:
                        self.stop_strategy(cancel_open_orders=True)
                    except Exception:  # noqa: BLE001 - host already tripped
                        pass
                    break

        t = threading.Thread(
            target=watchdog, name="longtrader-lease-watchdog", daemon=True
        )
        self._watchdog = t
        t.start()
        return t

    @property
    def base_url(self) -> str:
        """Worker control-plane base URL (no trailing slash)."""
        return self._base_url

    def close(self) -> None:
        self.stop()
        self._http.close()

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _unary(
        self,
        method: str,
        req_bytes: bytes,
        resp_type: Type[Any],
        service: str = SERVICE,
    ) -> Any:
        """One Connect unary call: POST application/proto, parse proto reply."""
        url = f"{self._base_url}/{service}/{method}"
        resp = self._http.post(
            url, content=req_bytes, headers={"Content-Type": "application/proto"}
        )
        if resp.status_code != 200:
            raise _error_from_response(resp)
        msg = resp_type()
        msg.ParseFromString(resp.content)
        return msg

    # -- strategy lifecycle -------------------------------------------------

    def register_strategy(
        self, name: str, params: Optional[dict[str, str]] = None
    ) -> str:
        """Register this session's strategy; returns the assigned strategy id."""
        pb = _pb()
        req = pb.RegisterStrategyRequest(session_id=self.session_id, name=name)
        for key, value in (params or {}).items():
            req.params[key] = str(value)
        resp = self._unary(
            "RegisterStrategy",
            req.SerializeToString(),
            pb.RegisterStrategyResponse,
        )
        return resp.strategy_id

    def strategy_status(self) -> Any:
        """Current lifecycle state, strategy id, and submission counters."""
        pb = _pb()
        req = pb.StrategyStatusRequest(session_id=self.session_id)
        resp = self._unary(
            "StrategyStatus", req.SerializeToString(), pb.StrategyStatusResponse
        )
        # The host is authoritative; mirror its state so a kill-switch trip or
        # a server-side stop is visible locally instead of being reported ACTIVE.
        name = pb.SessionState.Name(resp.state)
        if name != "SESSION_STATE_UNSPECIFIED":
            self._state = name.removeprefix("SESSION_STATE_")
        return resp

    def stop_strategy(self, cancel_open_orders: bool = True) -> Any:
        """Stop the strategy, optionally cancelling the session's open orders."""
        pb = _pb()
        req = pb.StopStrategyRequest(
            session_id=self.session_id, cancel_open_orders=cancel_open_orders
        )
        resp = self._unary(
            "StopStrategy", req.SerializeToString(), pb.StopStrategyResponse
        )
        name = pb.SessionState.Name(resp.final_state)
        if name != "SESSION_STATE_UNSPECIFIED":
            self._state = name.removeprefix("SESSION_STATE_")
        return resp

    def report_log(
        self, level: str, message: str, fields: Optional[dict[str, str]] = None
    ) -> int:
        """Send one log event; returns the number of events the host accepted.

        Uses the Connect client-streaming framing: each message is a 5-byte
        envelope (``flags:1`` + big-endian ``u32`` length) followed by the
        protobuf payload, and the single reply is an enveloped
        ``ReportLogResponse``.
        """
        pb = _pb()
        levels = {
            "DEBUG": pb.LOG_LEVEL_DEBUG,
            "INFO": pb.LOG_LEVEL_INFO,
            "WARN": pb.LOG_LEVEL_WARN,
            "ERROR": pb.LOG_LEVEL_ERROR,
        }
        if level not in levels:
            raise ValueError(
                f"unknown log level {level!r}; expected one of {sorted(levels)}"
            )
        events = [
            _log_event(pb, self.session_id, levels[level], message, fields),
        ]
        body = b"".join(_envelope(ev.SerializeToString()) for ev in events)
        url = f"{self._base_url}/{SERVICE}/ReportLog"
        resp = self._http.post(
            url,
            content=body,
            headers={
                "Content-Type": "application/connect+proto",
                "Connect-Protocol-Version": "1",
            },
        )
        if resp.status_code != 200:
            raise _error_from_response(resp)
        out = pb.ReportLogResponse()
        out.ParseFromString(_first_message_payload(resp.content))
        return out.accepted

    def stream_events(self, resume_token: str = ""):
        """Yield ``StrategyEvent`` messages, resuming after ``resume_token``.

        Server-streaming Connect: the response is a sequence of 5-byte-framed
        protobuf messages terminated by an end-of-stream frame. A yielded
        ``None`` marks a sequence gap, meaning the ring buffer overflowed and
        the caller must re-run :meth:`reconcile_state` instead of assuming the
        stream is continuous.
        """
        pb = _pb()
        req = pb.StreamStrategyEventsRequest(
            session_id=self.session_id, resume_token=resume_token
        )
        url = f"{self._base_url}/{SERVICE}/StreamStrategyEvents"
        with self._http.stream(
            "POST",
            url,
            content=_envelope(req.SerializeToString()),
            headers={
                "Content-Type": "application/connect+proto",
                "Connect-Protocol-Version": "1",
            },
            timeout=None,
        ) as resp:
            if resp.status_code != 200:
                raise _error_from_response(resp.read())
            yield from _iter_stream_events(resp.iter_bytes(), pb)

    # -- trading ------------------------------------------------------------

    def _require_active(self) -> None:
        if self._state != ACTIVE:
            raise ConnectError(
                412,
                "failed_precondition",
                f"{SYNC_IN_PROGRESS}: local session is {self._state}, not ACTIVE; "
                "call reconcile_state() first",
            )

    def create_order(
        self,
        symbol: str,
        amount: Any,
        *,
        price: Optional[Any] = None,
        side: str = "BUY",
        order_type: str = "LIMIT",
        time_in_force: str = "GTC",
        client_order_id: str = "",
        post_only: bool = False,
        reduce_only: bool = False,
        exchange_id: str = "",
    ) -> Any:
        """Place one order, attributed to this session.

        The ``session_id`` is what lets the host reject a pre-ACTIVE submission
        (``SYNC_IN_PROGRESS``) and record the order so the kill-switch can
        cancel it. Requires a reconciled (ACTIVE) session.
        """
        pb = _trading_pb()
        req = pb.CreateOrderRequest(
            exchange_id=_exchange_id(exchange_id),
            order=_order_request(
                pb,
                symbol=symbol,
                amount=amount,
                price=price,
                side=side,
                order_type=order_type,
                time_in_force=time_in_force,
                client_order_id=client_order_id,
                post_only=post_only,
                reduce_only=reduce_only,
            ),
            session_id=self.session_id,
        )
        self._require_active()
        resp = self._unary(
            "CreateOrder",
            req.SerializeToString(),
            pb.CreateOrderResponse,
            TRADING_SERVICE,
        )
        return resp.order

    def create_orders(self, orders: list[dict[str, Any]], exchange_id: str = "") -> Any:
        """Place a batch, attributed to this session.

        ``orders`` entries accept the same keys as :meth:`create_order` except
        ``session_id``. The gate is all-or-nothing: if the session is not
        ACTIVE the whole batch is rejected.
        """
        pb = _trading_pb()
        req = pb.CreateOrdersRequest(
            exchange_id=_exchange_id(exchange_id),
            session_id=self.session_id,
        )
        for spec in orders:
            req.orders.append(
                _order_request(
                    pb,
                    symbol=spec["symbol"],
                    amount=spec["amount"],
                    price=spec.get("price"),
                    side=spec.get("side", "BUY"),
                    order_type=spec.get("order_type", "LIMIT"),
                    time_in_force=spec.get("time_in_force", "GTC"),
                    client_order_id=spec.get("client_order_id", ""),
                    post_only=spec.get("post_only", False),
                    reduce_only=spec.get("reduce_only", False),
                )
            )
        self._require_active()
        return self._unary(
            "CreateOrders",
            req.SerializeToString(),
            pb.CreateOrdersResponse,
            TRADING_SERVICE,
        )

    def cancel_order(
        self, order_id: str, symbol: str = "", exchange_id: str = ""
    ) -> Any:
        """Cancel one order by venue order id."""
        pb = _trading_pb()
        req = pb.CancelOrderRequest(
            exchange_id=_exchange_id(exchange_id),
            order_id=order_id,
            symbol=symbol,
        )
        return self._unary(
            "CancelOrder",
            req.SerializeToString(),
            pb.CancelOrderResponse,
            TRADING_SERVICE,
        ).order

    def cancel_all_orders(self, symbol: str = "", exchange_id: str = "") -> Any:
        """Cancel every open order; empty ``symbol`` spans all symbols."""
        pb = _trading_pb()
        req = pb.CancelAllOrdersRequest(
            exchange_id=_exchange_id(exchange_id), symbol=symbol
        )
        return self._unary(
            "CancelAllOrders",
            req.SerializeToString(),
            pb.CancelAllOrdersResponse,
            TRADING_SERVICE,
        )

    def fetch_open_orders(
        self, symbol: str = "", limit: int = 0, exchange_id: str = ""
    ) -> Any:
        pb = _trading_pb()
        req = pb.FetchOpenOrdersRequest(
            exchange_id=_exchange_id(exchange_id),
            symbol=symbol,
            pagination=_pagination(limit=limit),
        )
        return self._unary(
            "FetchOpenOrders",
            req.SerializeToString(),
            pb.FetchOpenOrdersResponse,
            TRADING_SERVICE,
        ).orders

    def get_account(self, exchange_id: str = "") -> Any:
        pb = _trading_pb()
        req = pb.GetAccountRequest(exchange_id=_exchange_id(exchange_id))
        return self._unary(
            "GetAccount",
            req.SerializeToString(),
            pb.GetAccountResponse,
            TRADING_SERVICE,
        )

    def get_positions(
        self, symbols: Optional[list[str]] = None, exchange_id: str = ""
    ) -> Any:
        """Open positions; omit ``symbols`` for every symbol."""
        pb = _trading_pb()
        req = pb.GetPositionsRequest(
            exchange_id=_exchange_id(exchange_id), symbols=list(symbols or [])
        )
        return self._unary(
            "GetPositions",
            req.SerializeToString(),
            pb.GetPositionsResponse,
            TRADING_SERVICE,
        ).positions

    def get_order_history(
        self, limit: int = 100, since: int = 0, exchange_id: str = ""
    ) -> Any:
        pb = _trading_pb()
        req = pb.GetOrderHistoryRequest(
            exchange_id=_exchange_id(exchange_id),
            pagination=_pagination(limit=limit, since=since),
        )
        return self._unary(
            "GetOrderHistory",
            req.SerializeToString(),
            pb.GetOrderHistoryResponse,
            TRADING_SERVICE,
        )

    def get_closed_positions(self, limit: int = 100, exchange_id: str = "") -> Any:
        pb = _trading_pb()
        req = pb.GetClosedPositionsRequest(
            exchange_id=_exchange_id(exchange_id), pagination=_pagination(limit=limit)
        )
        return self._unary(
            "GetClosedPositions",
            req.SerializeToString(),
            pb.GetClosedPositionsResponse,
            TRADING_SERVICE,
        )

    def close_position(self, position_id: str, exchange_id: str = "") -> Any:
        pb = _trading_pb()
        req = pb.ClosePositionRequest(
            exchange_id=_exchange_id(exchange_id), position_id=position_id
        )
        return self._unary(
            "ClosePosition",
            req.SerializeToString(),
            pb.ClosePositionResponse,
            TRADING_SERVICE,
        )

    def close_all_positions(self, exchange_id: str = "") -> Any:
        pb = _trading_pb()
        req = pb.CloseAllPositionsRequest(exchange_id=_exchange_id(exchange_id))
        return self._unary(
            "CloseAllPositions",
            req.SerializeToString(),
            pb.CloseAllPositionsResponse,
            TRADING_SERVICE,
        )

    def modify_position(
        self,
        position_id: str,
        *,
        take_profit: Any = None,
        stop_loss: Any = None,
        exchange_id: str = "",
    ) -> Any:
        """Attach or replace a position's bracket orders.

        Both brackets are optional in the contract: omitting one leaves it
        unchanged, so a caller can move just the stop without clearing the
        target.
        """
        pb = _trading_pb()
        req = pb.ModifyPositionRequest(
            exchange_id=_exchange_id(exchange_id), position_id=position_id
        )
        if take_profit is not None:
            req.take_profit.CopyFrom(_to_decimal(take_profit))
        if stop_loss is not None:
            req.stop_loss.CopyFrom(_to_decimal(stop_loss))
        return self._unary(
            "ModifyPosition",
            req.SerializeToString(),
            pb.ModifyPositionResponse,
            TRADING_SERVICE,
        )

    # -- market data --------------------------------------------------------

    def fetch_ticker(self, symbol: str, exchange_id: str = "") -> Any:
        pb = _market_pb()
        req = pb.FetchTickerRequest(
            exchange_id=_exchange_id(exchange_id), symbol=symbol
        )
        return self._unary(
            "FetchTicker",
            req.SerializeToString(),
            pb.FetchTickerResponse,
            MARKET_SERVICE,
        ).ticker

    def fetch_order_book(
        self, symbol: str, depth: int = 10, exchange_id: str = ""
    ) -> Any:
        pb = _market_pb()
        req = pb.FetchOrderBookRequest(
            exchange_id=_exchange_id(exchange_id),
            symbol=symbol,
            pagination=_pagination(limit=depth),
        )
        return self._unary(
            "FetchOrderBook",
            req.SerializeToString(),
            pb.FetchOrderBookResponse,
            MARKET_SERVICE,
        ).orderbook

    def get_candles(
        self,
        symbol: str,
        timeframe: str = "M1",
        limit: int = 100,
        exchange_id: str = "",
    ) -> Any:
        """OHLCV candles. ``timeframe`` is the contract's string form (M1, H4, ...)."""
        pb = _market_pb()
        req = pb.GetCandlesRequest(
            exchange_id=_exchange_id(exchange_id),
            symbol=symbol,
            timeframe=timeframe.strip().upper(),
            pagination=_pagination(limit=limit),
        )
        return self._unary(
            "GetCandles", req.SerializeToString(), pb.GetCandlesResponse, MARKET_SERVICE
        )

    def list_symbols(self, exchange_id: str = "") -> Any:
        pb = _market_pb()
        req = pb.ListSymbolsRequest(exchange_id=_exchange_id(exchange_id))
        return self._unary(
            "ListSymbols",
            req.SerializeToString(),
            pb.ListSymbolsResponse,
            MARKET_SERVICE,
        )

    def stream_market_data(self, symbols: list[str], channel: str = "TICKER") -> Any:
        """Open a server-streaming market data subscription.

        Yields ``MarketDataEvent`` messages decoded from the Connect envelope
        stream. ``channel`` is one of TICKER / ORDERBOOK / TRADES / OHLCV.
        """
        pb = _market_pb()
        req = pb.StreamMarketDataRequest(
            subscriptions=[
                pb.StreamSubscription(
                    symbol=symbol, channel=_stream_channel(pb, channel)
                )
                for symbol in symbols
            ]
        )
        url = f"{self._base_url}/{MARKET_SERVICE}/StreamMarketData"
        with self._http.stream(
            "POST",
            url,
            content=_envelope(req.SerializeToString()),
            headers={
                "Content-Type": "application/connect+proto",
                "Connect-Protocol-Version": "1",
            },
            timeout=None,
        ) as resp:
            if resp.status_code != 200:
                raise _error_from_response(resp.read())
            yield from _iter_stream_events(resp.iter_bytes(), pb, event_attr=None)
