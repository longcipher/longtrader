"""Session: the single hand-written entry point of the Python SDK.

Mirrors the worker session semantics over bare Connect unary calls:
attach -> heartbeat -> reconcile -> trade. Generated stubs are imported
lazily so this module imports cleanly even before ``just sdk-generate``
has produced ``longtrader_sdk/proto``.

Wire format (see docs/bare-protocol-guide.md):
  POST {base_url}/longtrader.worker.v1.WorkerSessionService/{Method}
  Content-Type: application/proto
  Body: raw protobuf request; 200 body is the raw protobuf response.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional, Type

import httpx

SERVICE = "longtrader.worker.v1.WorkerSessionService"

# Server-enforced lifecycle states (proto/longtrader/worker/v1/worker.proto).
DISCONNECTED = "DISCONNECTED"
ATTACHED = "ATTACHED"
SYNCING = "SYNCING"
ACTIVE = "ACTIVE"


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

    def __init__(self, base_url: str, timeout: float = 10.0):
        self._base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=timeout)
        self._state = DISCONNECTED
        self.session_id = ""
        self.heartbeat_interval_ms = 0
        self._stop = threading.Event()
        self._watchdog_stop = threading.Event()
        self._heartbeat: Optional[threading.Thread] = None
        self._watchdog: Optional[threading.Thread] = None
        self._last_keepalive_ok = time.monotonic()

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def attach(cls, base_url: str, token: str, policy: Any = None, **kwargs: Any) -> "Session":
        """Validate the terminal API token and negotiate lease parameters.

        ``policy`` is an optional generated ``KillSwitchPolicy`` message;
        omit it to accept server defaults (scope SESSION_ORDERS,
        lease_timeout = 3x negotiated heartbeat interval).
        """
        pb = _pb()
        req = pb.AttachSessionRequest(
            token=token,
            client_name="longtrader-sdk-python",
            client_version="0.1.0",
        )
        if policy is not None:
            req.policy.CopyFrom(policy)
        session = cls(base_url, **kwargs)
        resp = session._unary("AttachSession", req.SerializeToString(), pb.AttachSessionResponse)
        session.session_id = resp.session_id
        session.heartbeat_interval_ms = resp.heartbeat_interval_ms
        session._last_keepalive_ok = time.monotonic()
        session._state = ATTACHED
        return session

    def keep_alive(self) -> Any:
        """Feed the lease watchdog; call at the negotiated interval."""
        pb = _pb()
        req = pb.KeepAliveRequest(session_id=self.session_id, client_time_ns=time.time_ns())
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
        req = pb.ReconcileStateRequest(session_id=self.session_id)
        resp = self._unary("ReconcileState", req.SerializeToString(), pb.ReconcileStateResponse)
        self._state = ACTIVE
        return resp

    def set_kill_switch_policy(self, policy: Any) -> None:
        """Parameterize cancel-on-disconnect behavior for this session."""
        pb = _pb()
        req = pb.SetKillSwitchPolicyRequest(session_id=self.session_id)
        req.policy.CopyFrom(policy)
        self._unary(
            "SetKillSwitchPolicy", req.SerializeToString(), pb.SetKillSwitchPolicyResponse
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
        self._heartbeat = threading.Thread(target=loop, name="longtrader-heartbeat", daemon=True)
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

    def spawn_lease_watchdog(self, lease_timeout_ms: int | None = None) -> threading.Thread:
        """Strategy-side lease timeout simulation: spawn 定时任务，每 heartbeat_interval 检查 lease 是否超时，超时则触发 cancel.

        Mirrors the Rust `spawn_strategy_lease_guard` helper (session/daemon/exchange 三级看门狗中的 L_session).
        Wakes every `heartbeat_interval_ms` and checks elapsed time since last `KeepAlive`;
        on `lease_timeout` (default 3x heartbeat) it closes the session locally.
        """
        lease_ms = lease_timeout_ms if lease_timeout_ms is not None else max(self.heartbeat_interval_ms * 3, 1500)
        interval = max(self.heartbeat_interval_ms / 1000.0, 0.5)
        self._watchdog_stop.clear()

        def watchdog() -> None:
            while not self._watchdog_stop.wait(interval):
                elapsed_ms = (time.monotonic() - self._last_keepalive_ok) * 1000
                if elapsed_ms > lease_ms:
                    # Strategy-side lease timeout detected; trigger cancel and stop.
                    try:
                        self.close()
                    except Exception:
                        pass
                    break

        t = threading.Thread(target=watchdog, name="longtrader-lease-watchdog", daemon=True)
        self._watchdog = t
        t.start()
        return t

    def close(self) -> None:
        self.stop()
        self._http.close()

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _unary(self, method: str, req_bytes: bytes, resp_type: Type[Any]) -> Any:
        """One Connect unary call: POST application/proto, parse proto reply."""
        url = f"{self._base_url}/{SERVICE}/{method}"
        resp = self._http.post(url, content=req_bytes, headers={"Content-Type": "application/proto"})
        if resp.status_code != 200:
            raise _error_from_response(resp)
        msg = resp_type()
        msg.ParseFromString(resp.content)
        return msg
