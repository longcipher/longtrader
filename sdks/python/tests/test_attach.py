"""Offline tests for ``Session.attach``.

``attach`` is the one place the SDK opens a long-lived ``httpx.Client``, so its
failure paths matter: a refused or unreachable handshake must close the client
it already opened rather than leak the connection pool. The transport is
replaced with ``httpx.MockTransport``; no server is involved.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from longtrader_sdk import session as sdk  # noqa: E402
from longtrader_sdk.session import Session  # noqa: E402


def _pb():
    return sdk._pb()


def _attach_reply() -> httpx.Response:
    pb = _pb()
    resp = pb.AttachSessionResponse(
        session_id="sess-1",
        heartbeat_interval_ms=500,
        capabilities=["reconcile", "kill_switch"],
    )
    return httpx.Response(200, content=resp.SerializeToString())


def _patch_client(monkeypatch, transport):
    """Bind ``transport`` into every ``httpx.Client`` the SDK constructs.

    The created clients are returned so a test can assert whether a failed
    attach closed the one it opened.
    """
    created: list[httpx.Client] = []
    real = httpx.Client

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        client = real(*args, **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(sdk.httpx, "Client", factory)
    return created


def test_a_successful_attach_populates_the_lease(monkeypatch):
    created = _patch_client(
        monkeypatch, httpx.MockTransport(lambda req: _attach_reply())
    )
    session = Session.attach("http://worker.local", "token")
    assert session.session_id == "sess-1"
    assert session.heartbeat_interval_ms == 500
    assert session.capabilities == ["reconcile", "kill_switch"]
    assert session.state == sdk.ATTACHED
    assert not session.can_trade
    assert created[0].is_closed is False, "a live session keeps its client"
    session.close()


def test_attach_trims_the_trailing_slash(monkeypatch):
    urls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        urls.append(str(req.url))
        return _attach_reply()

    _patch_client(monkeypatch, httpx.MockTransport(handler))
    session = Session.attach("http://worker.local/", "token")
    assert urls[0].endswith("/AttachSession")
    assert "//AttachSession" not in urls[0]
    session.close()


def test_the_token_and_client_identity_ride_in_the_body(monkeypatch):
    bodies: list[bytes] = []

    def handler(req: httpx.Request) -> httpx.Response:
        bodies.append(req.content)
        return _attach_reply()

    _patch_client(monkeypatch, httpx.MockTransport(handler))
    session = Session.attach("http://worker.local", "secret-token")
    req = _pb().AttachSessionRequest()
    req.ParseFromString(bodies[0])
    assert req.token == "secret-token"
    assert req.client_name == "longtrader-sdk-python"
    assert req.client_version
    session.close()


def test_a_session_id_is_resent_to_resume(monkeypatch):
    bodies: list[bytes] = []

    def handler(req: httpx.Request) -> httpx.Response:
        bodies.append(req.content)
        return _attach_reply()

    _patch_client(monkeypatch, httpx.MockTransport(handler))
    session = Session.attach("http://worker.local", "token", session_id="resume-me")
    req = _pb().AttachSessionRequest()
    req.ParseFromString(bodies[0])
    assert req.session_id == "resume-me"
    session.close()


@pytest.mark.parametrize(
    "reply",
    [
        httpx.Response(401, json={"code": "unauthenticated", "message": "bad token"}),
        httpx.Response(200, content=b"not a protobuf message"),
    ],
    ids=["refused", "undecodable"],
)
def test_a_failed_attach_closes_the_client_it_opened(monkeypatch, reply):
    created = _patch_client(monkeypatch, httpx.MockTransport(lambda req: reply))
    with pytest.raises(Exception):
        Session.attach("http://worker.local", "token")
    assert created, "attach must have constructed a client"
    assert created[0].is_closed, "a failed attach must not leak the connection pool"


def test_a_transport_failure_closes_the_client_it_opened(monkeypatch):
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    created = _patch_client(monkeypatch, httpx.MockTransport(handler))
    with pytest.raises(httpx.ConnectError):
        Session.attach("http://worker.local", "token")
    assert created[0].is_closed, "a failed attach must not leak the connection pool"
