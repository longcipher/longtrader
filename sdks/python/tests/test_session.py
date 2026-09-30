"""Offline contract-conformance tests for the hand-written Python SDK.

These run without a server. They assert the two classes of bug that unit
tests catch cheaply and integration tests catch late:

1. the generated stubs actually import (the vendored-proto package layout is
   easy to break and the failure mode is a first-call ``ModuleNotFoundError``);
2. every message the SDK sends decodes back, and carries the fields the host
   reads (notably the new ``trading.v1.*.session_id`` attribution field).
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from longtrader_sdk import session as sdk  # noqa: E402
from longtrader_sdk.session import Session  # noqa: E402


def test_generated_stubs_import():
    """Every generated package must import; this was a real shipping blocker.

    ``longtrader_sdk.proto`` is imported first on purpose: it is what installs
    the finder that makes the vendored ``longtrader.*`` tree importable, and a
    bare ``__import__("longtrader.trading.v1.trading_pb2")`` in a fresh
    interpreter is exactly the call that used to raise.
    """
    import longtrader_sdk.proto  # noqa: F401

    for name in (
        "longtrader.common.v1.types_pb2",
        "longtrader.market.v1.market_pb2",
        "longtrader.trading.v1.trading_pb2",
        "longtrader.stream.v1.stream_pb2",
        "longtrader.worker.v1.worker_pb2",
        "longtrader.terminal.v1.strategy_pb2",
        "longtrader.terminal.v1.runtime_pb2",
    ):
        __import__(name)


def test_proto_package_resolves_to_vendored_stubs():
    import longtrader
    import longtrader_sdk.proto  # noqa: F401

    paths = list(getattr(longtrader, "__path__", []))
    assert any("longtrader_sdk" in p for p in paths), paths


def test_preinstalled_longtrader_package_wins(monkeypatch):
    """A host app owning the name must not be shadowed by the vendored stubs."""
    import types

    import longtrader_sdk.proto as proto_pkg

    module = types.ModuleType("longtrader")
    monkeypatch.setitem(sys.modules, "longtrader", module)
    finder = proto_pkg._StubPackageFinder(proto_pkg._PROTO_ROOT)
    assert finder.find_spec("longtrader") is None


class TestDecimals:
    def test_decimal_populates_all_three_fields(self):
        d = sdk._to_decimal(Decimal("1.25"))
        assert d.unscaled == 125
        assert d.scale == 2
        assert d.raw_str == "1.25"

    def test_float_avoids_binary_representation(self):
        d = sdk._to_decimal(0.1)
        assert d.raw_str == "0.1"
        assert d.unscaled == 1
        assert d.scale == 1

    def test_string_input(self):
        d = sdk._to_decimal("90000")
        assert (d.unscaled, d.scale) == (90000, 0)

    def test_negative(self):
        d = sdk._to_decimal(Decimal("-0.5"))
        assert d.unscaled == -5
        assert d.scale == 1

    def test_zero(self):
        d = sdk._to_decimal(0)
        assert d.unscaled == 0

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError, match="decimal"):
            sdk._to_decimal("not-a-number")

    def test_rejects_non_finite(self):
        with pytest.raises(ValueError, match="finite"):
            sdk._to_decimal(float("nan"))


class TestEnums:
    def test_resolves_short_and_long_forms(self):
        pb = sdk._trading_pb()
        assert sdk._enum(pb, "OrderType", "LIMIT", "OrderType") == pb.ORDER_TYPE_LIMIT
        assert (
            sdk._enum(pb, "OrderType", "ORDER_TYPE_LIMIT", "OrderType")
            == pb.ORDER_TYPE_LIMIT
        )
        assert sdk._enum(pb, "OrderType", "limit", "OrderType") == pb.ORDER_TYPE_LIMIT

    def test_unknown_names_list_valid_options(self):
        pb = sdk._trading_pb()
        with pytest.raises(ValueError, match="ORDER_TYPE_LIMIT"):
            sdk._enum(pb, "OrderType", "LIMITT", "OrderType")

    def test_stream_channel(self):
        pb = sdk._market_pb()
        assert sdk._stream_channel(pb, "TICKER") == pb.STREAM_CHANNEL_TICKER
        assert sdk._stream_channel(pb, "orderbook") == pb.STREAM_CHANNEL_ORDERBOOK

    def test_screaming_snake_derives_prefix(self):
        assert sdk._screaming_snake("OrderType") == "ORDER_TYPE"
        assert sdk._screaming_snake("TimeInForce") == "TIME_IN_FORCE"
        assert sdk._screaming_snake("StreamChannel") == "STREAM_CHANNEL"


class TestEnvelope:
    def test_roundtrip(self):
        payload = b"\x08\x96\x01"
        frame = sdk._envelope(payload)
        assert len(frame) == 5 + len(payload)
        assert frame[0] == sdk.FLAG_MESSAGE
        assert int.from_bytes(frame[1:5], "big") == len(payload)
        assert sdk._decode_envelope(frame) == payload

    def test_end_of_stream_flag(self):
        assert sdk._envelope(b"", sdk.FLAG_END_OF_STREAM)[0] == sdk.FLAG_END_OF_STREAM

    def test_truncated_frame_raises(self):
        with pytest.raises(ValueError, match="truncated"):
            sdk._decode_envelope(b"\x00\x00\x00")

    def test_first_message_payload_skips_trailing_end_of_stream(self):
        """A streaming RPC replies with data frame + end-of-stream JSON frame.

        Decoding the whole body as protobuf fails on the trailing `{}`; the
        data frame has to be located first. This is the real ReportLog reply
        shape.
        """
        body = sdk._envelope(b"\x08\x01") + sdk._envelope(b"{}", sdk.FLAG_END_OF_STREAM)
        assert sdk._first_message_payload(body) == b"\x08\x01"

    def test_first_message_payload_raises_without_data_frame(self):
        with pytest.raises(ValueError, match="no message frame"):
            sdk._first_message_payload(sdk._envelope(b"{}", sdk.FLAG_END_OF_STREAM))

    def test_stream_decoder_handles_split_frames(self):
        """Frames may straddle chunk boundaries; the decoder must reassemble."""
        pb = sdk._pb()
        events = [
            pb.StrategyEvent(
                header=pb_common().EventHeader(sequence=n),
                resume_token=str(n),
            )
            for n in (1, 2, 3)
        ]
        wire = b"".join(sdk._envelope(e.SerializeToString()) for e in events)
        # Feed one byte at a time: the worst case for a naive reader.
        got = list(
            sdk._iter_events(
                [wire[i : i + 1] for i in range(len(wire))], pb.StrategyEvent
            )
        )
        assert [m.header.sequence for m in got] == [1, 2, 3]

    def test_stream_decoder_signals_gap(self):
        pb = sdk._pb()
        events = [
            pb.StrategyEvent(
                header=pb_common().EventHeader(sequence=n), resume_token=str(n)
            )
            for n in (1, 5)
        ]
        wire = b"".join(sdk._envelope(e.SerializeToString()) for e in events)
        got = list(sdk._iter_events([wire], pb.StrategyEvent))
        assert got[0].header.sequence == 1
        assert got[1] is None, "a sequence jump must be reported as a gap"
        assert got[2].header.sequence == 5

    def test_stream_decoder_stops_at_end_of_stream(self):
        pb = sdk._pb()
        ev = pb.StrategyEvent(header=pb_common().EventHeader(sequence=1))
        wire = sdk._envelope(ev.SerializeToString()) + sdk._envelope(
            b'{"error":null}', sdk.FLAG_END_OF_STREAM
        )
        got = list(sdk._iter_events([wire], pb.StrategyEvent))
        assert len(got) == 1


def pb_common():
    from longtrader.common.v1 import types_pb2

    return types_pb2


class TestPagination:
    def test_unconstrained_is_none(self):
        assert sdk._pagination() is None

    def test_limit_only(self):
        p = sdk._pagination(limit=100)
        assert p.limit == 100
        assert p.since == 0


class TestRequestConstruction:
    def test_create_order_carries_session_id(self):
        """The attribution field is the whole basis of kill-switch scoping."""
        pb = sdk._trading_pb()
        session_id = "sess-123"
        order = sdk._order_request(
            pb,
            symbol="BTC/USDT",
            amount=Decimal("0.001"),
            price=Decimal("95000"),
            side="BUY",
            order_type="LIMIT",
            time_in_force="GTC",
            client_order_id="grid-1",
            post_only=True,
            reduce_only=False,
        )
        req = pb.CreateOrderRequest(order=order, session_id=session_id)
        decoded = pb.CreateOrderRequest()
        decoded.ParseFromString(req.SerializeToString())
        assert decoded.session_id == "sess-123"
        assert decoded.order.client_order_id == "grid-1"
        assert decoded.order.amount.unscaled == 1
        assert decoded.order.amount.scale == 3
        assert decoded.order.price.unscaled == 95000
        assert decoded.order.post_only is True

    def test_market_order_omits_price(self):
        pb = sdk._trading_pb()
        order = sdk._order_request(
            pb,
            symbol="ETH/USDT",
            amount=1,
            price=None,
            side="SELL",
            order_type="MARKET",
            time_in_force="IOC",
            client_order_id="",
            post_only=False,
            reduce_only=False,
        )
        assert not order.HasField("price")
        assert order.type == pb.ORDER_TYPE_MARKET

    def test_exchange_id_empty_is_none(self):
        assert sdk._exchange_id("") is None
        assert sdk._exchange_id("mock").id == "mock"


def bare_session(state: str) -> Session:
    """A Session carrying only local state; the local gate needs no transport."""
    s = Session.__new__(Session)
    s.session_id = "sess-1"
    s._state = state
    return s


class TestSessionGate:
    def test_trading_before_active_raises_locally(self):
        with pytest.raises(sdk.ConnectError, match="SYNC_IN_PROGRESS"):
            bare_session(sdk.ATTACHED).create_order("BTC/USDT", 1)

    def test_trading_while_syncing_raises(self):
        with pytest.raises(sdk.ConnectError, match="SYNC_IN_PROGRESS"):
            bare_session(sdk.SYNCING).create_order("BTC/USDT", 1)

    def test_trading_while_kill_switch_tripped_raises(self):
        """A tripped session must never place an order, not even locally."""
        with pytest.raises(sdk.ConnectError, match="SYNC_IN_PROGRESS"):
            bare_session(sdk.KILL_SWITCH_TRIPPED).create_order("BTC/USDT", 1)

    def test_trading_after_graceful_shutdown_raises(self):
        with pytest.raises(sdk.ConnectError, match="SYNC_IN_PROGRESS"):
            bare_session(sdk.GRACEFUL_SHUTDOWN).create_order("BTC/USDT", 1)

    def test_batch_is_also_gated(self):
        """The gate is all-or-nothing for a batch, not just single orders."""
        with pytest.raises(sdk.ConnectError, match="SYNC_IN_PROGRESS"):
            bare_session(sdk.ATTACHED).create_orders(
                [{"symbol": "BTC/USDT", "amount": 1}]
            )

    def test_active_may_trade(self):
        bare_session(sdk.ACTIVE)._require_active()  # must not raise


class TestStateModel:
    def test_terminal_states_defined(self):
        assert sdk.KILL_SWITCH_TRIPPED in sdk.TERMINAL_STATES
        assert sdk.GRACEFUL_SHUTDOWN in sdk.TERMINAL_STATES
        assert sdk.ACTIVE not in sdk.TERMINAL_STATES

    def test_sync_in_progress_reason_exported(self):
        assert sdk.SYNC_IN_PROGRESS == "SYNC_IN_PROGRESS"

    def test_can_trade_reflects_state(self):
        assert bare_session(sdk.ACTIVE).can_trade
        for state in (
            sdk.DISCONNECTED,
            sdk.ATTACHED,
            sdk.SYNCING,
            sdk.KILL_SWITCH_TRIPPED,
            sdk.GRACEFUL_SHUTDOWN,
        ):
            assert not bare_session(state).can_trade, state


class TestReconcileOrdering:
    def test_reconcile_enters_syncing_before_the_rpc(self):
        """A reader during the snapshot fetch must not see ACTIVE."""
        s = bare_session(sdk.ATTACHED)
        seen: list[str] = []

        def fake_unary(method, req_bytes, resp_type, service=sdk.SERVICE):
            seen.append(s._state)
            return resp_type()

        s._unary = fake_unary
        s.reconcile_state()
        assert seen == [sdk.SYNCING], seen
        assert s._state == sdk.ACTIVE

    def test_reconcile_records_snapshot_watermark(self):
        s = bare_session(sdk.ATTACHED)

        def fake_unary(method, req_bytes, resp_type, service=sdk.SERVICE):
            resp = resp_type()
            resp.snapshot_sequence = 4242
            return resp

        s._unary = fake_unary
        snap = s.reconcile_state()
        assert snap.snapshot_sequence == 4242
        assert s.snapshot_sequence == 4242


class TestBaseUrlAccessor:
    def test_public_accessor(self, monkeypatch):
        # No real client: this asserts the accessor, not the transport. The
        # ambient environment may have proxy vars set, which httpx would try
        # to honour when constructing a transport.
        for var in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            monkeypatch.delenv(var, raising=False)
        s = Session("http://127.0.0.1:8810/")
        try:
            assert s.base_url == "http://127.0.0.1:8810"
        finally:
            s._http.close()


class TestAttribution:
    """The SDK must stamp its session id on every order-submitting request.

    Without it the host cannot gate a pre-ACTIVE submission and cannot record
    the order for `SCOPE_SESSION_ORDERS`, so the kill-switch silently becomes a
    no-op. These assert the field is populated, not merely accepted.
    """

    def _submitted(self, monkeypatch, state: str) -> tuple[bytes, bytes]:
        s = bare_session(state)
        captured: list[tuple[bytes, bytes]] = []

        def fake_unary(method, req_bytes, resp_type, service=sdk.TRADING_SERVICE):
            captured.append((method, req_bytes))
            return resp_type()

        monkeypatch.setattr(s, "_unary", fake_unary)
        s.create_order("BTC/USDT", Decimal("1"), price=Decimal("95000"))
        return captured[0]

    def test_create_order_stamps_session_id(self, monkeypatch):
        method, req = self._submitted(monkeypatch, sdk.ACTIVE)
        assert method == "CreateOrder"
        pb = sdk._trading_pb()
        msg = pb.CreateOrderRequest()
        msg.ParseFromString(req)
        assert msg.session_id == "sess-1", "order would be unattributed on the wire"

    def test_create_orders_stamps_session_id(self, monkeypatch):
        s = bare_session(sdk.ACTIVE)
        captured: list[tuple[str, bytes]] = []

        def fake_unary(method, req_bytes, resp_type, service=sdk.TRADING_SERVICE):
            captured.append((method, req_bytes))
            return resp_type()

        monkeypatch.setattr(s, "_unary", fake_unary)
        s.create_orders([{"symbol": "BTC/USDT", "amount": 1, "price": 100}])
        method, req = captured[0]
        assert method == "CreateOrders"
        msg = sdk._trading_pb().CreateOrdersRequest()
        msg.ParseFromString(req)
        assert msg.session_id == "sess-1"
        assert len(msg.orders) == 1

    def test_unscoped_reads_carry_no_session(self, monkeypatch):
        """Reads must not claim a session; only submissions are attributed."""
        s = bare_session(sdk.ACTIVE)
        captured: list[tuple[str, bytes]] = []
        monkeypatch.setattr(
            s,
            "_unary",
            lambda method, req_bytes, resp_type, service=sdk.TRADING_SERVICE: (
                captured.append((method, req_bytes)),
                resp_type(),
            )[1],
        )
        s.get_positions()
        assert captured and captured[0][0] == "GetPositions"
        msg = sdk._trading_pb().GetPositionsRequest()
        msg.ParseFromString(captured[0][1])
        assert not msg.HasField("exchange_id"), "empty exchange_id must stay unset"

    def test_captured_request_uses_protobuf_not_json(self, monkeypatch):
        """A hand-rolled JSON body would be a silent wire incompatibility."""
        s = bare_session(sdk.ACTIVE)
        seen: list[object] = []
        monkeypatch.setattr(
            s,
            "_unary",
            lambda method, req_bytes, resp_type, service=sdk.TRADING_SERVICE: (
                seen.append(req_bytes),
                resp_type(),
            )[1],
        )
        s.create_order("BTC/USDT", 1)
        body = seen[0]
        # JSON would start with '{' (0x7b). Protobuf starts with a tag byte
        # for a length-delimited field; field 2 (`order`) is tag 0x12 here
        # because an unset `exchange_id` is not serialized at all.
        assert body[0] != 0x7B, "body must not be JSON"
        assert body[0] & 0x07 == 0x02, "expected a length-delimited field"
        pb = sdk._trading_pb().CreateOrderRequest()
        pb.ParseFromString(body)  # must not raise
        assert pb.order.symbol == "BTC/USDT"


class TestPorts:
    """The ports are the hexagonal seam; they must be usable, not decorative."""

    def test_session_adapters_satisfy_the_abstract_ports(self):
        from longtrader_sdk import (
            MarketPort,
            SessionMarketPort,
            SessionTradingPort,
            TradingPort,
        )

        s = bare_session(sdk.ACTIVE)
        assert isinstance(SessionTradingPort(s), TradingPort)
        assert isinstance(SessionMarketPort(s), MarketPort)

    def test_trading_port_delegates_through_the_session(self):
        from longtrader_sdk import SessionTradingPort

        s = bare_session(sdk.ACTIVE)
        calls: list[tuple] = []
        s.fetch_open_orders = lambda symbol="", limit=0: (
            calls.append(("fetch_open_orders", symbol, limit)) or []
        )
        s.cancel_all_orders = lambda symbol="": (
            calls.append(("cancel_all_orders", symbol)) or []
        )
        port = SessionTradingPort(s)
        port.fetch_open_orders("BTC/USDT", 5)
        port.cancel_all_orders()
        assert calls == [
            ("fetch_open_orders", "BTC/USDT", 5),
            ("cancel_all_orders", ""),
        ]

    def test_overflow_defaults_match_the_worker(self):
        from longtrader_sdk import (
            DEFAULT_OVERFLOW,
            OverflowPolicy,
            overflow_policy_for_channel,
        )

        assert overflow_policy_for_channel("TICKER") is OverflowPolicy.DROP_OLDEST
        assert overflow_policy_for_channel("ORDERBOOK") is OverflowPolicy.COALESCE
        assert overflow_policy_for_channel("ORDERS") is OverflowPolicy.BLOCK
        assert overflow_policy_for_channel("nonsense") is OverflowPolicy.DROP_OLDEST
        assert set(DEFAULT_OVERFLOW) >= {"ticker", "orderbook", "orders", "positions"}

    def test_sequence_gap_helper_matches_the_host_rule(self):
        from longtrader_sdk import is_sequence_gap

        assert is_sequence_gap(1, 2) is False
        assert is_sequence_gap(1, 5) is True
        assert is_sequence_gap(0, 5) is False
        assert is_sequence_gap(5, 0) is False


class TestPublicSurface:
    @pytest.mark.parametrize(
        "name",
        [
            # worker.v1 lifecycle
            "attach",
            "keep_alive",
            "reconcile_state",
            "set_kill_switch_policy",
            "register_strategy",
            "strategy_status",
            "stop_strategy",
            "report_log",
            "stream_events",
            # trading.v1
            "create_order",
            "create_orders",
            "cancel_order",
            "cancel_all_orders",
            "fetch_open_orders",
            "get_account",
            "get_positions",
            "get_order_history",
            "get_closed_positions",
            "close_position",
            "close_all_positions",
            "modify_position",
            # market.v1
            "fetch_ticker",
            "fetch_order_book",
            "get_candles",
            "list_symbols",
            "stream_market_data",
            # host plumbing
            "start_heartbeat",
            "spawn_lease_watchdog",
            "stop",
            "close",
        ],
    )
    def test_method_exists(self, name):
        assert callable(getattr(Session, name)), name
