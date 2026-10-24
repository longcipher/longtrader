"""Offline tests for the hand-written ports layer.

`ports.py` is the hexagonal seam strategies program against, so its two jobs
are pinned here: the overflow table must match the worker's, and every port
method must delegate to the session with the arguments the session documents
-- no more, no fewer. No server is involved; the session is a recording stub.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from longtrader_sdk.ports import (  # noqa: E402
    DEFAULT_OVERFLOW,
    MarketPort,
    OverflowPolicy,
    SessionMarketPort,
    SessionTradingPort,
    TradingPort,
    _channel_name,
    is_sequence_gap,
    overflow_policy_for_channel,
)


class _StubSession:
    """A Session stand-in that records the calls the ports make on it."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def __getattr__(self, name: str):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return f"<{name}>"

        return call


# -- overflow policy table ------------------------------------------------------


def test_the_overflow_table_matches_the_worker_constants():
    """The worker's per-stream policies, exactly (design doc section 6.5)."""
    assert DEFAULT_OVERFLOW == {
        "ticker": OverflowPolicy.DROP_OLDEST,
        "trades": OverflowPolicy.DROP_OLDEST,
        "ohlcv": OverflowPolicy.DROP_OLDEST,
        "orderbook": OverflowPolicy.COALESCE,
        "orders": OverflowPolicy.BLOCK,
        "balances": OverflowPolicy.BLOCK,
        "positions": OverflowPolicy.BLOCK,
    }


@pytest.mark.parametrize(
    "channel,expected",
    [
        ("TICKER", OverflowPolicy.DROP_OLDEST),
        ("ticker", OverflowPolicy.DROP_OLDEST),
        ("  OrderBook  ", OverflowPolicy.COALESCE),
        ("TRADES", OverflowPolicy.DROP_OLDEST),
        ("OHLCV", OverflowPolicy.DROP_OLDEST),
    ],
)
def test_channel_lookup_is_case_and_whitespace_insensitive(channel, expected):
    assert overflow_policy_for_channel(channel) is expected


@pytest.mark.parametrize("channel", ["", "unknown", "orders", "balances", "positions"])
def test_an_unknown_channel_fails_closed_to_block(channel):
    """A dropping policy on a private stream would lose an order update."""
    assert overflow_policy_for_channel(channel) is OverflowPolicy.BLOCK


# -- sequence gap rule ----------------------------------------------------------


@pytest.mark.parametrize(
    "prev,next_,expected",
    [
        (0, 1, False),  # 0 is a valid watermark
        (1, 2, False),
        (41, 42, False),
        (1, 3, True),
        (5, 5, True),
        (2, 1, True),
        (0, 0, False),  # unset next is never a gap
        (2**64 - 1, 0, False),  # u64 wrap is contiguous
    ],
)
def test_the_gap_rule_matches_the_worker(prev, next_, expected):
    assert is_sequence_gap(prev, next_) is expected


# -- port contracts -------------------------------------------------------------


def test_the_abstract_ports_cannot_be_instantiated():
    with pytest.raises(TypeError):
        TradingPort()  # type: ignore[abstract]
    with pytest.raises(TypeError):
        MarketPort()  # type: ignore[abstract]


def test_the_trading_port_declares_the_whole_trading_surface():
    expected = {
        "create_order",
        "batch_create_orders",
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
        "sync_state",
    }
    assert set(TradingPort.__abstractmethods__) == expected


def test_the_market_port_declares_the_whole_market_surface():
    expected = {
        "fetch_ticker",
        "fetch_order_book",
        "get_candles",
        "list_symbols",
        "subscribe_market_data",
    }
    assert set(MarketPort.__abstractmethods__) == expected


# -- trading port delegation ------------------------------------------------------


def test_create_order_unpacks_a_mapping_spec():
    session = _StubSession()
    SessionTradingPort(session).create_order(
        {"symbol": "BTC/USDT", "amount": "0.5", "price": "100"}
    )
    assert session.calls == [
        (
            "create_order",
            (),
            {
                "symbol": "BTC/USDT",
                "amount": "0.5",
                "price": "100",
            },
        )
    ]


def test_create_order_unpacks_an_object_spec():
    session = _StubSession()
    spec = SimpleNamespace(symbol="BTC/USDT", amount=1)
    SessionTradingPort(session).create_order(spec)
    assert session.calls[0][0] == "create_order"
    assert session.calls[0][2] == {"symbol": "BTC/USDT", "amount": 1}


def test_symbol_defaults_to_empty_and_limit_defaults_to_zero():
    session = _StubSession()
    port = SessionTradingPort(session)
    port.cancel_order("o-1")
    port.fetch_open_orders()
    assert session.calls[0] == ("cancel_order", ("o-1", ""), {})
    assert session.calls[1] == ("fetch_open_orders", ("", 0), {})


def test_modify_position_forwards_bracket_kwargs():
    session = _StubSession()
    SessionTradingPort(session).modify_position(
        "pos-1", take_profit="105000", stop_loss="90000"
    )
    assert session.calls == [
        (
            "modify_position",
            ("pos-1",),
            {"take_profit": "105000", "stop_loss": "90000", "exchange_id": ""},
        )
    ]


def test_sync_state_maps_onto_reconcile_state():
    session = _StubSession()
    assert SessionTradingPort(session).sync_state() == "<reconcile_state>"
    assert session.calls == [("reconcile_state", (), {})]


def test_every_trading_method_delegates():
    session = _StubSession()
    port = SessionTradingPort(session)
    port.batch_create_orders([{"symbol": "BTC/USDT", "amount": 1}])
    port.cancel_all_orders("BTC/USDT")
    port.get_account("binance")
    port.get_positions(["BTC/USDT"], "binance")
    port.get_order_history(10, 5, "binance")
    port.get_closed_positions(20, "binance")
    port.close_position("pos-1", "binance")
    port.close_all_positions("binance")
    delegated = [name for name, _, _ in session.calls]
    assert delegated == [
        "create_orders",
        "cancel_all_orders",
        "get_account",
        "get_positions",
        "get_order_history",
        "get_closed_positions",
        "close_position",
        "close_all_positions",
    ]


# -- market port delegation -------------------------------------------------------


def test_market_unary_methods_delegate():
    session = _StubSession()
    port = SessionMarketPort(session)
    port.fetch_ticker("BTC/USDT")
    port.fetch_order_book("BTC/USDT", 5)
    port.get_candles("BTC/USDT", 1, 50)
    port.list_symbols()
    assert session.calls == [
        ("fetch_ticker", ("BTC/USDT",), {}),
        ("fetch_order_book", ("BTC/USDT", 5), {}),
        ("get_candles", ("BTC/USDT", 1, 50), {}),
        ("list_symbols", (), {}),
    ]


def test_subscribe_market_data_forwards_one_channel_per_call():
    """The port used to fold a per-symbol channel list onto the first channel.

    A TRADES symbol was then silently subscribed as ORDERBOOK. One channel per
    call (the Go SDK's shape) makes that unrepresentable.
    """
    session = _StubSession()
    SessionMarketPort(session).subscribe_market_data(
        ["BTC/USDT", "ETH/USDT"], "ORDERBOOK"
    )
    assert session.calls == [
        ("stream_market_data", (["BTC/USDT", "ETH/USDT"], "ORDERBOOK"), {})
    ]


def test_an_omitted_channel_subscribes_to_ticker():
    session = _StubSession()
    SessionMarketPort(session).subscribe_market_data(["BTC/USDT"])
    assert session.calls == [("stream_market_data", (["BTC/USDT"], "TICKER"), {})]


def test_a_numeric_channel_resolves_through_the_generated_constants():
    session = _StubSession()
    SessionMarketPort(session).subscribe_market_data(["BTC/USDT"], 2)
    assert session.calls == [("stream_market_data", (["BTC/USDT"], "ORDERBOOK"), {})]


@pytest.mark.parametrize("bad", [99, "sideways"])
def test_an_unknown_channel_raises_instead_of_defaulting(bad):
    session = _StubSession()
    with pytest.raises(ValueError, match="unknown StreamChannel"):
        SessionMarketPort(session).subscribe_market_data(["BTC/USDT"], bad)
    assert session.calls == [], "nothing may be subscribed for an unknown channel"


# -- channel name resolution -------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("TICKER", "TICKER"),
        ("orderbook", "ORDERBOOK"),
        (" Trades ", "TRADES"),
        ("OHLCV", "OHLCV"),
    ],
)
def test_channel_names_resolve_case_insensitively(value, expected):
    assert _channel_name(value) == expected


@pytest.mark.parametrize("bad", ["sideways", "", 99, None])
def test_channel_names_reject_anything_unknown(bad):
    with pytest.raises(ValueError, match="unknown StreamChannel"):
        _channel_name(bad)
