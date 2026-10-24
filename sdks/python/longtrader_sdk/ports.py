"""Overflow policies and the narrow ports strategies program against.

Ports mirror the Rust-side names 1:1 (design doc §6.8); only casing differs.
They keep strategy logic transport-agnostic and reviewable in one sitting.

`SessionTradingPort` / `SessionMarketPort` adapt a `Session` to these
interfaces, so a strategy written against the ports runs unchanged against any
transport the session is bound to.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .session import Session


class OverflowPolicy(Enum):
    """How a full event queue behaves under a slow consumer.

    DROP_OLDEST: discard the oldest buffered event, preserve the newest.
                 Sequence gaps then trigger snapshot resync/reconcile.
    COALESCE:    last-writer-wins per key (e.g. orderbook), drops stale
                 intermediates instead of queueing them.
    BLOCK:       never drop; apply backpressure to the producer until drained.
    """

    DROP_OLDEST = "drop_oldest"
    COALESCE = "coalesce"
    BLOCK = "block"


# Per-stream-kind default policy, mirroring the worker's `ports::OVERFLOW_*`
# constants: a book must be coalesced (intermediate snapshots are worthless), a
# ticker may drop (only the newest matters), and private order/balance/position
# streams must never drop.
DEFAULT_OVERFLOW: dict[str, OverflowPolicy] = {
    "ticker": OverflowPolicy.DROP_OLDEST,
    "trades": OverflowPolicy.DROP_OLDEST,
    "ohlcv": OverflowPolicy.DROP_OLDEST,
    "orderbook": OverflowPolicy.COALESCE,
    "orders": OverflowPolicy.BLOCK,
    "balances": OverflowPolicy.BLOCK,
    "positions": OverflowPolicy.BLOCK,
}


def overflow_policy_for_channel(channel: str) -> OverflowPolicy:
    """Default policy for a stream channel name (``TICKER``, ``ORDERBOOK``, ...).

    Fail-closed: unknown / future channels (including private order / balance /
    position streams) resolve to ``BLOCK``, never to a dropping policy.
    """
    key = channel.strip().lower()
    if key in DEFAULT_OVERFLOW:
        return DEFAULT_OVERFLOW[key]
    return OverflowPolicy.BLOCK


def is_sequence_gap(prev: int, next_: int) -> bool:
    """True when ``next_`` does not immediately follow ``prev``; triggers resync.

    ``next_ == 0`` is the unset proto default (never emitted by the worker,
    whose sequences start at ``1``), so it is never a gap. ``prev == 0`` is a
    *valid* watermark meaning "before the first sequence": ``0 -> 1`` is
    contiguous, ``0 -> 2`` missed event ``1``. This mirrors
    ``bin/longtrader-worker/src/ports.rs::is_sequence_gap`` (wrapping
    arithmetic); the worker's ``has_sequence_gap`` additionally ignores unset
    headers, which is what the stream iterators below do inline (``prev is
    not None`` + ``seq != 0``).
    """
    if next_ == 0:
        return False
    return next_ != (prev + 1) % 2**64


class TradingPort(ABC):
    """Order management surface (mirrors longtrader.trading.v1, 13 methods)."""

    @abstractmethod
    def create_order(self, request: Any) -> Any:
        """Submit one OrderRequest; backend dedupes on client_order_id."""

    @abstractmethod
    def batch_create_orders(self, requests: Iterable[Any]) -> Any:
        """Submit many OrderRequests atomically-ish via CreateOrders."""

    @abstractmethod
    def cancel_order(self, order_id: str, symbol: str | None = None) -> Any:
        """Cancel by venue order id."""

    @abstractmethod
    def cancel_all_orders(self, symbol: str | None = None) -> Any:
        """Cancel every open order; empty symbol spans all symbols."""

    @abstractmethod
    def fetch_open_orders(
        self, symbol: str | None = None, limit: int | None = None
    ) -> Any:
        """List currently open orders, optionally filtered by symbol."""

    @abstractmethod
    def get_account(self, exchange_id: str = "") -> Any:
        """Account margin summary."""

    @abstractmethod
    def get_positions(
        self, symbols: list[str] | None = None, exchange_id: str = ""
    ) -> Any:
        """Open positions; None means every symbol."""

    @abstractmethod
    def get_order_history(
        self, limit: int = 100, since: int = 0, exchange_id: str = ""
    ) -> Any:
        """Historical orders."""

    @abstractmethod
    def get_closed_positions(self, limit: int = 100, exchange_id: str = "") -> Any:
        """Closed positions."""

    @abstractmethod
    def close_position(self, position_id: str, exchange_id: str = "") -> Any:
        """Close one position by id."""

    @abstractmethod
    def close_all_positions(self, exchange_id: str = "") -> Any:
        """Close every open position."""

    @abstractmethod
    def modify_position(
        self,
        position_id: str,
        take_profit: Any = None,
        stop_loss: Any = None,
        exchange_id: str = "",
    ) -> Any:
        """Attach or replace a position's bracket orders."""

    @abstractmethod
    def sync_state(self) -> Any:
        """ReconcileState: best-effort snapshot for recovery (replay deltas after)."""


class MarketPort(ABC):
    """Market data surface (mirrors longtrader.market.v1)."""

    @abstractmethod
    def fetch_ticker(self, symbol: str) -> Any:
        """Unary latest ticker snapshot."""

    @abstractmethod
    def fetch_order_book(self, symbol: str, depth: int = 10) -> Any:
        """Unary order book snapshot."""

    @abstractmethod
    def get_candles(
        self, symbol: str, timeframe: int | None = None, limit: int = 100
    ) -> Any:
        """OHLCV candles; ``timeframe`` is the contract enum value."""

    @abstractmethod
    def list_symbols(self) -> Any:
        """Tradeable symbols on the bound venue."""

    @abstractmethod
    def subscribe_market_data(self, symbols: list[str], channel: Any = None) -> Any:
        """Stream MarketDataEvent for ``symbols`` on one channel; events carry
        resume_token for reconnect-with-replay and gap-free header.sequence.

        One channel per call, mirroring the Go SDK's port shape: the host opens
        one stream per (symbols, channel) request, so a list of per-symbol
        channels could only be served by subscribing every symbol to the first
        channel -- silent wrong data. Ask for a second channel with a second
        call.
        """


class SessionTradingPort(TradingPort):
    """Adapts a :class:`~longtrader_sdk.session.Session` to ``TradingPort``."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def create_order(self, request: Any) -> Any:
        """Accept either a mapping or an object exposing the same attributes."""
        spec = request if isinstance(request, dict) else vars(request)
        return self._session.create_order(**spec)

    def batch_create_orders(self, requests: Iterable[Any]) -> Any:
        return self._session.create_orders(list(requests))

    def cancel_order(self, order_id: str, symbol: str | None = None) -> Any:
        return self._session.cancel_order(order_id, symbol or "")

    def cancel_all_orders(self, symbol: str | None = None) -> Any:
        return self._session.cancel_all_orders(symbol or "")

    def fetch_open_orders(
        self, symbol: str | None = None, limit: int | None = None
    ) -> Any:
        return self._session.fetch_open_orders(symbol or "", limit or 0)

    def get_account(self, exchange_id: str = "") -> Any:
        return self._session.get_account(exchange_id)

    def get_positions(
        self, symbols: list[str] | None = None, exchange_id: str = ""
    ) -> Any:
        return self._session.get_positions(symbols, exchange_id)

    def get_order_history(
        self, limit: int = 100, since: int = 0, exchange_id: str = ""
    ) -> Any:
        return self._session.get_order_history(limit, since, exchange_id)

    def get_closed_positions(self, limit: int = 100, exchange_id: str = "") -> Any:
        return self._session.get_closed_positions(limit, exchange_id)

    def close_position(self, position_id: str, exchange_id: str = "") -> Any:
        return self._session.close_position(position_id, exchange_id)

    def close_all_positions(self, exchange_id: str = "") -> Any:
        return self._session.close_all_positions(exchange_id)

    def modify_position(
        self,
        position_id: str,
        take_profit: Any = None,
        stop_loss: Any = None,
        exchange_id: str = "",
    ) -> Any:
        return self._session.modify_position(
            position_id,
            take_profit=take_profit,
            stop_loss=stop_loss,
            exchange_id=exchange_id,
        )

    def sync_state(self) -> Any:
        return self._session.reconcile_state()


class SessionMarketPort(MarketPort):
    """Adapts a :class:`~longtrader_sdk.session.Session` to ``MarketPort``."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def fetch_ticker(self, symbol: str) -> Any:
        return self._session.fetch_ticker(symbol)

    def fetch_order_book(self, symbol: str, depth: int = 10) -> Any:
        return self._session.fetch_order_book(symbol, depth)

    def get_candles(
        self, symbol: str, timeframe: int | None = None, limit: int = 100
    ) -> Any:
        return self._session.get_candles(symbol, timeframe, limit)

    def list_symbols(self) -> Any:
        return self._session.list_symbols()

    def subscribe_market_data(self, symbols: list[str], channel: Any = None) -> Any:
        """One channel per call; see :meth:`MarketPort.subscribe_market_data`.

        ``channel`` accepts the generated numeric enum (what a
        ``StreamSubscription`` carries) or the short name string. An omitted
        channel subscribes to TICKER; an unknown one raises rather than
        silently defaulting.
        """
        if channel is None:
            return self._session.stream_market_data(symbols, "TICKER")
        return self._session.stream_market_data(symbols, _channel_name(channel))


def _channel_name(value: Any) -> str:
    """Map a channel -- numeric proto enum or short name -- to its short name.

    Numeric values resolve against the generated constants so a renumbering in
    the contract cannot drift from this table. An unknown value raises:
    defaulting it to TICKER would silently subscribe the caller to a channel it
    never asked for.
    """
    if isinstance(value, str):
        name = value.strip().upper()
        if name in ("TICKER", "ORDERBOOK", "TRADES", "OHLCV"):
            return name
        raise ValueError(f"unknown StreamChannel {value!r}")
    from .session import _market_pb

    pb = _market_pb()
    for name in ("TICKER", "ORDERBOOK", "TRADES", "OHLCV"):
        if getattr(pb, f"STREAM_CHANNEL_{name}") == value:
            return name
    raise ValueError(f"unknown StreamChannel {value!r}")
