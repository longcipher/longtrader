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
    """Default policy for a stream channel name (``TICKER``, ``ORDERBOOK``, ...)."""
    return DEFAULT_OVERFLOW.get(channel.strip().lower(), OverflowPolicy.DROP_OLDEST)


def is_sequence_gap(prev: int, next_: int) -> bool:
    """True when ``next_`` does not immediately follow ``prev``; triggers resync.

    A zero on either side means "no sequence yet", which is not a gap.
    """
    return next_ > 0 and prev > 0 and next_ != prev + 1


class TradingPort(ABC):
    """Order management surface (mirrors longtrader.trading.v1)."""

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
    def sync_state(self) -> Any:
        """ReconcileState: authoritative atomic snapshot for recovery."""


class MarketPort(ABC):
    """Market data surface (mirrors longtrader.market.v1)."""

    @abstractmethod
    def fetch_ticker(self, symbol: str) -> Any:
        """Unary latest ticker snapshot."""

    @abstractmethod
    def fetch_order_book(self, symbol: str, depth: int = 10) -> Any:
        """Unary order book snapshot."""

    @abstractmethod
    def get_candles(self, symbol: str, timeframe: str = "M1", limit: int = 100) -> Any:
        """OHLCV candles; ``timeframe`` is the contract's string form."""

    @abstractmethod
    def list_symbols(self) -> Any:
        """Tradeable symbols on the bound venue."""

    @abstractmethod
    def subscribe_market_data(self, subscriptions: Iterable[Any]) -> Any:
        """Stream MarketDataEvent for the given subscriptions; events carry
        resume_token for reconnect-with-replay and gap-free header.sequence."""


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

    def get_candles(self, symbol: str, timeframe: str = "M1", limit: int = 100) -> Any:
        return self._session.get_candles(symbol, timeframe, limit)

    def list_symbols(self) -> Any:
        return self._session.list_symbols()

    def subscribe_market_data(self, subscriptions: Iterable[Any]) -> Any:
        symbols = [s.symbol for s in subscriptions]
        channel = _channel_name(subscriptions[0].channel) if subscriptions else "TICKER"
        return self._session.stream_market_data(symbols, channel)


def _channel_name(value: int) -> str:
    """Map a numeric proto enum back to its short name (TICKER, ORDERBOOK, ...)."""
    from .session import _market_pb

    pb = _market_pb()
    for name in ("TICKER", "ORDERBOOK", "TRADES", "OHLCV"):
        if getattr(pb, f"STREAM_CHANNEL_{name}") == value:
            return name
    return "TICKER"
