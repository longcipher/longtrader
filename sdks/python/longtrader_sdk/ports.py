"""Overflow policies and the narrow ports strategies program against.

Ports mirror the Rust-side names 1:1 (design doc §6.8); only casing differs.
They keep strategy logic transport-agnostic and reviewable in one sitting.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Iterable


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
    def fetch_open_orders(self, symbol: str | None = None, limit: int | None = None) -> Any:
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
    def subscribe_market_data(self, subscriptions: Iterable[Any]) -> Any:
        """Stream MarketDataEvent for the given subscriptions; events carry
        resume_token for reconnect-with-replay and gap-free header.sequence."""
