#!/usr/bin/env python3
"""Minimal grid strategy over the bare longtrader protocol.

Demonstrates every contract concept a ported strategy needs:

  * AttachSession with a terminal API token (the only credential system)
  * negotiated heartbeat feeding the session lease watchdog (kill switch)
  * ReconcileState before trading (ATTACHED -> SYNCING -> ACTIVE gate;
    orders submitted before ACTIVE are rejected SYNC_IN_PROGRESS)
  * OrderRequest with a mandatory ULID client_order_id (backend dedupes,
    making retries safe)
  * Decimal dual representation: we use raw_str here for readability; hot
    paths should prefer the int64 fast path (unscaled + scale)
  * ticker polling through the unified market surface (streaming uses the
    connect+proto envelope; see docs/bare-protocol-guide.md section 5)

Usage:
  just sdk-generate && pip install -e sdks/python
  python sdks/python/examples/grid_strategy.py --help
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

# Run without installing: put sdks/python on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from longtrader_sdk import Session  # noqa: E402

# Generated stubs (exist after `just sdk-generate`).
from longtrader_sdk.proto.longtrader.common.v1 import types_pb2  # noqa: E402
from longtrader_sdk.proto.longtrader.market.v1 import market_pb2  # noqa: E402
from longtrader_sdk.proto.longtrader.trading.v1 import trading_pb2  # noqa: E402
from longtrader_sdk.proto.longtrader.worker.v1 import worker_pb2  # noqa: E402

# Trading/market unary RPCs ride the identical Connect convention as the
# worker control plane; service names come from the deployed contract.
MARKET_SERVICE = "longtrader.market.v1.MarketDataService"
TRADING_SERVICE = "longtrader.trading.v1.TradingService"


def ulid_like_id() -> str:
    """26-char uppercase id, ULID-shaped.

    The contract only demands uniqueness per account: the backend dedupes on
    client_order_id, so reusing an id across retries is safe and required.
    Swap in a real time-ordered ULID generator for production.
    """
    return uuid.uuid4().hex.upper()[:26]


def dec(text: str) -> types_pb2.Decimal:
    """Build common.v1.Decimal from a decimal string.

    Writers populate exactly one representation. We take the universal
    raw_str fallback; prefer unscaled/scale in hot paths.
    """
    return types_pb2.Decimal(raw_str=text)


def post_unary(base_url: str, service: str, method: str, req_bytes: bytes) -> bytes:
    """Raw Connect unary call - the whole wire protocol in four lines."""
    resp = httpx.post(
        f"{base_url}/{service}/{method}",
        content=req_bytes,
        headers={"Content-Type": "application/proto"},
        timeout=10.0,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"connect error {resp.status_code}: {resp.text[:200]}")
    return resp.content


def fetch_mid(base_url: str, exchange_id: str, symbol: str) -> float:
    """Poll one ticker via unary FetchTicker and compute the mid price."""
    req = market_pb2.FetchTickerRequest(
        exchange_id=types_pb2.ExchangeId(id=exchange_id), symbol=symbol
    )
    resp = market_pb2.FetchTickerResponse()
    resp.ParseFromString(post_unary(base_url, MARKET_SERVICE, "FetchTicker", req.SerializeToString()))
    t = resp.ticker
    # Fall back through bid/ask/last; unset Decimals carry empty raw_str.
    def f(d) -> float | None:
        return float(d.raw_str) if d.raw_str else None

    bid, ask, last = f(t.bid), f(t.ask), f(t.last)
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    if last is not None:
        return last
    raise RuntimeError(f"ticker for {symbol} has no usable price")


def make_limit(symbol: str, side: int, price: float, amount: str) -> trading_pb2.OrderRequest:
    """One grid rung as an OrderRequest (ULID client_order_id mandatory)."""
    return trading_pb2.OrderRequest(
        client_order_id=ulid_like_id(),
        symbol=symbol,
        type=trading_pb2.ORDER_TYPE_LIMIT,
        side=side,
        amount=dec(amount),
        price=dec(f"{price:.8f}".rstrip("0").rstrip(".")),
        time_in_force=trading_pb2.TIME_IN_FORCE_GTC,
        post_only=True,
    )


def refresh_grid(session: Session, args, live_ids: list[str]) -> list[str]:
    """Cancel stale rungs, then batch-place a fresh grid around the mid."""
    mid = fetch_mid(session._base_url, args.exchange_id, args.symbol)
    print(f"[grid] mid={mid:.2f}")

    # Cancel previous rungs first so the grid never doubles up.
    for order_id in live_ids:
        req = trading_pb2.CancelOrderRequest(
            exchange_id=types_pb2.ExchangeId(id=args.exchange_id),
            order_id=order_id,
            symbol=args.symbol,
        )
        post_unary(
            session._base_url,
            TRADING_SERVICE,
            "CancelOrder",
            req.SerializeToString(),
        )

    orders = []
    for i in range(1, args.levels + 1):
        step = args.step_pct / 100.0 * i
        orders.append(make_limit(args.symbol, trading_pb2.ORDER_SIDE_BUY, mid * (1 - step), args.amount))
        orders.append(make_limit(args.symbol, trading_pb2.ORDER_SIDE_SELL, mid * (1 + step), args.amount))

    req = trading_pb2.CreateOrdersRequest(
        exchange_id=types_pb2.ExchangeId(id=args.exchange_id), orders=orders
    )
    resp = trading_pb2.CreateOrdersResponse()
    resp.ParseFromString(
        post_unary(session._base_url, TRADING_SERVICE, "CreateOrders", req.SerializeToString())
    )
    print(f"[grid] placed {len(resp.orders)} rungs")
    return [o.id for o in resp.orders]


def main() -> None:
    p = argparse.ArgumentParser(description="Minimal longtrader grid strategy")
    p.add_argument("--base-url", required=True, help="worker control plane URL")
    p.add_argument("--token", required=True, help="terminal API token")
    p.add_argument("--exchange-id", default="", help="registered exchange instance id")
    p.add_argument("--symbol", default="BTC/USDT")
    p.add_argument("--levels", type=int, default=3, help="rungs per side")
    p.add_argument("--step-pct", type=float, default=0.1, help="spacing between rungs, %%")
    p.add_argument("--amount", default="0.001", help="order size per rung")
    p.add_argument("--refresh-secs", type=float, default=30.0)
    p.add_argument("--lease-timeout-secs", type=int, default=0,
                   help="kill-switch lease budget; 0 keeps server default (3x heartbeat)")
    args = p.parse_args()

    # Kill-switch policy: cancel-on-disconnect scope defaults to this
    # session's orders only (SESSION_ORDERS).
    policy = worker_pb2.KillSwitchPolicy(scope=worker_pb2.KillSwitchPolicy.SCOPE_SESSION_ORDERS)
    if args.lease_timeout_secs > 0:
        from google.protobuf.duration_pb2 import Duration

        policy.lease_timeout.CopyFrom(Duration(seconds=args.lease_timeout_secs))

    session = Session.attach(args.base_url, args.token, policy=policy)
    print(f"attached session={session.session_id} "
          f"heartbeat_ms={session.heartbeat_interval_ms} state={session.state}")
    session.start_heartbeat()

    # Recovery gate: authoritative snapshot before any order submission.
    snapshot = session.reconcile_state()
    print(f"reconciled seq={snapshot.snapshot_sequence} "
          f"balances={len(snapshot.balances)} positions={len(snapshot.positions)} "
          f"open_orders={len(snapshot.open_orders)} state={session.state}")

    live_ids: list[str] = []
    try:
        while True:
            live_ids = refresh_grid(session, args, live_ids)
            time.sleep(args.refresh_secs)
    except KeyboardInterrupt:
        print("\nstopping: cancelling grid rungs")
        for order_id in live_ids:
            req = trading_pb2.CancelOrderRequest(
                exchange_id=types_pb2.ExchangeId(id=args.exchange_id),
                order_id=order_id,
                symbol=args.symbol,
            )
            try:
                post_unary(session._base_url, TRADING_SERVICE, "CancelOrder", req.SerializeToString())
            except RuntimeError as err:
                print(f"cancel {order_id} failed: {err}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
