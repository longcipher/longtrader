#!/usr/bin/env python3
"""Grid strategy driven entirely through the Python SDK.

The point of this example is the *lifecycle*, not the grid maths. Every
concept a ported strategy needs is visible in one place:

  * ``Session.attach`` with a terminal API token (the only credential system)
  * negotiated heartbeat feeding the session lease watchdog (kill switch)
  * ``reconcile_state()`` before trading: the ATTACHED -> SYNCING -> ACTIVE
    gate, after which orders are admitted
  * ``register_strategy`` so the host can report status and counters
  * order submission carrying the session id, which is what lets the host
    scope the kill-switch to *this* strategy's orders
  * ``stop_strategy(cancel_open_orders=True)`` on shutdown, the same RPC the
    host uses to guarantee nothing is left resting

Every RPC below goes through the SDK, which is the recommended way to consume
the contract. When you need a capability the SDK does not wrap yet, the raw
Connect call is still three lines -- see `docs/bare-protocol-guide.md`.

Usage:
  just sdk-generate && pip install -e sdks/python
  python sdks/python/examples/grid_strategy.py --base-url http://127.0.0.1:9000 \\
      --token "$LONGTRADER_TOKEN" --iterations 3
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

# Run without installing: put sdks/python on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from longtrader_sdk import Session  # noqa: E402
from longtrader_sdk.proto.longtrader.common.v1 import types_pb2  # noqa: E402
from longtrader_sdk.proto.longtrader.worker.v1 import worker_pb2  # noqa: E402


def ulid_like_id() -> str:
    """26-char uppercase id, ULID-shaped.

    The contract only demands uniqueness per account: the backend dedupes on
    ``client_order_id``, so reusing an id across retries is safe and required.
    Swap in a real time-ordered ULID generator for production.
    """
    return uuid.uuid4().hex.upper()[:26]


def decimal_to_float(value: types_pb2.Decimal) -> float | None:
    """Read a ``common.v1.Decimal`` into a float.

    The contract carries two representations and a writer populates only one:
    the hot-path ``unscaled``/``scale`` pair, or the human-readable ``raw_str``
    fallback. A reader must accept both, or it silently sees zero whenever the
    producer chose the other form -- which is what the mock venue does.
    """
    if value.raw_str:
        return float(value.raw_str)
    if value.unscaled == 0:
        return 0.0
    return value.unscaled / (10**value.scale)


def fetch_mid(session: Session, symbol: str) -> float:
    """Poll one ticker and compute a mid price, falling back bid/ask/last."""
    ticker = session.fetch_ticker(symbol)
    bid = decimal_to_float(ticker.bid)
    ask = decimal_to_float(ticker.ask)
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    last = decimal_to_float(ticker.last)
    if last is not None:
        return last
    raise RuntimeError(f"ticker for {symbol} has no usable price")


def format_price(price: float) -> str:
    """Render a price without trailing zeros, for readability in the order."""
    return f"{price:.8f}".rstrip("0").rstrip(".")


def refresh_grid(
    session: Session, args: argparse.Namespace, live_ids: list[str]
) -> list[str]:
    """Cancel stale rungs, then batch-place a fresh grid around the mid."""
    mid = fetch_mid(session, args.symbol)
    print(f"[grid] mid={mid:.2f}")

    # Cancel previous rungs first so the grid never doubles up.
    for order_id in live_ids:
        try:
            session.cancel_order(order_id, symbol=args.symbol)
        except Exception as err:  # noqa: BLE001 - one rung failing is not fatal
            print(f"[grid] cancel {order_id} failed: {err}")

    rungs = []
    for i in range(1, args.levels + 1):
        step = args.step_pct / 100.0 * i
        for side, price in (("BUY", mid * (1 - step)), ("SELL", mid * (1 + step))):
            rungs.append(
                {
                    "symbol": args.symbol,
                    "amount": args.amount,
                    "price": format_price(price),
                    "side": side,
                    "order_type": "LIMIT",
                    "time_in_force": "GTC",
                    "client_order_id": ulid_like_id(),
                    "post_only": True,
                }
            )

    # One batch, one gate check: the host rejects the whole batch if the
    # session is not ACTIVE, so a grid is never half-placed.
    resp = session.create_orders(rungs)
    placed = [o.id for o in resp.orders]
    print(f"[grid] placed {len(placed)} rungs")
    return placed


def build_policy(args: argparse.Namespace) -> worker_pb2.KillSwitchPolicy:
    """Kill-switch policy: cancel this session's orders on lease loss."""
    policy = worker_pb2.KillSwitchPolicy(
        scope=worker_pb2.KillSwitchPolicy.SCOPE_SESSION_ORDERS
    )
    if args.lease_timeout_secs > 0:
        from google.protobuf.duration_pb2 import Duration

        policy.lease_timeout.CopyFrom(Duration(seconds=args.lease_timeout_secs))
    return policy


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="longtrader grid strategy (Python SDK)")
    p.add_argument("--base-url", required=True, help="worker control plane URL")
    p.add_argument("--token", default="", help="terminal API token")
    p.add_argument("--exchange-id", default="", help="registered exchange instance id")
    p.add_argument("--symbol", default="BTC/USDT")
    p.add_argument("--levels", type=int, default=3, help="rungs per side")
    p.add_argument(
        "--step-pct", type=float, default=0.1, help="spacing between rungs, percent"
    )
    p.add_argument("--amount", default="0.001", help="order size per rung")
    p.add_argument("--refresh-secs", type=float, default=30.0)
    p.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="grid refreshes before exiting; 0 runs until interrupted",
    )
    p.add_argument(
        "--lease-timeout-secs",
        type=int,
        default=0,
        help="kill-switch lease budget; 0 keeps server default (3x heartbeat)",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    policy = build_policy(args)

    session = Session.attach(args.base_url, args.token, policy=policy)
    print(
        f"attached session={session.session_id} "
        f"heartbeat_ms={session.heartbeat_interval_ms} state={session.state} "
        f"capabilities={session.capabilities}"
    )

    # Register before trading so host-side status and counters cover the run.
    strategy_id = session.register_strategy(
        "python_grid",
        {
            "symbol": args.symbol,
            "levels": str(args.levels),
            "step_pct": str(args.step_pct),
            "amount": args.amount,
        },
    )
    print(f"registered strategy={strategy_id}")

    session.start_heartbeat()
    # Local guard mirrors the host's lease watchdog: it stops this process
    # from trading past its own lease even if the host is unreachable.
    session.spawn_lease_watchdog()

    try:
        # Recovery gate: the authoritative snapshot before any submission.
        snapshot = session.reconcile_state()
        print(
            f"reconciled seq={snapshot.snapshot_sequence} "
            f"balances={len(snapshot.balances)} positions={len(snapshot.positions)} "
            f"open_orders={len(snapshot.open_orders)} state={session.state}"
        )

        live_ids: list[str] = []
        iteration = 0
        while args.iterations == 0 or iteration < args.iterations:
            if session.state != "ACTIVE":
                print(f"[grid] session is {session.state}; stopping")
                break
            live_ids = refresh_grid(session, args, live_ids)
            iteration += 1
            if args.iterations and iteration < args.iterations:
                time.sleep(args.refresh_secs)
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        # Let the host cancel exactly this session's resting orders. This is
        # the same path the kill-switch uses, so a crash-looping strategy
        # cannot leave ladders behind.
        try:
            result = session.stop_strategy(cancel_open_orders=True)
            print(f"stopped final_state={session.state}")
            del result
        except Exception as err:  # noqa: BLE001 - shutdown is best-effort
            print(f"stop_strategy failed: {err}")
        session.close()


if __name__ == "__main__":
    main()
