# Risk Controls

The worker can refuse an order before it reaches a venue. This document says
what is enforced, what it costs, and what is deliberately never gated.

## Why this exists

Every mainstream strategy host leaves the pre-trade layer out or charges for it:

- **Hummingbot** runs one bot per install. There is no supervisor that runs N
  strategies in one process, so there is nothing that could reconcile net
  exposure across them — scaling to a portfolio means building the
  orchestration layer yourself.
- **NautilusTrader** puts its risk engine (position limits, margin controls)
  behind the paid Pro tier; the open-source core ships no pre-trade limits at
  all.
- **Freqtrade** has per-strategy position sizing but no host-level gate that a
  strategy cannot route around.

LongTrader's worker already runs a session control plane with a kill switch, so
the natural place for a gate is between the strategy and its
`TradingGateway` — where it applies to every strategy, in every language, with
no strategy cooperating.

## Configuration

Optional `[risk]` table in the worker config. Every field is optional and
defaults to "not enforced":

```toml
[risk]
max_order_qty = "1"              # largest quantity one order may carry
max_order_notional = "50000"     # largest price * qty one order may carry
max_orders_per_minute = 120      # largest submission rate
max_open_orders = 200            # largest number of resting orders, all symbols
max_net_qty_per_symbol = "5"     # largest |net| resting quantity on one symbol
```

Quantities are `rust_decimal` decimals and are written as strings, like every
other decimal in the config.

An absent `[risk]` table, or an empty one, is a pass-through: the decorator is
not built at all, so there is nothing on the hot path to pay for. This is the
default.

## Local limits

`max_order_qty`, `max_order_notional` and `max_orders_per_minute` are decided
from the request alone. They cost nothing.

- Both size limits are **inclusive**: an order of exactly `max_order_qty`
  trades.
- A **market order carries no price**, so its notional is unknown. It is not
  measured against `max_order_notional` rather than sized against a guess —
  `max_order_qty` is the market-order control.
- The rate limit is a token bucket refilled continuously, not a window reset
  once a minute, so it cannot be doubled by straddling a boundary. It is exact
  at any rate, including one order per minute.
- A **refusal never consumes rate budget**. Otherwise a strategy rejected for
  size would also lose its rate allowance, and a loop that is already
  misbehaving would be throttled into silence without ever being told the real
  reason.

## Venue-backed limits

`max_open_orders` and `max_net_qty_per_symbol` are measured against the venue's
own resting orders, read once per submission (`FetchOpenOrders`, unscoped).

- They are exact, because the venue is the only authority on what is resting.
  A counter the host keeps itself drifts as orders fill.
- They cost one extra round-trip per submission. Leave them unset for a
  zero-overhead gate.
- If the read is unavailable, the submission is **refused**. Admitting an
  unmeasured order is the one outcome a risk gate exists to prevent.
- `max_net_qty_per_symbol` is **net, not gross**: buys are positive, sells are
  negative. A market maker resting both sides of one symbol nets toward zero and
  keeps quoting, which is the intent; use `max_open_orders` if you want a gross
  ceiling.
- It is **per symbol**, so one symbol at its cap does not consume another
  symbol's headroom.

## Batching

`batch_create_orders` is all-or-nothing, matching the batch RPC's own gate. One
leg over a limit refuses the whole batch, and no leg is placed; a grid never
lands half-quoted. A batch consumes one rate slot per leg, so batching cannot
multiply the submission rate.

## What is never gated

Cancel and close paths — `cancel_order`, `cancel_all_orders`,
`close_position`, `close_all_positions` — are never measured. A gate that could
refuse a cancel would be able to hold the kill switch shut, which is the one
outcome a risk control must never cause. The gate is installed on the control
plane's gateway too, and that is safe for exactly this reason.

## Errors

A refusal is returned to the strategy as `PortError::InvalidArgument` naming
the limit and the measured value. The order is never sent, so there is nothing
to reconcile and no venue-side state to clean up.

The gate is a control, not a strategy. It bounds what a strategy can do; it
does not decide what it should do.
