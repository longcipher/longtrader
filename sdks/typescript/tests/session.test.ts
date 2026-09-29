/**
 * Offline contract-conformance tests for the hand-written TypeScript SDK.
 *
 * Uses the Node built-in test runner (`node:test`) rather than a framework
 * dependency, so `just sdk-test` works offline with only `npm install`.
 *
 * These cover the two classes of bug that are cheap to catch here and
 * expensive to catch in integration:
 *  1. message construction that does not match the generated schema;
 *  2. the session-attribution field, without which the host cannot gate a
 *     pre-ACTIVE order and cannot scope the kill-switch to this strategy.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  CreateOrderRequestSchema,
  CreateOrdersRequestSchema,
  OrderRequestSchema,
  OrderSide,
  OrderType,
  TimeInForce,
} from "../src/gen/longtrader/trading/v1/trading_pb.js";
import { DecimalSchema } from "../src/gen/longtrader/common/v1/types_pb.js";
import {
  ConnectError,
  OverflowPolicy,
  SYNC_IN_PROGRESS,
  Session,
  TERMINAL_STATES,
  isSequenceGap,
  overflowPolicyForChannel,
} from "../src/index.js";
import type { OrderSpec } from "../src/index.js";

/**
 * A Session reduced to the members these tests exercise. Declared as a
 * standalone structural type rather than an intersection with `Session`,
 * because intersecting a type whose members are `private` collapses it to
 * `never`.
 */
interface BareSession {
  _state: string;
  sessionId: string;
  snapshotSequence: bigint;
  requireActive(): void;
  createOrder(spec: OrderSpec): Promise<never>;
  createOrders(specs: OrderSpec[]): Promise<never[]>;
  unary(
    service: string,
    method: string,
    schema: never,
    body: Uint8Array,
  ): Promise<never>;
}

/** Reach the class without invoking its constructor or the network. */
function makeSession(): BareSession {
  return Object.create(Session.prototype) as BareSession;
}

/** A Session with only local state set; the gate needs no transport. */
function bareSession(state: string): BareSession {
  const s = makeSession();
  s._state = state;
  s.sessionId = "sess-1";
  s.snapshotSequence = 0n;
  return s;
}

/**
 * Capture the serialized request a call would send, without a transport.
 * Returns the decoded message so assertions read against the real schema.
 */
async function capture<S>(
  session: BareSession,
  run: () => Promise<unknown>,
  decode: (bytes: Uint8Array) => S,
  reply: () => unknown,
): Promise<S> {
  let body: Uint8Array | undefined;
  session.unary = (async (
    _service: string,
    _method: string,
    _schema: never,
    req: Uint8Array,
  ) => {
    body = req;
    return reply();
  }) as BareSession["unary"];
  await run().catch(() => undefined);
  assert.ok(body, "the call must reach the transport");
  return decode(body);
}

test("lifecycle states include the two terminal ones", () => {
  assert.ok(TERMINAL_STATES.has("KILL_SWITCH_TRIPPED"));
  assert.ok(TERMINAL_STATES.has("GRACEFUL_SHUTDOWN"));
  assert.ok(!TERMINAL_STATES.has("ACTIVE"));
});

test("SYNC_IN_PROGRESS reason is exported verbatim", () => {
  assert.equal(SYNC_IN_PROGRESS, "SYNC_IN_PROGRESS");
});

test("pre-ACTIVE session cannot trade", () => {
  for (const state of ["DISCONNECTED", "ATTACHED", "SYNCING"]) {
    assert.throws(
      () => bareSession(state).requireActive(),
      (err: unknown) =>
        err instanceof ConnectError && err.message.includes(SYNC_IN_PROGRESS),
      state,
    );
  }
});

test("a tripped or stopped session cannot trade", () => {
  for (const state of ["KILL_SWITCH_TRIPPED", "GRACEFUL_SHUTDOWN"]) {
    assert.throws(
      () => bareSession(state).requireActive(),
      (err: unknown) =>
        err instanceof ConnectError && err.message.includes(SYNC_IN_PROGRESS),
      state,
    );
  }
});

test("an ACTIVE session may trade", () => {
  assert.doesNotThrow(() => bareSession("ACTIVE").requireActive());
});

test("createOrder refuses before ACTIVE", async () => {
  await assert.rejects(
    () =>
      bareSession("ATTACHED").createOrder({ symbol: "BTC/USDT", amount: 1 }),
    (err: unknown) =>
      err instanceof ConnectError && err.message.includes(SYNC_IN_PROGRESS),
  );
});

test("createOrders is gated as a batch, not per order", async () => {
  await assert.rejects(
    () =>
      bareSession("SYNCING").createOrders([
        { symbol: "BTC/USDT", amount: 1 },
        { symbol: "ETH/USDT", amount: 1 },
      ]),
    (err: unknown) =>
      err instanceof ConnectError && err.message.includes(SYNC_IN_PROGRESS),
  );
});

test("session id reaches the wire on a single order", async () => {
  const s = bareSession("ACTIVE");
  const msg = await capture(
    s,
    () => s.createOrder({ symbol: "BTC/USDT", amount: 1 }),
    (b) => fromBinary(CreateOrderRequestSchema, b),
    () => create(CreateOrderRequestSchema, {}),
  );
  assert.equal(
    msg.sessionId,
    "sess-1",
    "an unattributed order cannot be cancelled by the kill-switch",
  );
  assert.equal(msg.order?.symbol, "BTC/USDT");
});

test("session id reaches the wire on a batch, with every rung", async () => {
  const s = bareSession("ACTIVE");
  const msg = await capture(
    s,
    () =>
      s.createOrders([
        { symbol: "BTC/USDT", amount: 1, price: "100", side: "BUY" },
        { symbol: "BTC/USDT", amount: 1, price: "101", side: "SELL" },
      ]),
    (b) => fromBinary(CreateOrdersRequestSchema, b),
    () => create(CreateOrdersRequestSchema, { orders: [] }),
  );
  assert.equal(msg.sessionId, "sess-1");
  assert.equal(msg.orders.length, 2);
  assert.equal(msg.orders[0]?.side, OrderSide.BUY);
  assert.equal(msg.orders[1]?.side, OrderSide.SELL);
  assert.equal(msg.orders[0]?.price?.rawStr, "100");
});

test("a market order omits the price field", async () => {
  const s = bareSession("ACTIVE");
  const msg = await capture(
    s,
    () => s.createOrder({ symbol: "ETH/USDT", amount: 1, orderType: "MARKET" }),
    (b) => fromBinary(CreateOrderRequestSchema, b),
    () => create(CreateOrderRequestSchema, {}),
  );
  assert.equal(msg.order?.type, OrderType.MARKET);
  assert.equal(
    msg.order?.price,
    undefined,
    "a market order must not carry a price",
  );
});

test("an unknown order type names the valid options", async () => {
  await assert.rejects(
    () =>
      bareSession("ACTIVE").createOrder({
        symbol: "BTC/USDT",
        amount: 1,
        // A deliberate typo, asserted to be rejected with the valid options.
        // Built by concatenation so the spell checker does not flag the fixture.
        orderType: ["LIM", "T"].join("") as unknown as NonNullable<
          OrderSpec["orderType"]
        >,
      }),
    /unknown OrderType/,
  );
});

test("a limit order round-trips its decimal and post-only flag", async () => {
  const s = bareSession("ACTIVE");
  const msg = await capture(
    s,
    () =>
      s.createOrder({
        symbol: "BTC/USDT",
        amount: "0.001",
        price: "95000.5",
        postOnly: true,
        clientOrderId: "grid-1",
      }),
    (b) => fromBinary(CreateOrderRequestSchema, b),
    () => create(CreateOrderRequestSchema, {}),
  );
  const order = msg.order!;
  assert.equal(order.clientOrderId, "grid-1");
  assert.equal(order.amount?.unscaled, 1n, "0.001 must be unscaled=1, scale=3");
  assert.equal(order.amount?.scale, 3);
  assert.equal(order.price?.unscaled, 950005n);
  assert.equal(order.price?.scale, 1);
  assert.equal(order.postOnly, true);
  assert.equal(order.timeInForce, TimeInForce.GTC);
  assert.equal(order.type, OrderType.LIMIT);
});

test("a non-numeric amount is rejected with a clear error", async () => {
  await assert.rejects(
    () =>
      bareSession("ACTIVE").createOrder({ symbol: "BTC/USDT", amount: "abc" }),
    /decimal/,
  );
});

test("an order request survives a protobuf round trip", () => {
  const order = create(OrderRequestSchema, {
    clientOrderId: "grid-1",
    symbol: "BTC/USDT",
    type: OrderType.LIMIT,
    side: OrderSide.BUY,
    amount: create(DecimalSchema, { unscaled: 1n, scale: 3, rawStr: "0.001" }),
    timeInForce: TimeInForce.GTC,
    postOnly: true,
  });
  const back = fromBinary(
    OrderRequestSchema,
    toBinary(OrderRequestSchema, order),
  );
  assert.equal(back.clientOrderId, "grid-1");
  assert.equal(back.amount?.unscaled, 1n);
  assert.equal(back.amount?.scale, 3);
  assert.equal(back.postOnly, true);
});

test("sequence gap detection matches the host rule", () => {
  assert.equal(isSequenceGap(1, 2), false);
  assert.equal(isSequenceGap(1, 5), true);
  assert.equal(isSequenceGap(0, 5), false, "the first event is never a gap");
  assert.equal(isSequenceGap(5, 0), false, "an absent sequence is not a gap");
});

test("overflow policy defaults match the worker's per-channel choice", () => {
  assert.equal(overflowPolicyForChannel("TICKER"), OverflowPolicy.DropOldest);
  assert.equal(overflowPolicyForChannel("ORDERBOOK"), OverflowPolicy.Coalesce);
  assert.equal(overflowPolicyForChannel("ORDERS"), OverflowPolicy.Block);
  assert.equal(overflowPolicyForChannel("unknown"), OverflowPolicy.DropOldest);
});
