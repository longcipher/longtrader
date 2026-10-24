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
  CreateOrderResponseSchema,
  CreateOrdersRequestSchema,
  OrderRequestSchema,
  OrderSide,
  OrderType,
  TimeInForce,
} from "../src/gen/longtrader/trading/v1/trading_pb.js";
import {
  MarketDataEventSchema,
  StreamChannel,
} from "../src/gen/longtrader/market/v1/market_pb.js";
import {
  AttachSessionResponseSchema,
  StrategyEventSchema,
} from "../src/gen/longtrader/worker/v1/worker_pb.js";
import type { Order } from "../src/gen/longtrader/trading/v1/trading_pb.js";
import { DecimalSchema } from "../src/gen/longtrader/common/v1/types_pb.js";
import {
  ConnectError,
  OverflowPolicy,
  SYNC_IN_PROGRESS,
  Session,
  SessionMarketPort,
  SessionTradingPort,
  TERMINAL_STATES,
  decimalNumber,
  decimalText,
  isSequenceGap,
  overflowPolicyForChannel,
  toDecimal,
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
  assert.equal(msg.orders[0]?.price?.value, "100");
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
  assert.equal(order.amount?.value, "0.001", "one payload, no numeric twin");
  assert.equal(order.price?.value, "95000.5");
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

/**
 * A `common.v1.Decimal` carrying `text` verbatim, as a producer sent it.
 *
 * Built by hand rather than through `toDecimal` so the read path can be fed a
 * payload no conforming writer would have produced.
 */
function wireDecimal(text: string) {
  return create(DecimalSchema, { value: text });
}

test("a decimal encodes to one field carrying the text", () => {
  // 0a 04 "1.25": field 1, length-delimited, and nothing else. There is no
  // numeric companion on the wire for a reader to prefer over the text.
  assert.deepEqual(
    toBinary(DecimalSchema, toDecimal("1.25")),
    new Uint8Array([0x0a, 0x04, 0x31, 0x2e, 0x32, 0x35]),
  );
});

test("every decimal input type carries one payload", () => {
  assert.equal(toDecimal("0.001").value, "0.001");
  assert.equal(toDecimal(95000).value, "95000");
  assert.equal(toDecimal(95000n).value, "95000");
  assert.equal(toDecimal("-0.5").value, "-0.5");
  assert.equal(toDecimal(0).value, "0");
});

test("trailing zeros are part of the value", () => {
  // The host's "1.100" and "1.1" are different decimals.
  assert.equal(toDecimal("1.100").value, "1.100");
  assert.notEqual(toDecimal("1.100").value, toDecimal("1.1").value);
});

test("a bigint carries the 96 bits an int64 mantissa could not", () => {
  assert.equal(
    toDecimal(79228162514264337593543950335n).value,
    "79228162514264337593543950335",
  );
  assert.equal(
    toDecimal(7922816251426433759354395033n * 10n + 5n).value,
    "79228162514264337593543950335",
  );
});

test("a number is rendered without binary noise, exponents expanded", () => {
  // `String(n)` is the shortest decimal that round-trips to n, so 0.1 stays
  // "0.1" instead of becoming 0.1000000000000000055511151231257827.
  assert.equal(toDecimal(0.1).value, "0.1");
  assert.equal(toDecimal(-0.5).value, "-0.5");
  // `String` switches to exponent notation below 1e-7, which is exactly where a
  // satoshi price lives; the notation is expanded, not rejected.
  assert.equal(toDecimal(1e-7).value, "0.0000001");
  assert.equal(toDecimal(1e21).value, "1000000000000000000000");
  assert.equal(toDecimal(-1.5e-7).value, "-0.00000015");
  assert.equal(toDecimal(1.2345e-5).value, "0.000012345");
});

test("a value the contract cannot carry is rejected, never truncated", () => {
  // 1e30 is exactly representable as a double but needs 31 digits of
  // coefficient, and a decimal holds 96 bits.
  assert.throws(() => toDecimal(1e30), /96 bits/);
  assert.throws(() => toDecimal("79228162514264337593543950336"), /96 bits/);
  assert.throws(() => toDecimal(79228162514264337593543950336n), /96 bits/);
  // The point moves, the width does not shrink.
  assert.throws(() => toDecimal("7922816251426433759354395033.6"), /96 bits/);
  assert.throws(() => toDecimal(`0.${"0".repeat(29)}1`), /fractional digits/);
});

test("the grammar rejects every payload the host would reject", () => {
  const cases: [string, RegExp][] = [
    ["abc", /only digits before the decimal point/],
    ["1_000", /digit separators are not accepted/],
    ["1e3", /exponents are not accepted/],
    ["1E3", /exponents are not accepted/],
    ["1.5e-3", /exponents are not accepted/],
    ["1e", /exponents are not accepted/],
    ["+7", /a leading `\+` is not accepted/],
    [".5", /at least one digit before the decimal point/],
    ["1.", /at least one digit after the decimal point/],
    ["-", /at least one digit before the decimal point/],
    [".", /at least one digit before the decimal point/],
    ["1.2.3", /only digits after the decimal point/],
    ["1,25", /only digits before the decimal point/],
    ["NaN", /only digits before the decimal point/],
    ["Infinity", /only digits before the decimal point/],
    ["0x10", /only digits before the decimal point/],
    // Surrounding whitespace is not trimmed away: the contract defines no
    // whitespace, and padding is a payload the host will reject.
    [" 1", /only digits before the decimal point/],
    ["1 ", /only digits before the decimal point/],
    [" 12.50 ", /only digits before the decimal point/],
  ];
  for (const [text, reason] of cases) {
    assert.throws(() => toDecimal(text), reason, text);
    // The read path holds the same grammar: a writer may send anything.
    assert.throws(() => decimalText(wireDecimal(text)), reason, text);
  }
});

test("the error names the payload that arrived", () => {
  assert.throws(() => toDecimal("1_000"), /"1_000"/);
  assert.throws(() => toDecimal("abc"), /"abc"/);
});

test("a blank payload is never read as zero", () => {
  // It is the shape a message nobody populated has: presence lives on the
  // containing field, so this is a writer that sent nothing, not a price of 0.
  for (const blank of ["", "   ", "\t\n"]) {
    assert.throws(() => toDecimal(blank), /is empty/, JSON.stringify(blank));
    assert.throws(() => decimalText(wireDecimal(blank)), /is empty/);
    assert.throws(() => decimalNumber(wireDecimal(blank)), /is empty/);
  }
  assert.throws(() => decimalText(undefined), /absent/);
  assert.throws(() => decimalNumber(undefined), /absent/);
});

test("a non-canonical spelling is legal input and one number", () => {
  // Two payloads may spell one number, so equality is never byte equality.
  assert.notEqual(wireDecimal("007").value, wireDecimal("7").value);
  assert.equal(
    decimalNumber(wireDecimal("007")),
    decimalNumber(wireDecimal("7")),
  );
  assert.equal(decimalNumber(wireDecimal("00.5")), 0.5);
  assert.equal(decimalNumber(wireDecimal("1.500")), 1.5);
  // "-0" reads as negative zero, which `Object.is` (and therefore
  // `assert/strict`) tells apart from "0" -- one more reason to compare the
  // text of a decimal rather than a number read out of it.
  assert.equal(decimalNumber(wireDecimal("-0")), -0);
  assert.equal(decimalNumber(wireDecimal("0")), 0);
});

test("the exact text survives where an f64 cannot", () => {
  const wide = wireDecimal("1234567890123456789012345678");
  assert.equal(decimalText(wide), "1234567890123456789012345678");
  // The f64 view demonstrably is not the payload: 28 significant digits cannot
  // survive 53 bits. This is why decimalText exists.
  assert.notEqual(
    decimalNumber(wide).toString(),
    "1234567890123456789012345678",
  );
  const deepest = wireDecimal(`0.${"0".repeat(27)}1`);
  assert.equal(decimalText(deepest), `0.${"0".repeat(27)}1`);
  assert.equal(decimalNumber(deepest), 1e-28);
});

test("an order request survives a protobuf round trip", () => {
  const order = create(OrderRequestSchema, {
    clientOrderId: "grid-1",
    symbol: "BTC/USDT",
    type: OrderType.LIMIT,
    side: OrderSide.BUY,
    amount: create(DecimalSchema, { value: "0.001" }),
    timeInForce: TimeInForce.GTC,
    postOnly: true,
  });
  const back = fromBinary(
    OrderRequestSchema,
    toBinary(OrderRequestSchema, order),
  );
  assert.equal(back.clientOrderId, "grid-1");
  assert.equal(back.amount?.value, "0.001");
  assert.equal(back.postOnly, true);
});

test("sequence gap detection matches the host rule", () => {
  assert.equal(isSequenceGap(1, 2), false);
  assert.equal(isSequenceGap(1, 5), true);
  // Watermark 0 is "before the first sequence": 0 -> 1 contiguous, 0 -> 5
  // missed 1-4. A live stream's first event is never a gap because the pump
  // holds prev=null until it sees a sequence.
  assert.equal(isSequenceGap(0, 1), false);
  assert.equal(isSequenceGap(0, 5), true);
  assert.equal(isSequenceGap(5, 0), false, "an absent sequence is not a gap");
  const maxU64 = (1n << 64n) - 1n;
  assert.equal(isSequenceGap(maxU64, 0n), false, "wrap to zero is contiguous");
  assert.equal(isSequenceGap(maxU64, 1n), true, "wrapped past zero");
  assert.equal(isSequenceGap(maxU64 - 1n, maxU64), false);
});

test("overflow policy defaults match the worker's per-channel choice", () => {
  assert.equal(overflowPolicyForChannel("TICKER"), OverflowPolicy.DropOldest);
  assert.equal(overflowPolicyForChannel("ORDERBOOK"), OverflowPolicy.Coalesce);
  assert.equal(overflowPolicyForChannel("ORDERS"), OverflowPolicy.Block);
  assert.equal(overflowPolicyForChannel("unknown"), OverflowPolicy.Block);
});

// -- streaming: reader lifetime, frame bound, end-of-stream errors ----------
//
// These exercise the hand-written Connect envelope decoder, mirroring the
// cases sdks/go/session/envelope_test.go holds the Go decoder to.

/** Connect frame flags (docs/bare-protocol-guide.md). */
const FLAG_MESSAGE = 0x00;
const FLAG_END_OF_STREAM = 0x02;

/** Wrap `payload` in the 5-byte Connect envelope with `flags`. */
function frame(flags: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + payload.length);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, payload.length, false);
  out.set(payload, 5);
  return out;
}

/** A `StrategyEvent` on the wire, stamped with `sequence`. */
function eventBytes(sequence: bigint): Uint8Array {
  return toBinary(
    StrategyEventSchema,
    create(StrategyEventSchema, { header: { sequence } }),
  );
}

/**
 * A streaming reply over `frames`. The body is handed back so a test can
 * assert the reader was released once the generator finished or was
 * abandoned.
 */
function streamReply(frames: Uint8Array[]): {
  reply: Response;
  body: ReadableStream<Uint8Array>;
} {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of frames) controller.enqueue(chunk);
      controller.close();
    },
  });
  return { reply: { status: 200, body } as unknown as Response, body };
}

/** A unary reply whose whole body is `bytes`. */
function unaryReply(bytes: Uint8Array): Response {
  return {
    status: 200,
    arrayBuffer: async () =>
      bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer,
  } as unknown as Response;
}

/** A session bound to `reply`, with no lease negotiated. */
function streamingSession(reply: Response): Session {
  const session = Session.create(
    "http://worker.local",
    (async () => reply) as unknown as typeof fetch,
  );
  session.sessionId = "sess-1";
  return session;
}

/** Consume an async iterable into an array. */
async function drain<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const item of iterable) out.push(item);
  return out;
}

test("a clean end-of-stream ends the stream without an error", async () => {
  const { reply, body } = streamReply([
    frame(FLAG_MESSAGE, eventBytes(1n)),
    frame(
      FLAG_END_OF_STREAM,
      new TextEncoder().encode('{"error":null,"metadata":{}}'),
    ),
  ]);
  const events = await drain(streamingSession(reply).streamEvents());
  assert.equal(events.length, 1);
  assert.equal(events[0]?.header?.sequence, 1n);
  assert.equal(body.locked, false, "a finished stream must release the reader");
});

test("a sequence gap is reported as a null item", async () => {
  const { reply } = streamReply([
    frame(FLAG_MESSAGE, eventBytes(1n)),
    frame(FLAG_MESSAGE, eventBytes(3n)),
  ]);
  const events = await drain(streamingSession(reply).streamEvents());
  assert.equal(events.length, 3);
  assert.equal(events[1], null, "1 -> 3 missed event 2");
});

test("an end-of-stream error frame is thrown, not swallowed", async () => {
  // Connect answers a rejected subscription with HTTP 200 and puts the reason
  // in the final end-of-stream frame and nowhere else.
  const { reply } = streamReply([
    frame(FLAG_MESSAGE, eventBytes(1n)),
    frame(
      FLAG_END_OF_STREAM,
      new TextEncoder().encode(
        '{"error":{"code":"unauthenticated","message":"token expired"}}',
      ),
    ),
  ]);
  await assert.rejects(
    () => drain(streamingSession(reply).streamEvents()),
    (err: unknown) =>
      err instanceof ConnectError &&
      err.status === 200 &&
      err.code === "unauthenticated" &&
      err.message.includes("token expired"),
  );
});

test("a frame above the length bound is a protocol error", async () => {
  // The length is an untrusted u32; a 4 GiB claim must be refused before the
  // decoder buffers toward it.
  const header = new Uint8Array(5);
  header[0] = FLAG_MESSAGE;
  new DataView(header.buffer).setUint32(1, (64 << 20) + 1, false);
  const { reply } = streamReply([header]);
  await assert.rejects(
    () => drain(streamingSession(reply).streamEvents()),
    /frame exceeds the maximum length/,
  );
});

test("breaking out of a stream releases the body reader", async () => {
  const { reply, body } = streamReply([
    frame(FLAG_MESSAGE, eventBytes(1n)),
    frame(FLAG_MESSAGE, eventBytes(2n)),
  ]);
  for await (const event of streamingSession(reply).streamEvents()) {
    assert.equal(event?.header?.sequence, 1n);
    break;
  }
  assert.equal(
    body.locked,
    false,
    "an abandoned stream must not keep the body locked",
  );
});

test("attach negotiates through the injected fetch", async () => {
  const urls: string[] = [];
  const fetchImpl = (async (input: string) => {
    urls.push(input);
    return unaryReply(
      toBinary(
        AttachSessionResponseSchema,
        create(AttachSessionResponseSchema, {
          sessionId: "sess-9",
          heartbeatIntervalMs: 500,
          capabilities: ["orders"],
        }),
      ),
    );
  }) as unknown as typeof fetch;
  const session = await Session.attach(
    "http://worker.local/",
    "token",
    undefined,
    "",
    fetchImpl,
  );
  assert.equal(urls.length, 1);
  assert.ok(urls[0]?.endsWith("/AttachSession"));
  assert.equal(session.sessionId, "sess-9");
  assert.equal(session.heartbeatIntervalMs, 500);
});

// -- ports: order specs, bracket prices, channel grouping -------------------

/** A stand-in Session recording the market-stream calls it receives. */
function recordingMarketSession(): {
  calls: Array<[string[], string]>;
  session: Session;
} {
  const calls: Array<[string[], string]> = [];
  const session = {
    streamMarketData: async function* (symbols: string[], channel: string) {
      calls.push([symbols, channel]);
      yield create(MarketDataEventSchema, { resumeToken: channel });
    },
  } as unknown as Session;
  return { calls, session };
}

test("subscribeMarketData forwards one channel per call, unfolded", async () => {
  // The port used to take a list of per-symbol channels and fold them onto the
  // first one -- a TRADES symbol silently subscribed as ORDERBOOK. One channel
  // per call (the Go SDK's shape) makes that unrepresentable.
  const { calls, session } = recordingMarketSession();
  const events = await drain(
    new SessionMarketPort(session).subscribeMarketData(
      ["BTC/USDT", "ETH/USDT"],
      StreamChannel.ORDERBOOK,
    ),
  );
  assert.deepEqual(calls, [[["BTC/USDT", "ETH/USDT"], "ORDERBOOK"]]);
  assert.deepEqual(
    events.map((event) => event.resumeToken),
    ["ORDERBOOK"],
  );
});

test("an omitted channel subscribes to TICKER", async () => {
  const { calls, session } = recordingMarketSession();
  await drain(new SessionMarketPort(session).subscribeMarketData(["BTC/USDT"]));
  assert.deepEqual(calls, [[["BTC/USDT"], "TICKER"]]);
});

test("an unknown channel is rejected, not defaulted to TICKER", async () => {
  const { session } = recordingMarketSession();
  await assert.rejects(
    () =>
      drain(
        new SessionMarketPort(session).subscribeMarketData(
          ["BTC/USDT"],
          99 as StreamChannel,
        ),
      ),
    /unknown StreamChannel 99/,
  );
});

test("the trading port takes an OrderSpec, not a forged OrderRequest", async () => {
  const seen: OrderSpec[] = [];
  const session = {
    createOrder: async (spec: OrderSpec) => {
      seen.push(spec);
      return {} as unknown as Order;
    },
  } as unknown as Session;
  await new SessionTradingPort(session).createOrder({
    symbol: "BTC/USDT",
    amount: 1,
    timeInForce: TimeInForce.IOC,
  });
  assert.equal(seen[0]?.timeInForce, TimeInForce.IOC);
});

test("the batch port forwards order specs unchanged", async () => {
  const seen: OrderSpec[][] = [];
  const session = {
    createOrders: async (specs: OrderSpec[]) => {
      seen.push(specs);
      return [] as Order[];
    },
  } as unknown as Session;
  const specs: OrderSpec[] = [
    { symbol: "BTC/USDT", amount: 1 },
    { symbol: "ETH/USDT", amount: 2 },
  ];
  await new SessionTradingPort(session).batchCreateOrders(specs);
  assert.deepEqual(seen[0], specs);
});

test("modifyPosition forwards bracket prices without a cast", async () => {
  const seen: Array<{
    positionId: string;
    takeProfit?: unknown;
    stopLoss?: unknown;
  }> = [];
  const session = {
    modifyPosition: async (
      positionId: string,
      opts: { takeProfit?: unknown; stopLoss?: unknown },
    ) => {
      seen.push({ positionId, ...opts });
      return {};
    },
  } as unknown as Session;
  await new SessionTradingPort(session).modifyPosition(
    "pos-1",
    "105000",
    "90000",
  );
  assert.deepEqual(seen[0], {
    positionId: "pos-1",
    takeProfit: "105000",
    stopLoss: "90000",
  });
});

test("a GTD time in force reaches the wire from the generated enum", async () => {
  const s = bareSession("ACTIVE");
  const msg = await capture(
    s,
    () =>
      s.createOrder({
        symbol: "BTC/USDT",
        amount: 1,
        timeInForce: TimeInForce.GTD,
      }),
    (b) => fromBinary(CreateOrderRequestSchema, b),
    () => create(CreateOrderResponseSchema, {}),
  );
  assert.equal(msg.order?.timeInForce, TimeInForce.GTD);
});

test("POST_ONLY is not a TimeInForce value in the contract", () => {
  // The proto spells GTC/IOC/FOK/GTD and carries post_only as its own bool
  // field, so the hand-written union that accepted "POST_ONLY" is gone. This
  // is a compile-time assertion, checked by `tsc --noEmit`.
  const spec: OrderSpec = {
    symbol: "BTC/USDT",
    amount: 1,
    // @ts-expect-error POST_ONLY is not a TimeInForce value.
    timeInForce: "POST_ONLY",
  };
  void spec;
});
