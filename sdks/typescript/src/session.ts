/**
 * Session: the single hand-written entry point of the TypeScript SDK.
 * Mirrors sdks/python/longtrader_sdk/session.py 1:1 (design doc section 6.8).
 *
 * Wire format (see ../../docs/bare-protocol-guide.md):
 *   POST {base_url}/longtrader.worker.v1.WorkerSessionService/{Method}
 *   Content-Type: application/proto (unary) or application/connect+proto (streaming)
 *   Body: raw protobuf request; 200 body is the raw protobuf response.
 */
import {
  create,
  fromBinary,
  toBinary,
  type DescEnum,
  type DescMessage,
  type Message,
  type MessageShape,
} from "@bufbuild/protobuf";
// Node 18+ and browsers expose a global `fetch`. Importing `undici` directly is
// avoided: undici v8 constructs a CacheStorage at module load that calls
// `util.markAsUncloneable`, which is absent on Node 20.20.2 and crashes the SDK
// at import time. The platform fetch is identical for the raw-protobuf POSTs
// this session makes.
const fetch = globalThis.fetch;
import {
  AttachSessionRequestSchema,
  AttachSessionResponseSchema,
  KeepAliveRequestSchema,
  KeepAliveResponseSchema,
  LogEventSchema,
  ReconcileStateRequestSchema,
  ReconcileStateResponseSchema,
  ReportLogResponseSchema,
  RegisterStrategyRequestSchema,
  RegisterStrategyResponseSchema,
  SetKillSwitchPolicyRequestSchema,
  SetKillSwitchPolicyResponseSchema,
  StopStrategyRequestSchema,
  StopStrategyResponseSchema,
  StrategyStatusRequestSchema,
  StrategyStatusResponseSchema,
  StreamStrategyEventsRequestSchema,
  StrategyEventSchema,
  SessionState as ProtoSessionState,
  LogLevel as ProtoLogLevel,
  type KillSwitchPolicy,
  type StrategyEvent,
} from "./gen/longtrader/worker/v1/worker_pb.js";
import {
  CreateOrderRequestSchema,
  CreateOrderResponseSchema,
  CreateOrdersRequestSchema,
  CreateOrdersResponseSchema,
  CancelOrderRequestSchema,
  CancelOrderResponseSchema,
  CancelAllOrdersRequestSchema,
  CancelAllOrdersResponseSchema,
  FetchOpenOrdersRequestSchema,
  FetchOpenOrdersResponseSchema,
  GetAccountRequestSchema,
  GetAccountResponseSchema,
  GetPositionsRequestSchema,
  GetPositionsResponseSchema,
  GetOrderHistoryRequestSchema,
  GetOrderHistoryResponseSchema,
  GetClosedPositionsRequestSchema,
  GetClosedPositionsResponseSchema,
  ClosePositionRequestSchema,
  ClosePositionResponseSchema,
  CloseAllPositionsRequestSchema,
  CloseAllPositionsResponseSchema,
  ModifyPositionRequestSchema,
  ModifyPositionResponseSchema,
  OrderRequestSchema,
  OrderSide,
  OrderType,
  TimeInForce,
  type Order,
} from "./gen/longtrader/trading/v1/trading_pb.js";
import {
  FetchTickerRequestSchema,
  FetchTickerResponseSchema,
  FetchOrderBookRequestSchema,
  FetchOrderBookResponseSchema,
  GetCandlesRequestSchema,
  GetCandlesResponseSchema,
  ListSymbolsRequestSchema,
  ListSymbolsResponseSchema,
  Timeframe,
  StreamMarketDataRequestSchema,
  StreamSubscriptionSchema,
  StreamChannel,
  MarketDataEventSchema,
  type MarketDataEvent,
} from "./gen/longtrader/market/v1/market_pb.js";
import {
  DecimalSchema,
  ExchangeIdSchema,
  PaginationSchema,
  type Decimal,
} from "./gen/longtrader/common/v1/types_pb.js";

export const WORKER_SERVICE = "longtrader.worker.v1.WorkerSessionService";
export const TRADING_SERVICE = "longtrader.trading.v1.TradingService";
export const MARKET_SERVICE = "longtrader.market.v1.MarketDataService";

/**
 * Server-enforced lifecycle states (proto/longtrader/worker/v1/worker.proto).
 * `KILL_SWITCH_TRIPPED` and `GRACEFUL_SHUTDOWN` are terminal: a strategy that
 * sees either must not trade again on this session.
 */
export type SessionState =
  | "DISCONNECTED"
  | "ATTACHED"
  | "SYNCING"
  | "ACTIVE"
  | "KILL_SWITCH_TRIPPED"
  | "GRACEFUL_SHUTDOWN";

/** States from which no further transition is possible. */
export const TERMINAL_STATES: ReadonlySet<SessionState> = new Set([
  "KILL_SWITCH_TRIPPED",
  "GRACEFUL_SHUTDOWN",
]);

/**
 * Stable reason the host returns when a session-scoped order is submitted
 * before the session reaches ACTIVE (see
 * `SessionManager::authorize_order_submission`).
 */
export const SYNC_IN_PROGRESS = "SYNC_IN_PROGRESS";

/**
 * Connect streaming frame flags: 0x00 = message, 0x02 = end-of-stream (JSON).
 * See docs/bare-protocol-guide.md and `bin/longtrader-worker/src/envelope.rs`.
 */
const FLAG_END_OF_STREAM = 0x02;

/** Wrap a protobuf payload in the 5-byte Connect envelope. */
function envelope(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + payload.length);
  out[0] = 0x00; // message frame
  new DataView(out.buffer).setUint32(1, payload.length, false); // big-endian
  out.set(payload, 5);
  return out;
}

/**
 * Decode a Connect protobuf stream into messages, reporting sequence gaps.
 *
 * Yields `null` where the sequence jumped, which means the host's ring buffer
 * overflowed and the caller must re-run `reconcileState()` rather than assume
 * continuity.
 */
async function* iterEvents<S extends DescMessage>(
  body: ReadableStream<Uint8Array> | null,
  schema: S,
  gapAware: boolean,
): AsyncGenerator<MessageShape<S> | null> {
  if (body === null) return;
  const reader = body.getReader();
  let buf = new Uint8Array(0);
  let prev = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (value) {
      const merged = new Uint8Array(buf.length + value.length);
      merged.set(buf);
      merged.set(value, buf.length);
      buf = merged;
    }
    for (;;) {
      if (buf.length < 5) break;
      const flags = buf[0] ?? 0;
      const length = new DataView(
        buf.buffer,
        buf.byteOffset,
        buf.byteLength,
      ).getUint32(1, false);
      if (buf.length < 5 + length) break;
      const payload = buf.slice(5, 5 + length);
      buf = buf.slice(5 + length);
      if ((flags & FLAG_END_OF_STREAM) !== 0) {
        await reader.cancel();
        return;
      }
      const msg = fromBinary(schema, payload);
      if (gapAware) {
        const header = (msg as { header?: { sequence?: bigint } }).header;
        const seq = Number(header?.sequence ?? 0n);
        if (seq > 0) {
          if (prev > 0 && seq !== prev + 1) yield null;
          prev = seq;
        }
      }
      yield msg;
    }
    if (done) return;
  }
}

/**
 * A decimal-ish input the SDK accepts.
 *
 * `string` and `bigint` are exact. `number` is an f64, so its digits are
 * *already rounded before the SDK ever sees the value*: the SDK renders it as
 * the shortest decimal that round-trips to it, which invents and drops nothing,
 * but it cannot recover digits the f64 already lost. Pass a `string` when a
 * price has to be exact -- over a 96-bit coefficient and 28 decimal places it
 * usually has to be.
 */
export type DecimalLike = bigint | number | string;

/**
 * Fractional digits the contract's decimal keeps. A 29th would be accepted by a
 * rounding parser and silently altered, so the grammar stops at the ceiling.
 */
const DECIMAL_MAX_SCALE = 28;

/**
 * Widest coefficient the contract's decimal holds: a 96-bit unsigned integer,
 * exactly `rust_decimal`'s `Decimal::MAX`.
 */
const DECIMAL_MAX_MANTISSA = 79228162514264337593543950335n;

/**
 * Build the contract `Decimal`, whose one field is the number in base 10.
 *
 * There is deliberately no numeric companion beside it: a second representation
 * is what turned "which one is authoritative?" into a question every reader had
 * to answer, and it is why the retired int64 mantissa had to under-power the
 * host decimal's own 96-bit coefficient -- anything wider had to fall back to a
 * string anyway.
 *
 * The payload is validated against the grammar the message documents, so every
 * value rejected here is one the host would reject too, and a bad literal
 * surfaces here instead of as a rejected RPC.
 */
export function toDecimal(
  value: DecimalLike,
): MessageShape<typeof DecimalSchema> {
  return create(DecimalSchema, { value: renderDecimalText(value) });
}

/**
 * The payload of a contract `Decimal`, exactly as it arrived.
 *
 * This is the only exact view of a contract decimal in this SDK, and it is what
 * to use whenever the value must not lose a digit: the contract carries a 96-bit
 * coefficient over 28 decimal places, which an f64 cannot hold, so the payload
 * text is the value. A blank or out-of-grammar payload throws rather than
 * answering 0 -- presence lives on the containing field, so a writer that
 * populated nothing has not sent a price of zero.
 */
export function decimalText(value: Decimal | undefined): string {
  if (value === undefined) {
    throw new Error(
      "decimal is absent: check the containing field's presence first",
    );
  }
  validateDecimalText(value.value);
  return value.value;
}

/**
 * The payload of a contract `Decimal` as a JS number.
 *
 * Convenience arithmetic only, and correctly rounded rather than exact: a
 * decimal the contract can carry does not always survive the f64. Never compare,
 * accumulate or deduplicate prices through this -- use {@link decimalText}, which
 * keeps every digit.
 */
export function decimalNumber(value: Decimal | undefined): number {
  return Number(decimalText(value));
}

/**
 * The payload an input encodes to, validated.
 *
 * A string is already the wire form, so it is carried verbatim: an exponent or a
 * digit separator in it is a typo worth reporting, not something to quietly
 * re-render as a different number, and trailing zeros are part of the value --
 * the host's "1.100" and "1.1" are different decimals. A bigint is exact. A
 * number is an f64 and is rendered as described on {@link renderDouble}.
 */
function renderDecimalText(value: DecimalLike): string {
  const text =
    typeof value === "string"
      ? value
      : typeof value === "bigint"
        ? value.toString()
        : renderDouble(value);
  validateDecimalText(text);
  return text;
}

/**
 * Render an f64 as the base-10 payload the contract carries.
 *
 * `String(n)` is the shortest decimal that round-trips to `n`, which is what
 * makes this faithful rather than lossy; exponent notation is then expanded
 * positionally, because `String` switches to it outside [1e-7, 1e21) -- exactly
 * where a satoshi-denominated price lives, so `1e-7` must become `0.0000001`
 * rather than the `1e-7` the grammar forbids. What the f64 already lost is not
 * recoverable here: that is what {@link DecimalLike} documents.
 */
function renderDouble(value: number): string {
  if (!Number.isFinite(value)) {
    throw new Error(`decimal must be finite, got ${String(value)}`);
  }
  return expandExponent(String(value));
}

/** Rewrite `1e-7` as `0.0000001`, leaving `123.456` alone. */
function expandExponent(text: string): string {
  const sign = text.startsWith("-") ? "-" : "";
  // The exponent is located in the sign-stripped body, so its index cannot
  // drift by the sign the leading slice removed.
  const body = text.slice(sign.length);
  const at = body.indexOf("e");
  if (at < 0) return text;
  const parts = body.slice(0, at).split(".");
  const whole = parts[0] ?? "";
  const fraction = parts[1] ?? "";
  const exponent = Number(body.slice(at + 1));
  const digits = whole + fraction;
  const point = whole.length + exponent;
  if (point <= 0) return `${sign}0.${"0".repeat(-point)}${digits}`;
  if (point >= digits.length) {
    return `${sign}${digits}${"0".repeat(point - digits.length)}`;
  }
  return `${sign}${digits.slice(0, point)}.${digits.slice(point)}`;
}

/** Throw unless `text` is a payload the host would accept. */
function validateDecimalText(text: string): void {
  // Blank is its own case: it is the shape a message nobody populated has, so
  // it says "the writer populated nothing" rather than "the payload was garbage".
  if (text.trim() === "") {
    throw new Error(
      `decimal payload ${JSON.stringify(text)} is empty: a value is carried by ` +
        "the containing field's presence",
    );
  }
  // Named before the digit checks so the diagnosis points at the actual
  // surprise rather than at "expected only digits".
  if (text.includes("_")) {
    throw decimalGrammarError(text, "digit separators are not accepted");
  }
  if (/[eE]/.test(text)) {
    throw decimalGrammarError(text, "exponents are not accepted");
  }
  if (text.startsWith("+")) {
    throw decimalGrammarError(text, "a leading `+` is not accepted");
  }

  const body = text.startsWith("-") ? text.slice(1) : text;
  const point = body.indexOf(".");
  const whole = point < 0 ? body : body.slice(0, point);
  const fraction = point < 0 ? "" : body.slice(point + 1);
  if (whole === "") {
    throw decimalGrammarError(
      text,
      "expected at least one digit before the decimal point",
    );
  }
  if (!isDigits(whole)) {
    throw decimalGrammarError(
      text,
      "expected only digits before the decimal point",
    );
  }
  if (point >= 0) {
    if (fraction === "") {
      throw decimalGrammarError(
        text,
        "expected at least one digit after the decimal point",
      );
    }
    if (!isDigits(fraction)) {
      throw decimalGrammarError(
        text,
        "expected only digits after the decimal point",
      );
    }
    if (fraction.length > DECIMAL_MAX_SCALE) {
      throw decimalGrammarError(
        text,
        "more fractional digits than a decimal can hold",
      );
    }
  }

  // Dropping the point leaves the coefficient, which has to fit the 96 bits the
  // contract's decimal holds. This is the one check a grammar cannot make,
  // because "79228162514264337593543950336" looks like any other integer.
  if (BigInt(whole + fraction) > DECIMAL_MAX_MANTISSA) {
    throw new Error(
      `decimal ${JSON.stringify(text)} is out of range for a decimal: the ` +
        "mantissa needs more than the 96 bits a decimal holds",
    );
  }
}

function decimalGrammarError(text: string, reason: string): Error {
  return new Error(
    `decimal ${JSON.stringify(text)} is not a base-10 decimal: ${reason}`,
  );
}

/** ASCII `0`-`9` only: `\d` and `\w` would also accept other Unicode digits. */
function isDigits(text: string): boolean {
  return /^[0-9]+$/.test(text);
}

/** Build the contract `ExchangeId`; empty leaves the host default unset. */
function exchangeId(
  value: string,
): MessageShape<typeof ExchangeIdSchema> | undefined {
  return value ? create(ExchangeIdSchema, { id: value }) : undefined;
}

/** Build a `Pagination`; returns undefined when unconstrained. */
function pagination(
  limit = 0,
  since = 0,
  cursor = "",
): MessageShape<typeof PaginationSchema> | undefined {
  if (!limit && !since && !cursor) return undefined;
  return create(PaginationSchema, {
    limit: BigInt(limit),
    since: BigInt(since),
    cursor,
  });
}

/** Resolve `"LIMIT"` / `"ORDER_TYPE_LIMIT"` / `"limit"` against a proto enum. */
function enumValue<T extends Record<string, string | number>>(
  values: T,
  name: string,
  kind: string,
): T[keyof T] {
  const key = name
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, "_");
  const match = Object.entries(values).find(
    ([k]) =>
      k.toUpperCase() === key ||
      k.toUpperCase() === `${kind.toUpperCase()}_${key}`,
  );
  if (match === undefined) {
    throw new Error(
      `unknown ${kind} ${name}; expected one of ${Object.keys(values)
        .filter((k) => k !== "UNSPECIFIED")
        .join(", ")}`,
    );
  }
  return match[1] as T[keyof T];
}

/** A Connect error reply: non-200 unary response with a JSON body. */
export class ConnectError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    messageText: string,
    readonly details?: unknown,
  ) {
    super(`${code}: ${messageText} (http ${status})`);
  }
}

/** Options for {@link Session.createOrder} and friends. */
export interface OrderSpec {
  symbol: string;
  amount: DecimalLike;
  price?: DecimalLike;
  side?: "BUY" | "SELL";
  orderType?: "LIMIT" | "MARKET" | "STOP" | "STOP_LIMIT";
  timeInForce?: "GTC" | "IOC" | "FOK" | "POST_ONLY";
  clientOrderId?: string;
  postOnly?: boolean;
  reduceOnly?: boolean;
}

export class Session {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private watchdogTimer: ReturnType<typeof setInterval> | undefined;
  private lastKeepAliveOk = Date.now();
  private _state: SessionState = "DISCONNECTED";
  sessionId = "";
  heartbeatIntervalMs = 0;
  /**
   * Watermark of the last `reconcileState`, used to discard replayed deltas
   * the snapshot already covers.
   */
  snapshotSequence = 0n;
  /** Capabilities the host advertised at attach. */
  capabilities: string[] = [];

  private constructor(baseUrl: string, fetchImpl: typeof fetch) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.fetchImpl = fetchImpl;
  }

  /**
   * Validate the terminal API token and negotiate lease parameters.
   *
   * `sessionId` resumes a previous session after a network drop: the host
   * reuses it (refreshing the lease) rather than issuing a new one, which is
   * what keeps the kill-switch's tracked-order set intact across a reconnect.
   */
  static async attach(
    baseUrl: string,
    token: string,
    policy?: KillSwitchPolicy,
    sessionId = "",
  ): Promise<Session> {
    const req = create(AttachSessionRequestSchema, {
      token,
      clientName: "longtrader-sdk-typescript",
      clientVersion: "0.2.0",
      sessionId,
      ...(policy ? { policy } : {}),
    });
    const session = new Session(baseUrl, fetch);
    const resp = await session.unary(
      WORKER_SERVICE,
      "AttachSession",
      AttachSessionResponseSchema,
      toBinary(AttachSessionRequestSchema, req),
    );
    session.sessionId = resp.sessionId;
    // A resumed session keeps its negotiated lease rather than resetting to 0,
    // which would stall the heartbeat and watchdog.
    session.heartbeatIntervalMs =
      resp.heartbeatIntervalMs || session.heartbeatIntervalMs;
    session.capabilities = [...resp.capabilities];
    session.lastKeepAliveOk = Date.now();
    session._state = "ATTACHED";
    return session;
  }

  /** Local view of the lifecycle state (ATTACHED/SYNCING/ACTIVE/...). */
  get state(): SessionState {
    return this._state;
  }

  /** True once the session has reconciled and may trade. */
  get canTrade(): boolean {
    return this._state === "ACTIVE";
  }

  /** Feed the lease watchdog; call at the negotiated interval. */
  async keepAlive(): Promise<MessageShape<typeof KeepAliveResponseSchema>> {
    const req = create(KeepAliveRequestSchema, {
      sessionId: this.sessionId,
      clientTimeNs: BigInt(Date.now()) * 1_000_000n,
    });
    const resp = await this.unary(
      WORKER_SERVICE,
      "KeepAlive",
      KeepAliveResponseSchema,
      toBinary(KeepAliveRequestSchema, req),
    );
    this.lastKeepAliveOk = Date.now();
    return resp;
  }

  /**
   * Fetch the authoritative atomic snapshot (balances/positions/open orders)
   * stamped with `snapshotSequence`. The host drives ATTACHED -> SYNCING
   * inside this call, so the local state is set first: a concurrent reader
   * then sees SYNCING and refuses to trade rather than racing the snapshot.
   */
  async reconcileState(): Promise<
    MessageShape<typeof ReconcileStateResponseSchema>
  > {
    const req = create(ReconcileStateRequestSchema, {
      sessionId: this.sessionId,
    });
    this._state = "SYNCING";
    const resp = await this.unary(
      WORKER_SERVICE,
      "ReconcileState",
      ReconcileStateResponseSchema,
      toBinary(ReconcileStateRequestSchema, req),
    );
    this._state = "ACTIVE";
    this.snapshotSequence = resp.snapshotSequence;
    return resp;
  }

  /** Parameterize cancel-on-disconnect behavior for this session. */
  async setKillSwitchPolicy(policy: KillSwitchPolicy): Promise<void> {
    const req = create(SetKillSwitchPolicyRequestSchema, {
      sessionId: this.sessionId,
      policy,
    });
    await this.unary(
      WORKER_SERVICE,
      "SetKillSwitchPolicy",
      SetKillSwitchPolicyResponseSchema,
      toBinary(SetKillSwitchPolicyRequestSchema, req),
    );
  }

  // -- strategy lifecycle -------------------------------------------------

  /** Register this session's strategy; returns the assigned strategy id. */
  async registerStrategy(
    name: string,
    params: Record<string, string> = {},
  ): Promise<string> {
    const req = create(RegisterStrategyRequestSchema, {
      sessionId: this.sessionId,
      name,
      params,
    });
    const resp = await this.unary(
      WORKER_SERVICE,
      "RegisterStrategy",
      RegisterStrategyResponseSchema,
      toBinary(RegisterStrategyRequestSchema, req),
    );
    return resp.strategyId;
  }

  /** Current lifecycle state, strategy id, and submission counters. */
  async strategyStatus(): Promise<
    MessageShape<typeof StrategyStatusResponseSchema>
  > {
    const req = create(StrategyStatusRequestSchema, {
      sessionId: this.sessionId,
    });
    const resp = await this.unary(
      WORKER_SERVICE,
      "StrategyStatus",
      StrategyStatusResponseSchema,
      toBinary(StrategyStatusRequestSchema, req),
    );
    this.syncStateFromHost(resp.state);
    return resp;
  }

  /** Stop the strategy, optionally cancelling this session's open orders. */
  async stopStrategy(
    cancelOpenOrders = true,
  ): Promise<MessageShape<typeof StopStrategyResponseSchema>> {
    const req = create(StopStrategyRequestSchema, {
      sessionId: this.sessionId,
      cancelOpenOrders,
    });
    const resp = await this.unary(
      WORKER_SERVICE,
      "StopStrategy",
      StopStrategyResponseSchema,
      toBinary(StopStrategyRequestSchema, req),
    );
    this.syncStateFromHost(resp.finalState);
    return resp;
  }

  /**
   * Send one log event; resolves to the number of events the host accepted.
   *
   * Client-streaming: the request is a 5-byte-framed protobuf message and the
   * reply is one data frame followed by an end-of-stream JSON frame.
   */
  async reportLog(
    level: "DEBUG" | "INFO" | "WARN" | "ERROR",
    message: string,
    fields: Record<string, string> = {},
  ): Promise<bigint> {
    const now = new Date();
    const req = create(LogEventSchema, {
      sessionId: this.sessionId,
      level: enumValue(
        ProtoLogLevel as unknown as Record<string, number>,
        level,
        "LogLevel",
      ),
      message,
      timestamp: {
        seconds: BigInt(Math.floor(now.getTime() / 1000)),
        nanos: (now.getTime() % 1000) * 1_000_000,
      },
      fields,
    });
    const res = await this.fetchImpl(
      `${this.baseUrl}/${WORKER_SERVICE}/ReportLog`,
      {
        method: "POST",
        headers: {
          "content-type": "application/connect+proto",
          "connect-protocol-version": "1",
        },
        body: envelope(toBinary(LogEventSchema, req)),
      },
    );
    if (res.status !== 200) throw await toConnectError(res);
    const frame = firstMessagePayload(new Uint8Array(await res.arrayBuffer()));
    return fromBinary(ReportLogResponseSchema, frame).accepted;
  }

  /**
   * Yield `StrategyEvent` messages, resuming after `resumeToken`.
   *
   * A yielded `null` marks a sequence gap: the host's ring overflowed and the
   * caller must re-run `reconcileState()` instead of assuming continuity.
   */
  async *streamEvents(resumeToken = ""): AsyncGenerator<StrategyEvent | null> {
    const req = create(StreamStrategyEventsRequestSchema, {
      sessionId: this.sessionId,
      resumeToken,
    });
    const res = await this.fetchImpl(
      `${this.baseUrl}/${WORKER_SERVICE}/StreamStrategyEvents`,
      {
        method: "POST",
        headers: {
          "content-type": "application/connect+proto",
          "connect-protocol-version": "1",
        },
        body: envelope(toBinary(StreamStrategyEventsRequestSchema, req)),
      },
    );
    if (res.status !== 200) throw await toConnectError(res);
    yield* iterEvents<typeof StrategyEventSchema>(
      res.body,
      StrategyEventSchema,
      true,
    );
  }

  // -- heartbeat ----------------------------------------------------------

  /** Spawn an interval timer sending KeepAlive at the negotiated interval. */
  startHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) return;
    const ms = Math.max(this.heartbeatIntervalMs, 500);
    this.heartbeatTimer = setInterval(() => {
      // Transient failures are fine; the next tick retries.
      void this.keepAlive().catch(() => {});
    }, ms);
    // Never hold the process open on the SDK's account: a leaked session in a
    // long-running host must not block graceful shutdown.
    this.heartbeatTimer.unref?.();
  }

  /** Cancel the background heartbeat / watchdog timers. */
  stop(): void {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    if (this.watchdogTimer !== undefined) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = undefined;
    }
  }

  /** Close the session and release timers (mirrors Python `Session.close`). */
  close(): void {
    this.stop();
  }

  /**
   * Strategy-side lease guard: a timer that checks every `heartbeatInterval`
   * whether the lease has lapsed. On timeout it stops this strategy trading
   * and asks the host to cancel the session's orders.
   *
   * The host's own watchdog is the authority for the cancellation; this tier
   * exists so the process stops trading even if the host is unreachable.
   */
  spawnLeaseWatchdog(leaseTimeoutMs?: number): ReturnType<typeof setInterval> {
    const leaseMs =
      leaseTimeoutMs ?? Math.max(this.heartbeatIntervalMs * 3, 1500);
    const intervalMs = Math.max(this.heartbeatIntervalMs, 500);
    if (this.watchdogTimer !== undefined) {
      clearInterval(this.watchdogTimer);
    }
    this.watchdogTimer = setInterval(() => {
      if (Date.now() - this.lastKeepAliveOk <= leaseMs) return;
      this._state = "KILL_SWITCH_TRIPPED";
      void this.stopStrategy(true).catch(() => {});
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = undefined;
    }, intervalMs);
    this.watchdogTimer.unref?.();
    return this.watchdogTimer;
  }

  // -- trading ------------------------------------------------------------

  private requireActive(): void {
    if (this._state !== "ACTIVE") {
      throw new ConnectError(
        412,
        "failed_precondition",
        `${SYNC_IN_PROGRESS}: local session is ${this._state}, not ACTIVE; ` +
          "call reconcileState() first",
      );
    }
  }

  /**
   * Place one order, attributed to this session.
   *
   * The `sessionId` is what lets the host reject a pre-ACTIVE submission
   * (`SYNC_IN_PROGRESS`) and record the order so the kill-switch can cancel it.
   */
  async createOrder(spec: OrderSpec): Promise<Order> {
    this.requireActive();
    const req = create(CreateOrderRequestSchema, {
      order: buildOrderRequest(spec),
      sessionId: this.sessionId,
    });
    const resp = await this.unary(
      TRADING_SERVICE,
      "CreateOrder",
      CreateOrderResponseSchema,
      toBinary(CreateOrderRequestSchema, req),
    );
    if (resp.order === undefined)
      throw new Error("CreateOrder returned no order");
    return resp.order;
  }

  /** Place a batch, attributed to this session (all-or-nothing gate). */
  async createOrders(specs: OrderSpec[]): Promise<Order[]> {
    this.requireActive();
    const req = create(CreateOrdersRequestSchema, {
      orders: specs.map(buildOrderRequest),
      sessionId: this.sessionId,
    });
    const resp = await this.unary(
      TRADING_SERVICE,
      "CreateOrders",
      CreateOrdersResponseSchema,
      toBinary(CreateOrdersRequestSchema, req),
    );
    return [...resp.orders];
  }

  /** Cancel one order by venue order id. */
  async cancelOrder(orderId: string, symbol = ""): Promise<Order> {
    const req = create(CancelOrderRequestSchema, { orderId, symbol });
    const resp = await this.unary(
      TRADING_SERVICE,
      "CancelOrder",
      CancelOrderResponseSchema,
      toBinary(CancelOrderRequestSchema, req),
    );
    if (resp.order === undefined)
      throw new Error("CancelOrder returned no order");
    return resp.order;
  }

  /** Cancel every open order; an empty `symbol` spans all symbols. */
  async cancelAllOrders(symbol = ""): Promise<Order[]> {
    const req = create(CancelAllOrdersRequestSchema, { symbol });
    const resp = await this.unary(
      TRADING_SERVICE,
      "CancelAllOrders",
      CancelAllOrdersResponseSchema,
      toBinary(CancelAllOrdersRequestSchema, req),
    );
    return [...resp.orders];
  }

  /** Open orders; omit `symbol` for every symbol. */
  async fetchOpenOrders(symbol = "", limit = 0): Promise<Order[]> {
    const req = create(FetchOpenOrdersRequestSchema, {
      symbol,
      pagination: pagination(limit),
    });
    const resp = await this.unary(
      TRADING_SERVICE,
      "FetchOpenOrders",
      FetchOpenOrdersResponseSchema,
      toBinary(FetchOpenOrdersRequestSchema, req),
    );
    return [...resp.orders];
  }

  async getAccount() {
    return this.unary(
      TRADING_SERVICE,
      "GetAccount",
      GetAccountResponseSchema,
      toBinary(GetAccountRequestSchema, create(GetAccountRequestSchema, {})),
    );
  }

  /** Open positions; omit `symbols` for every symbol. */
  async getPositions(symbols: string[] = []) {
    const req = create(GetPositionsRequestSchema, { symbols });
    return this.unary(
      TRADING_SERVICE,
      "GetPositions",
      GetPositionsResponseSchema,
      toBinary(GetPositionsRequestSchema, req),
    );
  }

  async getOrderHistory(limit = 100, since = 0) {
    const req = create(GetOrderHistoryRequestSchema, {
      pagination: pagination(limit, since),
    });
    return this.unary(
      TRADING_SERVICE,
      "GetOrderHistory",
      GetOrderHistoryResponseSchema,
      toBinary(GetOrderHistoryRequestSchema, req),
    );
  }

  async getClosedPositions(limit = 100) {
    const req = create(GetClosedPositionsRequestSchema, {
      pagination: pagination(limit),
    });
    return this.unary(
      TRADING_SERVICE,
      "GetClosedPositions",
      GetClosedPositionsResponseSchema,
      toBinary(GetClosedPositionsRequestSchema, req),
    );
  }

  async closePosition(positionId: string) {
    const req = create(ClosePositionRequestSchema, { positionId });
    return this.unary(
      TRADING_SERVICE,
      "ClosePosition",
      ClosePositionResponseSchema,
      toBinary(ClosePositionRequestSchema, req),
    );
  }

  async closeAllPositions() {
    return this.unary(
      TRADING_SERVICE,
      "CloseAllPositions",
      CloseAllPositionsResponseSchema,
      toBinary(
        CloseAllPositionsRequestSchema,
        create(CloseAllPositionsRequestSchema, {}),
      ),
    );
  }

  /**
   * Attach or replace a position's bracket orders. Omitting one bracket leaves
   * it unchanged, so a caller can move just the stop without clearing the
   * target.
   */
  async modifyPosition(
    positionId: string,
    opts: {
      takeProfit?: DecimalLike;
      stopLoss?: DecimalLike;
    },
  ) {
    const req = create(ModifyPositionRequestSchema, {
      positionId,
      ...(opts.takeProfit !== undefined
        ? { takeProfit: toDecimal(opts.takeProfit) }
        : {}),
      ...(opts.stopLoss !== undefined
        ? { stopLoss: toDecimal(opts.stopLoss) }
        : {}),
    });
    return this.unary(
      TRADING_SERVICE,
      "ModifyPosition",
      ModifyPositionResponseSchema,
      toBinary(ModifyPositionRequestSchema, req),
    );
  }

  // -- market data --------------------------------------------------------

  async fetchTicker(symbol: string) {
    const req = create(FetchTickerRequestSchema, { symbol });
    const resp = await this.unary(
      MARKET_SERVICE,
      "FetchTicker",
      FetchTickerResponseSchema,
      toBinary(FetchTickerRequestSchema, req),
    );
    if (resp.ticker === undefined)
      throw new Error("FetchTicker returned no ticker");
    return resp.ticker;
  }

  async fetchOrderBook(symbol: string, depth = 10) {
    const req = create(FetchOrderBookRequestSchema, {
      symbol,
      pagination: pagination(depth),
    });
    const resp = await this.unary(
      MARKET_SERVICE,
      "FetchOrderBook",
      FetchOrderBookResponseSchema,
      toBinary(FetchOrderBookRequestSchema, req),
    );
    if (resp.orderbook === undefined)
      throw new Error("FetchOrderBook returned no book");
    return resp.orderbook;
  }

  /**
   * OHLCV candles. `timeframe` is the contract enum (`Timeframe.M1`,
   * `Timeframe.H4`, ...); it is no longer a string, so a typo is a type error
   * instead of a silently empty result from the backend.
   */
  async getCandles(
    symbol: string,
    timeframe: Timeframe = Timeframe.M1,
    limit = 100,
  ) {
    const req = create(GetCandlesRequestSchema, {
      symbol,
      timeframe,
      pagination: pagination(limit),
    });
    return this.unary(
      MARKET_SERVICE,
      "GetCandles",
      GetCandlesResponseSchema,
      toBinary(GetCandlesRequestSchema, req),
    );
  }

  async listSymbols() {
    return this.unary(
      MARKET_SERVICE,
      "ListSymbols",
      ListSymbolsResponseSchema,
      toBinary(ListSymbolsRequestSchema, create(ListSymbolsRequestSchema, {})),
    );
  }

  /**
   * Open a server-streaming market data subscription.
   *
   * `channel` is TICKER / ORDERBOOK / TRADES / OHLCV.
   */
  async *streamMarketData(
    symbols: string[],
    channel: "TICKER" | "ORDERBOOK" | "TRADES" | "OHLCV" = "TICKER",
  ): AsyncGenerator<MarketDataEvent> {
    const req = create(StreamMarketDataRequestSchema, {
      subscriptions: symbols.map((symbol) =>
        create(StreamSubscriptionSchema, {
          symbol,
          channel: enumValue(
            StreamChannel as unknown as Record<string, number>,
            channel,
            "StreamChannel",
          ),
        }),
      ),
    });
    const res = await this.fetchImpl(
      `${this.baseUrl}/${MARKET_SERVICE}/StreamMarketData`,
      {
        method: "POST",
        headers: {
          "content-type": "application/connect+proto",
          "connect-protocol-version": "1",
        },
        body: envelope(toBinary(StreamMarketDataRequestSchema, req)),
      },
    );
    if (res.status !== 200) throw await toConnectError(res);
    for await (const ev of iterEvents<typeof MarketDataEventSchema>(
      res.body,
      MarketDataEventSchema,
      false,
    )) {
      if (ev !== null) yield ev;
    }
  }

  // -- transport ----------------------------------------------------------

  /** One Connect unary call: POST application/proto, decode proto reply. */
  private async unary<S extends DescMessage>(
    service: string,
    method: string,
    respSchema: S,
    reqBytes: Uint8Array,
  ): Promise<MessageShape<S>> {
    const res = await this.fetchImpl(`${this.baseUrl}/${service}/${method}`, {
      method: "POST",
      headers: { "content-type": "application/proto" },
      body: reqBytes,
    });
    if (res.status !== 200) throw await toConnectError(res);
    return fromBinary(respSchema, new Uint8Array(await res.arrayBuffer()));
  }

  /** Mirror a host-reported state into the local view. */
  private syncStateFromHost(state: ProtoSessionState): void {
    const name = ProtoSessionState[state];
    if (name === undefined) return;
    this._state = name.replace("SESSION_STATE_", "") as SessionState;
  }
}

/** Build a contract `OrderRequest` from the ergonomic {@link OrderSpec}. */
function buildOrderRequest(
  spec: OrderSpec,
): MessageShape<typeof OrderRequestSchema> {
  return create(OrderRequestSchema, {
    clientOrderId: spec.clientOrderId ?? "",
    symbol: spec.symbol,
    type: enumValue(
      OrderType as unknown as Record<string, number>,
      spec.orderType ?? "LIMIT",
      "OrderType",
    ),
    side: enumValue(
      OrderSide as unknown as Record<string, number>,
      spec.side ?? "BUY",
      "OrderSide",
    ),
    amount: toDecimal(spec.amount),
    ...(spec.price !== undefined ? { price: toDecimal(spec.price) } : {}),
    timeInForce: enumValue(
      TimeInForce as unknown as Record<string, number>,
      spec.timeInForce ?? "GTC",
      "TimeInForce",
    ),
    postOnly: spec.postOnly ?? false,
    reduceOnly: spec.reduceOnly ?? false,
  });
}

/**
 * Return the first non-end-of-stream frame payload in `buf`.
 *
 * A streaming RPC that carries a single response message replies with one data
 * frame followed by an end-of-stream frame whose payload is JSON; decoding the
 * whole body as protobuf fails, so the data frame is located first.
 */
function firstMessagePayload(buf: Uint8Array): Uint8Array {
  let pos = 0;
  while (pos + 5 <= buf.length) {
    const flags = buf[pos] ?? 0;
    const length = new DataView(
      buf.buffer,
      buf.byteOffset,
      buf.byteLength,
    ).getUint32(pos + 1, false);
    const end = pos + 5 + length;
    if (end > buf.length) break;
    if ((flags & FLAG_END_OF_STREAM) === 0) return buf.slice(pos + 5, end);
    pos = end;
  }
  throw new Error("no message frame in connect response");
}

async function toConnectError(res: Response): Promise<ConnectError> {
  let code = "unknown";
  let messageText = "";
  let details: unknown;
  try {
    const body = (await res.json()) as {
      code?: string;
      message?: string;
      details?: unknown;
    };
    code = body.code ?? code;
    messageText = body.message ?? "";
    details = body.details;
  } catch {
    // Non-JSON error body; keep defaults.
  }
  return new ConnectError(res.status, code, messageText, details);
}
