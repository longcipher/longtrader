/**
 * Minimal grid strategy over the bare longtrader protocol (TypeScript twin of
 * sdks/python/examples/grid_strategy.py).
 *
 * Demonstrates every contract concept a ported strategy needs:
 *  - AttachSession with a terminal API token (the only credential system)
 *  - negotiated heartbeat feeding the session lease watchdog (kill switch)
 *  - ReconcileState before trading (ATTACHED -> SYNCING -> ACTIVE gate;
 *    orders submitted before ACTIVE are rejected SYNC_IN_PROGRESS)
 *  - OrderRequest with a mandatory ULID client_order_id (backend dedupes,
 *    making retries safe)
 *  - Decimal dual representation: raw_str here for readability; hot paths
 *    should prefer the int64 fast path (unscaled + scale)
 *  - ticker polling through the unified market surface
 *
 * Usage:
 *   just sdk-generate && cd sdks/typescript && npm install
 *   npx tsx examples/grid_strategy.ts --help
 */
import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { fetch } from "undici";
import { Session } from "../src/index.js";
import {
  CancelOrderRequestSchema,
  CreateOrdersRequestSchema,
  CreateOrdersResponseSchema,
  OrderRequest,
  OrderSide,
  OrderType,
  TimeInForce,
} from "../gen/longtrader/trading/v1/trading_pb.js";
import {
  FetchTickerRequestSchema,
  FetchTickerResponseSchema,
} from "../gen/longtrader/market/v1/market_pb.js";
import { DecimalSchema, ExchangeIdSchema } from "../gen/longtrader/common/v1/types_pb.js";
import {
  KillSwitchPolicySchema,
  KillSwitchPolicy_Scope,
} from "../gen/longtrader/worker/v1/worker_pb.js";

// Trading/market unary RPCs ride the identical Connect convention as the
// worker control plane; service names come from the deployed contract.
const MARKET_SERVICE = "longtrader.market.v1.MarketDataService";
const TRADING_SERVICE = "longtrader.trading.v1.TradingService";

// Minimal argv parsing to keep the example dependency-free.
const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1] ?? "");
const baseUrl = args.get("--base-url") ?? "http://127.0.0.1:8080";
const token = args.get("--token") ?? "";
const exchangeId = args.get("--exchange-id") ?? "";
const symbol = args.get("--symbol") ?? "BTC/USDT";
const levels = Number(args.get("--levels") ?? 3);
const stepPct = Number(args.get("--step-pct") ?? 0.1);
const amount = args.get("--amount") ?? "0.001";
const refreshSecs = Number(args.get("--refresh-secs") ?? 30);
if (!token) {
  console.error("usage: tsx examples/grid_strategy.ts --base-url URL --token TOKEN");
  process.exit(1);
}

/** 26-char uppercase id, ULID-shaped. The backend dedupes on it, so reuse
 * across retries is safe and required; swap in a real ULID lib for prod. */
function ulidLikeId(): string {
  return crypto.randomUUID().replaceAll("-", "").toUpperCase().slice(0, 26);
}

/** Build common.v1.Decimal from a string via the universal raw_str path. */
function dec(text: string) {
  return create(DecimalSchema, { rawStr: text });
}

/** Raw Connect unary call — the whole wire protocol in six lines. */
async function postUnary<S extends DescMessage>(
  service: string,
  method: string,
  schema: S,
  req: MessageShape<S>,
): Promise<MessageShape<S>> {
  const res = await fetch(`${baseUrl}/${service}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/proto" },
    body: toBinary(schema, req),
  });
  if (res.status !== 200) {
    throw new Error(`connect error ${res.status}: ${(await res.text()).slice(0, 200)}`);
  }
  return fromBinary(schema, new Uint8Array(await res.arrayBuffer()));
}

async function fetchMid(): Promise<number> {
  const resp = await postUnary(
    MARKET_SERVICE,
    "FetchTicker",
    FetchTickerResponseSchema,
    create(FetchTickerRequestSchema, {
      exchangeId: create(ExchangeIdSchema, { id: exchangeId }),
      symbol,
    }),
  );
  const num = (s: string): number | undefined => (s ? Number(s) : undefined);
  const t = resp.ticker;
  const bid = num(t.bid.rawStr);
  const ask = num(t.ask.rawStr);
  const last = num(t.last.rawStr);
  if (bid !== undefined && ask !== undefined) return (bid + ask) / 2;
  if (last !== undefined) return last;
  throw new Error(`ticker for ${symbol} has no usable price`);
}

function makeLimit(side: OrderSide, price: number): OrderRequest {
  // One grid rung as an OrderRequest (ULID client_order_id mandatory).
  const priceStr = price.toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
  return create(OrderRequestSchema, {
    clientOrderId: ulidLikeId(),
    symbol,
    type: OrderType.LIMIT,
    side,
    amount: dec(amount),
    price: dec(priceStr),
    timeInForce: TimeInForce.GTC,
    postOnly: true,
  });
}

let liveIds: string[] = [];

async function refreshGrid(mid: number): Promise<void> {
  console.log(`[grid] mid=${mid.toFixed(2)}`);
  // Cancel previous rungs first so the grid never doubles up.
  for (const orderId of liveIds) {
    await postUnary(
      TRADING_SERVICE,
      "CancelOrder",
      CancelOrderResponseSchema,
      create(CancelOrderRequestSchema, {
        exchangeId: create(ExchangeIdSchema, { id: exchangeId }),
        orderId,
        symbol,
      }),
    ).catch((err) => console.error(`cancel ${orderId} failed: ${err}`));
  }

  const orders: OrderRequest[] = [];
  for (let i = 1; i <= levels; i++) {
    const step = (stepPct / 100) * i;
    orders.push(makeLimit(OrderSide.BUY, mid * (1 - step)));
    orders.push(makeLimit(OrderSide.SELL, mid * (1 + step)));
  }
  const resp = await postUnary(
    TRADING_SERVICE,
    "CreateOrders",
    CreateOrdersResponseSchema,
    create(CreateOrdersRequestSchema, {
      exchangeId: create(ExchangeIdSchema, { id: exchangeId }),
      orders,
    }),
  );
  liveIds = resp.orders.map((o) => o.id);
  console.log(`[grid] placed ${liveIds.length} rungs`);
}

// Kill-switch policy: cancel-on-disconnect scope defaults to this session's
// orders only (SESSION_ORDERS).
const policy = create(KillSwitchPolicySchema, {
  scope: KillSwitchPolicy_Scope.SESSION_ORDERS,
});

const session = await Session.attach(baseUrl, token, policy);
console.log(`attached session=${session.sessionId} heartbeat_ms=${session.heartbeatIntervalMs} state=${session.state}`);
session.startHeartbeat();

// Recovery gate: authoritative snapshot before any order submission.
const snapshot = await session.reconcileState();
console.log(
  `reconciled seq=${snapshot.snapshotSequence} balances=${snapshot.balances.length} ` +
    `positions=${snapshot.positions.length} open_orders=${snapshot.openOrders.length} state=${session.state}`,
);

process.on("SIGINT", () => {
  console.log("\nstopping: cancelling grid rungs");
  void Promise.allSettled(
    liveIds.map((orderId) =>
      postUnary(
        TRADING_SERVICE,
        "CancelOrder",
        CancelOrderResponseSchema,
        create(CancelOrderRequestSchema, {
          exchangeId: create(ExchangeIdSchema, { id: exchangeId }),
          orderId,
          symbol,
        }),
      ),
    ),
  ).then(() => {
    session.stop();
    process.exit(0);
  });
});

while (true) {
  await refreshGrid(await fetchMid());
  await new Promise((r) => setTimeout(r, refreshSecs * 1000));
}
