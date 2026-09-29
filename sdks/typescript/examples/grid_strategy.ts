/**
 * Grid strategy driven entirely through the TypeScript SDK.
 * Twin of sdks/python/examples/grid_strategy.py.
 *
 * The point of this example is the *lifecycle*, not the grid maths. Every
 * concept a ported strategy needs is visible in one place:
 *  - `Session.attach` with a terminal API token (the only credential system)
 *  - negotiated heartbeat feeding the session lease watchdog (kill switch)
 *  - `reconcileState()` before trading: the ATTACHED -> SYNCING -> ACTIVE gate
 *  - `registerStrategy` so the host can report status and counters
 *  - order submission carrying the session id, which is what lets the host
 *    scope the kill-switch to *this* strategy's orders
 *  - `stopStrategy({ cancelOpenOrders: true })` on shutdown, the same RPC the
 *    host uses to guarantee nothing is left resting
 *
 * Every RPC below goes through the SDK. When you need a capability the SDK
 * does not wrap yet, the raw Connect call is still three lines -- see
 * docs/bare-protocol-guide.md.
 *
 * Usage:
 *   just sdk-generate && cd sdks/typescript && npm install
 *   npx tsx examples/grid_strategy.ts --base-url http://127.0.0.1:9000 --iterations 3
 */
import { randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import { Session, type OrderSpec } from "../src/index.js";
import {
  KillSwitchPolicySchema,
  KillSwitchPolicy_Scope,
} from "../src/gen/longtrader/worker/v1/worker_pb.js";

/**
 * 26-char uppercase id, ULID-shaped. The backend dedupes on
 * `client_order_id`, so reusing an id across retries is safe and required.
 * Swap in a real time-ordered ULID generator for production.
 */
function ulidLikeId(): string {
  return randomUUID().replace(/-/g, "").toUpperCase().slice(0, 26);
}

function formatPrice(price: number): string {
  return price.toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
}

interface Options {
  baseUrl: string;
  token: string;
  symbol: string;
  levels: number;
  stepPct: number;
  amount: string;
  refreshMs: number;
  iterations: number;
  leaseTimeoutSecs: number;
}

const USAGE = `usage: tsx examples/grid_strategy.ts [options]

  --base-url URL          worker control plane (default http://127.0.0.1:9000)
  --token TOKEN           terminal API token (default $LONGTRADER_TOKEN)
  --symbol SYM            instrument (default BTC/USDT)
  --levels N              rungs per side (default 3)
  --step-pct PCT          spacing between rungs (default 0.1)
  --amount QTY            order size per rung (default 0.001)
  --refresh-secs SECS     delay between refreshes (default 30)
  --iterations N          refreshes before exit; 0 runs until Ctrl-C
  --lease-timeout-secs S  kill-switch lease budget; 0 keeps the server default
  --help                  show this message`;

function parseArgs(argv: string[]): Options {
  const map = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (key === undefined || !key.startsWith("--")) continue;
    // `--flag value` and `--flag=value` are both accepted.
    const eq = key.indexOf("=");
    if (eq !== -1) {
      map.set(key.slice(0, eq), key.slice(eq + 1));
    } else {
      map.set(key, argv[i + 1] ?? "");
      i += 1;
    }
  }
  if (map.has("--help")) {
    console.log(USAGE);
    process.exit(0);
  }
  return {
    baseUrl: map.get("--base-url") ?? "http://127.0.0.1:9000",
    token: map.get("--token") ?? process.env.LONGTRADER_TOKEN ?? "",
    symbol: map.get("--symbol") ?? "BTC/USDT",
    levels: Number(map.get("--levels") ?? 3),
    stepPct: Number(map.get("--step-pct") ?? 0.1),
    amount: map.get("--amount") ?? "0.001",
    refreshMs: Number(map.get("--refresh-secs") ?? 30) * 1000,
    iterations: Number(map.get("--iterations") ?? 0),
    leaseTimeoutSecs: Number(map.get("--lease-timeout-secs") ?? 0),
  };
}

/** Poll one ticker and compute a mid price (bid -> ask -> last). */
async function fetchMid(session: Session, symbol: string): Promise<number> {
  const ticker = await session.fetchTicker(symbol);
  const bid = decimalToNumber(ticker.bid);
  const ask = decimalToNumber(ticker.ask);
  if (bid !== undefined && ask !== undefined) return (bid + ask) / 2;
  const last = decimalToNumber(ticker.last);
  if (last !== undefined) return last;
  throw new Error(`ticker for ${symbol} has no usable price`);
}

/**
 * Read a `common.v1.Decimal` into a JS number.
 *
 * The contract carries two representations and a writer populates only one:
 * the hot-path `unscaled`/`scale` pair, or the human-readable `rawStr`
 * fallback. A reader must accept both, or it silently sees zero whenever the
 * producer chose the other form.
 */
function decimalToNumber(
  value: { unscaled: bigint; scale: number; rawStr: string } | undefined,
): number | undefined {
  if (value === undefined) return undefined;
  if (value.rawStr !== "") return Number(value.rawStr);
  if (value.unscaled === 0n) return 0;
  return Number(value.unscaled) / 10 ** value.scale;
}

/** Cancel stale rungs, then batch-place a fresh grid around the mid. */
async function refreshGrid(
  session: Session,
  opts: Options,
  liveIds: string[],
): Promise<string[]> {
  const mid = await fetchMid(session, opts.symbol);
  console.log(`[grid] mid=${mid.toFixed(2)}`);

  // Cancel previous rungs first so the grid never doubles up.
  for (const orderId of liveIds) {
    try {
      await session.cancelOrder(orderId, opts.symbol);
    } catch (err) {
      console.log(`[grid] cancel ${orderId} failed: ${String(err)}`);
    }
  }

  const rungs: OrderSpec[] = [];
  for (let i = 1; i <= opts.levels; i += 1) {
    const step = (opts.stepPct / 100) * i;
    for (const [side, price] of [
      ["BUY", mid * (1 - step)],
      ["SELL", mid * (1 + step)],
    ] as const) {
      rungs.push({
        symbol: opts.symbol,
        amount: opts.amount,
        price: formatPrice(price),
        side,
        orderType: "LIMIT",
        timeInForce: "GTC",
        clientOrderId: ulidLikeId(),
        postOnly: true,
      });
    }
  }

  // One batch, one gate check: the host rejects the whole batch if the session
  // is not ACTIVE, so a grid is never half-placed.
  const placed = await session.createOrders(rungs);
  console.log(`[grid] placed ${placed.length} rungs`);
  return placed.map((o) => o.id);
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));

  // Kill-switch policy: cancel-on-disconnect scope defaults to this session's
  // orders only (SESSION_ORDERS).
  const policy = create(KillSwitchPolicySchema, {
    scope: KillSwitchPolicy_Scope.SESSION_ORDERS,
    ...(opts.leaseTimeoutSecs > 0
      ? { leaseTimeout: { seconds: BigInt(opts.leaseTimeoutSecs), nanos: 0 } }
      : {}),
  });

  const session = await Session.attach(opts.baseUrl, opts.token, policy);
  console.log(
    `attached session=${session.sessionId} heartbeatMs=${session.heartbeatIntervalMs} ` +
      `state=${session.state} capabilities=${session.capabilities.join(",")}`,
  );

  // Register before trading so host-side status and counters cover the run.
  const strategyId = await session.registerStrategy("typescript_grid", {
    symbol: opts.symbol,
    levels: String(opts.levels),
    stepPct: String(opts.stepPct),
    amount: opts.amount,
  });
  console.log(`registered strategy=${strategyId}`);

  session.startHeartbeat();
  // Local guard mirrors the host's lease watchdog: it stops this process from
  // trading past its own lease even if the host is unreachable.
  session.spawnLeaseWatchdog();

  try {
    // Recovery gate: the authoritative snapshot before any submission.
    const snapshot = await session.reconcileState();
    console.log(
      `reconciled seq=${snapshot.snapshotSequence} balances=${snapshot.balances.length} ` +
        `positions=${snapshot.positions.length} openOrders=${snapshot.openOrders.length} ` +
        `state=${session.state}`,
    );

    let liveIds: string[] = [];
    let iteration = 0;
    for (;;) {
      if (!session.canTrade) {
        console.log(`[grid] session is ${session.state}; stopping`);
        break;
      }
      liveIds = await refreshGrid(session, opts, liveIds);
      iteration += 1;
      if (opts.iterations !== 0 && iteration >= opts.iterations) break;
      await new Promise((resolve) => setTimeout(resolve, opts.refreshMs));
    }
  } catch (err) {
    console.error(`grid failed: ${String(err)}`);
    process.exitCode = 1;
  } finally {
    // Let the host cancel exactly this session's resting orders. This is the
    // same path the kill-switch uses, so a crash-looping strategy cannot leave
    // ladders behind.
    try {
      await session.stopStrategy(true);
      console.log(`stopped state=${session.state}`);
    } catch (err) {
      console.log(`stopStrategy failed: ${String(err)}`);
    }
    session.close();
  }
}

void main();
