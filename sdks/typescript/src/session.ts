/**
 * Session: the single hand-written entry point of the TypeScript SDK.
 * Mirrors sdks/python/longtrader_sdk/session.py 1:1 (design doc section 6.8).
 *
 * Wire format (see docs/bare-protocol-guide.md):
 *   POST {base_url}/longtrader.worker.v1.WorkerSessionService/{Method}
 *   Content-Type: application/proto
 *   Body: raw protobuf request; 200 body is the raw protobuf response.
 */
import {
  create,
  fromBinary,
  toBinary,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";
import { fetch } from "undici";
import {
  AttachSessionRequestSchema,
  AttachSessionResponseSchema,
  KeepAliveRequestSchema,
  KeepAliveResponseSchema,
  ReconcileStateRequestSchema,
  ReconcileStateResponseSchema,
  SetKillSwitchPolicyRequestSchema,
  SetKillSwitchPolicyResponseSchema,
  type KillSwitchPolicy,
} from "../gen/longtrader/worker/v1/worker_pb.js";

export const WORKER_SERVICE = "longtrader.worker.v1.WorkerSessionService";

/** Server-enforced lifecycle states (proto/longtrader/worker/v1/worker.proto). */
export type SessionState = "DISCONNECTED" | "ATTACHED" | "SYNCING" | "ACTIVE";

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

export class Session {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private watchdogTimer: ReturnType<typeof setInterval> | undefined;
  private lastKeepAliveOk = Date.now();
  private _state: SessionState = "DISCONNECTED";
  sessionId = "";
  heartbeatIntervalMs = 0;

  private constructor(baseUrl: string, fetchImpl: typeof fetch) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.fetchImpl = fetchImpl;
  }

  /** Validate the terminal API token and negotiate lease parameters. */
  static async attach(
    baseUrl: string,
    token: string,
    policy?: KillSwitchPolicy,
  ): Promise<Session> {
    const req = create(AttachSessionRequestSchema, {
      token,
      clientName: "longtrader-sdk-typescript",
      clientVersion: "0.1.0",
      ...(policy ? { policy } : {}),
    });
    const session = new Session(baseUrl, fetch);
    const resp = await session.unary(
      "AttachSession",
      AttachSessionResponseSchema,
      toBinary(AttachSessionRequestSchema, req),
    );
    session.sessionId = resp.sessionId;
    session.heartbeatIntervalMs = resp.heartbeatIntervalMs;
    session.lastKeepAliveOk = Date.now();
    session._state = "ATTACHED";
    return session;
  }

  /** Local view of the lifecycle state (ATTACHED/SYNCING/ACTIVE/...). */
  get state(): SessionState {
    return this._state;
  }

  /** Feed the lease watchdog; call at the negotiated interval. */
  async keepAlive(): Promise<MessageShape<typeof KeepAliveResponseSchema>> {
    const req = create(KeepAliveRequestSchema, {
      sessionId: this.sessionId,
      clientTimeNs: BigInt(Date.now()) * 1_000_000n,
    });
    const resp = await this.unary(
      "KeepAlive",
      KeepAliveResponseSchema,
      toBinary(KeepAliveRequestSchema, req),
    );
    this.lastKeepAliveOk = Date.now();
    return resp;
  }

  /**
   * Fetch the authoritative atomic snapshot (balances/positions/open orders)
   * stamped with snapshot_sequence. A successful reconcile is the local gate
   * out of syncing; pre-ACTIVE orders are rejected SYNC_IN_PROGRESS.
   */
  async reconcileState(): Promise<MessageShape<typeof ReconcileStateResponseSchema>> {
    const req = create(ReconcileStateRequestSchema, { sessionId: this.sessionId });
    const resp = await this.unary(
      "ReconcileState",
      ReconcileStateResponseSchema,
      toBinary(ReconcileStateRequestSchema, req),
    );
    this._state = "ACTIVE";
    return resp;
  }

  /** Parameterize cancel-on-disconnect behavior for this session. */
  async setKillSwitchPolicy(policy: KillSwitchPolicy): Promise<void> {
    const req = create(SetKillSwitchPolicyRequestSchema, {
      sessionId: this.sessionId,
      policy,
    });
    await this.unary(
      "SetKillSwitchPolicy",
      SetKillSwitchPolicyResponseSchema,
      toBinary(SetKillSwitchPolicyRequestSchema, req),
    );
  }

  /** Spawn an interval timer sending KeepAlive at the negotiated interval. */
  startHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) return;
    const ms = Math.max(this.heartbeatIntervalMs, 500);
    this.heartbeatTimer = setInterval(() => {
      // Transient failures are fine; the next tick retries.
      void this.keepAlive().catch(() => {});
    }, ms);
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
   * Strategy-side lease timeout simulation: spawn 定时任务，每 heartbeat_interval 检查 lease 是否超时，超时则触发 cancel.
   * Mirrors Rust `spawn_strategy_lease_guard` (session/daemon/exchange 三级看门狗中的 L_session).
   * Wakes every `heartbeatIntervalMs` and checks elapsed since last KeepAlive; on `leaseTimeout` (default 3x heartbeat) it stops.
   */
  spawnLeaseWatchdog(leaseTimeoutMs?: number): ReturnType<typeof setInterval> {
    const leaseMs = leaseTimeoutMs ?? Math.max(this.heartbeatIntervalMs * 3, 1500);
    const intervalMs = Math.max(this.heartbeatIntervalMs, 500);
    if (this.watchdogTimer !== undefined) {
      clearInterval(this.watchdogTimer);
    }
    this.watchdogTimer = setInterval(() => {
      const elapsed = Date.now() - this.lastKeepAliveOk;
      if (elapsed > leaseMs) {
        // Keep parity with Python: close transport and stop both timers.
        this.close();
      }
    }, intervalMs);
    return this.watchdogTimer;
  }

  /** One Connect unary call: POST application/proto, decode proto reply. */
  private async unary<S extends DescMessage>(
    method: string,
    respSchema: S,
    reqBytes: Uint8Array,
  ): Promise<MessageShape<S>> {
    const url = `${this.baseUrl}/${WORKER_SERVICE}/${method}`;
    const res = await this.fetchImpl(url, {
      method: "POST",
      headers: { "content-type": "application/proto" },
      body: reqBytes,
    });
    if (res.status !== 200) throw await toConnectError(res);
    return fromBinary(respSchema, new Uint8Array(await res.arrayBuffer()));
  }
}

async function toConnectError(res: Response): Promise<ConnectError> {
  let code = "unknown";
  let messageText = "";
  let details: unknown;
  try {
    const body = (await res.json()) as { code?: string; message?: string; details?: unknown };
    code = body.code ?? code;
    messageText = body.message ?? "";
    details = body.details;
  } catch {
    // Non-JSON error body; keep defaults.
  }
  return new ConnectError(res.status, code, messageText, details);
}
