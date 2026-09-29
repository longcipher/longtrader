/** @longcipher/longtrader-sdk — Tier-2 thin wrapper over generated Connect stubs. */

export {
  Session,
  ConnectError,
  WORKER_SERVICE,
  TRADING_SERVICE,
  MARKET_SERVICE,
  TERMINAL_STATES,
  SYNC_IN_PROGRESS,
} from "./session.js";
export type { SessionState, OrderSpec } from "./session.js";
export {
  OverflowPolicy,
  TradingPort,
  MarketPort,
  SessionTradingPort,
  SessionMarketPort,
  DEFAULT_OVERFLOW,
  overflowPolicyForChannel,
  isSequenceGap,
} from "./ports.js";
