/**
 * Overflow policies and the narrow ports strategies program against.
 * Mirrors sdks/python/longtrader_sdk/ports.py 1:1 (design doc section 6.8).
 *
 * `TradingPort` / `MarketPort` are the hexagonal seam: a strategy depends only
 * on these, never on the transport. `Session` is the concrete implementation,
 * so `new TradingSessionPort(session)` gives a strategy a swappable backend.
 */
import {
  StreamChannel,
  Timeframe,
} from "./gen/longtrader/market/v1/market_pb.js";
import type {
  MarketDataEvent,
  Ticker,
} from "./gen/longtrader/market/v1/market_pb.js";
import type { Order } from "./gen/longtrader/trading/v1/trading_pb.js";
import type { ReconcileStateResponse } from "./gen/longtrader/worker/v1/worker_pb.js";
import type { DecimalLike, Session } from "./session.js";
import type { OrderSpec } from "./session.js";

/** How a full event queue behaves under a slow consumer. */
export enum OverflowPolicy {
  /** Discard the oldest buffered event, preserve the newest. */
  DropOldest = "DROP_OLDEST",
  /** Last-writer-wins per key (e.g. orderbook); drops stale intermediates. */
  Coalesce = "COALESCE",
  /** Never drop; apply backpressure to the producer until drained. */
  Block = "BLOCK",
}

/**
 * Per-stream-kind default policy, mirroring `longtrader-worker`'s
 * `ports::OVERFLOW_*` constants: a book must be coalesced (intermediate
 * snapshots are worthless), a ticker may drop (only the newest matters), and
 * private order/balance/position streams must never drop.
 */
export const DEFAULT_OVERFLOW: Readonly<Record<string, OverflowPolicy>> =
  Object.freeze({
    ticker: OverflowPolicy.DropOldest,
    trades: OverflowPolicy.DropOldest,
    ohlcv: OverflowPolicy.DropOldest,
    orderbook: OverflowPolicy.Coalesce,
    orders: OverflowPolicy.Block,
    balances: OverflowPolicy.Block,
    positions: OverflowPolicy.Block,
  });

/** Default policy for a stream channel name (`TICKER`, `ORDERBOOK`, ...). Fail-closed. */
export function overflowPolicyForChannel(channel: string): OverflowPolicy {
  const key = channel.trim().toLowerCase();
  const found = (DEFAULT_OVERFLOW as Record<string, OverflowPolicy>)[key];
  if (found !== undefined) return found;
  return OverflowPolicy.Block;
}

/**
 * True when `next` does not immediately follow `prev`; triggers a resync.
 * `next == 0` is the unset proto default (never emitted; worker sequences
 * start at `1`), so it is never a gap. `prev == 0` is a *valid* watermark
 * meaning "before the first sequence": `0 -> 1` is contiguous, `0 -> 5`
 * missed events `1-4`. `u64::MAX -> 0` is contiguous. Inputs are `u64`
 * sequences; the rule mirrors the worker's `has_sequence_gap(Some(prev),
 * header{sequence: next})` exactly.
 */
export function isSequenceGap(
  prev: number | bigint,
  next: number | bigint,
): boolean {
  const p = typeof prev === "bigint" ? prev : BigInt(prev);
  const n = typeof next === "bigint" ? next : BigInt(next);
  if (n === 0n) return false;
  const MAX_U64 = (1n << 64n) - 1n;
  if (p === MAX_U64) return n !== 0n;
  return n !== p + 1n;
}

/**
 * Channel names the market stream accepts, as the proto enum spells them.
 * Kept in sync with the generated `StreamChannel` by `channelName` below.
 */
export type StreamChannelName = "TICKER" | "ORDERBOOK" | "TRADES" | "OHLCV";

/** Order management surface (mirrors longtrader.trading.v1, 13 methods). */
export abstract class TradingPort {
  /** Submit one order spec; the backend dedupes on client_order_id. */
  abstract createOrder(spec: OrderSpec): Promise<Order>;
  /** Submit many order specs via CreateOrders. */
  abstract batchCreateOrders(specs: OrderSpec[]): Promise<Order[]>;
  /** Cancel by venue order id. */
  abstract cancelOrder(orderId: string, symbol?: string): Promise<Order>;
  /** Cancel every open order; an empty symbol spans all symbols. */
  abstract cancelAllOrders(symbol?: string): Promise<Order[]>;
  /** List currently open orders, optionally filtered by symbol. */
  abstract fetchOpenOrders(symbol?: string): Promise<Order[]>;
  /** Account margin summary. */
  abstract getAccount(): Promise<unknown>;
  /** Open positions; empty means every symbol. */
  abstract getPositions(symbols?: string[]): Promise<unknown>;
  /** Historical orders. */
  abstract getOrderHistory(limit?: number, since?: number): Promise<unknown>;
  /** Closed positions. */
  abstract getClosedPositions(limit?: number): Promise<unknown>;
  /** Close one position by id. */
  abstract closePosition(positionId: string): Promise<unknown>;
  /** Close every open position. */
  abstract closeAllPositions(): Promise<unknown>;
  /** Attach or replace a position's bracket orders. */
  abstract modifyPosition(
    positionId: string,
    takeProfit?: DecimalLike,
    stopLoss?: DecimalLike,
  ): Promise<unknown>;
  /** ReconcileState: best-effort snapshot for recovery (replay deltas after). */
  abstract syncState(): Promise<ReconcileStateResponse>;
}

/** Market data surface (mirrors longtrader.market.v1). */
export abstract class MarketPort {
  /** Unary latest ticker snapshot. */
  abstract fetchTicker(symbol: string): Promise<Ticker>;
  /** Unary order book snapshot. */
  abstract fetchOrderBook(symbol: string, depth?: number): Promise<unknown>;
  /** OHLCV candles; `timeframe` is the contract enum (`Timeframe.M1`, ...). */
  abstract getCandles(
    symbol: string,
    timeframe?: Timeframe,
    limit?: number,
  ): Promise<unknown>;
  /** Tradeable symbols on the bound venue. */
  abstract listSymbols(): Promise<unknown>;
  /**
   * Stream MarketDataEvent for `symbols` on one channel; events carry
   * resume_token for reconnect-with-replay and gap-free header.sequence.
   *
   * One channel per call, mirroring the Go SDK's port shape: the host opens
   * one stream per (symbols, channel) request, so a list of per-symbol
   * channels could only be served by subscribing every symbol to the first
   * channel -- silent wrong data. Ask for a second channel with a second call.
   */
  abstract subscribeMarketData(
    symbols: string[],
    channel?: StreamChannel,
  ): AsyncIterable<MarketDataEvent>;
}

/** Adapts a `Session` to the `TradingPort` / `MarketPort` seam. */
export class SessionTradingPort extends TradingPort {
  constructor(private readonly session: Session) {
    super();
  }

  override async createOrder(spec: OrderSpec): Promise<Order> {
    return this.session.createOrder(spec);
  }

  override async batchCreateOrders(specs: OrderSpec[]): Promise<Order[]> {
    return this.session.createOrders(specs);
  }

  override async cancelOrder(orderId: string, symbol = ""): Promise<Order> {
    return this.session.cancelOrder(orderId, symbol);
  }

  override async cancelAllOrders(symbol = ""): Promise<Order[]> {
    return this.session.cancelAllOrders(symbol);
  }

  override async fetchOpenOrders(symbol = ""): Promise<Order[]> {
    return this.session.fetchOpenOrders(symbol);
  }

  override async getAccount(): Promise<unknown> {
    return this.session.getAccount();
  }

  override async getPositions(symbols: string[] = []): Promise<unknown> {
    return this.session.getPositions(symbols);
  }

  override async getOrderHistory(limit = 100, since = 0): Promise<unknown> {
    return this.session.getOrderHistory(limit, since);
  }

  override async getClosedPositions(limit = 100): Promise<unknown> {
    return this.session.getClosedPositions(limit);
  }

  override async closePosition(positionId: string): Promise<unknown> {
    return this.session.closePosition(positionId);
  }

  override async closeAllPositions(): Promise<unknown> {
    return this.session.closeAllPositions();
  }

  override async modifyPosition(
    positionId: string,
    takeProfit?: DecimalLike,
    stopLoss?: DecimalLike,
  ): Promise<unknown> {
    return this.session.modifyPosition(positionId, {
      ...(takeProfit !== undefined ? { takeProfit } : {}),
      ...(stopLoss !== undefined ? { stopLoss } : {}),
    });
  }

  override async syncState(): Promise<ReconcileStateResponse> {
    return this.session.reconcileState();
  }
}

/** Adapts a `Session` to the `MarketPort` seam. */
export class SessionMarketPort extends MarketPort {
  constructor(private readonly session: Session) {
    super();
  }

  override async fetchTicker(symbol: string): Promise<Ticker> {
    return this.session.fetchTicker(symbol);
  }

  override fetchOrderBook(symbol: string, depth = 10): Promise<unknown> {
    return this.session.fetchOrderBook(symbol, depth);
  }

  override getCandles(
    symbol: string,
    timeframe = Timeframe.M1,
    limit = 100,
  ): Promise<unknown> {
    return this.session.getCandles(symbol, timeframe, limit);
  }

  override listSymbols(): Promise<unknown> {
    return this.session.listSymbols();
  }

  override async *subscribeMarketData(
    symbols: string[],
    channel: StreamChannel = StreamChannel.TICKER,
  ): AsyncIterable<MarketDataEvent> {
    // One channel per call (see the port's doc): the per-symbol channel list
    // this used to take could only be served by folding every symbol onto the
    // first channel, which delivered the wrong data for the rest.
    yield* this.session.streamMarketData(symbols, channelName(channel));
  }
}

/** Channel names the market stream accepts, as the proto enum spells them. */
const STREAM_CHANNEL_NAMES: readonly StreamChannelName[] = [
  "TICKER",
  "ORDERBOOK",
  "TRADES",
  "OHLCV",
];

/**
 * Map a numeric proto enum back to its short name, resolving against the
 * generated constants so a renumbering in the contract cannot drift from this
 * hand-written table (mirrors sdks/python/longtrader_sdk/ports.py). An
 * unknown value throws: defaulting it to TICKER would silently subscribe the
 * caller to a channel it never asked for.
 */
function channelName(value: number): StreamChannelName {
  for (const name of STREAM_CHANNEL_NAMES) {
    if (StreamChannel[name] === value) return name;
  }
  throw new Error(`unknown StreamChannel ${value}`);
}
