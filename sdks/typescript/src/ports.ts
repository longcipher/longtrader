/**
 * Overflow policies and the narrow ports strategies program against.
 * Mirrors sdks/python/longtrader_sdk/ports.py 1:1 (design doc section 6.8).
 *
 * `TradingPort` / `MarketPort` are the hexagonal seam: a strategy depends only
 * on these, never on the transport. `Session` is the concrete implementation,
 * so `new TradingSessionPort(session)` gives a strategy a swappable backend.
 */
import { Timeframe } from "./gen/longtrader/market/v1/market_pb.js";
import type {
  MarketDataEvent,
  StreamSubscription,
  Ticker,
} from "./gen/longtrader/market/v1/market_pb.js";
import type {
  Order,
  OrderRequest,
} from "./gen/longtrader/trading/v1/trading_pb.js";
import type { ReconcileStateResponse } from "./gen/longtrader/worker/v1/worker_pb.js";
import type { Session } from "./session.js";
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

/** Default policy for a stream channel name (`TICKER`, `ORDERBOOK`, ...). */
export function overflowPolicyForChannel(channel: string): OverflowPolicy {
  return (
    DEFAULT_OVERFLOW[channel.trim().toLowerCase()] ?? OverflowPolicy.DropOldest
  );
}

/** True when `next` does not immediately follow `prev`; triggers a resync. */
export function isSequenceGap(prev: number, next: number): boolean {
  return next > 0 && prev > 0 && next !== prev + 1;
}

/** Order management surface (mirrors longtrader.trading.v1). */
export abstract class TradingPort {
  /** Submit one OrderRequest; backend dedupes on client_order_id. */
  abstract createOrder(request: OrderRequest | OrderSpec): Promise<Order>;
  /** Submit many OrderRequests via CreateOrders. */
  abstract batchCreateOrders(
    requests: (OrderRequest | OrderSpec)[],
  ): Promise<Order[]>;
  /** Cancel by venue order id. */
  abstract cancelOrder(orderId: string, symbol?: string): Promise<Order>;
  /** Cancel every open order; an empty symbol spans all symbols. */
  abstract cancelAllOrders(symbol?: string): Promise<Order[]>;
  /** List currently open orders, optionally filtered by symbol. */
  abstract fetchOpenOrders(symbol?: string): Promise<Order[]>;
  /** ReconcileState: authoritative atomic snapshot for recovery. */
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
   * Stream MarketDataEvent for the given subscriptions; events carry
   * resume_token for reconnect-with-replay and gap-free header.sequence.
   */
  abstract subscribeMarketData(
    subscriptions: StreamSubscription[],
  ): AsyncIterable<MarketDataEvent>;
}

/** Adapts a `Session` to the `TradingPort` / `MarketPort` seam. */
export class SessionTradingPort extends TradingPort {
  constructor(private readonly session: Session) {
    super();
  }

  override async createOrder(
    request: OrderRequest | OrderSpec,
  ): Promise<Order> {
    return this.session.createOrder(request as OrderSpec);
  }

  override async batchCreateOrders(
    requests: (OrderRequest | OrderSpec)[],
  ): Promise<Order[]> {
    return this.session.createOrders(requests as OrderSpec[]);
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
    subscriptions: StreamSubscription[],
  ): AsyncIterable<MarketDataEvent> {
    const symbols = subscriptions.map((s) => s.symbol);
    const first = subscriptions[0];
    const channel = first === undefined ? "TICKER" : channelName(first.channel);
    yield* this.session.streamMarketData(symbols, channel);
  }
}

/** Map a numeric proto enum back to its short name (TICKER, ORDERBOOK, ...). */
function channelName(
  value: number,
): "TICKER" | "ORDERBOOK" | "TRADES" | "OHLCV" {
  // Imported lazily to keep this module free of a runtime dependency on the
  // generated market stubs; the numeric values are the contract's.
  switch (value) {
    case 2:
      return "ORDERBOOK";
    case 3:
      return "TRADES";
    case 4:
      return "OHLCV";
    default:
      return "TICKER";
  }
}
