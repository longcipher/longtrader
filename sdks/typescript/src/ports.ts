/**
 * Overflow policies and the narrow ports strategies program against.
 * Mirrors sdks/python/longtrader_sdk/ports.py 1:1 (design doc section 6.8).
 */
import type {
  MarketDataEvent,
  StreamSubscription,
  Ticker,
} from "../gen/longtrader/market/v1/market_pb.js";
import type { Order, OrderRequest } from "../gen/longtrader/trading/v1/trading_pb.js";
import type { ReconcileStateResponse } from "../gen/longtrader/worker/v1/worker_pb.js";

/** How a full event queue behaves under a slow consumer. */
export enum OverflowPolicy {
  /** Discard the oldest buffered event, preserve the newest. */
  DropOldest = "DROP_OLDEST",
  /** Last-writer-wins per key (e.g. orderbook); drops stale intermediates. */
  Coalesce = "COALESCE",
  /** Never drop; apply backpressure to the producer until drained. */
  Block = "BLOCK",
}

/** Order management surface (mirrors longtrader.trading.v1). */
export abstract class TradingPort {
  /** Submit one OrderRequest; backend dedupes on client_order_id. */
  abstract createOrder(request: OrderRequest): Promise<Order>;
  /** Submit many OrderRequests via CreateOrders. */
  abstract batchCreateOrders(requests: OrderRequest[]): Promise<Order[]>;
  /** Cancel by venue order id. */
  abstract cancelOrder(orderId: string, symbol?: string): Promise<Order>;
  /** List currently open orders, optionally filtered by symbol. */
  abstract fetchOpenOrders(symbol?: string): Promise<Order[]>;
  /** ReconcileState: authoritative atomic snapshot for recovery. */
  abstract syncState(): Promise<ReconcileStateResponse>;
}

/** Market data surface (mirrors longtrader.market.v1). */
export abstract class MarketPort {
  /** Unary latest ticker snapshot. */
  abstract fetchTicker(symbol: string): Promise<Ticker>;
  /**
   * Stream MarketDataEvent for the given subscriptions; events carry
   * resume_token for reconnect-with-replay and gap-free header.sequence.
   */
  abstract subscribeMarketData(subscriptions: StreamSubscription[]): AsyncIterable<MarketDataEvent>;
}
