/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

// empty

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type OrderBookSnapshotValue = { bids: unknown[]; asks: unknown[] };

export type SelectedEventMeta = { source: string; eventType: string; ingestedAt: number; deltaMs: number };

export type DataPointCoverage = { missingFields: string[]; selectedEvents: SelectedEventMeta[] };

export type MarketDataPoint = {
  timestamp: number;
  marketSlug: string;
  symbol: "btc" | "eth" | "sol" | "xrp" | null;
  cryptoPricesBySource: Record<string, number | null>;
  polymarket: { upPrice: number | null; downPrice: number | null; orderbook: OrderBookSnapshotValue | null };
  exchangeOrderbooksBySource: Record<string, OrderBookSnapshotValue | null>;
  coverage: DataPointCoverage;
};
