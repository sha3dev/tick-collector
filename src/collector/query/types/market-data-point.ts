/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

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
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  marketStartAt: number;
  cryptoPricesBySource: Record<string, number | null>;
  polymarket: { upPrice: number | null; downPrice: number | null; orderbook: OrderBookSnapshotValue | null };
  exchangeOrderbooksBySource: Record<string, OrderBookSnapshotValue | null>;
  coverage: DataPointCoverage;
};
