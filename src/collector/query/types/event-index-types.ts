/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { StoredEvent } from "../../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type EventIndexCandidate = {
  partPath: string;
  ingestedAt: number;
  sequence: number;
  lineIndex: number;
  source: StoredEvent["source"];
  eventType: string;
  provider?: string;
  symbol?: string;
  marketType?: string;
  marketSlug?: string;
  marketStartAt?: number;
  marketEventIndex?: number;
  assetId?: string;
};

export type EventIndexFile = { candidates: EventIndexCandidate[] };

export type EventSelectionQuery = {
  timestamp: number;
  eventType: string;
  source: StoredEvent["source"];
  provider?: string;
  symbol?: string;
  marketType?: string;
  marketSlug?: string;
  marketStartAt?: number;
  marketEventIndex?: number;
  assetId?: string;
  maxDistanceMs: number;
};

export type EventRangeQuery = { startTimestamp: number; endTimestampExclusive: number; symbol: string; marketType: string };

export type SymbolMarketTypeBoundsQuery = { symbol: string; marketType: string };
