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
  provider: string | null;
  symbol: string | null;
  marketSlug: string | null;
  assetId: string | null;
};

export type EventIndexFile = { candidates: EventIndexCandidate[] };

export type EventSelectionQuery = {
  timestamp: number;
  eventType: string;
  source: StoredEvent["source"];
  provider?: string | null;
  symbol?: string | null;
  marketSlug?: string | null;
  assetId?: string | null;
  maxDistanceMs: number;
};
