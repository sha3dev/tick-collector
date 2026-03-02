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

export type StoredEvent = {
  eventId: string;
  source: "crypto" | "polymarket";
  eventType: string;
  ingestedAt: number;
  exchangeTs: number | null;
  sequence: number;
  symbol: string | null;
  provider: string | null;
  marketSlug: string | null;
  assetId: string | null;
  payload: unknown;
};
