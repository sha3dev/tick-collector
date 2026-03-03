/**
 * @section imports:externals
 */

import type { CryptoMarketWindow } from "@sha3/polymarket";

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
  exchangeTs?: number;
  sequence: number;
  symbol?: string;
  provider?: string;
  marketType?: CryptoMarketWindow;
  marketStartAt?: number;
  assetId?: string;
  payload: unknown;
};
