/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type CreateWindowIteratorOptions = {
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  startTimestamp?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
};
