/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";
import type { StoredEvent } from "../../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type WindowEventBatch = {
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  windowStartAt: number;
  windowEndAt: number;
  events: StoredEvent[];
  stats: { totalEvents: number; cryptoEvents: number; polymarketEvents: number; polymarketDistinctMarkets: number };
};
