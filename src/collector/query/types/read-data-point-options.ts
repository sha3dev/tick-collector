/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";
import type { ReadSourcesFilter } from "./read-sources-filter.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type ReadDataPointOptions = {
  timestamp: number;
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  sources?: ReadSourcesFilter;
  maxDistanceMs?: number;
  orderbookLevels?: number;
};
