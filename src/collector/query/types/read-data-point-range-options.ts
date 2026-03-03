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

export type ReadDataPointRangeOptions = {
  startTimestamp: number;
  endTimestamp: number;
  stepMs: number;
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  sources?: ReadSourcesFilter;
  maxDistanceMs?: number;
  orderbookLevels?: number;
};
