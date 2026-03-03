/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

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
  marketSlug: string;
  sources?: ReadSourcesFilter;
  maxDistanceMs?: number;
  orderbookLevels?: number;
};
