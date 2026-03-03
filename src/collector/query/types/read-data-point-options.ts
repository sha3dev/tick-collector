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

export type ReadDataPointOptions = { timestamp: number; marketSlug: string; sources?: ReadSourcesFilter; maxDistanceMs?: number; orderbookLevels?: number };
