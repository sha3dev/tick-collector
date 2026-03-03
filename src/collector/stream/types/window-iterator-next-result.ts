/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { WindowEventBatch } from "./window-event-batch.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type WindowIteratorNextResult = { done: boolean; value: WindowEventBatch | null };
