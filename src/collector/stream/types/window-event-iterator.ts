/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { WindowIteratorAvailability } from "./window-iterator-availability.ts";
import type { WindowIteratorNextResult } from "./window-iterator-next-result.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type WindowEventIterator = { next: () => Promise<WindowIteratorNextResult>; getAvailability: () => Promise<WindowIteratorAvailability> };
