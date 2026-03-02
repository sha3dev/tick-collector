/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { StoredEvent } from "./stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type CollectorEventHandler = (event: StoredEvent) => Promise<void>;
