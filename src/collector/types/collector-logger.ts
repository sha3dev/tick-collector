/**
 * @section imports:externals
 */

import type { LogOptions } from "@sha3/logger";

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

export type CollectorLogger = {
  debug(value: string, options?: LogOptions): void;
  info(value: string, options?: LogOptions): void;
  warn(value: string, options?: LogOptions): void;
  error(value: string, options?: LogOptions): void;
};
