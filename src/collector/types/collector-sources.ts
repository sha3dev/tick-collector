/**
 * @section imports:externals
 */

import type { CryptoProviderId } from "@sha3/crypto";

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

export type CollectorSource = CryptoProviderId | "polymarket";
export type CollectorSources = CollectorSource[];
