/**
 * @section imports:externals
 */

// empty

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

export type PartManifest = {
  file: string;
  indexFile: string;
  minIngestedAt: number;
  maxIngestedAt: number;
  eventCount: number;
  sources: string[];
  eventTypes: string[];
  createdAt: string;
};

export type GzipRotatingWriterOptions = { outputDir: string; maxPartBytes: number; flushIntervalMs: number };
