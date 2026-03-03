/**
 * @section imports:externals
 */

import { readdir, readFile } from "node:fs/promises";
import * as path from "node:path";

/**
 * @section imports:internals
 */

import { StorageWriteError } from "../errors/storage-write-error.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type RestartRecoveryServiceOptions = { outputDir: string };
type NextPartSequenceOptions = { hourBucketStartAt: number };
type StoredManifest = { partSequence?: number };

export class RestartRecoveryService {
  /**
   * @section private:attributes
   */

  // empty

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly outputDir: string;
  private readonly nextSequenceByHour: Map<number, number>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: RestartRecoveryServiceOptions) {
    this.outputDir = options.outputDir;
    this.nextSequenceByHour = new Map<number, number>();
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: RestartRecoveryServiceOptions): RestartRecoveryService {
    const service = new RestartRecoveryService(options);
    return service;
  }

  /**
   * @section private:methods
   */

  private toHourFolder(hourBucketStartAt: number): string {
    const date = new Date(hourBucketStartAt);
    const year = String(date.getUTCFullYear());
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hour = String(date.getUTCHours()).padStart(2, "0");
    const folder = path.join(this.outputDir, "manifests", year, month, day, hour);
    return folder;
  }

  private parseManifest(raw: string): StoredManifest {
    const parsed = JSON.parse(raw) as StoredManifest;
    return parsed;
  }

  private async loadInitialNextSequence(hourBucketStartAt: number): Promise<number> {
    let nextSequence = 1;
    try {
      const hourFolder = this.toHourFolder(hourBucketStartAt);
      const entries = await readdir(hourFolder, { withFileTypes: true });
      for (const entry of entries) {
        const isManifest = entry.isFile() && entry.name.endsWith(".manifest.json");
        if (isManifest) {
          const manifestPath = path.join(hourFolder, entry.name);
          const raw = await readFile(manifestPath, "utf8");
          const manifest = this.parseManifest(raw);
          const sequence = manifest.partSequence ?? 0;
          nextSequence = Math.max(nextSequence, sequence + 1);
        }
      }
    } catch (error: unknown) {
      const errorCode = (error as NodeJS.ErrnoException).code ?? "";
      const missingFolder = errorCode === "ENOENT";
      if (!missingFolder) {
        throw StorageWriteError.fromCause(`failed recovering storage state for hour=${hourBucketStartAt}`, error);
      }
    }
    return nextSequence;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async nextPartSequence(options: NextPartSequenceOptions): Promise<number> {
    let nextSequence = this.nextSequenceByHour.get(options.hourBucketStartAt) ?? 0;
    if (nextSequence === 0) {
      nextSequence = await this.loadInitialNextSequence(options.hourBucketStartAt);
    }
    this.nextSequenceByHour.set(options.hourBucketStartAt, nextSequence + 1);
    return nextSequence;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { RestartRecoveryServiceOptions };
