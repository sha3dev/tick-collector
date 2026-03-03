/**
 * @section imports:externals
 */

import { mkdir, writeFile } from "node:fs/promises";
import * as path from "node:path";

/**
 * @section imports:internals
 */

import { StorageWriteError } from "../errors/storage-write-error.ts";
import type { EventIndexFile } from "../query/types/event-index-types.ts";
import type { PartManifest } from "../types/storage-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type BuildPartFilePathsOptions = { outputDir: string; hourBucketStartAt: number; runId: string; partSequence: number };
type PartFilePaths = { partPath: string; indexPath: string; manifestPath: string };

type PersistPartMetadataOptions = { manifestPath: string; indexPath: string; manifest: PartManifest; indexFile: EventIndexFile };

export class IndexManifestStore {
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

  // empty

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  // empty

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(): IndexManifestStore {
    const store = new IndexManifestStore();
    return store;
  }

  /**
   * @section private:methods
   */

  private toHourPath(hourBucketStartAt: number): { year: string; month: string; day: string; hour: string } {
    const date = new Date(hourBucketStartAt);
    const year = String(date.getUTCFullYear());
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hour = String(date.getUTCHours()).padStart(2, "0");
    const hourPath = { year, month, day, hour };
    return hourPath;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async buildPartFilePaths(options: BuildPartFilePathsOptions): Promise<PartFilePaths> {
    const hourPath = this.toHourPath(options.hourBucketStartAt);
    const baseName = `part-${options.runId}-${String(options.partSequence).padStart(8, "0")}`;
    const journalDir = path.join(options.outputDir, "journal", hourPath.year, hourPath.month, hourPath.day, hourPath.hour);
    const manifestsDir = path.join(options.outputDir, "manifests", hourPath.year, hourPath.month, hourPath.day, hourPath.hour);
    await mkdir(journalDir, { recursive: true });
    await mkdir(manifestsDir, { recursive: true });
    const paths: PartFilePaths = {
      partPath: path.join(journalDir, `${baseName}.ndjson.gz`),
      indexPath: path.join(manifestsDir, `${baseName}.index.json`),
      manifestPath: path.join(manifestsDir, `${baseName}.manifest.json`)
    };
    return paths;
  }

  public async persistPartMetadata(options: PersistPartMetadataOptions): Promise<void> {
    try {
      await writeFile(options.indexPath, JSON.stringify(options.indexFile), "utf8");
      await writeFile(options.manifestPath, JSON.stringify(options.manifest, null, 2), "utf8");
    } catch (error: unknown) {
      throw StorageWriteError.fromCause(`failed persisting index/manifest for part=${options.manifest.file}`, error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { BuildPartFilePathsOptions, PartFilePaths, PersistPartMetadataOptions };
