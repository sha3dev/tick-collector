/**
 * @section imports:externals
 */

import Logger from "@sha3/logger";

/**
 * @section imports:internals
 */

import { CollectorBootstrapError } from "../errors/collector-bootstrap-error.ts";
import type { CollectorLogger } from "../types/collector-logger.ts";
import type { CollectorApp } from "./collector-app.ts";

/**
 * @section consts
 */

const DEFAULT_LOGGER_NAME = "collector:cli";

/**
 * @section types
 */

type CollectorCliOptions = { app: CollectorApp; logger: CollectorLogger };
type CollectorCliFactoryOptions = { app: CollectorApp; logger?: CollectorLogger };

export class CollectorCli {
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

  private readonly app: CollectorApp;
  private readonly logger: CollectorLogger;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: CollectorCliOptions) {
    this.app = options.app;
    this.logger = options.logger;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: CollectorCliFactoryOptions): CollectorCli {
    const logger = options.logger ?? new Logger({ loggerName: DEFAULT_LOGGER_NAME });
    const cli = new CollectorCli({ app: options.app, logger });
    return cli;
  }

  /**
   * @section private:methods
   */

  private async waitForSignals(): Promise<void> {
    const completion = await new Promise<void>((resolve) => {
      const stop = (): void => {
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
        resolve();
      };
      process.on("SIGINT", stop);
      process.on("SIGTERM", stop);
    });
    return completion;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async run(): Promise<void> {
    try {
      this.logger.info("starting collector app");
      await this.app.start();
      this.logger.info("collector app started");
      await this.waitForSignals();
      this.logger.info("shutdown signal received");
      await this.app.stop();
      this.logger.info("collector app stopped");
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`collector cli failed: ${message}`);
      throw CollectorBootstrapError.fromCause("collector cli failed", error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
