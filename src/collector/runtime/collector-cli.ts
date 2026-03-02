/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import { CollectorBootstrapError } from "../errors/collector-bootstrap-error.ts";
import type { CollectorApp } from "./collector-app.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type CollectorCliOptions = { app: CollectorApp };

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

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: CollectorCliOptions) {
    this.app = options.app;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: CollectorCliOptions): CollectorCli {
    const cli = new CollectorCli(options);
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
      await this.app.start();
      await this.waitForSignals();
      await this.app.stop();
    } catch (error: unknown) {
      throw CollectorBootstrapError.fromCause("collector cli failed", error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
