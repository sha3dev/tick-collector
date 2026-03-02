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

// empty

export class InvalidCollectorSourcesError extends Error {
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

  public readonly configuredSources: readonly string[];

  /**
   * @section constructor
   */

  public constructor(configuredSources: readonly string[]) {
    super(`invalid collector sources: expected at least one source, got [${configuredSources.join(", ")}]`);
    this.name = "InvalidCollectorSourcesError";
    this.configuredSources = configuredSources;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static fromConfiguredSources(configuredSources: readonly string[]): InvalidCollectorSourcesError {
    const error = new InvalidCollectorSourcesError(configuredSources);
    return error;
  }

  /**
   * @section private:methods
   */

  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  // empty

  /**
   * @section static:methods
   */

  // empty
}
