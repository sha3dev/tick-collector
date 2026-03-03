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
  public readonly unsupportedSource: string | null;

  /**
   * @section constructor
   */

  public constructor(configuredSources: readonly string[], unsupportedSource: string | null = null) {
    const message =
      unsupportedSource === null
        ? `invalid collector sources: expected at least one source, got [${configuredSources.join(", ")}]`
        : `invalid collector sources: unsupported source="${unsupportedSource}" in [${configuredSources.join(", ")}]`;
    super(message);
    this.name = "InvalidCollectorSourcesError";
    this.configuredSources = configuredSources;
    this.unsupportedSource = unsupportedSource;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static fromConfiguredSources(configuredSources: readonly string[]): InvalidCollectorSourcesError {
    const error = new InvalidCollectorSourcesError(configuredSources, null);
    return error;
  }

  public static fromUnsupportedSource(configuredSources: readonly string[], unsupportedSource: string): InvalidCollectorSourcesError {
    const error = new InvalidCollectorSourcesError(configuredSources, unsupportedSource);
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
