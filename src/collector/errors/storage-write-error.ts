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

export class StorageWriteError extends Error {
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

  public readonly causeValue: unknown;

  /**
   * @section constructor
   */

  public constructor(message: string, causeValue: unknown) {
    super(message);
    this.name = "StorageWriteError";
    this.causeValue = causeValue;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static fromCause(message: string, causeValue: unknown): StorageWriteError {
    const error = new StorageWriteError(message, causeValue);
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
