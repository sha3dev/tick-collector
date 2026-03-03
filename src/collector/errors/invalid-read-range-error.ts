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

export class InvalidReadRangeError extends Error {
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

  public constructor(message: string) {
    super(message);
    this.name = "InvalidReadRangeError";
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static fromInvalidStep(stepMs: number): InvalidReadRangeError {
    const error = new InvalidReadRangeError(`invalid read range: stepMs must be > 0, got stepMs=${stepMs}`);
    return error;
  }

  public static fromInvalidBounds(startTimestamp: number, endTimestamp: number): InvalidReadRangeError {
    const error = new InvalidReadRangeError(
      `invalid read range: endTimestamp must be >= startTimestamp, got startTimestamp=${startTimestamp} endTimestamp=${endTimestamp}`
    );
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
