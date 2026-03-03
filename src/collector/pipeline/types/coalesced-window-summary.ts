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

export type CoalescedWindowEventTypeCount = { eventType: string; count: number };

export type CoalescedWindowSummary = {
  bucketId: number;
  windowStartAt: number;
  windowEndAt: number;
  eventCount: number;
  eventTypeCounts: CoalescedWindowEventTypeCount[];
};
