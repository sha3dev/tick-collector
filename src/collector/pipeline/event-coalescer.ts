/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { StoredEvent } from "../types/stored-event.ts";
import type { CoalescedWindowEventTypeCount, CoalescedWindowSummary } from "./types/coalesced-window-summary.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type EventCoalescerOptions = {
  intervalMs: number;
  onEmitMany: (events: StoredEvent[]) => Promise<void>;
  onWindowEmitted?: (summary: CoalescedWindowSummary) => void;
};

type BucketEventMap = Map<string, StoredEvent>;

export class EventCoalescer {
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

  private readonly intervalMs: number;
  private readonly onEmitMany: (events: StoredEvent[]) => Promise<void>;
  private readonly onWindowEmitted: ((summary: CoalescedWindowSummary) => void) | null;
  private readonly buckets: Map<number, BucketEventMap>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: EventCoalescerOptions) {
    this.intervalMs = options.intervalMs;
    this.onEmitMany = options.onEmitMany;
    this.onWindowEmitted = options.onWindowEmitted ?? null;
    this.buckets = new Map<number, BucketEventMap>();
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: EventCoalescerOptions): EventCoalescer {
    const coalescer = new EventCoalescer(options);
    return coalescer;
  }

  /**
   * @section private:methods
   */

  private toBucketId(ingestedAt: number): number {
    const bucketId = Math.floor(ingestedAt / this.intervalMs);
    return bucketId;
  }

  private toEventKey(event: StoredEvent): string {
    let key = "";

    if (event.source === "crypto") {
      key = `${event.source}|${event.eventType}|${event.provider ?? "na"}|${event.symbol ?? "na"}`;
    } else {
      key = `${event.source}|${event.eventType}|${event.assetId ?? "na"}`;
    }

    return key;
  }

  private upsertEvent(event: StoredEvent): void {
    const bucketId = this.toBucketId(event.ingestedAt);
    const bucket = this.buckets.get(bucketId) ?? new Map<string, StoredEvent>();
    const eventKey = this.toEventKey(event);
    bucket.set(eventKey, event);
    this.buckets.set(bucketId, bucket);
  }

  private async emitBuckets(bucketIds: number[]): Promise<void> {
    for (const bucketId of bucketIds) {
      const bucket = this.buckets.get(bucketId);
      if (bucket) {
        const events = Array.from(bucket.values()).sort((left, right) => {
          const ingestedOrder = left.ingestedAt - right.ingestedAt;
          const sequenceOrder = left.sequence - right.sequence;
          const order = ingestedOrder === 0 ? sequenceOrder : ingestedOrder;
          return order;
        });
        await this.onEmitMany(events);
        this.emitWindowSummary(bucketId, events);
        this.buckets.delete(bucketId);
      }
    }
  }

  private emitWindowSummary(bucketId: number, events: StoredEvent[]): void {
    if (this.onWindowEmitted) {
      const summary = this.toWindowSummary(bucketId, events);
      this.onWindowEmitted(summary);
    }
  }

  private toWindowSummary(bucketId: number, events: StoredEvent[]): CoalescedWindowSummary {
    const countsByType = new Map<string, number>();
    for (const event of events) {
      const currentCount = countsByType.get(event.eventType) ?? 0;
      countsByType.set(event.eventType, currentCount + 1);
    }
    const eventTypeCounts: CoalescedWindowEventTypeCount[] = Array.from(countsByType.entries())
      .map(([eventType, count]) => {
        const row: CoalescedWindowEventTypeCount = { eventType, count };
        return row;
      })
      .sort((left, right) => {
        return left.eventType.localeCompare(right.eventType);
      });
    const summary: CoalescedWindowSummary = {
      bucketId,
      windowStartAt: bucketId * this.intervalMs,
      windowEndAt: (bucketId + 1) * this.intervalMs,
      eventCount: events.length,
      eventTypeCounts
    };
    return summary;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async append(event: StoredEvent): Promise<void> {
    this.upsertEvent(event);
    await this.flushReady(event.ingestedAt);
  }

  public async flushReady(nowMs: number): Promise<void> {
    const currentBucketId = this.toBucketId(nowMs);
    const readyBucketIds = Array.from(this.buckets.keys())
      .filter((bucketId) => {
        return bucketId < currentBucketId;
      })
      .sort((left, right) => {
        return left - right;
      });
    await this.emitBuckets(readyBucketIds);
  }

  public async flushAll(): Promise<void> {
    const allBucketIds = Array.from(this.buckets.keys()).sort((left, right) => {
      return left - right;
    });
    await this.emitBuckets(allBucketIds);
  }

  /**
   * @section static:methods
   */

  // empty
}
