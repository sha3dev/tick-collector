/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section imports:internals
 */

import type { FeedEvent } from "@sha3/crypto";
import type { MarketEvent } from "@sha3/polymarket";
import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type BuildCryptoEventOptions = { event: FeedEvent; sequence: number; ingestedAt: number };

type BuildPolymarketEventOptions = { event: MarketEvent; sequence: number; ingestedAt: number; marketSlug: string | null };

export class EventEnvelopeFactory {
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

  public static create(): EventEnvelopeFactory {
    const factory = new EventEnvelopeFactory();
    return factory;
  }

  /**
   * @section private:methods
   */

  private createBaseEvent(sequence: number, ingestedAt: number): Pick<StoredEvent, "eventId" | "sequence" | "ingestedAt"> {
    const baseEvent: Pick<StoredEvent, "eventId" | "sequence" | "ingestedAt"> = { eventId: randomUUID(), sequence, ingestedAt };
    return baseEvent;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public fromCrypto(options: BuildCryptoEventOptions): StoredEvent {
    const baseEvent = this.createBaseEvent(options.sequence, options.ingestedAt);
    const symbol = "symbol" in options.event ? options.event.symbol : null;
    const provider = "provider" in options.event ? options.event.provider : null;
    const storedEvent: StoredEvent = {
      ...baseEvent,
      source: "crypto",
      eventType: `crypto.${options.event.type}`,
      exchangeTs: "ts" in options.event ? options.event.ts : null,
      symbol,
      provider,
      marketSlug: null,
      assetId: null,
      payload: options.event
    };
    return storedEvent;
  }

  public fromPolymarket(options: BuildPolymarketEventOptions): StoredEvent {
    const baseEvent = this.createBaseEvent(options.sequence, options.ingestedAt);
    const storedEvent: StoredEvent = {
      ...baseEvent,
      source: "polymarket",
      eventType: `polymarket.${options.event.type}`,
      exchangeTs: options.event.date.getTime(),
      symbol: null,
      provider: null,
      marketSlug: options.marketSlug,
      assetId: options.event.assetId,
      payload: options.event
    };
    return storedEvent;
  }

  /**
   * @section static:methods
   */

  // empty
}
