/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section imports:internals
 */

import type { FeedEvent } from "@sha3/crypto";
import type { CryptoMarketWindow, CryptoSymbol, MarketEvent } from "@sha3/polymarket";
import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type BuildCryptoEventOptions = { event: FeedEvent; sequence: number; ingestedAt: number };

type BuildPolymarketEventOptions = {
  event: MarketEvent;
  sequence: number;
  ingestedAt: number;
  symbol: CryptoSymbol | null;
  marketType: CryptoMarketWindow | null;
  marketSlug: string | null;
  marketSide?: "up" | "down" | null;
  marketStartAt: number | null;
  marketEventIndex: number | null;
};

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
    const symbol = "symbol" in options.event ? options.event.symbol : undefined;
    const provider = "provider" in options.event ? options.event.provider : undefined;
    const exchangeTs = "ts" in options.event ? options.event.ts : undefined;
    const storedEvent: StoredEvent = {
      ...baseEvent,
      source: "crypto",
      eventType: `crypto.${options.event.type}`,
      ...(exchangeTs !== undefined ? { exchangeTs } : {}),
      ...(symbol !== undefined ? { symbol } : {}),
      ...(provider !== undefined ? { provider } : {}),
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
      ...(options.symbol !== null ? { symbol: options.symbol } : {}),
      ...(options.marketType !== null ? { marketType: options.marketType } : {}),
      ...(options.marketSlug !== null ? { marketSlug: options.marketSlug } : {}),
      ...(options.marketSide !== undefined && options.marketSide !== null ? { marketSide: options.marketSide } : {}),
      ...(options.marketStartAt !== null ? { marketStartAt: options.marketStartAt } : {}),
      ...(options.marketEventIndex !== null ? { marketEventIndex: options.marketEventIndex } : {}),
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
