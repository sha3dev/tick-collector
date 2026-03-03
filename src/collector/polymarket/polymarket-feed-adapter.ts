/**
 * @section imports:externals
 */

import { PolymarketClient } from "@sha3/polymarket";
import type { MarketEvent, PolymarketClientOptions } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import { FeedConnectionError } from "../errors/feed-connection-error.ts";
import type { EventEnvelopeFactory } from "../pipeline/event-envelope-factory.ts";
import { PolymarketMarketScheduler } from "./polymarket-market-scheduler.ts";
import type { CollectorEventHandler } from "../types/collector-event-handler.ts";
import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type PolymarketFeedAdapterOptions = {
  client: PolymarketClient;
  scheduler: PolymarketMarketScheduler;
  eventHandler: CollectorEventHandler;
  envelopeFactory: EventEnvelopeFactory;
  clock: () => number;
  nextSequence: () => number;
};

export class PolymarketFeedAdapter {
  /**
   * @section private:attributes
   */

  private removeListener: (() => void) | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly client: PolymarketClient;
  private readonly scheduler: PolymarketMarketScheduler;
  private readonly eventHandler: CollectorEventHandler;
  private readonly envelopeFactory: EventEnvelopeFactory;
  private readonly clock: () => number;
  private readonly nextSequence: () => number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: PolymarketFeedAdapterOptions) {
    this.client = options.client;
    this.scheduler = options.scheduler;
    this.eventHandler = options.eventHandler;
    this.envelopeFactory = options.envelopeFactory;
    this.clock = options.clock;
    this.nextSequence = options.nextSequence;
    this.removeListener = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: {
    eventHandler: CollectorEventHandler;
    envelopeFactory: EventEnvelopeFactory;
    symbols: CryptoSymbol[];
    windows: CryptoMarketWindow[];
    nextSequence: () => number;
    clock?: () => number;
    clientOptions?: PolymarketClientOptions;
  }): PolymarketFeedAdapter {
    const client = PolymarketClient.create(options.clientOptions);
    const scheduler = PolymarketMarketScheduler.create({
      marketsService: client.markets,
      streamService: client.stream,
      symbols: options.symbols,
      windows: options.windows,
      clock: options.clock ?? Date.now
    });
    const adapter = new PolymarketFeedAdapter({
      client,
      scheduler,
      eventHandler: options.eventHandler,
      envelopeFactory: options.envelopeFactory,
      clock: options.clock ?? Date.now,
      nextSequence: options.nextSequence
    });
    return adapter;
  }

  /**
   * @section private:methods
   */

  private async onEvent(event: MarketEvent): Promise<void> {
    const marketContext = this.scheduler.getMarketContext(event.assetId);
    const storedEvent = this.envelopeFactory.fromPolymarket({
      event,
      sequence: this.nextSequence(),
      ingestedAt: this.clock(),
      symbol: marketContext?.symbol ?? null,
      marketType: marketContext?.marketType ?? null,
      marketStartAt: marketContext?.marketStartAt ?? null
    });
    await this.eventHandler(storedEvent);
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    try {
      this.removeListener = this.client.stream.addListener({
        listener: (event) => {
          void this.onEvent(event);
        }
      });
      await this.client.connect();
      await this.scheduler.start();
    } catch (error: unknown) {
      throw FeedConnectionError.fromCause("failed to start polymarket feed adapter", error);
    }
  }

  public async stop(): Promise<void> {
    try {
      await this.scheduler.stop();
      if (this.removeListener) {
        this.removeListener();
        this.removeListener = null;
      }
      await this.client.disconnect();
    } catch (error: unknown) {
      throw FeedConnectionError.fromCause("failed to stop polymarket feed adapter", error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
