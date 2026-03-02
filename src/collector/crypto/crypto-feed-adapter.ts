/**
 * @section imports:externals
 */

import { CryptoFeedClient } from "@sha3/crypto";
import type { ClientOptions, FeedEvent, Subscription } from "@sha3/crypto";

/**
 * @section imports:internals
 */

import { FeedConnectionError } from "../errors/feed-connection-error.ts";
import type { EventEnvelopeFactory } from "../pipeline/event-envelope-factory.ts";
import type { CollectorEventHandler } from "../types/collector-event-handler.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type CryptoFeedAdapterOptions = {
  client: CryptoFeedClient;
  eventHandler: CollectorEventHandler;
  envelopeFactory: EventEnvelopeFactory;
  clock: () => number;
  nextSequence: () => number;
};

export class CryptoFeedAdapter {
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

  private readonly client: CryptoFeedClient;
  private readonly eventHandler: CollectorEventHandler;
  private readonly envelopeFactory: EventEnvelopeFactory;
  private readonly clock: () => number;
  private readonly nextSequence: () => number;
  private subscription: Subscription | null;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: CryptoFeedAdapterOptions) {
    this.client = options.client;
    this.eventHandler = options.eventHandler;
    this.envelopeFactory = options.envelopeFactory;
    this.clock = options.clock;
    this.nextSequence = options.nextSequence;
    this.subscription = null;
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
    clock?: () => number;
    nextSequence: () => number;
    clientOptions?: ClientOptions;
  }): CryptoFeedAdapter {
    const client = CryptoFeedClient.create(options.clientOptions);
    const adapter = new CryptoFeedAdapter({
      client,
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

  private async onEvent(event: FeedEvent): Promise<void> {
    const storedEvent = this.envelopeFactory.fromCrypto({ event, sequence: this.nextSequence(), ingestedAt: this.clock() });
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
      this.subscription = this.client.subscribe((event) => {
        void this.onEvent(event);
      });
      await this.client.connect();
    } catch (error: unknown) {
      throw FeedConnectionError.fromCause("failed to start crypto feed adapter", error);
    }
  }

  public async stop(): Promise<void> {
    try {
      if (this.subscription) {
        this.subscription.unsubscribe();
        this.subscription = null;
      }
      await this.client.disconnect();
    } catch (error: unknown) {
      throw FeedConnectionError.fromCause("failed to stop crypto feed adapter", error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
