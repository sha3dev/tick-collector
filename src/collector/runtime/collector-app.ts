/**
 * @section imports:externals
 */

import type { ClientOptions, CryptoProviderId } from "@sha3/crypto";
import type { CryptoMarketWindow, CryptoSymbol, PolymarketClientOptions } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import { CollectorBootstrapError } from "../errors/collector-bootstrap-error.ts";
import { InvalidCollectorSourcesError } from "../errors/invalid-collector-sources-error.ts";
import { CryptoFeedAdapter } from "../crypto/crypto-feed-adapter.ts";
import { EventCoalescer } from "../pipeline/event-coalescer.ts";
import { EventEnvelopeFactory } from "../pipeline/event-envelope-factory.ts";
import { PolymarketFeedAdapter } from "../polymarket/polymarket-feed-adapter.ts";
import { EventStorageService } from "../storage/event-storage-service.ts";
import type { CollectorSources } from "../types/collector-sources.ts";
import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

const MIN_COALESCER_FLUSH_MS = 50;
const SUPPORTED_CRYPTO_PROVIDERS = ["binance", "coinbase", "kraken", "okx", "chainlink"] as const;

/**
 * @section types
 */

type CollectorAppOptions = {
  storage: EventStorageService;
  coalescer: EventCoalescer;
  cryptoAdapter: CryptoFeedAdapter | null;
  polymarketAdapter: PolymarketFeedAdapter | null;
  coalesceIntervalMs: number;
  clock: () => number;
};

type CollectorAppFactoryOptions = {
  outputDir: string;
  flushIntervalMs: number;
  maxGzipPartBytes: number;
  symbols: CryptoSymbol[];
  windows: CryptoMarketWindow[];
  enabledSources: CollectorSources;
  coalesceIntervalMs: number;
  cryptoClientOptions?: ClientOptions;
  polymarketClientOptions?: PolymarketClientOptions;
};

type EnabledSourceSplit = { cryptoProviders: CryptoProviderId[]; polymarketEnabled: boolean };

export class CollectorApp {
  /**
   * @section private:attributes
   */

  private coalescerFlushTimer: NodeJS.Timeout | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly storage: EventStorageService;
  private readonly coalescer: EventCoalescer;
  private readonly cryptoAdapter: CryptoFeedAdapter | null;
  private readonly polymarketAdapter: PolymarketFeedAdapter | null;
  private readonly coalesceIntervalMs: number;
  private readonly clock: () => number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: CollectorAppOptions) {
    this.storage = options.storage;
    this.coalescer = options.coalescer;
    this.cryptoAdapter = options.cryptoAdapter;
    this.polymarketAdapter = options.polymarketAdapter;
    this.coalesceIntervalMs = options.coalesceIntervalMs;
    this.clock = options.clock;
    this.coalescerFlushTimer = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: CollectorAppFactoryOptions): CollectorApp {
    const sourceSplit = CollectorApp.splitEnabledSources(options.enabledSources);
    const hasEffectiveSources = sourceSplit.cryptoProviders.length > 0 || sourceSplit.polymarketEnabled;
    if (!hasEffectiveSources) {
      throw InvalidCollectorSourcesError.fromConfiguredSources(options.enabledSources);
    }

    const envelopeFactory = EventEnvelopeFactory.create();
    const storage = EventStorageService.create({
      outputDir: options.outputDir,
      flushIntervalMs: options.flushIntervalMs,
      maxPartBytes: options.maxGzipPartBytes
    });
    const sequenceRef = { value: 0 };
    const nextSequence = (): number => {
      sequenceRef.value += 1;
      return sequenceRef.value;
    };
    const onEmitMany = async (events: StoredEvent[]): Promise<void> => {
      await storage.appendMany(events);
    };
    const coalescer = EventCoalescer.create({ intervalMs: options.coalesceIntervalMs, onEmitMany });
    const onStoredEvent = async (event: StoredEvent): Promise<void> => {
      await coalescer.append(event);
    };
    const cryptoAdapter = CollectorApp.buildCryptoAdapter({ sourceSplit, options, onStoredEvent, envelopeFactory, nextSequence });
    const polymarketAdapter = CollectorApp.buildPolymarketAdapter({ sourceSplit, options, onStoredEvent, envelopeFactory, nextSequence });
    const app = new CollectorApp({ storage, coalescer, cryptoAdapter, polymarketAdapter, coalesceIntervalMs: options.coalesceIntervalMs, clock: Date.now });
    return app;
  }

  /**
   * @section private:methods
   */

  private static splitEnabledSources(enabledSources: CollectorSources): EnabledSourceSplit {
    const cryptoProviders: CryptoProviderId[] = [];
    let polymarketEnabled = false;

    for (const source of enabledSources) {
      if (source === "polymarket") {
        polymarketEnabled = true;
      } else {
        const isSupported = SUPPORTED_CRYPTO_PROVIDERS.includes(source);
        if (!isSupported) {
          throw InvalidCollectorSourcesError.fromUnsupportedSource(enabledSources, source);
        }
        cryptoProviders.push(source);
      }
    }

    const split: EnabledSourceSplit = { cryptoProviders, polymarketEnabled };
    return split;
  }

  private static buildCryptoAdapter(options: {
    sourceSplit: EnabledSourceSplit;
    options: CollectorAppFactoryOptions;
    onStoredEvent: (event: StoredEvent) => Promise<void>;
    envelopeFactory: EventEnvelopeFactory;
    nextSequence: () => number;
  }): CryptoFeedAdapter | null {
    let adapter: CryptoFeedAdapter | null = null;

    if (options.sourceSplit.cryptoProviders.length > 0) {
      const cryptoClientOptions: ClientOptions = {
        ...(options.options.cryptoClientOptions ?? {}),
        symbols: options.options.symbols,
        providers: options.sourceSplit.cryptoProviders
      };
      adapter = CryptoFeedAdapter.create({
        eventHandler: options.onStoredEvent,
        envelopeFactory: options.envelopeFactory,
        nextSequence: options.nextSequence,
        clientOptions: cryptoClientOptions
      });
    }

    return adapter;
  }

  private static buildPolymarketAdapter(options: {
    sourceSplit: EnabledSourceSplit;
    options: CollectorAppFactoryOptions;
    onStoredEvent: (event: StoredEvent) => Promise<void>;
    envelopeFactory: EventEnvelopeFactory;
    nextSequence: () => number;
  }): PolymarketFeedAdapter | null {
    let adapter: PolymarketFeedAdapter | null = null;

    if (options.sourceSplit.polymarketEnabled) {
      const polymarketAdapterOptions = options.options.polymarketClientOptions ? { clientOptions: options.options.polymarketClientOptions } : {};
      adapter = PolymarketFeedAdapter.create({
        eventHandler: options.onStoredEvent,
        envelopeFactory: options.envelopeFactory,
        symbols: options.options.symbols,
        windows: options.options.windows,
        nextSequence: options.nextSequence,
        ...polymarketAdapterOptions
      });
    }

    return adapter;
  }

  private startCoalescerLoop(): void {
    const timerMs = Math.max(MIN_COALESCER_FLUSH_MS, this.coalesceIntervalMs);
    this.coalescerFlushTimer = setInterval(() => {
      void this.coalescer.flushReady(this.clock());
    }, timerMs);
  }

  private stopCoalescerLoop(): void {
    if (this.coalescerFlushTimer) {
      clearInterval(this.coalescerFlushTimer);
      this.coalescerFlushTimer = null;
    }
  }

  private async startAdapter(adapter: CryptoFeedAdapter | PolymarketFeedAdapter | null): Promise<void> {
    if (adapter) {
      await adapter.start();
    }
  }

  private async stopAdapter(adapter: CryptoFeedAdapter | PolymarketFeedAdapter | null): Promise<void> {
    if (adapter) {
      await adapter.stop();
    }
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
      await this.storage.start();
      this.startCoalescerLoop();
      await this.startAdapter(this.cryptoAdapter);
      await this.startAdapter(this.polymarketAdapter);
    } catch (error: unknown) {
      throw CollectorBootstrapError.fromCause("failed to start collector app", error);
    }
  }

  public async stop(): Promise<void> {
    try {
      this.stopCoalescerLoop();
      await this.stopAdapter(this.polymarketAdapter);
      await this.stopAdapter(this.cryptoAdapter);
      await this.coalescer.flushAll();
      await this.storage.stop();
    } catch (error: unknown) {
      throw CollectorBootstrapError.fromCause("failed to stop collector app", error);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
