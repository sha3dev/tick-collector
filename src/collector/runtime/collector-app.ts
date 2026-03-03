/**
 * @section imports:externals
 */

import type { ClientOptions, CryptoProviderId } from "@sha3/crypto";
import Logger from "@sha3/logger";
import type { CryptoMarketWindow, CryptoSymbol, PolymarketClientOptions } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import { CollectorBootstrapError } from "../errors/collector-bootstrap-error.ts";
import { InvalidCollectorSourcesError } from "../errors/invalid-collector-sources-error.ts";
import { CryptoFeedAdapter } from "../crypto/crypto-feed-adapter.ts";
import { EventCoalescer } from "../pipeline/event-coalescer.ts";
import { EventEnvelopeFactory } from "../pipeline/event-envelope-factory.ts";
import type { CoalescedWindowSummary } from "../pipeline/types/coalesced-window-summary.ts";
import { PolymarketFeedAdapter } from "../polymarket/polymarket-feed-adapter.ts";
import { EventStorageService } from "../storage/event-storage-service.ts";
import type { CollectorLogger } from "../types/collector-logger.ts";
import type { CollectorSources } from "../types/collector-sources.ts";
import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

const MIN_COALESCER_FLUSH_MS = 50;
const DEFAULT_LOGGER_NAME = "collector:app";
const SUPPORTED_CRYPTO_PROVIDERS = ["binance", "coinbase", "kraken", "okx", "chainlink"] as const;
const ORDERBOOK_AND_TRADE_PROVIDERS = ["binance", "coinbase", "kraken", "okx"] as const;

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
  logger?: CollectorLogger;
  cryptoClientOptions?: ClientOptions;
  polymarketClientOptions?: PolymarketClientOptions;
};

type EnabledSourceSplit = { cryptoProviders: CryptoProviderId[]; polymarketEnabled: boolean };
type WindowCoverage = { coverage: "complete" | "partial"; expected: string[]; missing: string[]; extras: string[] };

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
    const logger = options.logger ?? new Logger({ loggerName: DEFAULT_LOGGER_NAME });
    const expectedEventTypes = CollectorApp.resolveExpectedEventTypes(sourceSplit);
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
    const onWindowEmitted = (summary: CoalescedWindowSummary): void => {
      const message = CollectorApp.formatWindowSummary(summary, expectedEventTypes);
      logger.info(message);
    };
    const coalescer = EventCoalescer.create({ intervalMs: options.coalesceIntervalMs, onEmitMany, onWindowEmitted });
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

  private static formatWindowSummary(summary: CoalescedWindowSummary, expectedEventTypes: string[]): string {
    const coverage = CollectorApp.resolveWindowCoverage(summary, expectedEventTypes);
    const countsText = CollectorApp.toCoverageCountsText(summary, coverage.expected);
    const missingText = coverage.missing.length > 0 ? coverage.missing.join("|") : "none";
    const extrasText = coverage.extras.length > 0 ? coverage.extras.join("|") : "none";
    const message = `[WINDOW] closed start=${summary.windowStartAt} end=${summary.windowEndAt} events=${summary.eventCount} coverage=${coverage.coverage} counts=${countsText} missing=${missingText} extra=${extrasText}`;
    return message;
  }

  private static resolveExpectedEventTypes(sourceSplit: EnabledSourceSplit): string[] {
    const expectedEventTypes: string[] = [];
    const hasCrypto = sourceSplit.cryptoProviders.length > 0;
    const hasOrderbookAndTradeProvider = sourceSplit.cryptoProviders.some((provider) => {
      return ORDERBOOK_AND_TRADE_PROVIDERS.includes(provider as (typeof ORDERBOOK_AND_TRADE_PROVIDERS)[number]);
    });
    if (hasCrypto) {
      expectedEventTypes.push("crypto.status");
      expectedEventTypes.push("crypto.price");
    }
    if (hasOrderbookAndTradeProvider) {
      expectedEventTypes.push("crypto.orderbook");
      expectedEventTypes.push("crypto.trade");
    }
    if (sourceSplit.polymarketEnabled) {
      expectedEventTypes.push("polymarket.price");
      expectedEventTypes.push("polymarket.book");
    }
    return expectedEventTypes;
  }

  private static resolveWindowCoverage(summary: CoalescedWindowSummary, expectedEventTypes: string[]): WindowCoverage {
    const observedEventTypes = summary.eventTypeCounts.map((eventTypeCount) => {
      return eventTypeCount.eventType;
    });
    const expected = expectedEventTypes.length > 0 ? expectedEventTypes : observedEventTypes;
    const missing = expected.filter((eventType) => {
      const count = summary.eventTypeCounts.find((eventTypeCount) => {
        return eventTypeCount.eventType === eventType;
      });
      return (count?.count ?? 0) === 0;
    });
    const extras = observedEventTypes.filter((eventType) => {
      return !expected.includes(eventType);
    });
    const coverage: "complete" | "partial" = missing.length === 0 ? "complete" : "partial";
    const output: WindowCoverage = { coverage, expected, missing, extras };
    return output;
  }

  private static toCoverageCountsText(summary: CoalescedWindowSummary, expectedEventTypes: string[]): string {
    const countsByType = new Map<string, number>();
    for (const eventTypeCount of summary.eventTypeCounts) {
      countsByType.set(eventTypeCount.eventType, eventTypeCount.count);
    }
    const rows = expectedEventTypes.map((eventType) => {
      const count = countsByType.get(eventType) ?? 0;
      return `${eventType}:${count}`;
    });
    const text = rows.join(",");
    return text;
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
