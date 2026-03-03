/**
 * @section imports:externals
 */

import { GammaMarketCatalogService, type PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import { InvalidReadRangeError } from "../errors/invalid-read-range-error.ts";
import { PersistedEventReadError } from "../errors/persisted-event-read-error.ts";
import { DatapointAssembler } from "./datapoint-assembler.ts";
import { EventIndexRepository } from "./event-index-repository.ts";
import type { EventSelectionQuery } from "./types/event-index-types.ts";
import type { MarketDataPoint } from "./types/market-data-point.ts";
import type { ReadDataPointOptions } from "./types/read-data-point-options.ts";
import type { ReadDataPointRangeOptions } from "./types/read-data-point-range-options.ts";
import type { ReadSourcesFilter } from "./types/read-sources-filter.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type EventSelectionResult = Awaited<ReturnType<EventIndexRepository["findClosestEvent"]>>;

type ReaderMetrics = { reads: number; readLatencyMsTotal: number; selectedEventHits: number; selectedEventMisses: number; missingFieldsTotal: number };

type MarketCatalogContract = { loadMarketBySlug: (options: { slug: string }) => Promise<PolymarketMarket> };

type EventIndexRepositoryContract = { findClosestEvent: (query: EventSelectionQuery) => Promise<EventSelectionResult> };

type DatapointAssemblerContract = {
  assemble: (options: {
    timestamp: number;
    marketSlug: string;
    market: PolymarketMarket | null;
    cryptoProviders: string[];
    includeChainlink: boolean;
    includePolymarket: boolean;
    orderbookLevels: number;
    cryptoPriceByProvider: Record<string, EventSelectionResult>;
    cryptoOrderBookByProvider: Record<string, EventSelectionResult>;
    polymarketBook: EventSelectionResult;
    polymarketPriceByAssetId: Record<string, EventSelectionResult>;
  }) => MarketDataPoint;
};

type MarketDataPointReaderOptions = {
  folder: string;
  defaultSources: ReadSourcesFilter;
  defaultMaxDistanceMs: number;
  defaultOrderbookLevels: number;
  indexRepository?: EventIndexRepositoryContract;
  assembler?: DatapointAssemblerContract;
  marketsService?: MarketCatalogContract;
  clock?: () => number;
};

type NormalizedReadOptions = { timestamp: number; marketSlug: string; sources: ReadSourcesFilter; maxDistanceMs: number; orderbookLevels: number };

type NormalizedReadRangeOptions = {
  startTimestamp: number;
  endTimestamp: number;
  stepMs: number;
  marketSlug: string;
  sources: ReadSourcesFilter;
  maxDistanceMs: number;
  orderbookLevels: number;
};

export class MarketDataPointReader {
  /**
   * @section private:attributes
   */

  private readonly metrics: ReaderMetrics;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly defaultSources: ReadSourcesFilter;
  private readonly defaultMaxDistanceMs: number;
  private readonly defaultOrderbookLevels: number;
  private readonly indexRepository: EventIndexRepositoryContract;
  private readonly assembler: DatapointAssemblerContract;
  private readonly marketsService: MarketCatalogContract;
  private readonly marketCacheBySlug: Map<string, PolymarketMarket | null>;
  private readonly clock: () => number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: MarketDataPointReaderOptions) {
    this.defaultSources = options.defaultSources;
    this.defaultMaxDistanceMs = options.defaultMaxDistanceMs;
    this.defaultOrderbookLevels = options.defaultOrderbookLevels;
    this.indexRepository = options.indexRepository ?? EventIndexRepository.create({ folder: options.folder });
    this.assembler = options.assembler ?? DatapointAssembler.create();
    this.marketsService = options.marketsService ?? GammaMarketCatalogService.create();
    this.marketCacheBySlug = new Map<string, PolymarketMarket | null>();
    this.clock = options.clock ?? Date.now;
    this.metrics = { reads: 0, readLatencyMsTotal: 0, selectedEventHits: 0, selectedEventMisses: 0, missingFieldsTotal: 0 };
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: MarketDataPointReaderOptions): MarketDataPointReader {
    const reader = new MarketDataPointReader(options);
    return reader;
  }

  /**
   * @section private:methods
   */

  private normalizeSources(sources: ReadSourcesFilter | undefined): ReadSourcesFilter {
    const normalized: ReadSourcesFilter = sources
      ? { cryptoProviders: [...sources.cryptoProviders], includeChainlink: sources.includeChainlink, includePolymarket: sources.includePolymarket }
      : {
          cryptoProviders: [...this.defaultSources.cryptoProviders],
          includeChainlink: this.defaultSources.includeChainlink,
          includePolymarket: this.defaultSources.includePolymarket
        };
    return normalized;
  }

  private normalizeReadOptions(options: ReadDataPointOptions): NormalizedReadOptions {
    const normalized: NormalizedReadOptions = {
      timestamp: options.timestamp,
      marketSlug: options.marketSlug,
      sources: this.normalizeSources(options.sources),
      maxDistanceMs: options.maxDistanceMs ?? this.defaultMaxDistanceMs,
      orderbookLevels: options.orderbookLevels ?? this.defaultOrderbookLevels
    };
    return normalized;
  }

  private normalizeReadRangeOptions(options: ReadDataPointRangeOptions): NormalizedReadRangeOptions {
    const normalized: NormalizedReadRangeOptions = {
      startTimestamp: options.startTimestamp,
      endTimestamp: options.endTimestamp,
      stepMs: options.stepMs,
      marketSlug: options.marketSlug,
      sources: this.normalizeSources(options.sources),
      maxDistanceMs: options.maxDistanceMs ?? this.defaultMaxDistanceMs,
      orderbookLevels: options.orderbookLevels ?? this.defaultOrderbookLevels
    };
    return normalized;
  }

  private assertRangeOptions(options: NormalizedReadRangeOptions): void {
    const isValidStep = options.stepMs > 0;
    if (!isValidStep) {
      throw InvalidReadRangeError.fromInvalidStep(options.stepMs);
    }

    const isValidBounds = options.endTimestamp >= options.startTimestamp;
    if (!isValidBounds) {
      throw InvalidReadRangeError.fromInvalidBounds(options.startTimestamp, options.endTimestamp);
    }
  }

  private async loadMarket(marketSlug: string): Promise<PolymarketMarket | null> {
    let market = this.marketCacheBySlug.get(marketSlug) ?? null;
    if (!this.marketCacheBySlug.has(marketSlug)) {
      try {
        market = await this.marketsService.loadMarketBySlug({ slug: marketSlug });
      } catch {
        market = null;
      }
      this.marketCacheBySlug.set(marketSlug, market);
    }
    return market;
  }

  private async selectOne(query: EventSelectionQuery): Promise<EventSelectionResult> {
    const selection = await this.indexRepository.findClosestEvent(query);
    if (selection) {
      this.metrics.selectedEventHits += 1;
    } else {
      this.metrics.selectedEventMisses += 1;
    }
    return selection;
  }

  private async selectCryptoPriceEvents(options: {
    timestamp: number;
    symbol: string | null;
    maxDistanceMs: number;
    cryptoProviders: string[];
    includeChainlink: boolean;
  }): Promise<Record<string, EventSelectionResult>> {
    const selections: Record<string, EventSelectionResult> = {};
    const providers = [...options.cryptoProviders];
    if (options.includeChainlink) {
      providers.push("chainlink");
    }

    for (const provider of providers) {
      const query: EventSelectionQuery = {
        timestamp: options.timestamp,
        source: "crypto",
        eventType: "crypto.price",
        provider,
        symbol: options.symbol,
        maxDistanceMs: options.maxDistanceMs
      };
      const selection = await this.selectOne(query);
      selections[provider] = selection;
    }

    return selections;
  }

  private async selectCryptoOrderbookEvents(options: {
    timestamp: number;
    symbol: string | null;
    maxDistanceMs: number;
    cryptoProviders: string[];
  }): Promise<Record<string, EventSelectionResult>> {
    const selections: Record<string, EventSelectionResult> = {};

    for (const provider of options.cryptoProviders) {
      const query: EventSelectionQuery = {
        timestamp: options.timestamp,
        source: "crypto",
        eventType: "crypto.orderbook",
        provider,
        symbol: options.symbol,
        maxDistanceMs: options.maxDistanceMs
      };
      const selection = await this.selectOne(query);
      selections[provider] = selection;
    }

    return selections;
  }

  private async selectPolymarketEvents(options: {
    timestamp: number;
    market: PolymarketMarket | null;
    marketSlug: string;
    maxDistanceMs: number;
    includePolymarket: boolean;
  }): Promise<{ book: EventSelectionResult; pricesByAssetId: Record<string, EventSelectionResult> }> {
    let book: EventSelectionResult = null;
    const pricesByAssetId: Record<string, EventSelectionResult> = {};

    if (options.includePolymarket) {
      const bookQuery: EventSelectionQuery = {
        timestamp: options.timestamp,
        source: "polymarket",
        eventType: "polymarket.book",
        marketSlug: options.marketSlug,
        maxDistanceMs: options.maxDistanceMs
      };
      book = await this.selectOne(bookQuery);

      const upAssetId = options.market?.upTokenId ?? null;
      const downAssetId = options.market?.downTokenId ?? null;
      const assetIds = [upAssetId, downAssetId].filter((value): value is string => {
        const keep = value !== null;
        return keep;
      });

      for (const assetId of assetIds) {
        const priceQuery: EventSelectionQuery = {
          timestamp: options.timestamp,
          source: "polymarket",
          eventType: "polymarket.price",
          marketSlug: options.marketSlug,
          assetId,
          maxDistanceMs: options.maxDistanceMs
        };
        const selection = await this.selectOne(priceQuery);
        pricesByAssetId[assetId] = selection;
      }
    }

    const selections = { book, pricesByAssetId };
    return selections;
  }

  private updateMetrics(datapoint: MarketDataPoint, startedAt: number): void {
    this.metrics.reads += 1;
    this.metrics.readLatencyMsTotal += this.clock() - startedAt;
    this.metrics.missingFieldsTotal += datapoint.coverage.missingFields.length;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async read(options: ReadDataPointOptions): Promise<MarketDataPoint | null> {
    const startedAt = this.clock();
    let datapoint: MarketDataPoint | null = null;
    try {
      const normalized = this.normalizeReadOptions(options);
      const market = await this.loadMarket(normalized.marketSlug);
      const hasMarket = market !== null;
      if (hasMarket) {
        const symbol = market.symbol;
        const cryptoPriceByProvider = await this.selectCryptoPriceEvents({
          timestamp: normalized.timestamp,
          symbol,
          maxDistanceMs: normalized.maxDistanceMs,
          cryptoProviders: normalized.sources.cryptoProviders,
          includeChainlink: normalized.sources.includeChainlink
        });
        const cryptoOrderBookByProvider = await this.selectCryptoOrderbookEvents({
          timestamp: normalized.timestamp,
          symbol,
          maxDistanceMs: normalized.maxDistanceMs,
          cryptoProviders: normalized.sources.cryptoProviders
        });
        const polymarketSelections = await this.selectPolymarketEvents({
          timestamp: normalized.timestamp,
          market,
          marketSlug: normalized.marketSlug,
          maxDistanceMs: normalized.maxDistanceMs,
          includePolymarket: normalized.sources.includePolymarket
        });
        datapoint = this.assembler.assemble({
          timestamp: normalized.timestamp,
          marketSlug: normalized.marketSlug,
          market,
          cryptoProviders: normalized.sources.cryptoProviders,
          includeChainlink: normalized.sources.includeChainlink,
          includePolymarket: normalized.sources.includePolymarket,
          orderbookLevels: normalized.orderbookLevels,
          cryptoPriceByProvider,
          cryptoOrderBookByProvider,
          polymarketBook: polymarketSelections.book,
          polymarketPriceByAssetId: polymarketSelections.pricesByAssetId
        });
        this.updateMetrics(datapoint, startedAt);
      }
    } catch (error: unknown) {
      throw PersistedEventReadError.fromCause(`failed reading datapoint for marketSlug=${options.marketSlug} at timestamp=${options.timestamp}`, error);
    }
    return datapoint;
  }

  public async readRange(options: ReadDataPointRangeOptions): Promise<MarketDataPoint[]> {
    const points: MarketDataPoint[] = [];
    try {
      const normalized = this.normalizeReadRangeOptions(options);
      this.assertRangeOptions(normalized);
      let timestamp = normalized.startTimestamp;

      while (timestamp <= normalized.endTimestamp) {
        const point = await this.read({
          timestamp,
          marketSlug: normalized.marketSlug,
          sources: normalized.sources,
          maxDistanceMs: normalized.maxDistanceMs,
          orderbookLevels: normalized.orderbookLevels
        });
        if (point) {
          points.push(point);
        }
        timestamp += normalized.stepMs;
      }
    } catch (error: unknown) {
      const isRangeError = error instanceof InvalidReadRangeError;
      if (isRangeError) {
        throw error;
      }
      throw PersistedEventReadError.fromCause(
        `failed reading datapoint range for marketSlug=${options.marketSlug} from=${options.startTimestamp} to=${options.endTimestamp}`,
        error
      );
    }
    return points;
  }

  public getMetrics(): ReaderMetrics {
    const snapshot: ReaderMetrics = { ...this.metrics };
    return snapshot;
  }

  /**
   * @section static:methods
   */

  // empty
}
