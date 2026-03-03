/**
 * @section imports:externals
 */

import { GammaMarketCatalogService, type PolymarketMarket } from "@sha3/polymarket";
import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

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

type MarketCatalogContract = {
  loadCryptoWindowMarkets: (options: { date: Date; window: CryptoMarketWindow; symbols?: CryptoSymbol[] }) => Promise<PolymarketMarket[]>;
};

type EventIndexRepositoryContract = { findClosestEvent: (query: EventSelectionQuery) => Promise<EventSelectionResult> };

type DatapointAssemblerContract = {
  assemble: (options: {
    timestamp: number;
    market: PolymarketMarket | null;
    symbol: CryptoSymbol;
    marketType: CryptoMarketWindow;
    marketStartAt: number;
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

type NormalizedReadOptions = {
  timestamp: number;
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  sources: ReadSourcesFilter;
  maxDistanceMs: number;
  orderbookLevels: number;
};

type NormalizedReadRangeOptions = {
  startTimestamp: number;
  endTimestamp: number;
  stepMs: number;
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
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
  private readonly marketCacheByBucket: Map<string, PolymarketMarket | null>;
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
    this.marketCacheByBucket = new Map<string, PolymarketMarket | null>();
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
      symbol: options.symbol,
      marketType: options.marketType,
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
      symbol: options.symbol,
      marketType: options.marketType,
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

  private toWindowMs(marketType: CryptoMarketWindow): number {
    const windowMs = marketType === "5m" ? 5 * 60_000 : 15 * 60_000;
    return windowMs;
  }

  private toMarketBucketCacheKey(timestamp: number, symbol: CryptoSymbol, marketType: CryptoMarketWindow): string {
    const windowMs = this.toWindowMs(marketType);
    const bucketStart = Math.floor(timestamp / windowMs) * windowMs;
    const key = `${symbol}|${marketType}|${bucketStart}`;
    return key;
  }

  private pickClosestMarket(markets: PolymarketMarket[], timestamp: number, symbol: CryptoSymbol): PolymarketMarket | null {
    const symbolMarkets = markets.filter((market) => {
      const match = market.symbol === symbol;
      return match;
    });
    const sorted = [...symbolMarkets].sort((left, right) => {
      const leftStartMs = left.start.getTime();
      const leftEndMs = left.end.getTime();
      const rightStartMs = right.start.getTime();
      const rightEndMs = right.end.getTime();
      const leftContainsTimestamp = timestamp >= leftStartMs && timestamp <= leftEndMs;
      const rightContainsTimestamp = timestamp >= rightStartMs && timestamp <= rightEndMs;
      const leftContainOrder = leftContainsTimestamp ? 0 : 1;
      const rightContainOrder = rightContainsTimestamp ? 0 : 1;
      const containOrder = leftContainOrder - rightContainOrder;
      const leftDistance = Math.abs(leftStartMs - timestamp);
      const rightDistance = Math.abs(rightStartMs - timestamp);
      const distanceOrder = containOrder === 0 ? leftDistance - rightDistance : containOrder;
      return distanceOrder;
    });
    const market = sorted[0] ?? null;
    return market;
  }

  private async loadMarket(timestamp: number, symbol: CryptoSymbol, marketType: CryptoMarketWindow): Promise<PolymarketMarket | null> {
    const cacheKey = this.toMarketBucketCacheKey(timestamp, symbol, marketType);
    let market = this.marketCacheByBucket.get(cacheKey) ?? null;
    if (!this.marketCacheByBucket.has(cacheKey)) {
      try {
        const markets = await this.marketsService.loadCryptoWindowMarkets({ date: new Date(timestamp), window: marketType, symbols: [symbol] });
        market = this.pickClosestMarket(markets, timestamp, symbol);
      } catch {
        market = null;
      }
      this.marketCacheByBucket.set(cacheKey, market);
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
    symbol: string;
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
    symbol: string;
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
    marketType: CryptoMarketWindow;
    marketStartAt: number;
    maxDistanceMs: number;
    includePolymarket: boolean;
  }): Promise<{ book: EventSelectionResult; pricesByAssetId: Record<string, EventSelectionResult> }> {
    let book: EventSelectionResult = null;
    const pricesByAssetId: Record<string, EventSelectionResult> = {};

    if (options.includePolymarket) {
      const bookAssetId = options.market?.upTokenId ?? options.market?.downTokenId ?? null;
      const bookQuery: EventSelectionQuery = {
        timestamp: options.timestamp,
        source: "polymarket",
        eventType: "polymarket.book",
        marketType: options.marketType,
        marketStartAt: options.marketStartAt,
        ...(bookAssetId !== null ? { assetId: bookAssetId } : {}),
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
          marketType: options.marketType,
          marketStartAt: options.marketStartAt,
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
      const market = await this.loadMarket(normalized.timestamp, normalized.symbol, normalized.marketType);
      const hasMarket = market !== null;
      if (hasMarket) {
        const symbol = normalized.symbol;
        const marketType = normalized.marketType;
        const marketStartAt = market.start.getTime();
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
          marketType,
          marketStartAt,
          maxDistanceMs: normalized.maxDistanceMs,
          includePolymarket: normalized.sources.includePolymarket
        });
        datapoint = this.assembler.assemble({
          timestamp: normalized.timestamp,
          market,
          symbol,
          marketType,
          marketStartAt,
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
      throw PersistedEventReadError.fromCause(
        `failed reading datapoint for symbol=${options.symbol} marketType=${options.marketType} at timestamp=${options.timestamp}`,
        error
      );
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
          symbol: normalized.symbol,
          marketType: normalized.marketType,
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
        `failed reading datapoint range for symbol=${options.symbol} marketType=${options.marketType} from=${options.startTimestamp} to=${options.endTimestamp}`,
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
