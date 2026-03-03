/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { CryptoMarketWindow, CryptoSymbol, GammaMarketCatalogService, MarketStreamService, PolymarketMarket } from "@sha3/polymarket";
import { MarketDiscoveryError } from "../errors/market-discovery-error.ts";

/**
 * @section consts
 */

const MINUTES_TO_MS = 60_000;

/**
 * @section types
 */

type SetTimeoutFn = (handler: () => void, timeoutMs: number) => NodeJS.Timeout;
type ClearTimeoutFn = (timeout: NodeJS.Timeout) => void;
type MarketContext = { symbol: CryptoSymbol | null; marketType: CryptoMarketWindow; marketStartAt: number };

type PolymarketMarketSchedulerOptions = {
  marketsService: GammaMarketCatalogService;
  streamService: MarketStreamService;
  symbols: CryptoSymbol[];
  windows: CryptoMarketWindow[];
  clock: () => number;
  setTimeoutFn?: SetTimeoutFn;
  clearTimeoutFn?: ClearTimeoutFn;
};

export class PolymarketMarketScheduler {
  /**
   * @section private:attributes
   */

  private isRunning: boolean;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly marketsService: GammaMarketCatalogService;
  private readonly streamService: MarketStreamService;
  private readonly symbols: CryptoSymbol[];
  private readonly windows: CryptoMarketWindow[];
  private readonly clock: () => number;
  private readonly setTimeoutFn: SetTimeoutFn;
  private readonly clearTimeoutFn: ClearTimeoutFn;
  private readonly timersByWindow: Map<CryptoMarketWindow, NodeJS.Timeout>;
  private readonly subscribedAssetIds: Set<string>;
  private readonly marketContextByAssetId: Map<string, MarketContext>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: PolymarketMarketSchedulerOptions) {
    this.marketsService = options.marketsService;
    this.streamService = options.streamService;
    this.symbols = options.symbols;
    this.windows = options.windows;
    this.clock = options.clock;
    this.setTimeoutFn = options.setTimeoutFn ?? setTimeout;
    this.clearTimeoutFn = options.clearTimeoutFn ?? clearTimeout;
    this.timersByWindow = new Map<CryptoMarketWindow, NodeJS.Timeout>();
    this.subscribedAssetIds = new Set<string>();
    this.marketContextByAssetId = new Map<string, MarketContext>();
    this.isRunning = false;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: PolymarketMarketSchedulerOptions): PolymarketMarketScheduler {
    const scheduler = new PolymarketMarketScheduler(options);
    return scheduler;
  }

  /**
   * @section private:methods
   */

  private parseWindowMinutes(window: CryptoMarketWindow): number {
    const value = Number(window.replace("m", ""));
    return value;
  }

  private toWindowMilliseconds(window: CryptoMarketWindow): number {
    const windowMinutes = this.parseWindowMinutes(window);
    const intervalMs = windowMinutes * MINUTES_TO_MS;
    return intervalMs;
  }

  private toDelayUntilNextBoundary(window: CryptoMarketWindow): number {
    const intervalMs = this.toWindowMilliseconds(window);
    const nowMs = this.clock();
    const elapsedInWindowMs = nowMs % intervalMs;
    const delayMs = intervalMs - elapsedInWindowMs;
    return delayMs;
  }

  private trackMarketAssets(markets: PolymarketMarket[], marketType: CryptoMarketWindow): string[] {
    const toSubscribe: string[] = [];

    for (const market of markets) {
      for (const assetId of market.clobTokenIds) {
        this.marketContextByAssetId.set(assetId, { symbol: market.symbol, marketType, marketStartAt: market.start.getTime() });
        const shouldSubscribe = !this.subscribedAssetIds.has(assetId);
        if (shouldSubscribe) {
          this.subscribedAssetIds.add(assetId);
          toSubscribe.push(assetId);
        }
      }
    }

    return toSubscribe;
  }

  private async discoverAndSubscribeForWindow(window: CryptoMarketWindow, date: Date): Promise<void> {
    const markets = await this.marketsService.loadCryptoWindowMarkets({ date, window, symbols: this.symbols });
    const toSubscribe = this.trackMarketAssets(markets, window);
    const hasNewAssets = toSubscribe.length > 0;
    if (hasNewAssets) {
      this.streamService.subscribe({ assetIds: toSubscribe });
    }
  }

  private scheduleWindow(window: CryptoMarketWindow): void {
    const delayMs = this.toDelayUntilNextBoundary(window);
    const timer = this.setTimeoutFn(() => {
      void this.onWindowBoundary(window);
    }, delayMs);
    this.timersByWindow.set(window, timer);
  }

  private async onWindowBoundary(window: CryptoMarketWindow): Promise<void> {
    const now = new Date(this.clock());
    const isActive = this.isRunning;

    if (isActive) {
      try {
        await this.discoverAndSubscribeForWindow(window, now);
      } finally {
        this.scheduleWindow(window);
      }
    }
  }

  private async subscribeCurrentWindows(): Promise<void> {
    const now = new Date(this.clock());

    for (const window of this.windows) {
      await this.discoverAndSubscribeForWindow(window, now);
    }
  }

  private scheduleAllWindows(): void {
    for (const window of this.windows) {
      this.scheduleWindow(window);
    }
  }

  private clearAllTimers(): void {
    for (const timer of this.timersByWindow.values()) {
      this.clearTimeoutFn(timer);
    }
    this.timersByWindow.clear();
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
      this.isRunning = true;
      await this.subscribeCurrentWindows();
      this.scheduleAllWindows();
    } catch (error: unknown) {
      throw MarketDiscoveryError.fromCause("failed starting polymarket market scheduler", error);
    }
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    this.clearAllTimers();
  }

  public getMarketContext(assetId: string): MarketContext | null {
    const context = this.marketContextByAssetId.get(assetId) ?? null;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
