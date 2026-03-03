/**
 * @section imports:externals
 */

import type { CryptoSymbol, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import type { EventIndexCandidate } from "./types/event-index-types.ts";
import type { MarketDataPoint, OrderBookSnapshotValue, SelectedEventMeta } from "./types/market-data-point.ts";
import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type SelectedEvent = { event: StoredEvent; candidate: EventIndexCandidate };

type DatapointAssemblerOptions = {
  timestamp: number;
  market: PolymarketMarket | null;
  symbol: CryptoSymbol;
  marketType: "5m" | "15m";
  marketStartAt: number;
  cryptoProviders: string[];
  includeChainlink: boolean;
  includePolymarket: boolean;
  orderbookLevels: number;
  cryptoPriceByProvider: Record<string, SelectedEvent | null>;
  cryptoOrderBookByProvider: Record<string, SelectedEvent | null>;
  polymarketBook: SelectedEvent | null;
  polymarketPriceByAssetId: Record<string, SelectedEvent | null>;
};

export class DatapointAssembler {
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

  public static create(): DatapointAssembler {
    const assembler = new DatapointAssembler();
    return assembler;
  }

  /**
   * @section private:methods
   */

  private toMeta(candidate: EventIndexCandidate, timestamp: number): SelectedEventMeta {
    const meta: SelectedEventMeta = {
      source: candidate.source,
      eventType: candidate.eventType,
      ingestedAt: candidate.ingestedAt,
      deltaMs: candidate.ingestedAt - timestamp
    };
    return meta;
  }

  private extractPrice(event: StoredEvent): number | null {
    const payload = event.payload as Record<string, unknown>;
    const value = payload.price;
    const isNumber = typeof value === "number";
    const price = isNumber ? value : null;
    return price;
  }

  private sliceOrderBookLevels(levels: unknown, maxLevels: number): unknown[] {
    const isArray = Array.isArray(levels);
    const sliced = isArray ? levels.slice(0, maxLevels) : [];
    return sliced;
  }

  private extractOrderbook(event: StoredEvent, orderbookLevels: number): OrderBookSnapshotValue | null {
    const payload = event.payload as Record<string, unknown>;
    const bids = this.sliceOrderBookLevels(payload.bids, orderbookLevels);
    const asks = this.sliceOrderBookLevels(payload.asks, orderbookLevels);
    const hasLevels = bids.length > 0 || asks.length > 0;
    const orderbook = hasLevels ? { bids, asks } : null;
    return orderbook;
  }

  private appendMissing(missingFields: string[], field: string, condition: boolean): void {
    if (condition) {
      missingFields.push(field);
    }
  }

  private appendSelectionMeta(selectedEvents: SelectedEventMeta[], selection: SelectedEvent | null, timestamp: number): void {
    if (selection) {
      const meta = this.toMeta(selection.candidate, timestamp);
      selectedEvents.push(meta);
    }
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public assemble(options: DatapointAssemblerOptions): MarketDataPoint {
    const missingFields: string[] = [];
    const selectedEvents: SelectedEventMeta[] = [];
    const cryptoPricesBySource: Record<string, number | null> = {};
    const exchangeOrderbooksBySource: Record<string, OrderBookSnapshotValue | null> = {};

    for (const provider of options.cryptoProviders) {
      const priceSelection = options.cryptoPriceByProvider[provider] ?? null;
      const orderbookSelection = options.cryptoOrderBookByProvider[provider] ?? null;
      const price = priceSelection ? this.extractPrice(priceSelection.event) : null;
      const orderbook = orderbookSelection ? this.extractOrderbook(orderbookSelection.event, options.orderbookLevels) : null;
      cryptoPricesBySource[provider] = price;
      exchangeOrderbooksBySource[provider] = orderbook;
      this.appendMissing(missingFields, `crypto.price.${provider}`, price === null);
      this.appendMissing(missingFields, `crypto.orderbook.${provider}`, orderbook === null);
      this.appendSelectionMeta(selectedEvents, priceSelection, options.timestamp);
      this.appendSelectionMeta(selectedEvents, orderbookSelection, options.timestamp);
    }

    if (options.includeChainlink) {
      const chainlinkPriceSelection = options.cryptoPriceByProvider.chainlink ?? null;
      const chainlinkPrice = chainlinkPriceSelection ? this.extractPrice(chainlinkPriceSelection.event) : null;
      cryptoPricesBySource.chainlink = chainlinkPrice;
      this.appendMissing(missingFields, "crypto.price.chainlink", chainlinkPrice === null);
      this.appendSelectionMeta(selectedEvents, chainlinkPriceSelection, options.timestamp);
    }

    let upPrice: number | null = null;
    let downPrice: number | null = null;
    let polymarketOrderbook: OrderBookSnapshotValue | null = null;
    if (options.includePolymarket) {
      const upAssetId = options.market?.upTokenId ?? null;
      const downAssetId = options.market?.downTokenId ?? null;
      const upSelection = upAssetId ? (options.polymarketPriceByAssetId[upAssetId] ?? null) : null;
      const downSelection = downAssetId ? (options.polymarketPriceByAssetId[downAssetId] ?? null) : null;
      upPrice = upSelection ? this.extractPrice(upSelection.event) : null;
      downPrice = downSelection ? this.extractPrice(downSelection.event) : null;
      polymarketOrderbook = options.polymarketBook ? this.extractOrderbook(options.polymarketBook.event, options.orderbookLevels) : null;
      this.appendMissing(missingFields, "polymarket.price.up", upPrice === null);
      this.appendMissing(missingFields, "polymarket.price.down", downPrice === null);
      this.appendMissing(missingFields, "polymarket.book", polymarketOrderbook === null);
      this.appendSelectionMeta(selectedEvents, upSelection, options.timestamp);
      this.appendSelectionMeta(selectedEvents, downSelection, options.timestamp);
      this.appendSelectionMeta(selectedEvents, options.polymarketBook, options.timestamp);
    }

    const datapoint: MarketDataPoint = {
      timestamp: options.timestamp,
      symbol: options.symbol,
      marketType: options.marketType,
      marketStartAt: options.marketStartAt,
      cryptoPricesBySource,
      polymarket: { upPrice, downPrice, orderbook: polymarketOrderbook },
      exchangeOrderbooksBySource,
      coverage: { missingFields, selectedEvents }
    };
    return datapoint;
  }

  /**
   * @section static:methods
   */

  // empty
}
