import * as assert from "node:assert/strict";
import { test } from "node:test";

import { MarketDataPointReader } from "../../../src/collector/query/market-data-point-reader.ts";
import { InvalidReadRangeError } from "../../../src/collector/errors/invalid-read-range-error.ts";
import type { EventSelectionQuery } from "../../../src/collector/query/types/event-index-types.ts";
import type { PolymarketMarket } from "@sha3/polymarket";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

type EventSelectionResult = {
  event: StoredEvent;
  candidate: {
    partPath: string;
    ingestedAt: number;
    sequence: number;
    lineIndex: number;
    source: "crypto" | "polymarket";
    eventType: string;
    provider: string | null;
    symbol: string | null;
    marketSlug: string | null;
    assetId: string | null;
  };
} | null;

type FakeRepository = { findClosestEvent: (query: EventSelectionQuery) => Promise<EventSelectionResult>; calls: EventSelectionQuery[] };

type FakeMarketsService = { loadMarketBySlug: (options: { slug: string }) => Promise<PolymarketMarket> };

function buildEvent(options: {
  eventId: string;
  source: "crypto" | "polymarket";
  eventType: string;
  ingestedAt: number;
  sequence: number;
  provider?: string | null;
  symbol?: string | null;
  marketSlug?: string | null;
  assetId?: string | null;
  payload: Record<string, unknown>;
}): StoredEvent {
  const event: StoredEvent = {
    eventId: options.eventId,
    source: options.source,
    eventType: options.eventType,
    ingestedAt: options.ingestedAt,
    exchangeTs: options.ingestedAt,
    sequence: options.sequence,
    symbol: options.symbol ?? null,
    provider: options.provider ?? null,
    marketSlug: options.marketSlug ?? null,
    assetId: options.assetId ?? null,
    payload: options.payload
  };
  return event;
}

function buildSelection(event: StoredEvent): EventSelectionResult {
  const selection: EventSelectionResult = {
    event,
    candidate: {
      partPath: "data/journal/part-00000001.ndjson.gz",
      ingestedAt: event.ingestedAt,
      sequence: event.sequence,
      lineIndex: 0,
      source: event.source,
      eventType: event.eventType,
      provider: event.provider,
      symbol: event.symbol,
      marketSlug: event.marketSlug,
      assetId: event.assetId
    }
  };
  return selection;
}

function createFakeRepository(resolver: (query: EventSelectionQuery) => EventSelectionResult): FakeRepository {
  const calls: EventSelectionQuery[] = [];
  const repository: FakeRepository = {
    calls,
    findClosestEvent: async (query): Promise<EventSelectionResult> => {
      calls.push(query);
      const selected = resolver(query);
      return selected;
    }
  };
  return repository;
}

function createFakeMarketsService(market: PolymarketMarket): FakeMarketsService {
  const service: FakeMarketsService = {
    loadMarketBySlug: async (): Promise<PolymarketMarket> => {
      return market;
    }
  };
  return service;
}

function createReader(options: {
  repository: FakeRepository;
  marketsService: FakeMarketsService;
  maxDistanceMs?: number;
  orderbookLevels?: number;
}): MarketDataPointReader {
  const reader = MarketDataPointReader.create({
    folder: "data",
    defaultSources: { cryptoProviders: ["binance", "coinbase"], includeChainlink: true, includePolymarket: true },
    defaultMaxDistanceMs: options.maxDistanceMs ?? 30_000,
    defaultOrderbookLevels: options.orderbookLevels ?? 2,
    indexRepository: options.repository,
    marketsService: options.marketsService
  });
  return reader;
}

test("market data point reader merges nearest events for configured sources", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "btc-updown-5m-1",
    question: "q",
    symbol: "btc",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-1", "down-1"],
    upTokenId: "up-1",
    downTokenId: "down-1",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const timestamp = 1_000;
  const byKey: Record<string, EventSelectionResult> = {
    "crypto.price.binance": buildSelection(
      buildEvent({
        eventId: "1",
        source: "crypto",
        eventType: "crypto.price",
        ingestedAt: 990,
        sequence: 1,
        provider: "binance",
        symbol: "btc",
        payload: { price: 100 }
      })
    ),
    "crypto.price.coinbase": buildSelection(
      buildEvent({
        eventId: "2",
        source: "crypto",
        eventType: "crypto.price",
        ingestedAt: 1005,
        sequence: 2,
        provider: "coinbase",
        symbol: "btc",
        payload: { price: 101 }
      })
    ),
    "crypto.price.chainlink": buildSelection(
      buildEvent({
        eventId: "3",
        source: "crypto",
        eventType: "crypto.price",
        ingestedAt: 1002,
        sequence: 3,
        provider: "chainlink",
        symbol: "btc",
        payload: { price: 99 }
      })
    ),
    "crypto.orderbook.binance": buildSelection(
      buildEvent({
        eventId: "4",
        source: "crypto",
        eventType: "crypto.orderbook",
        ingestedAt: 996,
        sequence: 4,
        provider: "binance",
        symbol: "btc",
        payload: {
          bids: [
            { price: 1, size: 1 },
            { price: 2, size: 2 },
            { price: 3, size: 3 }
          ],
          asks: [
            { price: 4, size: 4 },
            { price: 5, size: 5 }
          ]
        }
      })
    ),
    "crypto.orderbook.coinbase": buildSelection(
      buildEvent({
        eventId: "5",
        source: "crypto",
        eventType: "crypto.orderbook",
        ingestedAt: 997,
        sequence: 5,
        provider: "coinbase",
        symbol: "btc",
        payload: { bids: [{ price: 10, size: 10 }], asks: [{ price: 11, size: 11 }] }
      })
    ),
    "polymarket.book": buildSelection(
      buildEvent({
        eventId: "6",
        source: "polymarket",
        eventType: "polymarket.book",
        ingestedAt: 998,
        sequence: 6,
        marketSlug: "btc-updown-5m-1",
        assetId: "up-1",
        payload: {
          bids: [
            { price: 0.4, size: 10 },
            { price: 0.41, size: 8 }
          ],
          asks: [
            { price: 0.6, size: 9 },
            { price: 0.61, size: 5 }
          ]
        }
      })
    ),
    "polymarket.price.up-1": buildSelection(
      buildEvent({
        eventId: "7",
        source: "polymarket",
        eventType: "polymarket.price",
        ingestedAt: 999,
        sequence: 7,
        marketSlug: "btc-updown-5m-1",
        assetId: "up-1",
        payload: { price: 0.62 }
      })
    ),
    "polymarket.price.down-1": buildSelection(
      buildEvent({
        eventId: "8",
        source: "polymarket",
        eventType: "polymarket.price",
        ingestedAt: 1_001,
        sequence: 8,
        marketSlug: "btc-updown-5m-1",
        assetId: "down-1",
        payload: { price: 0.38 }
      })
    )
  };
  const repository = createFakeRepository((query) => {
    const key = query.eventType === "polymarket.price" ? `${query.eventType}.${query.assetId ?? "na"}` : `${query.eventType}.${query.provider ?? "na"}`;
    const polymarketBookSelection = byKey["polymarket.book"] ?? null;
    const selection = byKey[key] ?? (query.eventType === "polymarket.book" ? polymarketBookSelection : null);
    return selection;
  });
  const marketsService = createFakeMarketsService(market);
  const reader = createReader({ repository, marketsService, orderbookLevels: 2 });

  const datapoint = await reader.read({ timestamp, marketSlug: "btc-updown-5m-1" });

  assert.equal(datapoint?.cryptoPricesBySource.binance, 100);
  assert.equal(datapoint?.cryptoPricesBySource.coinbase, 101);
  assert.equal(datapoint?.cryptoPricesBySource.chainlink, 99);
  assert.equal(datapoint?.polymarket.upPrice, 0.62);
  assert.equal(datapoint?.polymarket.downPrice, 0.38);
  assert.equal(datapoint?.exchangeOrderbooksBySource.binance?.bids.length, 2);
  assert.equal(datapoint?.coverage.missingFields.length, 0);
});

test("market data point reader reports partial coverage when events are missing", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "eth-updown-5m-1",
    question: "q",
    symbol: "eth",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-2", "down-2"],
    upTokenId: "up-2",
    downTokenId: "down-2",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const repository = createFakeRepository((query) => {
    const selection =
      query.eventType === "crypto.price" && query.provider === "binance"
        ? buildSelection(
            buildEvent({
              eventId: "9",
              source: "crypto",
              eventType: "crypto.price",
              ingestedAt: 100,
              sequence: 1,
              provider: "binance",
              symbol: "eth",
              payload: { price: 2500 }
            })
          )
        : null;
    return selection;
  });
  const reader = createReader({ repository, marketsService: createFakeMarketsService(market) });

  const datapoint = await reader.read({
    timestamp: 120,
    marketSlug: "eth-updown-5m-1",
    sources: { cryptoProviders: ["binance"], includeChainlink: false, includePolymarket: true }
  });

  assert.equal(datapoint?.cryptoPricesBySource.binance, 2500);
  assert.equal(datapoint?.polymarket.upPrice, null);
  assert.equal(datapoint?.coverage.missingFields.includes("polymarket.price.up"), true);
  assert.equal(datapoint?.coverage.missingFields.includes("polymarket.book"), true);
});

test("market data point reader applies source filters and maxDistance", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "sol-updown-5m-1",
    question: "q",
    symbol: "sol",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-3", "down-3"],
    upTokenId: "up-3",
    downTokenId: "down-3",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const repository = createFakeRepository((query) => {
    const maybeSelection =
      query.provider === "coinbase"
        ? buildSelection(
            buildEvent({
              eventId: "10",
              source: "crypto",
              eventType: "crypto.price",
              ingestedAt: 1000,
              sequence: 1,
              provider: "coinbase",
              symbol: "sol",
              payload: { price: 130 }
            })
          )
        : null;
    const deltaMs = maybeSelection ? Math.abs(maybeSelection.candidate.ingestedAt - query.timestamp) : Number.MAX_SAFE_INTEGER;
    const selection = deltaMs <= query.maxDistanceMs ? maybeSelection : null;
    return selection;
  });
  const reader = createReader({ repository, marketsService: createFakeMarketsService(market), maxDistanceMs: 10 });

  const datapoint = await reader.read({
    timestamp: 1_040,
    marketSlug: "sol-updown-5m-1",
    sources: { cryptoProviders: ["coinbase"], includeChainlink: false, includePolymarket: false }
  });

  assert.equal(datapoint?.cryptoPricesBySource.coinbase, null);
  assert.equal(datapoint?.coverage.missingFields.includes("crypto.price.coinbase"), true);
  assert.equal(datapoint?.polymarket.orderbook, null);
});

test("market data point reader returns null for unknown market", async () => {
  const repository = createFakeRepository(() => null);
  const marketsService: FakeMarketsService = {
    loadMarketBySlug: async (): Promise<PolymarketMarket> => {
      throw new Error("not found");
    }
  };
  const reader = createReader({ repository, marketsService });

  const datapoint = await reader.read({ timestamp: 100, marketSlug: "missing-market" });

  assert.equal(datapoint, null);
});

test("market data point reader uses indexed selections only (no full-scan contract)", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "xrp-updown-5m-1",
    question: "q",
    symbol: "xrp",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-4", "down-4"],
    upTokenId: "up-4",
    downTokenId: "down-4",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const repository = createFakeRepository(() => null);
  const reader = createReader({ repository, marketsService: createFakeMarketsService(market) });

  await reader.read({
    timestamp: 1000,
    marketSlug: "xrp-updown-5m-1",
    sources: { cryptoProviders: ["binance", "coinbase"], includeChainlink: true, includePolymarket: true }
  });

  assert.equal(repository.calls.length, 8);
});

test("market data point reader readRange returns datapoints across timestamps", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "btc-updown-5m-range",
    question: "q",
    symbol: "btc",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-1", "down-1"],
    upTokenId: "up-1",
    downTokenId: "down-1",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const repository = createFakeRepository((query) => {
    const selection = buildSelection(
      buildEvent({
        eventId: `${query.eventType}-${query.timestamp}`,
        source: query.source,
        eventType: query.eventType,
        ingestedAt: query.timestamp,
        sequence: 1,
        provider: query.provider ?? null,
        symbol: "btc",
        marketSlug: query.marketSlug ?? null,
        assetId: query.assetId ?? null,
        payload: query.eventType.includes("orderbook") || query.eventType.includes("book") ? { bids: [], asks: [] } : { price: 100 }
      })
    );
    return selection;
  });
  const reader = createReader({ repository, marketsService: createFakeMarketsService(market) });

  const points = await reader.readRange({
    startTimestamp: 1_000,
    endTimestamp: 1_200,
    stepMs: 100,
    marketSlug: "btc-updown-5m-range",
    sources: { cryptoProviders: ["binance"], includeChainlink: false, includePolymarket: false }
  });

  assert.equal(points.length, 3);
  assert.equal(points[0]?.timestamp, 1_000);
  assert.equal(points[1]?.timestamp, 1_100);
  assert.equal(points[2]?.timestamp, 1_200);
});

test("market data point reader readRange validates bounds and step", async () => {
  const market: PolymarketMarket = {
    id: "m1",
    slug: "btc-updown-5m-range",
    question: "q",
    symbol: "btc",
    conditionId: "c",
    outcomes: [],
    clobTokenIds: ["up-1", "down-1"],
    upTokenId: "up-1",
    downTokenId: "down-1",
    orderMinSize: 1,
    orderPriceMinTickSize: null,
    eventStartTime: "",
    endDate: "",
    start: new Date(),
    end: new Date(),
    raw: {}
  };
  const repository = createFakeRepository(() => null);
  const reader = createReader({ repository, marketsService: createFakeMarketsService(market) });

  await assert.rejects(async () => {
    await reader.readRange({ startTimestamp: 2_000, endTimestamp: 1_000, stepMs: 100, marketSlug: "btc-updown-5m-range" });
  }, InvalidReadRangeError);

  await assert.rejects(async () => {
    await reader.readRange({ startTimestamp: 1_000, endTimestamp: 2_000, stepMs: 0, marketSlug: "btc-updown-5m-range" });
  }, InvalidReadRangeError);
});
