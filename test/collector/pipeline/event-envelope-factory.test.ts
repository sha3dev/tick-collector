import * as assert from "node:assert/strict";
import { test } from "node:test";

import { EventEnvelopeFactory } from "../../../src/collector/pipeline/event-envelope-factory.ts";

test("event envelope factory maps crypto and polymarket events", () => {
  const factory = EventEnvelopeFactory.create();

  const cryptoEvent = factory.fromCrypto({ sequence: 1, ingestedAt: 100, event: { type: "price", provider: "binance", symbol: "btc", ts: 90, price: 11 } });

  const polymarketEvent = factory.fromPolymarket({
    sequence: 2,
    ingestedAt: 200,
    symbol: "btc",
    marketType: "5m",
    marketSlug: "btc-updown-5m-1700000000",
    marketSide: "up",
    marketStartAt: 1700000000000,
    marketEventIndex: 0,
    event: { type: "book", source: "polymarket", assetId: "1", index: 1, date: new Date(190), bids: [], asks: [] }
  });

  assert.equal(cryptoEvent.source, "crypto");
  assert.equal(cryptoEvent.eventType, "crypto.price");
  assert.equal(cryptoEvent.ingestedAt, 100);
  assert.equal(cryptoEvent.exchangeTs, 90);

  assert.equal(polymarketEvent.source, "polymarket");
  assert.equal(polymarketEvent.eventType, "polymarket.book");
  assert.equal(polymarketEvent.symbol, "btc");
  assert.equal(polymarketEvent.marketType, "5m");
  assert.equal(polymarketEvent.marketSlug, "btc-updown-5m-1700000000");
  assert.equal(polymarketEvent.marketSide, "up");
  assert.equal(polymarketEvent.marketStartAt, 1700000000000);
  assert.equal(polymarketEvent.marketEventIndex, 0);
  assert.equal(polymarketEvent.ingestedAt, 200);
  assert.equal(polymarketEvent.exchangeTs, 190);
});
