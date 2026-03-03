import * as assert from "node:assert/strict";
import { test } from "node:test";

import { EventEnvelopeFactory } from "../../../src/collector/pipeline/event-envelope-factory.ts";
import { PolymarketFeedAdapter } from "../../../src/collector/polymarket/polymarket-feed-adapter.ts";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

type FakeMarketEvent = {
  type: "price" | "book";
  source: "polymarket";
  assetId: string;
  index: number;
  date: Date;
  price?: number;
  bids?: unknown[];
  asks?: unknown[];
};

type FakeStream = { addListener: (options: { listener: (event: FakeMarketEvent) => void }) => () => void; emit: (event: FakeMarketEvent) => void };

type FakeClient = { stream: FakeStream; connect: () => Promise<void>; disconnect: () => Promise<void> };

type FakeScheduler = {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  getMarketContext: (assetId: string) => null | { symbol: "btc"; marketType: "5m"; marketSlug: string; marketSide: "up" | "down"; marketStartAt: number };
};

function createFakeStream(): FakeStream {
  let listener: ((event: FakeMarketEvent) => void) | null = null;
  const stream: FakeStream = {
    addListener: (options): (() => void) => {
      listener = options.listener;
      const remove = (): void => {
        listener = null;
      };
      return remove;
    },
    emit: (event): void => {
      if (listener) {
        listener(event);
      }
    }
  };
  return stream;
}

async function waitForAsyncHandlers(): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

test("polymarket feed adapter stamps marketSlug and marketEventIndex by event class", async () => {
  const stream = createFakeStream();
  const events: StoredEvent[] = [];
  const client: FakeClient = {
    stream,
    connect: async (): Promise<void> => {
      // empty
    },
    disconnect: async (): Promise<void> => {
      // empty
    }
  };
  const scheduler: FakeScheduler = {
    start: async (): Promise<void> => {
      // empty
    },
    stop: async (): Promise<void> => {
      // empty
    },
    getMarketContext: (): { symbol: "btc"; marketType: "5m"; marketSlug: string; marketSide: "up" | "down"; marketStartAt: number } => {
      return { symbol: "btc", marketType: "5m", marketSlug: "btc-updown-5m-1772544600", marketSide: "up", marketStartAt: 1772544600000 };
    }
  };
  const adapter = new PolymarketFeedAdapter({
    client: client as never,
    scheduler: scheduler as never,
    eventHandler: async (event: StoredEvent): Promise<void> => {
      events.push(event);
    },
    envelopeFactory: EventEnvelopeFactory.create(),
    clock: () => 1772544600123,
    nextSequence: (() => {
      let sequence = 0;
      return (): number => {
        sequence += 1;
        return sequence;
      };
    })()
  });

  await adapter.start();
  stream.emit({ type: "price", source: "polymarket", assetId: "asset-up", index: 1, date: new Date(1772544600100), price: 0.55 });
  stream.emit({ type: "price", source: "polymarket", assetId: "asset-up", index: 2, date: new Date(1772544600200), price: 0.56 });
  stream.emit({ type: "book", source: "polymarket", assetId: "asset-up", index: 3, date: new Date(1772544600300), bids: [], asks: [] });
  await waitForAsyncHandlers();
  await adapter.stop();

  assert.equal(events.length, 3);
  assert.equal(events[0]?.marketSlug, "btc-updown-5m-1772544600");
  assert.equal(events[0]?.marketEventIndex, 0);
  assert.equal(events[1]?.marketEventIndex, 1);
  assert.equal(events[2]?.marketEventIndex, 0);
});
