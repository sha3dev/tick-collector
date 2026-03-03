const CONFIG = {
  COLLECTOR: {
    symbols: ["btc", "eth", "sol", "xrp"],
    windows: ["5m", "15m"],
    enabledSources: ["binance", "chainlink", "polymarket", "coinbase", "kraken", "okx"],
    coalesceIntervalMs: 500,
    outputDir: "data",
    flushIntervalMs: 60_000,
    maxGzipPartBytes: 64 * 1024 * 1024
  },
  READER: {
    defaultSources: ["binance", "coinbase", "kraken", "okx", "chainlink", "polymarket"],
    maxDistanceMs: 1_000,
    orderbookLevels: 5,
    tieBreakerPolicy: "prefer-past"
  }
} as const;

export default CONFIG;
