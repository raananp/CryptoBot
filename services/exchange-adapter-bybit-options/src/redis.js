import Redis from "ioredis";

export const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

export async function publishBook(prefix, symbol, book) {
  const key = `orderbook:${prefix}:${symbol}`;
  await redis.set(key, JSON.stringify(book));
}

export async function publishSymbols(prefix, symbols) {
  const key = `meta:${prefix}:symbols`;
  await redis.set(key, JSON.stringify(symbols));
}