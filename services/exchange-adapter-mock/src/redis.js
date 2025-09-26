
import Redis from 'ioredis';

export function createRedis() {
  const url = process.env.REDIS_URL || 'redis://redis:6379';
  return new Redis(url);
}

export async function xadd(client, stream, obj) {
  return client.xadd(stream, '*', 'data', JSON.stringify(obj));
}
