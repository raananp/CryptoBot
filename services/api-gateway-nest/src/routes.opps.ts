
import { Controller, Get } from '@nestjs/common';
import Redis from 'ioredis';
const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

@Controller('opportunities')
export class OpportunitiesController {
  @Get()
  async getRecent(){
    const items = await redis.lrange('arb.opportunities.recent', -100, -1);
    return items.map(i=>JSON.parse(i));
  }
}
