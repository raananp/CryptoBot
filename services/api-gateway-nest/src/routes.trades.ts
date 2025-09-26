import { Controller, Get, Query } from '@nestjs/common';
import mongoose, { Schema } from 'mongoose';

const mongoUrl = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';

let connPromise: Promise<typeof mongoose> | null = null;
function getConn() {
  if (!connPromise) connPromise = mongoose.connect(mongoUrl);
  return connPromise;
}

// Minimal trade doc typing to appease TS
interface TradeDoc {
  ts: number;
  mode: 'paper' | 'live' | string;
  legs: any[];
  realizedPnl: number;
}

// Reuse/compile once
function getTradeModel() {
  const schema =
    (mongoose.models.Trade as mongoose.Model<TradeDoc>)?.schema ||
    new Schema<TradeDoc>(
      { ts: Number, mode: String, legs: Array, realizedPnl: Number },
      { collection: 'trades' },
    );
  // Cast to concrete Model<TradeDoc> to avoid union overload issues
  return (mongoose.models.Trade as mongoose.Model<TradeDoc>) || mongoose.model<TradeDoc>('Trade', schema);
}

@Controller('trades')
export class TradesController {
  @Get()
  async list(@Query('mode') mode: string = 'paper') {
    await getConn();
    const Trade = getTradeModel(); // Model<TradeDoc>
    const items = await Trade.find({ mode } as any).sort({ ts: -1 }).limit(100).lean();
    return items;
  }
}