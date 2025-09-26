
import { Module } from '@nestjs/common';
import { AppController } from './routes';
import { OpportunitiesController } from './routes.opps';
import { TradesController } from './routes.trades';

@Module({
  controllers: [AppController, OpportunitiesController, TradesController],
  providers: [],
})
export class AppModule {}
