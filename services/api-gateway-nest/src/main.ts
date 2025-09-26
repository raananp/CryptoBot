
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './module';
import * as express from 'express';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { cors: true });
  app.use(express.json());
  await app.listen(8081);
  console.log('api-gateway-nest on 8081');
}
bootstrap();
