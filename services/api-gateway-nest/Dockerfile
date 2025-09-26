FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --quiet || npm i --quiet

COPY tsconfig.json nest-cli.json ./
COPY src ./src

RUN npm run build

EXPOSE 8081
CMD ["node","dist/main.js"]