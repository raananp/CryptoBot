# services/scanner-spot-arb/Dockerfile
FROM node:20-alpine
WORKDIR /app

COPY package*.json ./
# prefer ci if lock exists; otherwise install
RUN [ -f package-lock.json ] && npm ci --omit=dev || npm install --omit=dev

COPY src ./src
ENV NODE_ENV=production
EXPOSE 9301
CMD ["node", "src/index.js"]