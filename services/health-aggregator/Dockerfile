
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --silent || npm i --silent
COPY src ./src
EXPOSE 9405
CMD ["node","src/index.js"]
