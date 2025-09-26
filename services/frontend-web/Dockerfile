# Build
FROM node:20-alpine as build
WORKDIR /app

# Needed for esbuild on Alpine
RUN apk add --no-cache libc6-compat

COPY package.json package-lock.json* ./
# Force devDependencies to be installed for build
RUN npm ci --silent --include=dev || npm i --silent --include=dev

COPY index.html vite.config.js ./
COPY public ./public
COPY src ./src

ENV CI=1
RUN npm run build

# Serve
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx","-g","daemon off;"]