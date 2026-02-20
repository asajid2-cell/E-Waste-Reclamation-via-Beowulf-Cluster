FROM node:22-alpine AS base

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

COPY . .
RUN chown -R node:node /app

USER node
EXPOSE 8080

CMD ["node", "server/index.js"]
