FROM node:26.3.0-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js .

EXPOSE 8080

CMD ["node", "server.js"]