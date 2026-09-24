FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
COPY backend/package.json backend/package.json
COPY frontend/package.json frontend/package.json
RUN npm install
COPY backend backend
COPY database database
RUN npm run build -w backend
FROM node:22-alpine
WORKDIR /app
COPY --from=build /app/node_modules node_modules
COPY --from=build /app/backend/dist dist
COPY --from=build /app/backend/package.json package.json
COPY --from=build /app/database database
EXPOSE 3000
CMD ["node","dist/server.js"]
