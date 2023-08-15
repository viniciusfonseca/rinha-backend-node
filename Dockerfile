FROM node:20-alpine3.17
WORKDIR /app
COPY package.json package.json
COPY index.ts index.ts
COPY index.d.ts index.d.ts
RUN npm i
RUN npm run build
EXPOSE 80
ENTRYPOINT ["node", "dist/index.js"]