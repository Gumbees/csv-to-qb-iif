# Build a small production image
FROM node:18-alpine

WORKDIR /usr/src/app

# Install dependencies separately for better caching
COPY package.json ./
RUN npm install --production

# Copy the rest of the app
COPY . .

EXPOSE 3000

ENV NODE_ENV=production

# Optional: simple container healthcheck
HEALTHCHECK --interval=30s --timeout=3s --retries=5 CMD node -e "require('http').get('http://localhost:3000/healthz', r=>{if(r.statusCode!==200)process.exit(1);}).on('error',()=>process.exit(1))"

CMD [ "node", "src/server.js" ]
