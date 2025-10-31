# Build a small production image
FROM node:22-alpine

WORKDIR /usr/src/app

# Install Python for native dependency compilation
RUN apk add --no-cache python3 make g++

# Copy package files first for better caching
COPY package*.json ./

# Install all dependencies (including dev) for development
RUN npm install

# Copy source code only (exclude unnecessary files)
COPY src/ ./src/
COPY requirements.txt ./

# Create data directory for SQLite
RUN mkdir -p /usr/src/app/data

EXPOSE 3000

ENV NODE_ENV=production

# Optional: simple container healthcheck
HEALTHCHECK --interval=30s --timeout=3s --retries=5 CMD node -e "require('http').get('http://localhost:3000/healthz', r=>{if(r.statusCode!==200)process.exit(1);}).on('error',()=>process.exit(1))"

# Use node directly instead of npm start (which uses nodemon)
CMD [ "node", "src/server.js" ]
