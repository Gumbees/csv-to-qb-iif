csv-to-qb-iif (Web + Docker)
=============================

Centralized web application to convert CSV exports into QuickBooks IIF Bills, with inventory tracking and export history. Runs in Docker with PostgreSQL.

Features
--------
- Drag-and-drop CSV import via web UI (renderer.html)
- Duplicate import detection using checksum
- Transaction and inventory tracking in PostgreSQL
- IIF export (download) with export history recorded
- Dashboard with stats, transactions, inventory, imports, exports

Stack
-----
- Node.js + Express (server)
- PostgreSQL (db)
- Vanilla HTML/CSS/JS (client)
- Docker + docker-compose

Quick start (Docker)
--------------------
```bash
# Build and run
docker compose up --build -d

# App will be available at
open http://localhost:3000   # use your OS equivalent
```

Environment
-----------
- DATABASE_URL: connection string for Postgres (default in compose)

Local dev (without Docker)
--------------------------
```bash
# Install deps
npm install

# Set your DB URL (example)
export DATABASE_URL=postgres://user:password@localhost:5432/mydb

# Start server
node src/server.js
```

Project layout
--------------
- src/server.js: Express server and API endpoints
- src/database.js: PostgreSQL access layer and schema
- src/renderer.html: Single-page UI

Notes
-----
- This replaces the previous Electron and Python/Streamlit implementations.
- If migrating existing data from sqlite, a one-time ETL will be needed.

License
-------
Proprietary or internal use. Update as needed.


