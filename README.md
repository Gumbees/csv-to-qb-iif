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

QuickBooks Web Connector Integration
------------------------------------

The application supports QuickBooks Web Connector (QBWC) for automated synchronization with QuickBooks Desktop 2024.

### Setting up QBWC for Testing

1. **Start the application:**
   ```bash
   npm start
   ```

2. **Download the QWC configuration file:**
   - Visit: `http://localhost:3000/qbwc/config`
   - This will download `csv-to-qb-sync.qwc`

3. **Configure QuickBooks Web Connector:**
   - Open QuickBooks Web Connector (should be installed with QuickBooks 2024)
   - Click "Add an application"
   - Browse and select the downloaded `.qwc` file
   - Click "OK"

4. **Authentication:**
   - Username: `halopsa_user`
   - Password: `password123`

5. **Update Process:**
   - Make sure QuickBooks 2024 is open and a company file is loaded
   - Click "Update" in the Web Connector
   - The application will send any pending purchase orders to QuickBooks

### Testing with Standalone Server

For dedicated QBWC testing without the full application:
```bash
npm run test-qbwc
```

This starts a minimal QBWC server on port 3000 with detailed logging.

### QBWC Endpoints

- **SOAP Endpoint:** `POST /qbwc` - Main QBWC communication
- **Config Endpoint:** `GET /qbwc/config` - Download QWC file
- **Test Endpoint:** `POST /api/qbwc/generate-test-xml` - Generate sample QBXML

### Features Supported

- ✅ Vendor synchronization
- ✅ Inventory item creation  
- ✅ Purchase order (Bill) import
- ✅ Authentication and session management
- ✅ Error handling and logging
- ✅ QBXML 13.0 compliance

### QuickBooks Setup Requirements

- QuickBooks Desktop 2024 (US Edition)
- QuickBooks Web Connector installed
- Company file must be open during updates
- Appropriate permissions for vendor and item creation

Notes
-----
- This replaces the previous Electron and Python/Streamlit implementations.
- If migrating existing data from sqlite, a one-time ETL will be needed.
- QBWC integration allows automated sync without manual IIF file imports.

License
-------
Proprietary or internal use. Update as needed.


