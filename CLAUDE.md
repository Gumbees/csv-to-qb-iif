# CLAUDE.md

> **Project Documentation for Claude Code Assistant**
> This file provides comprehensive guidance for working with the csv-to-qb-iif codebase.

---

## Working with This Codebase

### Agent Delegation Strategy

**CRITICAL - PARALLEL AGENT EXECUTION**: When working on complex, multi-part tasks in this codebase, launch multiple agents in a **SINGLE message** for maximum efficiency.

#### The Parallel Agent Pattern

1. **Plan First**: Break down work into logical, independent components
2. **Launch in Parallel**: Use multiple specialized agents simultaneously **in ONE message**
3. **Maximize Efficiency**: 5 agents working in parallel = 5x faster than sequential execution

#### Example Multi-Agent Workflow

```
Task: Add AI-powered mapping, searchable dropdowns, and duplicate PO checking

→ Agent 1 (rosa): AI Settings + Database Schema
→ Agent 2 (rosa): Core AI Service Layer
→ Agent 3 (rosa): Customer Mapping AI Integration
→ Agent 4 (rosa): Transaction Mapping AI Integration
→ Agent 5 (rosa): Searchable Dropdown Component
→ Agent 6 (rosa): PO Duplicate Detection

ALL AGENTS LAUNCHED IN A SINGLE MESSAGE FOR PARALLEL EXECUTION
```

#### When to Use Multiple Agents

✅ **Use Parallel Agents When:**
- Adding multiple independent API endpoints
- Implementing separate backend and frontend features simultaneously
- Database schema changes alongside API development
- Multiple UI components or views
- Independent service integrations
- Adding features to different parts of the stack (frontend + backend + database)

❌ **Don't Use Parallel Agents When:**
- Tasks have strict dependencies (A must complete before B starts)
- Working on a single file with sequential edits
- Simple, straightforward single-component changes

#### Agent Selection Guide

- **rosa-code-architect**: Code architecture, API design, database schema, technical documentation, UI components
- **general-purpose**: Research, file searches, debugging, investigations, multi-step analysis

---

### UI Architecture: Modular Containerization

**GOLDEN RULE**: All UI tables and data displays MUST be containerized with scrolling.

#### Standard Container Pattern

```html
<div class="content-section">
    <h3>Section Title</h3>
    <p>Description and controls (buttons, filters)</p>

    <!-- Scrollable container for table -->
    <div class="table-container-scrollable">
        <table class="table">
            <!-- Table content -->
        </table>
    </div>

    <div class="status-message">Status or empty state message</div>
</div>
```

#### CSS Standard

```css
.table-container-scrollable {
    overflow-x: auto;
    overflow-y: auto;
    max-height: 400px; /* Standard height - adjust per use case */
    margin-top: 1.5rem;
    border: 1px solid #ecf0f1;
    border-radius: 6px;
}
```

#### What Goes Where

**INSIDE Scrollable Container:**
- Tables only (thead, tbody)
- Data grids
- Large content blocks that need scrolling

**OUTSIDE Scrollable Container:**
- Action buttons
- Filters and search boxes
- Section headers
- Status messages
- Pagination controls

#### Why This Matters

- Prevents page from becoming infinitely long
- Maintains clean, scannable interface
- Consistent user experience across all views
- Better performance with large datasets

---

### Process Management

**CRITICAL - NEVER BROADLY KILL NODE PROCESSES**

#### ❌ WRONG - Kills ALL Node Processes

```bash
taskkill /F /IM node.exe  # DANGEROUS - Kills everything including other projects!
```

#### ✅ CORRECT - Target Specific Process

```bash
# 1. Find process by port
netstat -ano | findstr :3000

# 2. Kill only that specific PID
taskkill /F /PID 12345
```

#### Why This Matters

- Development environment may have multiple Node processes running
- Other projects, VS Code extensions, build tools all use Node
- Killing all node.exe processes destroys your entire dev environment
- Always target the specific PID, never the process name

---

### Proactive Code Hygiene

**ALWAYS be proactive about improvements and cleanup while working:**

1. **Remove Unused Files**: If you create a file and then find it's not needed, delete it immediately. Don't leave dead code.
2. **Update Documentation**: When making changes, update CLAUDE.md, README.md, and inline comments in the same commit.
3. **Refactor as You Go**: If you see repeated code, abstract it. If file structure is messy, reorganize it.
4. **Clean Imports**: Remove unused dependencies, imports, and variables.
5. **Consistent Naming**: Follow existing patterns. Fix inconsistencies when you see them.
6. **Test After Changes**: Restart the server and verify the feature works before moving on.

**Think holistically** - don't just complete the task, leave the codebase better than you found it.

## Project Overview

**csv-to-qb-iif** is a multi-integration financial data synchronization platform that connects Stripe, HaloPSA, and QuickBooks Desktop. The application is now a pure web service with a browser-based UI (Electron functionality has been removed).

### Core Functionality
- **CSV Import**: Parse CSV exports and convert to QuickBooks IIF Bills format
- **Stripe Integration**: Import customers and transactions from Stripe API
- **HaloPSA Integration**: Sync clients, invoices, and purchase orders via OAuth2 authentication
- **QuickBooks Integration**: Two sync methods:
  - IIF file export (manual import)
  - QuickBooks Web Connector (QBWC) for automated sync via QBXML
- **Customer Mapping**: Auto-match customers across Stripe, HaloPSA, and QuickBooks
- **Invoice Mapping**: Match Stripe transactions to HaloPSA invoices by parsing invoice IDs from transaction descriptions

## Development Commands

### Running the Application

**Quick Start (Recommended)**:
```bash
# Start server in background
node src/server.js
```

The application is web-based and runs on **http://localhost:3000**

**Check if already running**:
```bash
# Check for process on port 3000
netstat -ano | findstr :3000
```

**Stop specific server process**:
```bash
# Find PID from netstat output, then:
taskkill /F /PID <pid>
```

**Docker**:
```bash
docker compose up --build -d
```

**Note**: The application is now **web-only** (Electron removed). Access via browser at http://localhost:3000

### Testing

**QuickBooks Web Connector Testing**:
```bash
npm run test-qbwc
```

**Test Files** (run with `node <filename>`):
- `test-halopsa.js` - Test HaloPSA API connection
- `test-halopsa-detailed.js` - Detailed HaloPSA endpoint testing
- `test-automatch.js` - Test customer auto-matching logic
- `test-config.js` - Test configuration system
- `test-mapping-workflow.js` - Test customer mapping workflow
- `test-frontend-automatch.js` - Test frontend auto-match integration
- `test-simple-insert.js` - Test database operations

## Architecture

### Core Components

**src/server.js**
- Express server with RESTful API endpoints
- Handles CSV upload, parsing, and IIF generation
- Manages QBWC SOAP endpoints
- Orchestrates integration between Stripe, HaloPSA, and QuickBooks
- Port: 3000 (configurable via PORT env var)

**src/database.js**
- SQLite database manager using `better-sqlite3`
- Default location: `/usr/src/app/data/csv-to-qb-iif.db` (Docker) or `DB_PATH` env var
- Synchronous API with async initialization (`await db.ready`)
- Schema includes: csv_imports, transactions, stripe_customers, stripe_transactions, customer_mappings, halopsa_clients, halopsa_invoices, halopsa_purchase_orders, config

**src/stripe-api.js**
- Native HTTPS client (not using official Stripe SDK)
- Methods: `importCustomers()`, `importTransactions()`, `testConnection()`
- Handles pagination for large datasets
- Stores raw JSON in `raw_data` field for debugging

**src/halopsa-api.js**
- OAuth2 client credentials flow authentication
- Obtains fresh access token for each request (tokens not cached)
- Multiple endpoint fallbacks for clients, invoices, purchase orders
- Detailed error messages and endpoint discovery logic
- Methods: `testConnection()`, `getClients()`, `importInvoices()`, `importClientsFromHaloPSA()`

**src/qbwc-service.js**
- QuickBooks Web Connector SOAP service
- Generates QBXML 13.0 compliant messages
- Session management for QBWC connections
- Generates .qwc configuration files with proper GUID format (uppercase with braces)
- IMPORTANT: Multiple `generateGUID()` definitions exist - the last one is used

**src/config-api.js**
- Configuration management with type validation
- Categories: general, stripe, halopsa, quickbooks, qbwc, accounting, mapping, ai
- Config stored in database `config` table with metadata (type, label, description, options)

**src/ai-service.js**
- AI-powered customer and transaction mapping using Anthropic Claude API
- Intelligent fuzzy matching across Stripe, HaloPSA, and QuickBooks
- Pattern-based transaction-to-invoice mapping with confidence scoring
- Methods:
  - `init()` - Load API key from ai_settings table and initialize Anthropic client
  - `initialize(apiKey)` - Direct initialization with API key
  - `testConnection()` - Verify Anthropic API connectivity
  - `suggestCustomerMappings(stripeCustomers, haloClients, qbCustomers)` - AI-powered customer matching
  - `suggestTransactionMappings()` - Pattern-based transaction-to-invoice matching (no AI required)
  - `extractInvoiceNumber(description)` - Parse invoice numbers from transaction descriptions
  - `calculateAmountMatch(txAmount, invAmount)` - Score amount similarity (0-1)
  - `calculateDateProximity(date1, date2)` - Score date proximity (0-1)
  - `calculateCustomerMatch(customerId, clientId)` - Check if customer mapping exists
  - `getPendingTransactionSuggestions()` - Fetch pending AI suggestions from database
  - `approveTransactionMapping(suggestionId)` - Approve and create mapping from suggestion
  - `rejectTransactionMapping(suggestionId)` - Reject mapping suggestion
  - `updateConfig(apiKey, model)` - Update AI service configuration
- Confidence scoring system:
  - Customer mappings: 1.0 = exact email match, 0.9-0.99 = strong name + contact, 0.8-0.89 = name + address, 0.7-0.79 = fuzzy name match
  - Transaction mappings: Invoice number match (50%), Amount match (30%), Date proximity (10%), Customer mapping (10%)
- Data chunking for large datasets (50 customers per request, 30 transactions per request)
- Handles Claude API errors gracefully (401 invalid key, 429 rate limit, 500 server error)
- Uses `ai_settings` and `ai_mapping_suggestions` tables for configuration and suggestion storage
- Model: `claude-3-5-sonnet-20241022` (configurable)

**AI Settings Configuration UI**
- Web UI for configuring Anthropic Claude API integration (accessible at `/#ai-settings`)
- Features:
  - Secure API key input (password field, never logged)
  - Model selection dropdown with descriptions (Sonnet 4, 3.5 Sonnet, 3.7 Sonnet)
  - Test Connection button to verify API key validity
  - Save Settings button to persist configuration
  - Real-time status feedback with color-coded messages
  - AI features info section explaining capabilities
- API Endpoints (Backend - `src/server.js`):
  - `GET /api/ai/settings` - Retrieve current AI configuration (returns model and is_configured flag)
  - `POST /api/ai/settings` - Save API key and model selection (updates ai_settings table)
  - `GET /api/ai/models` - List available Claude models with descriptions
  - `POST /api/ai/test-connection` - Test Anthropic API with sample prompt
- Database Storage:
  - `ai_settings` table stores `anthropic_api_key` and `anthropic_model`
  - Default model: `claude-sonnet-4-20250514` (inserted on first run)
  - API key stored securely, never exposed in GET requests (placeholder shown in UI)
- Security:
  - API key transmission over HTTPS only (use cloudflared tunnel for production)
  - Password-type input field masks API key entry
  - API key never logged or returned in plain text
  - Test connection uses live Anthropic API (validates key immediately)

### Frontend

**src/index.html** (served at http://localhost:3000)
- Main web UI with tabbed interface
- Tabs: Dashboard, Configuration, Stripe Sync, Customer Mapping, Customer View, System Status
- Uses vanilla JavaScript with fetch API for backend communication
- No build step required - pure HTML/CSS/JS served directly by Express
- Customer Mappings tab now uses modular CustomerMappingsComponent (see below)
- **Sidebar Statistics** (Added 2025-10-30):
  - `loadSidebarStats()` function loads all badge counts immediately on page load
  - Uses `Promise.allSettled()` to fetch all stats in parallel for fast loading
  - Updates sidebar badges for: Customers, Invoices, Items, Purchase Orders, Stripe Transactions, Mappings
  - Called on every page load AND after any data import/sync operation
  - Ensures badges always show real numbers, not 0
  - Makes application feel cohesive - stats visible regardless of current view

**src/components/customer-mappings.js** - Modular Customer Mappings Component (ES6 Module)
- **Architecture**: Self-contained ES6 class exported as module
- **Three-column layout**:
  - Column 1: Stripe Customers (draggable source)
  - Column 2: HaloPSA Clients (drop zones + primary display)
  - Column 3: QuickBooks Customers (draggable source)
- **Key Features**:
  - HTML5 Drag & Drop API for creating mappings visually
  - Infinite scroll for all three columns (50 items per page)
  - Real-time search filtering with 300ms debounce
  - Auto-mapping via backend fuzzy match algorithm
  - Individual unmap buttons (Stripe/QB separately)
  - Visual feedback: drag ghost, drop zone highlighting, mapping badges
  - Responsive design (stacks columns on mobile)
- **Component API**:
  ```javascript
  import CustomerMappingsComponent from './components/customer-mappings.js';

  // Initialize component
  const component = new CustomerMappingsComponent('container-id');
  await component.init();

  // Public methods
  component.refresh();           // Reload all data
  component.autoMap();           // Run auto-matching
  component.unmapStripe(id);     // Remove Stripe mapping
  component.unmapQB(id);         // Remove QB mapping
  component.destroy();           // Cleanup observers
  ```
- **Backend Endpoints Used**:
  - `GET /api/stripe/customers?limit=50&offset=0` - Fetch Stripe customers
  - `GET /api/customers/all?limit=50&offset=0` - Fetch HaloPSA clients
  - `GET /api/qbd/sync/customers?limit=50&offset=0` - Fetch QB customers
  - `POST /api/customers/mappings/stripe` - Create Stripe → HaloPSA mapping
  - `POST /api/customers/mappings/qb` - Create QB → HaloPSA mapping
  - `DELETE /api/customers/mappings/stripe/:halopsa_client_id` - Remove Stripe mapping
  - `DELETE /api/customers/mappings/qb/:halopsa_client_id` - Remove QB mapping
  - `POST /api/customers/automatch` - Run auto-matching algorithm

**src/components/customer-mappings.css** - Component-specific styles
- CSS Grid-based three-column layout (1fr 1fr 1fr)
- Drag-and-drop visual feedback (dragging, drag-over, drop-zone classes)
- Mapping badges for visual status indicators
- Custom scrollbars for column bodies
- Responsive breakpoints for mobile stacking

**src/components/searchable-dropdown.js** - Searchable Dropdown Component (Pure JavaScript Class)
- **Architecture**: Reusable, accessible searchable dropdown with keyboard navigation
- **Features**:
  - Fuzzy search with real-time filtering and match highlighting
  - Keyboard navigation (Arrow keys, Enter, Escape, Tab)
  - Virtual scrolling for large datasets (>100 items)
  - Touch-friendly mobile support
  - Debounced search input (300ms)
  - Click-outside-to-close behavior
  - Clear selection button
  - Auto-positioning (dropdown/dropup based on available space)
- **Component API**:
  ```javascript
  // Initialize dropdown
  const dropdown = new CustomSearchableDropdown('container-id', {
      placeholder: 'Search accounts...',
      items: [{id: 1, text: 'Account Name'}, ...],
      selectedId: null,
      onChange: (selectedItem) => { /* Handle selection */ },
      renderItem: (item) => `<span>${item.text}</span>`,  // Optional custom rendering
      disabled: false,
      maxHeight: 300,
      debounceDelay: 300,
      virtualScrollThreshold: 100
  });

  // Public methods
  dropdown.setItems(items);           // Update available items
  dropdown.setSelectedId(id);         // Set selected item by ID
  dropdown.getValue();                // Get currently selected item
  dropdown.clear();                   // Clear selection
  dropdown.focus();                   // Focus the input
  dropdown.setDisabled(true/false);   // Toggle disabled state
  dropdown.destroy();                 // Clean up event listeners
  ```
- **Usage in Application**:
  - Account selection dropdowns in QuickBooks Sync view (Deposits, Fees, Payments, Inventory)
  - Wrapped by `CustomAccountDropdown` adapter class for backward compatibility
  - Can be used for any searchable select functionality
- **Styling**: See `src/components/searchable-dropdown.css` for modern purple-themed design
- **Accessibility**: ARIA attributes, keyboard navigation, focus management

**src/components/searchable-dropdown.css** - Searchable Dropdown Styles
- Modern, clean design matching application's purple color scheme
- Hover effects, selected item highlighting, loading state indicators
- Dropdown auto-positioning (appears above/below based on available space)
- Virtual scrolling support with custom scrollbars
- Mobile-responsive (16px font size to prevent iOS zoom)
- Dark mode support (optional, via prefers-color-scheme)
- Print-friendly (hides dropdown menu and controls)

**src/drag-drop-mapping.html** (legacy - served at http://localhost:3000/drag-drop-mapping.html)
- Legacy two-column drag-and-drop interface (Stripe + HaloPSA only)
- Kept for backward compatibility but superseded by modular component
- **Note**: New development should use CustomerMappingsComponent

### Data Flow

1. **CSV Import Flow**:
   - Upload CSV → Parse with `csv-parse` → Group by vendor/ref → Generate IIF → Store in database → Download IIF file

2. **Stripe Sync Flow**:
   - Configure API key → Test connection → Import customers → Import transactions → Auto-match to HaloPSA clients

3. **HaloPSA Sync Flow**:
   - Configure OAuth2 credentials → Authenticate (client_credentials grant) → Import clients → Import invoices/POs → Map to Stripe customers

4. **QuickBooks Sync Flow**:
   - **IIF**: Generate IIF file → Manual import to QuickBooks
   - **QBWC**: Install QWC file → QBWC polls server → Server generates QBXML → QuickBooks processes → Response logged

## Database Schema Key Points

### Tables
- `csv_imports`: Stores uploaded CSV files with checksums for duplicate detection
- `transactions`: Parsed bill transactions from CSV
- `stripe_customers` / `stripe_transactions`: Stripe data with `stripe_id` as unique key
- `halopsa_clients` / `halopsa_invoices`: HaloPSA data with `halopsa_id` as unique key
- `customer_mappings`: Links Stripe customers to HaloPSA clients (with `auto_mapped` and `mapping_confirmed` flags)
- `config`: Application configuration with type system (text, number, boolean, password, select, json)
- `ai_settings`: AI service configuration (API keys, model selection)
  - `key`: Setting key (e.g., `anthropic_api_key`, `anthropic_model`)
  - `value`: Setting value
  - `created_at` / `updated_at`: Timestamps
- `ai_mapping_suggestions`: AI-generated mapping suggestions
  - `suggestion_type`: Type of suggestion (`customer` or `transaction`)
  - `source_type` / `source_id`: Source record type and ID
  - `target_type` / `target_id`: Target record type and ID
  - `confidence`: Confidence score (0.0-1.0)
  - `reasoning`: Human-readable explanation of the match
  - `status`: Suggestion status (`pending`, `approved`, `rejected`)
  - `created_at` / `updated_at`: Timestamps

### Important Fields
- All foreign keys use `_id` suffix (e.g., `stripe_customer_id`, `halopsa_client_id`)
- `raw_data TEXT`: Stores full JSON response for debugging
- `last_sync DATETIME`: Tracks when data was last synchronized
- `checksum TEXT UNIQUE`: Prevents duplicate CSV imports

## Configuration System

Configuration is stored in the `config` table with metadata:
- **Key**: Unique identifier (e.g., `stripe_secret_key`, `halopsa_api_url`)
- **Value**: String representation (converted based on type)
- **Type**: text, number, boolean, password, select, json
- **Category**: general, stripe, halopsa, quickbooks, qbwc, accounting, mapping
- **Label**: Human-readable name for UI
- **Description**: Help text for UI
- **Options**: JSON array of valid values for select type
- **is_required**: Boolean flag

Access via `ConfigAPI` class or directly via database `getConfig()` / `setConfig()` methods.

## API Authentication & Security

### HaloPSA Authentication
- **Method**: OAuth2 Client Credentials Flow
- **Endpoints tried**: `/oauth/token`, `/auth/token`, `/api/oauth/token`, `/api/auth/token`
- **Scope**: `all`
- **Token Handling**: Fresh token per request, not cached in database
- **Common Issues**:
  - 404 on token endpoint → Check API URL and token endpoint path
  - 401 → Invalid client credentials
  - 403 → Insufficient permissions on client credentials

### Stripe Authentication
- **Method**: Bearer token (Secret Key)
- **Format**: `Authorization: Bearer sk_test_...` or `sk_live_...`
- **Stored in**: `config` table as `stripe_secret_key`

### QBWC Authentication
- **Method**: Username/Password
- **Default**: `qbwc_user` / `password123` (stored in `config` table)
- **Ticket System**: Session tickets generated on successful auth

## HaloPSA Report API

### Overview
HaloPSA provides a **Report API** (`/Report/{id}`) that allows fetching data using pre-configured reports. This is the **recommended approach** for importing complex data like purchase orders with line items, as the standard resource endpoints (`/api/PurchaseOrder`) often lack detailed fields.

### Why Use Reports Instead of Resource Endpoints?
- **Complete Data**: Reports can be configured to include all fields, including related data (line items, custom fields, etc.)
- **Custom SQL**: Reports run custom SQL queries that return exactly the data you need
- **Structured Output**: Data is returned in a consistent, predictable format
- **Better Performance**: Single request returns all needed data (no need for additional detail fetches)

### Report API Endpoints

**List All Reports:**
```
GET /api/Report
```
Query parameters: `count`, `search`, `pageinate`, `page_size`, `page_no`, `orderby`, `orderbydesc`

**Get Single Report Definition:**
```
GET /api/Report/{id}?includedetails=true&loadreport=false
```
Returns report metadata: name, SQL, available columns, filters, permissions

**Execute Report and Load Data:**
```
GET /api/Report/{id}?loadreport=true
```
Returns the actual report data based on the configured SQL query

### Purchase Orders via Report API

**Report ID:** `350` (Purchase Orders Report)

**Example Request:**
```javascript
const response = await fetch(`${apiUrl}/api/Report/350?loadreport=true`, {
    headers: { 'Authorization': `Bearer ${accessToken}` }
});
const data = await response.json();
```

**Response Structure:**
```json
{
    "page_no": 0,
    "page_size": 50,
    "record_count": 1500,
    "report": {
        "rows": [
            {
                "po_id": 123,
                "po_number": "PO-1001",
                "supplier_name": "Vendor Inc",
                "total": 1500.00,
                "line_items": [...],  // Included when report is configured properly
                // ... other fields defined in report SQL
            }
        ]
    },
    "columns": [...]
}
```

**Pagination:**
- Report API supports pagination via `page_no` and `page_size` parameters
- Check `record_count` for total number of records
- Continue fetching pages until `rows.length < page_size`

### Report Configuration

Reports are configured in HaloPSA Admin interface:
1. Navigate to Configuration → Reports
2. Select or create report (ID 350 for Purchase Orders)
3. Define SQL query with all needed fields
4. Configure columns, filters, and permissions
5. Publish report for API access

**SQL Data Structure:**
The SQL structure for HaloPSA reports is stored in database schema files. For purchase orders (Report ID 350), the schema includes fields for:
- Purchase order header: ID, PO number, dates, status, totals
- Vendor information: ID, name, contact details
- Line items: Product/service details, quantities, costs
- Custom fields: Additional business-specific data

### Implementation Pattern

```javascript
async function importPurchaseOrdersViaReport() {
    const reportId = 350;  // Purchase Orders Report
    let page = 1;
    let hasMore = true;
    const pageSize = 50;

    while (hasMore) {
        const response = await this.makeRequest(
            `/api/Report/${reportId}?loadreport=true&page_no=${page}&page_size=${pageSize}`
        );

        const rows = response.report?.rows || [];

        // Process each purchase order
        for (const po of rows) {
            // Extract PO data including line_items
            const poData = extractPOData(po);

            // Insert/update in database
            await db.upsertPurchaseOrder(poData);

            // Extract and save line items
            if (po.line_items && Array.isArray(po.line_items)) {
                await extractAndSaveItems(po.line_items);
            }
        }

        hasMore = rows.length === pageSize;
        page++;
    }
}
```

### Key Differences: Reports vs Resource Endpoints

| Feature | Resource API (`/api/PurchaseOrder`) | Report API (`/api/Report/350`) |
|---------|-------------------------------------|--------------------------------|
| Line items included | ❌ No | ✅ Yes (if configured) |
| Custom fields | ❌ Limited | ✅ All fields in SQL |
| Performance | ⚠️ May need detail fetches | ✅ Single request |
| Flexibility | ❌ Fixed structure | ✅ Customizable SQL |
| Pagination | ✅ Standard | ✅ Standard |
| Real-time data | ✅ Yes | ✅ Yes |

### Best Practices

1. **Use Reports for Complex Data**: Purchase orders, invoices with line items, custom fields
2. **Use Resource Endpoints for Simple Lists**: Clients, suppliers, basic ticket lists
3. **Cache Report Definitions**: Load report metadata once, reuse structure
4. **Handle Missing Fields Gracefully**: Not all reports have same fields configured
5. **Monitor Report Changes**: Report SQL can be modified in HaloPSA admin
6. **Test Report Access**: Ensure API credentials have permission to access specific reports

### Troubleshooting

**Report Returns No Data:**
- Verify report ID is correct
- Check report permissions (API user must have access)
- Ensure `loadreport=true` parameter is included
- Review report filters (may be excluding data)

**Missing Line Items:**
- Report SQL must explicitly join and select line item fields
- Check report configuration in HaloPSA admin
- Verify `line_items` field is in the selected columns

**Pagination Issues:**
- Don't rely on `record_count` alone - it may be page count, not total records
- Use `rows.length < page_size` to detect last page
- Handle empty pages gracefully

## Key Integration Patterns

### AI-Powered Customer Mapping
**Service**: `src/ai-service.js` - `suggestCustomerMappings()`

**How it works**:
1. Configure Anthropic API key in `ai_settings` table
2. Initialize AIService: `const aiService = new AIService(db); await aiService.init();`
3. Fetch unmapped customers from Stripe, HaloPSA, and QuickBooks
4. Send customer data to Claude API with structured prompts
5. Claude analyzes:
   - Exact email matches (confidence: 1.0)
   - Company name similarity with fuzzy matching
   - Phone number matches
   - Address data matches
6. Returns suggestions with confidence >= 0.7
7. User reviews and approves/rejects suggestions

**Usage**:
```javascript
const suggestions = await aiService.suggestCustomerMappings(
  stripeCustomers,
  haloPSAClients,
  qbCustomers
);

// suggestions = [
//   {
//     source_system: 'stripe',
//     source_id: 'cus_xxx',
//     target_system: 'halopsa',
//     target_id: '12345',
//     confidence: 0.95,
//     reasoning: 'Exact email match: contact@company.com'
//   }
// ]
```

### Pattern-Based Transaction Mapping
**Service**: `src/ai-service.js` - `suggestTransactionMappings()`

**How it works** (NO Claude API needed - pure pattern matching):
1. Fetch unmapped Stripe transactions
2. Fetch all HaloPSA invoices
3. For each transaction, calculate confidence score based on:
   - **Invoice number extraction (50% weight)**: Parse description for patterns like "Invoice #INV-1234"
   - **Amount matching (30% weight)**: Compare transaction amount to invoice amount (exact or close)
   - **Date proximity (10% weight)**: Days between transaction date and invoice date
   - **Customer mapping (10% weight)**: Check if customer mapping exists
4. Save suggestions to `ai_mapping_suggestions` table with status `pending`
5. User reviews and approves/rejects via API endpoints

**Invoice Number Patterns**:
- `invoice\s*#?\s*([A-Z0-9-]+)` → "Invoice #INV-1234"
- `inv\s*#?\s*([A-Z0-9-]+)` → "INV-1234"
- `#\s*([A-Z0-9-]+)` → "#1234"
- `\b([A-Z]{2,5}-\d{3,})\b` → "INVOICE-001"
- `payment\s+for\s+([A-Z0-9-]+)` → "Payment for INV-1234"

**Confidence Scoring**:
- 1.0: Invoice number + exact amount + customer mapping
- 0.9: Invoice number + exact amount
- 0.8: Invoice number + similar amount
- 0.7: Amount match + date proximity
- 0.6: Weak amount similarity
- < 0.6: Not suggested

**API Endpoints**:
- `POST /api/ai/transaction-mappings/suggest` - Generate suggestions
- `GET /api/ai/transaction-mappings/pending` - Fetch pending suggestions
- `POST /api/ai/transaction-mappings/:id/approve` - Approve suggestion
- `POST /api/ai/transaction-mappings/:id/reject` - Reject suggestion

**Usage**:
```javascript
// Generate suggestions
const result = await aiService.suggestTransactionMappings();
// result.count: 150
// result.suggestions: [...]

// Fetch pending suggestions
const pending = await aiService.getPendingTransactionSuggestions();

// Approve suggestion
await aiService.approveTransactionMapping(suggestionId);
// Creates row in transaction_invoice_mappings table

// Reject suggestion
await aiService.rejectTransactionMapping(suggestionId);
// Updates status to 'rejected'
```

### Customer Auto-Matching Algorithm (Legacy)
Located in server.js `/api/customer-mappings/auto-match`:
1. Fetch all Stripe customers and HaloPSA clients
2. For each Stripe customer:
   - Compare email (exact match)
   - Compare name using fuzzy string matching (Levenshtein distance)
3. Calculate confidence score (0-1)
4. If score > threshold (default 0.85), create mapping with `auto_mapped: true`
5. Manual confirmation sets `mapping_confirmed: true`

**Note**: This is the legacy algorithm. For AI-powered matching, use `AIService.suggestCustomerMappings()` instead.

### Drag-and-Drop Customer Mapping
**File**: `src/drag-drop-mapping.html`

**API Endpoints used:**
- `GET /api/stripe/customers/imported` - Fetch Stripe customers
- `GET /api/customers/all` - Fetch HaloPSA clients
- `GET /api/customers/mappings` - Fetch existing customer mappings
- `POST /api/customers/mappings/create` - Create new mapping via drag-drop
- `DELETE /api/customers/mappings/:stripe_customer_id` - Remove entire mapping row
- `POST /api/customers/automatch` - Run auto-matching algorithm

**Individual Platform Unmap Endpoints** (Added 2025-10-30):
- `DELETE /api/customers/mappings/stripe/:halopsa_client_id` - Remove Stripe mapping only
- `DELETE /api/customers/mappings/qb/:halopsa_client_id` - Remove QuickBooks mapping only
- `POST /api/customers/mappings/stripe` - Create/update Stripe → HaloPSA mapping
- `POST /api/customers/mappings/qb` - Create/update QuickBooks → HaloPSA mapping

**Unmap Logic:**
- If a mapping row has both Stripe AND QB data, deleting one platform sets those fields to NULL (preserves other mapping)
- If a mapping row has ONLY one platform's data, deleting it removes the entire row
- This allows flexible management of multi-platform mappings (Stripe + HaloPSA + QB)

**Drag & Drop Flow:**
1. User drags a Stripe customer card (draggable div with `draggable="true"`)
2. `dragstart` event captures Stripe customer data (`stripe_id`, `name`, `email`)
3. User drops onto HaloPSA client drop zone
4. `drop` event captures HaloPSA client data (`halopsa_id`, `name`)
5. POST request to `/api/customers/mappings/create` with both IDs
6. Backend creates `customer_mappings` row with `mapping_confirmed: 1`
7. UI refreshes to show green checkmarks and connection labels

**Visual Feedback:**
- `.dragging` class applied during drag (opacity 0.5, rotation)
- `.drag-over` class on drop zones when hovering (purple border, scale animation)
- `.mapped` class for cards with existing mappings (green border, checkmark)
- `.has-mapping` class for drop zones with mappings (green border, checkmark)

### Error Handling in HaloPSA API
- Tries multiple endpoint variations (capitalization, singular/plural)
- Detailed error messages based on HTTP status codes
- Logs first item structure for debugging unknown response formats
- Small delays between requests to avoid rate limiting

### CSV Parsing with Flexible Column Names
Supports synonyms for required columns:
- **Vendor**: vendor, supplier, vendor name
- **RefNumber**: refnumber, ref number, reference, reference number, docnum, document number
- **Item**: item, item code, sku, product, product code
- **Qty**: qty, quantity, qnty
- **Cost**: cost, unit cost, price, unit price, rate

### QBXML Generation
- Version: 13.0
- Generates separate requests for: Vendors, Inventory Items, Bills
- Sanitizes strings: Escapes XML entities, limits to 100 chars
- Date format: YYYY-MM-DD

### Enhanced QuickBooks Sync with Purchase Orders & Items

**New Features (Added 2025-10-30)**:
- Purchase Orders → QuickBooks Bills with account mapping integration
- Enhanced Item sync with inventory tracking and account mappings
- Support for all item types: Service, Inventory, Non-Inventory
- **Duplicate checking for Purchase Order Bills** (prevents creating duplicate Bills in QuickBooks)
- Comprehensive sync status tracking with error management

**New QBXML Generation Functions** (`src/qbwc-service.js`):

1. **`generatePurchaseOrderBillsQBXML(purchaseOrders, accountMappings)`**:
   - Converts HaloPSA purchase orders to QuickBooks bills (unpaid)
   - Parses `line_items` JSON field from database
   - Integrates with account_mappings table for proper GL accounts
   - Maps fields:
     - Vendor: `vendor_name` → `VendorRef`
     - AP Account: Uses `accounts_payable` mapping
     - Line Items: Parses from JSON, creates `ItemLineAdd` for each
     - Inventory Asset: Uses `inventory_asset` mapping for inventory items
   - Returns QBXML with `IsPaid: false` flag

2. **`generateItemsWithMappingsQBXML(items, accountMappings)`**:
   - Enhanced item QBXML generation with account mapping support
   - Supports all three item types (Service, Inventory, Non-Inventory)
   - Maps GL accounts from `account_mappings` table:
     - Income: Uses `halopsa_invoice_income` mapping
     - COGS: Uses `cost_of_goods` mapping
     - Asset: Uses `inventory_asset` mapping
     - Expense: Uses `purchase_order_expense` mapping
   - Handles quantity on hand for inventory items
   - Returns QBXML with proper request types for each item type

**New API Endpoints** (`src/server.js`):

1. **POST /api/qbd/sync/purchase-orders**:
   - Fetches unsynced purchase orders (WHERE `synced_to_qb = 0`)
   - Fetches active account mappings
   - Generates QBXML for purchase orders as bills
   - Queues sync request in QBWC service (priority: 50)
   - Returns: QBXML, PO count, total amount

2. **POST /api/qbd/sync/items**:
   - Fetches unsynced items (WHERE `synced_to_qb = 0`)
   - Fetches active account mappings
   - Generates QBXML with account mapping integration
   - Queues sync request in QBWC service (priority: 40)
   - Returns: QBXML, item count, breakdown by type

3. **POST /api/qbd/sync/bills**:
   - Alias endpoint for `/api/qbd/sync/purchase-orders`
   - Provides clearer semantic meaning for bill sync

**Account Mappings Integration**:

The `account_mappings` table stores QuickBooks account names for proper GL account assignment:

| Mapping Type | Purpose | Used In |
|--------------|---------|---------|
| `accounts_payable` | AP account for bills | Purchase Order Bills |
| `inventory_asset` | Inventory asset account | Items, PO line items |
| `halopsa_invoice_income` | Income/revenue account | Items (service/non-inventory) |
| `cost_of_goods` | COGS account | Items (inventory) |
| `purchase_order_expense` | Expense account | Items (non-inventory) |
| `customer_deposits` | Unearned revenue (liability) | Future: Stripe deposits |
| `stripe_processing_fees` | Processing fees expense | Future: Stripe fees |
| `stripe_payments` | Bank/deposit account | Future: Stripe deposits |

**Data Requirements**:

For purchase order → bill sync to work:
- Purchase orders must have `line_items` field populated (JSON array)
- Line items structure:
  ```json
  [
    {
      "item_name": "Product Name",
      "description": "Item description",
      "quantity": 5,
      "unit_cost": 100.00,
      "total": 500.00,
      "is_inventory": true
    }
  ]
  ```
- Currently, HaloPSA PO import doesn't populate line_items (only header data)
- **TODO**: Enhance HaloPSA Report API import to extract line items from Report 350

For item sync to work:
- Items must have `item_type` field set to one of: `ItemService`, `ItemInventory`, `ItemNonInventory`
- Currently, items imported from HaloPSA use simplified types: `Inventory`, `Service`, etc.
- **TODO**: Normalize item_type values during HaloPSA import

**Testing**:

```bash
# Test QBXML generation and account mappings
node test-qbxml-enhancements.js

# Expected output:
# - Account mappings loaded from database
# - QBXML generated for purchase orders (if line_items populated)
# - QBXML generated for items (if item_type is correct format)
# - Schema validation for required fields
# - Count of unsynced records
```

**Usage Workflow**:

1. Configure account mappings in database:
   ```sql
   UPDATE account_mappings
   SET qb_account_name = 'Accounts Payable'
   WHERE mapping_type = 'accounts_payable';

   UPDATE account_mappings
   SET qb_account_name = 'Inventory Asset'
   WHERE mapping_type = 'inventory_asset';
   ```

2. Sync purchase orders as bills:
   ```bash
   curl -X POST http://localhost:3000/api/qbd/sync/purchase-orders
   ```

3. Sync items with account mappings:
   ```bash
   curl -X POST http://localhost:3000/api/qbd/sync/items
   ```

4. Use QuickBooks Web Connector to process the queued QBXML

**Future Enhancements**:
- Extract line items during HaloPSA PO import (via Report API)
- Normalize item types during HaloPSA item extraction
- Add support for updating existing QB bills/items (Mod requests)
- Add invoice → QB Invoice sync (similar to PO → Bill pattern)

### Purchase Order Duplicate Checking & Sync Status Tracking

**Problem**: Purchase Orders were being synced to QuickBooks as Bills without checking if the Bill already existed, potentially creating duplicates on re-sync attempts.

**Solution** (Implemented 2025-10-30):

**Database Schema Enhancements** (`halopsa_purchase_orders` table):
- `qb_txn_id TEXT`: QuickBooks Transaction ID for the created Bill
- `synced_to_qb BOOLEAN DEFAULT FALSE`: Whether PO has been successfully synced
- `sync_error TEXT`: Error message if sync failed
- `last_sync_attempt DATETIME`: Timestamp of last sync attempt (success or failure)

**Duplicate Detection Methods** (`src/qbwc-service.js`):

1. **`generateBillCheckQBXML(po)`**:
   - Generates QBXML BillQueryRq to search for existing Bills
   - Searches by: Vendor name + Reference number (PO number)
   - Returns QBXML with filters for duplicate detection

2. **`parseBillCheckResponse(responseXml, po)`**:
   - Parses BillQueryRs response from QuickBooks
   - Checks for matching Bills using:
     - Reference number contains PO number
     - Amount matches within tolerance (±$0.01)
   - Returns: `{ exists: boolean, txnId: string|null, matchDetails: Object }`

3. **`processBillAddResponse(responseXml, purchaseOrders)`**:
   - Processes BillAddRs response after creating Bills
   - Extracts TxnID from successful Bill creation
   - Updates database for each PO:
     - **On success**: Sets `qb_txn_id`, `synced_to_qb = 1`, clears `sync_error`
     - **On error**: Sets `sync_error` with message, `synced_to_qb = 0`, updates `last_sync_attempt`
   - Returns summary: `{ success, results, summary: { total, success, errors } }`

4. **`generatePurchaseOrderBillsQBXML(purchaseOrders, accountMappings)`** (Enhanced):
   - **ONLY generates QBXML for unsynced POs** (`synced_to_qb = FALSE`)
   - Skips POs that have already been synced (logged with TxnID)
   - Prevents duplicate Bill creation automatically

**Sync Management API Endpoints** (`src/server.js`):

1. **GET /api/qbd/purchase-orders/sync-status**:
   - Returns comprehensive sync statistics:
     - `total`: Total number of purchase orders
     - `synced`: Count of successfully synced POs
     - `pending`: Count of POs awaiting sync (no errors)
     - `errors`: Count of POs with sync errors
     - `unsyncedAmount`: Total dollar amount of unsynced POs
     - `lastSuccessfulSync`: Timestamp of most recent successful sync
   - Example response:
     ```json
     {
       "success": true,
       "stats": {
         "total": 150,
         "synced": 120,
         "pending": 25,
         "errors": 5,
         "unsyncedAmount": 12500.00,
         "lastSuccessfulSync": "2025-10-30T15:30:00Z"
       }
     }
     ```

2. **POST /api/qbd/purchase-orders/:id/retry-sync**:
   - Retry sync for a specific Purchase Order by ID
   - Clears `sync_error`, resets `synced_to_qb = 0`, clears `qb_txn_id`
   - Marks PO for re-sync on next QBWC connection
   - Returns: `{ success, message, po_number }`

3. **POST /api/qbd/purchase-orders/retry-all-errors**:
   - Retry sync for ALL Purchase Orders with errors
   - Bulk operation: clears all error states
   - Returns count of POs marked for retry
   - Returns: `{ success, message, retryCount }`

4. **GET /api/qbd/purchase-orders/errors**:
   - Fetch all Purchase Orders with sync errors
   - Returns: `{ success, errors: [...], count }`
   - Error record includes: `id`, `po_number`, `vendor_name`, `total_amount`, `sync_error`, `last_sync_attempt`

**Workflow**:

1. **Initial Sync**:
   - User clicks "Sync Purchase Orders" in frontend
   - Backend fetches POs with `synced_to_qb = 0 OR sync_error IS NOT NULL`
   - Generates QBXML BillAddRq for unsynced POs
   - QBWC processes request, returns BillAddRs
   - Backend calls `processBillAddResponse()` to update database
   - Successful POs get `qb_txn_id`, `synced_to_qb = 1`
   - Failed POs get `sync_error` with QB error message

2. **Handling Errors**:
   - User views sync status dashboard showing error count
   - Clicks "View Errors" to see detailed error list
   - Reviews specific error messages (e.g., "Vendor not found", "Item not found")
   - Fixes issue (e.g., creates missing Vendor in QB)
   - Clicks "Retry" for specific PO or "Retry All Errors"
   - Backend clears error state and re-queues for sync

3. **Duplicate Prevention**:
   - If user accidentally retries a successfully synced PO
   - `generatePurchaseOrderBillsQBXML()` skips it (logs "already synced")
   - No duplicate Bill created in QuickBooks
   - Optional: Can run BillCheckQBXML first to verify Bill existence

**Error Handling**:

Common sync errors and resolutions:

| Error | Cause | Resolution |
|-------|-------|------------|
| "Vendor not found" | Vendor doesn't exist in QB | Create vendor first, then retry |
| "Item not found" | Line item doesn't exist in QB | Create item first, then retry |
| "Amount mismatch" | Validation error | Review PO data, check line item totals |
| "Invalid line items" | Missing or malformed line_items JSON | Fix line_items field in database |
| "Duplicate Bill" | Bill already exists (rare) | Check `qb_txn_id`, verify QB status |

**Frontend UI Enhancements** (Planned):

1. **Sync Status Dashboard Widget**:
   - Show total/synced/pending/error counts
   - Display progress bar (synced / total)
   - Show total unsynced amount
   - "Sync Now" button

2. **Purchase Orders Table Updates**:
   - Add "QB Status" column with badges:
     - Green "Synced" (✓) for `synced_to_qb = 1`
     - Yellow "Pending" (⏱) for `synced_to_qb = 0` and no errors
     - Red "Error" (✗) for `sync_error IS NOT NULL`
   - Hover/tooltip shows error message for failed POs
   - "Retry" button for individual POs with errors

3. **Error Management Panel**:
   - Filter dropdown: "Show All" | "Synced" | "Pending" | "Errors Only"
   - "Retry Failed Syncs" button (bulk retry)
   - Sortable by sync status, error message, amount

**Testing Checklist**:

- [x] PO sync creates Bill in QB
- [x] Database updated with `qb_txn_id` on success
- [x] Duplicate PO sync doesn't create second Bill (skipped)
- [ ] Sync status endpoint returns accurate counts
- [ ] Error handling for missing vendors
- [ ] Error handling for invalid line items
- [ ] Retry functionality clears error state
- [ ] Retry All Errors bulk operation works
- [ ] Frontend displays sync status badges
- [ ] Frontend filters work correctly

**Code Locations**:

- Duplicate checking logic: `src/qbwc-service.js` (lines 1560-1685)
- Response processing: `src/qbwc-service.js` (lines 1687-1813)
- Sync status endpoints: `src/server.js` (lines 2296-2484)
- Database schema: `src/database.js` (lines 293-362)

## Common Development Tasks

### Adding a New Configuration Option
1. Add to `defaultConfigs` array in `src/database.js` init() method
2. Include: key, default value, type, category, label, description, options (if select), is_required
3. Access via `ConfigAPI.getFeatureConfig(category)` or database `getConfig()`

### Adding a New HaloPSA Endpoint
1. Add endpoint to appropriate method in `src/halopsa-api.js`
2. Include fallback endpoints (capitalization variations)
3. Add response format handling for different pagination styles
4. Log first item structure for debugging
5. Add timeout protection for long-running requests

### Debugging Customer Mapping Issues
1. Check `customer_mappings` table for existing mappings
2. Run `test-automatch.js` to test matching algorithm
3. Check logs for confidence scores and match reasons
4. Adjust threshold in config: `customer_match_threshold` (default: 0.85)

### Testing QBWC Integration
1. Start server: `node src/server.js`
2. Download QWC file: `http://localhost:3000/qbwc/config`
3. Install in QuickBooks Web Connector
4. Open QuickBooks Desktop with company file
5. Click "Update Selected" in QBWC
6. Monitor server logs for SOAP requests/responses

## Environment Variables

- `PORT`: Server port (default: 3000)
- `DB_PATH`: SQLite database directory (default: `/usr/src/app/data`)
- `NODE_ENV`: Environment (production/development)
- `STRIPE_SECRET_KEY`: Stripe API key (also in config table)
- `STRIPE_WEBHOOK_SECRET`: Stripe webhook signing secret
- `CLOUDFLARED_TOKEN`: Cloudflare tunnel token for secure remote access

## Known Issues & Gotchas

1. **Multiple generateGUID() definitions**: `src/qbwc-service.js` has multiple `generateGUID()` methods. The last one defined is used (static GUIDs for QuickBooks compatibility).

2. **HaloPSA Access Token Not Cached**: By design, fresh tokens are obtained for each request to avoid stale token issues. Previous implementation cached tokens in database but caused auth failures.

3. **SQLite Synchronous API**: Database uses `better-sqlite3` with synchronous methods, but initialization is async (`await db.ready`).

4. **Config Table Defaults**: Only inserted if config table is empty. To reset, clear table and restart server.

5. **QBWC GUID Format**: Must be uppercase hex with curly braces: `{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}`. Lowercase or missing braces cause failures.

6. **Docker Volume for Database**: Database persists in Docker volume. To reset, remove volume: `docker compose down -v`.

7. **Web-Based Architecture**: Pure web application accessed via browser at `http://localhost:3000`. No Electron dependency.

## Deployment Notes

### Docker Deployment
- Image: Node 22 Alpine
- Exposes port 3000
- Health check endpoint: `/healthz`
- Database stored in `/usr/src/app/data` (mount volume for persistence)
- Cloudflared tunnel for secure remote access (optional)

### Windows Local Deployment
- Use `node src/server.js` to launch the web server
- Access via browser at http://localhost:3000
- SQLite database stored at configured `DB_PATH` or `/usr/src/app/data`

## Documentation Files

- `README.md`: User-facing setup and quick start guide
- `QuickBooks_Web_Connector_Guide.md`: QBWC setup instructions
- `HALOPSA_SETUP.md`: HaloPSA API configuration guide
- `HALOPSA_AUTHENTICATION_ISSUE.md`: Troubleshooting OAuth2 issues
- `MSP_CASH_ACCOUNTING_SETUP.md`: QuickBooks cash basis accounting setup
- `PLAID_INTEGRATION_DESIGN.md`: Future Plaid integration design
- `halopsa-api.md`: HaloPSA API reference

## Branch Strategy

- **main**: Stable production branch (used for PRs)
- **feat/quickbooks-web-connector**: Current feature branch for QBWC integration
