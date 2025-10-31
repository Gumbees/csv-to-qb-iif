# AI Service Implementation

## Overview

This document describes the AI-powered customer and transaction mapping service implemented for csv-to-qb-iif using Anthropic's Claude API.

## What Was Implemented

### Core AI Service (`src/ai-service.js`)

**Class**: `AIService`

**Constructor**:
```javascript
const aiService = new AIService(database);
```

**Key Methods**:

1. **`async init()`**
   - Loads API key and model from `ai_settings` table
   - Initializes Anthropic client
   - Returns `{success: boolean, message: string}`

2. **`initialize(apiKey)`** (Legacy compatibility)
   - Direct initialization with API key
   - Used for backward compatibility

3. **`async testConnection()`**
   - Verifies Anthropic API connectivity
   - Sends simple test prompt to Claude
   - Returns `{success: boolean, message: string}`
   - Handles specific error codes: 401 (invalid key), 429 (rate limit), 500 (server error)

4. **`async suggestCustomerMappings(stripeCustomers, haloPSAClients, qbCustomers)`**
   - AI-powered customer matching across three systems
   - Uses Claude to analyze:
     - Exact email matches
     - Company name similarity (fuzzy matching)
     - Phone number matches
     - Address matches
   - Chunks data: max 50 customers per system per request
   - Returns array of suggestions with confidence >= 0.7

5. **`async suggestTransactionMappings()`** (Pattern-based, NO AI required)
   - Intelligent transaction-to-invoice mapping
   - Fetches unmapped Stripe transactions and HaloPSA invoices from database
   - Uses weighted scoring algorithm:
     - Invoice number extraction from description (50%)
     - Amount matching (30%)
     - Date proximity (10%)
     - Customer mapping exists (10%)
   - Saves suggestions to `ai_mapping_suggestions` table
   - Returns `{success, count, suggestions}`

6. **`extractInvoiceNumber(description)`**
   - Parses invoice numbers from transaction descriptions
   - Supports patterns:
     - "Invoice #INV-1234"
     - "INV-1234"
     - "#1234"
     - "INVOICE-001"
     - "Payment for INV-1234"
   - Returns extracted invoice number or null

7. **`calculateAmountMatch(transactionAmount, invoiceAmount)`**
   - Compares Stripe amount (cents) to invoice amount (dollars)
   - Returns confidence score (0-1):
     - 1.0: Exact match (< $0.01 difference)
     - 0.95: < 1% difference
     - 0.85: < 5% difference
     - 0.6: < 10% difference
     - 0.2: > 10% difference

8. **`calculateDateProximity(date1, date2)`**
   - Measures days between two dates
   - Returns confidence score (0-1):
     - 1.0: Same day
     - 0.9: Within 3 days
     - 0.7: Within 7 days
     - 0.5: Within 14 days
     - 0.3: Within 30 days
     - 0.1: > 30 days

9. **`calculateCustomerMatch(transactionCustomerId, invoiceClientId)`**
   - Checks if customer mapping exists in database
   - Returns 1.0 if confirmed mapping exists, 0 otherwise

10. **`async getPendingTransactionSuggestions()`**
    - Fetches pending transaction mapping suggestions from database
    - Joins with stripe_transactions, halopsa_invoices, stripe_customers
    - Returns enriched suggestion data for UI display

11. **`async approveTransactionMapping(suggestionId)`**
    - Creates actual mapping in `transaction_invoice_mappings` table
    - Sets auto_mapped flag and confidence score
    - Updates suggestion status to 'approved'

12. **`async rejectTransactionMapping(suggestionId)`**
    - Updates suggestion status to 'rejected'
    - Keeps suggestion record for audit trail

13. **`async updateConfig(apiKey, model)`**
    - Updates AI service configuration in `ai_settings` table
    - Re-initializes Anthropic client
    - Supports model selection

## Database Schema

### `ai_settings` Table
```sql
CREATE TABLE ai_settings (
  key TEXT PRIMARY KEY,
  value TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

**Default Settings**:
- `anthropic_api_key`: (empty - user must configure)
- `anthropic_model`: `claude-3-5-sonnet-20241022`

### `ai_mapping_suggestions` Table
```sql
CREATE TABLE ai_mapping_suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  suggestion_type TEXT NOT NULL,        -- 'customer' or 'transaction'
  source_type TEXT NOT NULL,            -- 'stripe_customer', 'stripe_transaction', etc.
  source_id INTEGER NOT NULL,
  target_type TEXT NOT NULL,            -- 'halopsa_client', 'halopsa_invoice', etc.
  target_id INTEGER NOT NULL,
  confidence REAL NOT NULL,             -- 0.0 to 1.0
  reasoning TEXT,                       -- Human-readable explanation
  status TEXT DEFAULT 'pending',        -- 'pending', 'approved', 'rejected'
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

## Dependencies

### Package.json Addition
```json
{
  "dependencies": {
    "@anthropic-ai/sdk": "^0.32.1"
  }
}
```

**Installed**: 16 new packages (SDK and its dependencies)

## Confidence Scoring

### Customer Mapping (Claude AI)
- **1.0**: Exact email match across systems
- **0.9-0.99**: Very strong match (email domain + name similarity)
- **0.8-0.89**: Strong match (name + contact info)
- **0.7-0.79**: Good match (name similarity with supporting data)
- **0.6-0.69**: Moderate match (name similarity only)
- **< 0.6**: Not suggested (too uncertain)

### Transaction Mapping (Pattern-Based)
- **1.0**: Invoice number + exact amount + customer mapping (all 4 factors)
- **0.9**: Invoice number + exact amount
- **0.8**: Invoice number + similar amount
- **0.7**: Amount match + date proximity
- **0.6**: Weak amount similarity
- **< 0.6**: Not suggested

## Error Handling

**Anthropic API Errors**:
- **401 Unauthorized**: Invalid API key message
- **429 Too Many Requests**: Rate limit exceeded message
- **500+ Server Error**: Anthropic API server error message

**JSON Parsing**:
- Handles markdown code blocks: ` ```json [...] ``` `
- Extracts JSON arrays from arbitrary text
- Graceful fallback to empty array on parse failure

**Database Errors**:
- Checks for database availability (`if (!this.db)`)
- Try-catch blocks with detailed error logging
- Returns `{success: false, error: message}` format

## Usage Examples

### Initialize AI Service
```javascript
const Database = require('./src/database');
const AIService = require('./src/ai-service');

const db = new Database();
await db.ready;

const aiService = new AIService(db);
await aiService.init();

// Test connection
const testResult = await aiService.testConnection();
console.log(testResult); // {success: true, message: 'AI service connection successful'}
```

### AI-Powered Customer Mapping
```javascript
// Fetch unmapped customers
const stripeCustomers = db.all('SELECT * FROM stripe_customers WHERE ...');
const haloPSAClients = db.all('SELECT * FROM halopsa_clients WHERE ...');
const qbCustomers = db.all('SELECT * FROM qb_customers WHERE ...');

// Get AI suggestions
const suggestions = await aiService.suggestCustomerMappings(
  stripeCustomers,
  haloPSAClients,
  qbCustomers
);

// suggestions = [
//   {
//     source_system: 'stripe',
//     source_id: 'cus_abc123',
//     target_system: 'halopsa',
//     target_id: '12345',
//     confidence: 0.95,
//     reasoning: 'Exact email match: contact@company.com + company name similarity'
//   }
// ]
```

### Pattern-Based Transaction Mapping
```javascript
// Generate suggestions (automatically fetches from database)
const result = await aiService.suggestTransactionMappings();

console.log(result.count); // 150 suggestions
console.log(result.suggestions[0]);
// {
//   stripe_transaction_id: 42,
//   halopsa_invoice_id: 78,
//   confidence: 0.95,
//   reasoning: 'Invoice #INV-1234 found in description • Exact amount match: $1500.00',
//   transaction_data: {...},
//   invoice_data: {...}
// }

// Fetch pending suggestions for UI
const pending = await aiService.getPendingTransactionSuggestions();

// User approves suggestion
await aiService.approveTransactionMapping(suggestionId);
// Creates mapping in transaction_invoice_mappings table

// User rejects suggestion
await aiService.rejectTransactionMapping(suggestionId);
// Marks suggestion as rejected
```

### Update Configuration
```javascript
// Update API key and model
await aiService.updateConfig('sk-ant-api03-xxx', 'claude-sonnet-4-20250514');
```

## Invoice Number Extraction Patterns

The service uses regex patterns to extract invoice numbers from transaction descriptions:

```javascript
const patterns = [
  /invoice\s*#?\s*([A-Z0-9-]+)/i,       // "Invoice #INV-1234"
  /inv\s*#?\s*([A-Z0-9-]+)/i,           // "INV-1234"
  /#\s*([A-Z0-9-]+)/i,                   // "#1234"
  /\b([A-Z]{2,5}-\d{3,})\b/i,            // "INV-1234", "INVOICE-001"
  /invoice\s+(\d{4,})/i,                 // "Invoice 123456"
  /payment\s+for\s+([A-Z0-9-]+)/i        // "Payment for INV-1234"
];
```

**Examples**:
- "Payment for invoice #INV-2024-001" → Extracts "INV-2024-001"
- "Stripe charge for 12345" → Extracts "12345"
- "Monthly service fee INV-123" → Extracts "INV-123"

## Prompt Engineering

### Customer Mapping System Prompt
- Expert financial data analyst persona
- Handles name variations, abbreviations, typos
- Prioritizes exact email matches
- Returns structured JSON format
- Confidence-based filtering (>= 0.7)

### Customer Mapping User Prompt
- Lists Stripe customers with ID, name, email, phone, address
- Lists HaloPSA clients with same fields
- Lists QuickBooks customers (optional)
- Requests specific JSON output format
- Max 50 customers per system per request (chunking)

## Data Chunking Strategy

To avoid token limits and ensure efficient processing:

**Customer Mapping**:
- Max 50 customers per system per request
- Processes in chunks if datasets are larger
- Aggregates all suggestions from chunks

**Transaction Mapping**:
- Fetches max 1000 unmapped transactions from database
- Fetches max 1000 invoices from database
- No chunking needed (pattern-based, runs locally)

## Future Enhancements

1. **AI-Powered Transaction Mapping**: Currently uses pattern matching. Could use Claude for fuzzy description analysis.
2. **Confidence Threshold Configuration**: Allow users to adjust minimum confidence levels.
3. **Bulk Approval**: Approve all suggestions above certain confidence threshold.
4. **Learning from Corrections**: Track rejected suggestions to improve patterns.
5. **Multi-Language Support**: Handle invoice descriptions in different languages.
6. **Custom Pattern Configuration**: Allow users to define custom invoice number patterns.

## Files Modified/Created

### Created
- `G:\DTC Github\csv-to-qb-iif\src\ai-service.js` (597 lines)
- `G:\DTC Github\csv-to-qb-iif\AI_SERVICE_IMPLEMENTATION.md` (this file)

### Modified
- `G:\DTC Github\csv-to-qb-iif\package.json` - Added @anthropic-ai/sdk dependency
- `G:\DTC Github\csv-to-qb-iif\src\database.js` - Added ai_settings and ai_mapping_suggestions tables
- `G:\DTC Github\csv-to-qb-iif\CLAUDE.md` - Added AI service documentation

## Testing

**Connection Test**:
```javascript
const testResult = await aiService.testConnection();
// Should return {success: true, message: '...'} if API key is valid
```

**Transaction Mapping Test**:
```javascript
const result = await aiService.suggestTransactionMappings();
console.log(`Generated ${result.count} suggestions`);
```

**Customer Mapping Test** (requires valid Anthropic API key):
```javascript
const suggestions = await aiService.suggestCustomerMappings(
  stripeCustomers,
  haloPSAClients,
  []
);
console.log(`Generated ${suggestions.length} customer mapping suggestions`);
```

## Summary

The AI service provides intelligent customer and transaction mapping capabilities:

✅ **Customer Mapping**: Uses Claude AI for fuzzy matching across Stripe, HaloPSA, and QuickBooks
✅ **Transaction Mapping**: Pattern-based invoice matching with weighted confidence scoring
✅ **Confidence Scores**: Transparent scoring system with human-readable reasoning
✅ **Database Integration**: Stores suggestions in database for review workflow
✅ **Error Handling**: Graceful handling of API errors and edge cases
✅ **Data Chunking**: Handles large datasets efficiently
✅ **Extensible**: Easy to add new matching algorithms and patterns

The implementation follows best practices for API integration, error handling, and database design, making it a robust foundation for intelligent data mapping in financial applications.
