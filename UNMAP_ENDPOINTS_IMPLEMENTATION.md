# Unmap API Endpoints Implementation

**Date**: 2025-10-30
**Feature**: Individual platform unmapping for customer mappings
**Location**: `src/server.js` lines 3479-3741

## Overview

Implemented four new API endpoints to enable granular control over customer mappings across Stripe, HaloPSA, and QuickBooks platforms. Previously, removing a mapping would delete the entire row - now you can remove individual platform connections while preserving others.

## Endpoints Implemented

### 1. DELETE /api/customers/mappings/stripe/:halopsa_client_id

**Purpose**: Remove Stripe mapping from a HaloPSA client

**Request**:
```http
DELETE /api/customers/mappings/stripe/123
```

**Behavior**:
- Finds mapping row by `halopsa_client_id`
- If row has QB mapping: Sets `stripe_customer_id`, `stripe_customer_name`, `stripe_customer_email` to NULL
- If row has NO QB mapping: Deletes entire row
- Updates `updated_at` timestamp

**Response**:
```json
{
  "success": true,
  "message": "Stripe mapping removed"
}
```

**Error Cases**:
- `400`: Invalid HaloPSA client ID (NaN or falsy)
- `404`: No mapping found for this HaloPSA client
- `500`: Database error

---

### 2. DELETE /api/customers/mappings/qb/:halopsa_client_id

**Purpose**: Remove QuickBooks mapping from a HaloPSA client

**Request**:
```http
DELETE /api/customers/mappings/qb/123
```

**Behavior**:
- Finds mapping row by `halopsa_client_id`
- If row has Stripe mapping: Sets `qb_customer_id`, `qb_customer_name` to NULL
- If row has NO Stripe mapping: Deletes entire row
- Updates `updated_at` timestamp

**Response**:
```json
{
  "success": true,
  "message": "QuickBooks mapping removed"
}
```

**Error Cases**:
- `400`: Invalid HaloPSA client ID
- `404`: No mapping found
- `500`: Database error

---

### 3. POST /api/customers/mappings/stripe

**Purpose**: Create or update Stripe → HaloPSA mapping

**Request**:
```http
POST /api/customers/mappings/stripe
Content-Type: application/json

{
  "stripe_customer_id": "cus_abc123",
  "halopsa_client_id": 123
}
```

**Behavior**:
1. Validates both IDs are provided
2. Validates HaloPSA client exists in `halopsa_clients` table
3. Validates Stripe customer exists in `stripe_customers` table
4. If mapping row exists for this HaloPSA client: UPDATE to add/change Stripe fields
5. If no mapping exists: INSERT new row
6. Fetches customer name and email from `stripe_customers` table to populate denormalized fields
7. Sets `mapping_confirmed: 0` and `auto_mapped: 0` (manual mapping)
8. Updates `updated_at` timestamp

**Response**:
```json
{
  "success": true,
  "message": "Stripe mapping created"
}
```

**Error Cases**:
- `400`: Missing required fields
- `404`: HaloPSA client not found
- `404`: Stripe customer not found
- `500`: Database error

---

### 4. POST /api/customers/mappings/qb

**Purpose**: Create or update QuickBooks → HaloPSA mapping

**Request**:
```http
POST /api/customers/mappings/qb
Content-Type: application/json

{
  "qb_customer_id": "QB-12345",
  "halopsa_client_id": 123
}
```

**Behavior**:
1. Validates both IDs are provided
2. Validates HaloPSA client exists in `halopsa_clients` table
3. If mapping row exists for this HaloPSA client: UPDATE to add/change QB fields
4. If no mapping exists: INSERT new row
5. Uses `qb_customer_id` as the name temporarily (until QB customers table is implemented)
6. Sets `mapping_confirmed: 0` and `auto_mapped: 0` (manual mapping)
7. Updates `updated_at` timestamp

**Response**:
```json
{
  "success": true,
  "message": "QuickBooks mapping created"
}
```

**Error Cases**:
- `400`: Missing required fields
- `404`: HaloPSA client not found
- `500`: Database error

**TODO**: When `qb_customers` table is implemented, fetch QB customer name from there instead of using the ID as the name

---

## Implementation Details

### Database Schema

The `customer_mappings` table supports multi-platform mappings:

```sql
CREATE TABLE customer_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stripe_customer_id TEXT,
    stripe_customer_name TEXT,
    stripe_customer_email TEXT,
    halopsa_client_id INTEGER,
    halopsa_client_name TEXT,
    qb_customer_id TEXT,
    qb_customer_name TEXT,
    auto_mapped BOOLEAN DEFAULT FALSE,
    mapping_confirmed BOOLEAN DEFAULT FALSE,
    mapping_source TEXT DEFAULT 'manual',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stripe_customer_id, halopsa_client_id, qb_customer_id)
);
```

### Mapping States

A single row can represent:
- **Stripe + HaloPSA only**: `stripe_customer_id` and `halopsa_client_id` populated, `qb_customer_id` NULL
- **QB + HaloPSA only**: `qb_customer_id` and `halopsa_client_id` populated, `stripe_customer_id` NULL
- **All three platforms**: All three ID fields populated

### Unmap Logic

The smart unmap logic prevents orphaned rows:

```javascript
// Example: Removing Stripe mapping
const hasQBMapping = mapping.qb_customer_id !== null && mapping.qb_customer_id !== '';

if (hasQBMapping) {
    // Keep row, clear Stripe fields only
    UPDATE customer_mappings
    SET stripe_customer_id = NULL,
        stripe_customer_name = NULL,
        stripe_customer_email = NULL
    WHERE halopsa_client_id = ?
} else {
    // No other mappings, delete entire row
    DELETE FROM customer_mappings WHERE halopsa_client_id = ?
}
```

### Logging

All operations log to console with `[UNMAP]` or `[MAP]` prefixes:

```javascript
console.log(`[UNMAP] Removing Stripe mapping for HaloPSA client ${halopsa_client_id}, keeping QB mapping`);
console.log(`[MAP] Creating new mapping for HaloPSA client ${halopsaClientIdNum} with Stripe customer ${stripe_customer_id}`);
```

## Testing

Use the provided test script:

```bash
node test-unmap-endpoints.js
```

**Test Flow**:
1. Fetches sample HaloPSA client and Stripe customer
2. Creates Stripe mapping
3. Creates QB mapping
4. Verifies both mappings exist
5. Removes Stripe mapping (should keep row with QB only)
6. Verifies Stripe removed, QB remains
7. Removes QB mapping (should delete entire row)
8. Verifies row deleted

## Frontend Integration

The drag-and-drop customer mapping UI (`src/drag-drop-mapping.html` and customer mappings component) can now:

- Show "Unmap Stripe" and "Unmap QB" buttons separately
- Allow users to remove individual platform connections
- Preserve other platform mappings when unmapping one

**Example UI Flow**:
1. User sees HaloPSA client mapped to both Stripe and QB
2. Clicks "Unmap Stripe" button
3. Frontend calls `DELETE /api/customers/mappings/stripe/123`
4. UI refreshes - Stripe connection removed, QB connection remains
5. User later clicks "Unmap QB"
6. Frontend calls `DELETE /api/customers/mappings/qb/123`
7. Entire mapping row deleted

## Error Handling

All endpoints include comprehensive error handling:

- **Input validation**: Check for required fields and valid IDs
- **Database validation**: Verify related records exist (HaloPSA clients, Stripe customers)
- **Null checks**: Handle cases where mappings or records don't exist
- **Try/catch blocks**: Capture and log all database errors
- **Informative errors**: Return specific error messages to help debugging

## Future Enhancements

1. **QB Customers Table**: Implement `qb_customers` table and fetch customer names properly
2. **Bulk Operations**: Add endpoints to unmap multiple platforms at once
3. **Mapping History**: Track mapping changes in an audit log table
4. **Cascade Delete**: Handle orphaned transactions/invoices when unmapping
5. **Webhook Support**: Notify external systems when mappings change

## Files Modified

- **src/server.js**: Added 4 endpoints (lines 3479-3741)
- **CLAUDE.md**: Updated API documentation (lines 425-434)
- **test-unmap-endpoints.js**: Created test script

## Version Control

**Branch**: `feat/quickbooks-web-connector`
**Commit Message**:
```
feat(api): Add individual platform unmap endpoints for customer mappings

- DELETE /api/customers/mappings/stripe/:halopsa_client_id
- DELETE /api/customers/mappings/qb/:halopsa_client_id
- POST /api/customers/mappings/stripe
- POST /api/customers/mappings/qb

Smart unmap logic: removes individual platform mappings while preserving
others, or deletes row if it's the last platform connection.

Enables granular control over multi-platform customer mappings.
```
