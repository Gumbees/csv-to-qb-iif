# Mappings View Rewrite - HaloPSA Client-Centric Design

## Summary
The mappings view has been completely rewritten to show HaloPSA clients as the primary entity, with Stripe and QuickBooks mappings displayed as separate columns in the same table.

## Changes Made

### 1. HTML Structure (`src/index.html` lines 642-685)

**Before:**
- Two separate tables: "Stripe → HaloPSA Mappings" and "QuickBooks → HaloPSA Mappings"
- Stripe-focused filter dropdown with only 3 options
- 4-column table showing Stripe customers as primary

**After:**
- Single unified table showing HaloPSA clients as primary
- Comprehensive filter dropdown with 7 options:
  - All Clients
  - Unmapped from Stripe
  - Mapped to Stripe
  - Unmapped from QuickBooks
  - Mapped to QuickBooks
  - Fully Unmapped (no Stripe or QB)
  - Fully Mapped (has both)
- 5-column table structure:
  1. **HaloPSA Client** - Client name + email
  2. **Stripe Customer** - Shows checkmark + name if mapped, "Not Mapped" badge if unmapped
  3. **QuickBooks Customer** - Shows checkmark + name if mapped, "Not Mapped" badge if unmapped
  4. **Mapping Status** - Combined status badges (Fully Mapped, Stripe Only, QB Only, Unmapped, Auto-Matched)
  5. **Actions** - Unmap buttons for Stripe and/or QB (only shown when mapped)

### 2. JavaScript Functions (`src/index.html` lines 1575-1701)

#### `loadMappings()` - Completely Rewritten

**Before:**
- Used `/api/stripe/customers/imported?filter=${filter}` endpoint
- No infinite scrolling (loaded all at once)
- Showed Stripe customers with HaloPSA mapping column
- Only supported 3 filter options (all, unmapped, mapped)

**After:**
- Uses `/api/customers/all?limit=${limit}&offset=${offset}&filter=${filter}` endpoint
- Full InfiniteScroll implementation with 50 items per page
- Shows HaloPSA clients as primary with both Stripe and QB mapping columns
- Supports 7 filter options passed to backend
- Dynamic status badge generation based on mapping state:
  - Fully Mapped (has both Stripe and QB)
  - Stripe Only (has Stripe, no QB)
  - QB Only (has QB, no Stripe)
  - Unmapped (has neither)
  - Auto-Matched (if auto_mapped flag is true)
- Conditional action buttons (only shows unmap buttons when mappings exist)

#### New Helper Functions

**`unmapStripe(haloPsaClientId)`**
- Confirms before unmapping
- Shows toast notifications
- Reloads mappings after unmap
- TODO: Backend API endpoint needs implementation

**`unmapQB(haloPsaClientId)`**
- Same pattern as unmapStripe
- TODO: Backend API endpoint needs implementation

#### Removed Functions

**`filterStripeMappings()`** - No longer needed (dropdown now calls `loadMappings()` directly)

### 3. Backend API Requirements

The frontend now expects the `/api/customers/all` endpoint to support:

**Query Parameters:**
- `limit` - Number of records per page (default: 50)
- `offset` - Starting position for pagination
- `filter` - One of:
  - `all` - All HaloPSA clients
  - `stripe-unmapped` - Clients without Stripe mapping
  - `stripe-mapped` - Clients with Stripe mapping
  - `qb-unmapped` - Clients without QB mapping
  - `qb-mapped` - Clients with QB mapping
  - `fully-unmapped` - Clients with neither Stripe nor QB mapping
  - `fully-mapped` - Clients with both Stripe and QB mappings

**Expected Response:**
```json
[
  {
    "id": 123,
    "name": "Client Name",
    "email": "client@example.com",
    "stripe_customer_id": 456,
    "stripe_customer_name": "Stripe Customer Name",
    "qb_customer_id": 789,
    "qb_customer_name": "QB Customer Name",
    "auto_mapped": true
  },
  ...
]
```

### 4. Visual Design Improvements

**Status Badges:**
- Green "mapped" badge for mapped items
- Yellow "unmapped" badge for unmapped items
- Blue "synced" badge for partial mappings (Stripe Only, QB Only, Auto-Matched)

**Layout:**
- Checkmark (✓) icons for mapped customers
- Client email shown in gray under client name
- Action buttons only appear when relevant (no empty action columns)
- Consistent spacing and alignment

**Infinite Scroll:**
- Loads 50 clients at a time
- Shows spinner while loading
- Shows "All data loaded" message when complete
- Smooth scrolling experience

## Benefits

1. **HaloPSA-Centric View**: HaloPSA is the source of truth, so clients should be primary
2. **Unified Interface**: All mappings visible in one table instead of separate sections
3. **Better Filtering**: 7 filter options cover all mapping scenarios
4. **Infinite Scroll**: Handles large datasets efficiently
5. **Action-Oriented**: Unmap buttons right where you need them
6. **Status Clarity**: Combined status badges show mapping state at a glance

## Testing Checklist

- [ ] Load mappings view and verify table structure
- [ ] Test all 7 filter options
- [ ] Verify infinite scroll loads more data when scrolling
- [ ] Test unmap buttons (when backend endpoints are implemented)
- [ ] Verify status badges show correct states
- [ ] Test with clients that have:
  - [ ] No mappings
  - [ ] Stripe mapping only
  - [ ] QB mapping only
  - [ ] Both mappings
  - [ ] Auto-matched flag

## Backend TODO

The backend needs to implement or verify:

1. **`GET /api/customers/all`** with pagination and filtering
2. **`DELETE /api/customers/{id}/stripe-mapping`** for unmapping Stripe
3. **`DELETE /api/customers/{id}/qb-mapping`** for unmapping QB
4. Ensure response includes all necessary fields (especially `stripe_customer_name` and `qb_customer_name`)

## Files Modified

- `G:\DTC Github\csv-to-qb-iif\src\index.html`
  - Lines 642-685: HTML structure
  - Lines 1575-1701: JavaScript functions
