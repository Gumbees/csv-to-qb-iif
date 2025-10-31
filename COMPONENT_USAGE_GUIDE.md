# Customer Mappings Component - Usage Guide

## Overview

The CustomerMappingsComponent is a modular, reusable web component for managing customer mappings across three platforms: Stripe, HaloPSA, and QuickBooks Desktop. It provides an intuitive drag-and-drop interface for creating and managing mappings.

## Component Structure

```
src/
├── components/
│   ├── customer-mappings.js   # ES6 module - main component class
│   └── customer-mappings.css  # Component-specific styles
└── index.html                 # Integration point
```

## Features

### Three-Column Layout
- **Left Column**: Stripe Customers (draggable)
- **Center Column**: HaloPSA Clients (drop zones + primary display)
- **Right Column**: QuickBooks Customers (draggable)

### Drag and Drop
- Drag Stripe or QuickBooks customer cards onto HaloPSA client drop zones
- Visual feedback during drag operations
- Drop zone highlighting when hovering
- Real-time mapping creation

### Infinite Scroll
- Loads 50 items per column per page
- Automatic loading when scrolling near bottom
- Supports thousands of records without performance issues

### Search Filtering
- Independent search boxes for each column
- Case-insensitive, real-time filtering
- 300ms debounce to prevent excessive rendering

### Mapping Management
- Visual badges show mapping status (Stripe, QB, Unmapped)
- Individual unmap buttons for each platform
- Auto-mapping algorithm via backend fuzzy matching
- Refresh functionality to reload all data

## Installation & Integration

### 1. Import the Component (ES6 Module)

```html
<!-- In your HTML file -->
<script type="module">
    import CustomerMappingsComponent from './components/customer-mappings.js';

    // Make it globally accessible
    window.mappingsComponent = null;

    function initMappingsComponent() {
        if (!window.mappingsComponent) {
            window.mappingsComponent = new CustomerMappingsComponent('customer-mappings-container');
            window.mappingsComponent.init();
        }
    }

    window.initMappingsComponent = initMappingsComponent;
</script>
```

### 2. Add Container Element

```html
<!-- Customer Mappings View -->
<div id="view-mappings" class="view">
    <div class="content-header">
        <h2>Customer Mappings</h2>
        <button onclick="mappingsComponent && mappingsComponent.autoMap()">
            Auto-Map Customers
        </button>
        <button onclick="mappingsComponent && mappingsComponent.refresh()">
            Refresh
        </button>
    </div>

    <!-- Component mounts here -->
    <div id="customer-mappings-container"></div>
</div>
```

### 3. Include Component CSS

```html
<link rel="stylesheet" href="components/customer-mappings.css">
```

### 4. Initialize Component

```javascript
// Load component when view becomes visible
function loadViewData(viewName) {
    if (viewName === 'mappings') {
        initMappingsComponent();
    }
}
```

## Component API

### Constructor

```javascript
const component = new CustomerMappingsComponent('container-id');
```

**Parameters:**
- `containerId` (string): ID of the HTML element where component will mount

**Throws:**
- Error if container element not found

### Methods

#### `async init()`
Initialize the component and load all data.

```javascript
await component.init();
```

#### `async refresh()`
Reload all data from APIs and reset scroll state.

```javascript
await component.refresh();
```

#### `async autoMap()`
Run backend auto-matching algorithm to automatically map customers based on name/email similarity.

```javascript
await component.autoMap();
// Shows toast: "Auto-mapped X customers!"
```

#### `async unmapStripe(haloPsaClientId)`
Remove Stripe mapping for a specific HaloPSA client.

```javascript
await component.unmapStripe(123);
// Prompts for confirmation
// Shows toast: "Stripe mapping removed"
```

#### `async unmapQB(haloPsaClientId)`
Remove QuickBooks mapping for a specific HaloPSA client.

```javascript
await component.unmapQB(123);
// Prompts for confirmation
// Shows toast: "QB mapping removed"
```

#### `destroy()`
Clean up component resources (disconnect observers, clear timers).

```javascript
component.destroy();
```

## Backend API Endpoints

The component requires the following API endpoints:

### GET `/api/stripe/customers`
Fetch Stripe customers with pagination.

**Query Parameters:**
- `limit` (number): Items per page (default: 50)
- `offset` (number): Skip N items

**Response:**
```json
{
  "customers": [
    {
      "id": 1,
      "stripe_id": "cus_xxx",
      "name": "John Doe",
      "email": "john@example.com"
    }
  ]
}
```

### GET `/api/customers/all`
Fetch HaloPSA clients with pagination.

**Query Parameters:**
- `limit` (number): Items per page
- `offset` (number): Skip N items

**Response:**
```json
[
  {
    "id": 1,
    "halopsa_id": "123",
    "name": "Acme Corp",
    "email": "info@acme.com",
    "phone": "555-0100",
    "stripe_customer_id": 1,
    "stripe_customer_name": "John Doe",
    "qb_customer_id": 456,
    "qb_customer_name": "Acme Corporation"
  }
]
```

### GET `/api/qbd/sync/customers`
Fetch QuickBooks customers with pagination.

**Query Parameters:**
- `limit` (number): Items per page
- `offset` (number): Skip N items

**Response:**
```json
{
  "customers": [
    {
      "id": 456,
      "qb_full_name": "Acme Corporation",
      "email": "billing@acme.com",
      "company_name": "Acme Corp"
    }
  ]
}
```

### POST `/api/customers/map-stripe`
Create or update Stripe → HaloPSA mapping.

**Request Body:**
```json
{
  "halopsa_client_id": 1,
  "stripe_customer_id": "cus_xxx"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Mapping created successfully"
}
```

### POST `/api/customers/map-qb`
Create or update QuickBooks → HaloPSA mapping.

**Request Body:**
```json
{
  "halopsa_client_id": 1,
  "qb_customer_id": 456
}
```

### DELETE `/api/customers/:id/unmap-stripe`
Remove Stripe mapping for HaloPSA client.

**URL Parameters:**
- `id` (number): HaloPSA client ID

**Response:**
```json
{
  "success": true,
  "message": "Stripe mapping removed"
}
```

### DELETE `/api/customers/:id/unmap-qb`
Remove QuickBooks mapping for HaloPSA client.

**URL Parameters:**
- `id` (number): HaloPSA client ID

**Response:**
```json
{
  "success": true,
  "message": "QB mapping removed"
}
```

### POST `/api/customers/automatch`
Run auto-matching algorithm.

**Response:**
```json
{
  "success": true,
  "matches": [
    { "stripe_id": "cus_xxx", "halopsa_id": "123", "confidence": 0.95 }
  ]
}
```

## CSS Classes

### Layout Classes
- `.customer-mappings-grid` - Main three-column grid container
- `.mapping-column` - Individual column container
- `.stripe-column` - Stripe column with purple accent
- `.halopsa-column` - HaloPSA column with blue accent
- `.qb-column` - QuickBooks column with green accent

### Card Classes
- `.mapping-card` - Base card style
- `.draggable` - Applied to Stripe/QB cards (cursor: move)
- `.drop-zone` - Applied to HaloPSA cards (drop targets)
- `.dragging` - Applied during drag operation (opacity 0.5)
- `.drag-over` - Applied to drop zone on hover (purple border)

### Badge Classes
- `.mapping-badge` - Base badge style
- `.stripe-badge` - Purple Stripe badge
- `.qb-badge` - Green QB badge
- `.unmapped-badge` - Yellow unmapped warning badge

### State Classes
- `.loading-spinner` - Loading indicator
- `.no-more-data` - End of scroll message

## Customization

### Changing Page Size

```javascript
// In customer-mappings.js constructor
this.pageSize = 100; // Default is 50
```

### Changing Search Debounce

```javascript
// In handleSearch method
this.searchDebounceTimers[column] = setTimeout(() => {
    this.filterColumn(column);
}, 500); // Default is 300ms
```

### Customizing Colors

Edit `customer-mappings.css`:

```css
/* Stripe column accent */
.stripe-column {
    border-top: 4px solid #635bff; /* Stripe purple */
}

/* HaloPSA column accent */
.halopsa-column {
    border-top: 4px solid #667eea; /* Custom blue */
}

/* QuickBooks column accent */
.qb-column {
    border-top: 4px solid #2ca01c; /* QB green */
}
```

## Troubleshooting

### Component Not Loading
- Check browser console for module import errors
- Ensure `customer-mappings.js` is served with correct MIME type (`text/javascript`)
- Verify container element exists before calling constructor

### Drag and Drop Not Working
- Ensure `draggable="true"` is set on source cards
- Check that drop zone event listeners are attached
- Verify `preventDefault()` is called in `dragover` handler

### Infinite Scroll Not Triggering
- Check that IntersectionObserver is supported (all modern browsers)
- Verify sentinel element is inside scrollable container
- Check `hasMore` flag is not stuck on `false`

### API Errors
- Open browser DevTools Network tab to inspect API responses
- Verify backend endpoints return expected JSON structure
- Check for CORS issues if backend on different domain

## Performance Considerations

### Large Datasets
- Component uses IntersectionObserver for efficient infinite scroll
- Only renders visible items (50 per page)
- Debounced search prevents excessive filtering

### Memory Management
- Call `destroy()` when switching away from mappings view
- This disconnects observers and prevents memory leaks

### Network Optimization
- API requests are paginated (50 items per request)
- Search filtering is client-side (no API calls)
- Consider caching API responses if data rarely changes

## Examples

### Basic Integration

```javascript
// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    const component = new CustomerMappingsComponent('customer-mappings-container');
    component.init();
});
```

### With Navigation System

```javascript
function switchView(viewName) {
    document.querySelectorAll('.view').forEach(v => v.classList.add('view-hidden'));
    document.getElementById(`view-${viewName}`).classList.remove('view-hidden');

    if (viewName === 'mappings') {
        if (!window.mappingsComponent) {
            window.mappingsComponent = new CustomerMappingsComponent('customer-mappings-container');
            window.mappingsComponent.init();
        } else {
            window.mappingsComponent.refresh();
        }
    }
}
```

### Programmatic Mapping Creation

```javascript
// Create mapping without drag-drop
const stripeData = { stripe_id: 'cus_xxx', name: 'John Doe', email: 'john@example.com' };
const haloPsaClient = { id: 123, name: 'Acme Corp' };

await component.createMapping('stripe', stripeData, haloPsaClient);
```

## Future Enhancements

### Planned Features
- Bulk selection and mapping (select multiple + map all)
- Undo/redo functionality for mapping operations
- Export mapping report as CSV
- Mapping confidence scores displayed on cards
- Filter by mapping status (mapped/unmapped/partial)
- Keyboard shortcuts for power users

### Extensibility Points
- Override `renderStripeCard()` / `renderHaloPSACard()` / `renderQBCard()` for custom card layouts
- Hook into `createMapping()` for custom validation logic
- Extend `fetchStripeCustomers()` / `fetchHaloPSAClients()` / `fetchQBCustomers()` for custom data sources

## Support

For issues or feature requests, please check:
- CLAUDE.md for architecture details
- README.md for project setup
- Browser console for error messages
- Network tab for API response inspection

## Version History

- **v1.0.0** (2025-10-30): Initial release
  - Three-column drag-and-drop interface
  - Infinite scroll for all columns
  - Real-time search filtering
  - Auto-mapping algorithm
  - Individual platform unmapping
