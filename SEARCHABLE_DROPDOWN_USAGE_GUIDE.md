# Searchable Dropdown - Usage Guide

## Quick Start

### 1. Include Required Files

```html
<!-- In your HTML head -->
<link rel="stylesheet" href="components/searchable-dropdown.css">

<!-- Before closing body tag -->
<script src="components/searchable-dropdown.js"></script>
```

### 2. Create Container

```html
<!-- Simple container div with unique ID -->
<div id="my-dropdown"></div>
```

### 3. Initialize Component

```javascript
const dropdown = new CustomSearchableDropdown('my-dropdown', {
    placeholder: 'Search items...',
    items: [
        {id: 1, text: 'Option One'},
        {id: 2, text: 'Option Two'},
        {id: 3, text: 'Option Three'}
    ],
    onChange: (selectedItem) => {
        console.log('Selected:', selectedItem);
        // Handle selection here
    }
});
```

That's it! The component will render a fully functional searchable dropdown.

## Visual States

### Default State (Closed)
```
┌────────────────────────────────────────┐
│ Search accounts...                  ▼ │  ← Input field with arrow
└────────────────────────────────────────┘
```

### Open State (Dropdown Shown)
```
┌────────────────────────────────────────┐
│ bank                                ▲ │  ← Typing filters results
├────────────────────────────────────────┤
│ Account 1001 - Bank              ✓   │  ← Keyboard highlighted
│ Account 1050 - Savings Bank          │
│ Account 2100 - Bank Fees              │
│                                        │
│ ...and 15 more (keep typing)          │  ← Virtual scroll indicator
└────────────────────────────────────────┘
```

### Selected State
```
┌────────────────────────────────────────┐
│ Account 1001 - Bank            ✕    ▼ │  ← Selected value with clear button
└────────────────────────────────────────┘
```

### Searching State
```
┌────────────────────────────────────────┐
│ processing                          ▲ │
├────────────────────────────────────────┤
│ Stripe Processing Fees                 │  ← Highlighting matches
│    ^^^^^^^^^^ (highlighted in yellow)  │
│ Credit Card Processing                 │
│             ^^^^^^^^^^ (highlighted)   │
└────────────────────────────────────────┘
```

### No Results State
```
┌────────────────────────────────────────┐
│ xyzabc                              ▲ │
├────────────────────────────────────────┤
│                                        │
│        No results found                │  ← Empty state message
│                                        │
└────────────────────────────────────────┘
```

## Keyboard Navigation

| Key | Action |
|-----|--------|
| **Type** | Filter items in real-time |
| **↓ (Down Arrow)** | Highlight next item |
| **↑ (Up Arrow)** | Highlight previous item |
| **Enter** | Select highlighted item |
| **Escape** | Close dropdown |
| **Tab** | Close dropdown and move to next field |
| **Click outside** | Close dropdown |

## Common Use Cases

### Use Case 1: Account Selection (Current Implementation)

```javascript
// In src/index.html - Account mapping dropdowns
const depositAccountDropdown = new CustomAccountDropdown('deposit', 'customer_deposits');

// When accounts are loaded from QuickBooks
depositAccountDropdown.setAccounts([
    {id: 1, account_name: 'Customer Deposits', account_type: 'Other Current Liability'},
    {id: 2, account_name: 'Unearned Revenue', account_type: 'Other Current Liability'},
    // ... more accounts
]);

// Component automatically formats items as:
// "Customer Deposits (Other Current Liability)"
```

### Use Case 2: Category Selection

```javascript
const categoryDropdown = new CustomSearchableDropdown('category-selector', {
    placeholder: 'Select category...',
    items: [
        {id: 'income', text: 'Income'},
        {id: 'expense', text: 'Expense'},
        {id: 'asset', text: 'Asset'},
        {id: 'liability', text: 'Liability'},
        {id: 'equity', text: 'Equity'}
    ],
    onChange: (category) => {
        filterAccountsByCategory(category.id);
    }
});
```

### Use Case 3: Customer Selection with Icons

```javascript
const customerIcons = {
    'Stripe': '💳',
    'HaloPSA': '🔧',
    'QuickBooks': '📊'
};

const customerDropdown = new CustomSearchableDropdown('customer-selector', {
    placeholder: 'Search customers...',
    items: customers.map(c => ({
        id: c.id,
        text: c.name,
        source: c.source  // Custom property
    })),
    renderItem: (item) => {
        const icon = customerIcons[item.source] || '👤';
        return `<span>${icon} ${item.text}</span>`;
    },
    onChange: (customer) => {
        loadCustomerDetails(customer.id);
    }
});
```

### Use Case 4: Dynamic Item Loading

```javascript
// Initialize empty
const vendorDropdown = new CustomSearchableDropdown('vendor-selector', {
    placeholder: 'Loading vendors...',
    items: [],
    onChange: (vendor) => {
        populateVendorForm(vendor);
    }
});

// Load items asynchronously
async function loadVendors() {
    const vendors = await fetch('/api/vendors').then(r => r.json());
    const items = vendors.map(v => ({
        id: v.id,
        text: `${v.name} (${v.email})`
    }));

    vendorDropdown.setItems(items);
}

loadVendors();
```

### Use Case 5: Pre-selected Value

```javascript
// Initialize with pre-selected item
const accountDropdown = new CustomSearchableDropdown('account-selector', {
    placeholder: 'Select account...',
    items: accounts,
    selectedId: 42,  // Pre-select account with ID 42
    onChange: (account) => {
        updateAccountMapping(account);
    }
});

// Or set after initialization
accountDropdown.setSelectedId(42);
```

## Integration with Forms

### Simple Form Integration

```html
<form id="account-form">
    <label for="account-dropdown">QuickBooks Account</label>
    <div id="account-dropdown"></div>

    <button type="submit">Save</button>
</form>

<script>
let selectedAccountId = null;

const dropdown = new CustomSearchableDropdown('account-dropdown', {
    placeholder: 'Select account...',
    items: accountsList,
    onChange: (account) => {
        selectedAccountId = account ? account.id : null;
    }
});

document.getElementById('account-form').addEventListener('submit', (e) => {
    e.preventDefault();

    if (!selectedAccountId) {
        alert('Please select an account');
        return;
    }

    saveAccountMapping(selectedAccountId);
});
</script>
```

### Form Validation Example

```javascript
function validateForm() {
    const account = accountDropdown.getValue();

    if (!account) {
        showError('Account is required');
        accountDropdown.focus();  // Focus the dropdown
        return false;
    }

    return true;
}

function resetForm() {
    accountDropdown.clear();  // Clear selection
}
```

## Styling Customization

### Override Colors (Purple Theme)

```css
/* In your custom CSS file */
.searchable-dropdown-control:hover,
.searchable-dropdown-control:focus-within {
    border-color: #9333ea;  /* Purple accent */
}

.searchable-dropdown-item-selected {
    background-color: #f3e8ff;  /* Light purple */
    color: #7c3aed;
}

.searchable-dropdown-item mark {
    background-color: #fef08a;  /* Yellow highlight */
}
```

### Custom Height

```javascript
const dropdown = new CustomSearchableDropdown('my-dropdown', {
    items: items,
    maxHeight: 400  // Make dropdown taller (default: 300px)
});
```

### Custom Debounce Delay

```javascript
const dropdown = new CustomSearchableDropdown('my-dropdown', {
    items: items,
    debounceDelay: 500  // Wait 500ms instead of 300ms before filtering
});
```

## Performance Tips

### For Large Datasets (100+ items)

```javascript
// Component automatically uses virtual scrolling when items > threshold
const dropdown = new CustomSearchableDropdown('large-dropdown', {
    items: largeDataset,  // e.g., 1000 items
    virtualScrollThreshold: 100,  // Start virtual scroll at 100 items
    placeholder: 'Type to narrow results...'
});

// Only first 100 items are rendered initially
// Remaining items show: "...and 900 more (keep typing to narrow results)"
```

### Async Search (Future Enhancement Placeholder)

```javascript
// Current: All items loaded upfront
// Future: Could fetch items as user types

const dropdown = new CustomSearchableDropdown('async-dropdown', {
    placeholder: 'Search...',
    items: [],  // Start empty
    debounceDelay: 500  // Wait longer for typing to finish
});

// Listen to search input changes (theoretical - not yet implemented)
// dropdown.onSearch((searchTerm) => {
//     fetchResults(searchTerm).then(results => {
//         dropdown.setItems(results);
//     });
// });
```

## Troubleshooting

### Dropdown Not Showing

**Problem**: Dropdown renders but menu doesn't appear when clicked.

**Solution**: Check z-index conflicts. The dropdown uses `z-index: 1000`.

```css
/* Fix: Ensure parent doesn't have higher z-index */
.parent-container {
    z-index: auto;  /* or lower than 1000 */
}
```

### Dropdown Cut Off at Bottom

**Problem**: Dropdown menu is cut off by container overflow.

**Solution**: The component auto-positions (dropup vs dropdown). Ensure container allows overflow:

```css
.container {
    overflow: visible;  /* Don't use overflow: hidden */
}
```

### Clear Button Not Appearing

**Problem**: Selected item shows but clear button (×) doesn't appear.

**Solution**: The clear button is added dynamically. Check `updateClearButton()` is being called. This should happen automatically on item selection.

### Search Not Working

**Problem**: Typing doesn't filter items.

**Solution**: Check items array format. Each item must have `id` and `text` properties:

```javascript
// CORRECT
items: [{id: 1, text: 'Account Name'}, ...]

// WRONG
items: [{value: 1, label: 'Account Name'}, ...]  // Different property names
```

## Browser Support

- ✅ Chrome/Edge (latest)
- ✅ Firefox (latest)
- ✅ Safari (latest)
- ✅ Mobile Safari (iOS 12+)
- ✅ Chrome Mobile (Android)

**IE11**: Not supported (uses modern ES6+ features)

## Accessibility

The component follows WCAG 2.1 guidelines:

- ✅ Keyboard navigation (Arrow keys, Enter, Escape, Tab)
- ✅ ARIA attributes (`role="combobox"`, `aria-expanded`, `aria-haspopup`, `role="listbox"`, `role="option"`)
- ✅ Focus management (input receives focus, highlighted items scrolled into view)
- ✅ Screen reader friendly (labels and state changes announced)
- ✅ High contrast mode support

## Live Demo

**Test Page**: Open `test-searchable-dropdown.html` in your browser to see live examples.

**Application**: Visit `http://localhost:3000/#sync` and scroll to "Transaction Account Mappings" section to see the component in action for:
- Down Payments account selector
- Stripe Fees account selector
- Stripe Payments account selector
- Inventory Assets account selector

## API Reference Summary

### Constructor
```javascript
new CustomSearchableDropdown(containerId, options)
```

### Options Object
```javascript
{
    placeholder: String,           // Input placeholder text
    items: Array<{id, text}>,     // Array of selectable items
    selectedId: Any,               // Pre-selected item ID (optional)
    onChange: Function,            // Callback when item selected
    renderItem: Function,          // Custom item renderer (optional)
    disabled: Boolean,             // Disabled state (default: false)
    maxHeight: Number,             // Max dropdown height in px (default: 300)
    debounceDelay: Number,         // Search debounce in ms (default: 300)
    virtualScrollThreshold: Number // Virtual scroll threshold (default: 100)
}
```

### Public Methods
```javascript
dropdown.setItems(items)           // Update items array
dropdown.setSelectedId(id)         // Set selected item by ID
dropdown.getValue()                // Get {id, text} of selected item
dropdown.clear()                   // Clear selection
dropdown.focus()                   // Focus the input field
dropdown.setDisabled(boolean)      // Enable/disable dropdown
dropdown.destroy()                 // Remove event listeners & cleanup
```

### Events
```javascript
// onChange callback
onChange: (selectedItem) => {
    // selectedItem = {id: ..., text: ...} or null if cleared
}
```

## Support

For issues or questions:
1. Check this usage guide
2. Review `SEARCHABLE_DROPDOWN_IMPLEMENTATION.md` for implementation details
3. Check `CLAUDE.md` for architecture documentation
4. Review source code comments in `src/components/searchable-dropdown.js`
