# Modular UI System Guide

## Overview

The new UI system is built around three core concepts:
1. **Centralized Dashboard** - Shows key metrics across all tabs
2. **Tab Manager** - Easy registration and management of tabs
3. **Component System** - Reusable JavaScript components

## Architecture

### Files Structure
```
src/
├── renderer-new.html          # Main application (NEW modular version)
├── renderer.html              # Legacy version (kept for reference)
├── styles.css                 # Modern design system with dashboard styles
├── components/
│   └── dashboard.js           # Dashboard stats & Tab Manager components
└── tabs/
    ├── config.html            # Configuration tab content
    ├── stripe.html            # Stripe integration content
    ├── halopsa.html           # HaloPSA integration content
    ├── mapping.html           # Customer mapping content
    ├── customer.html          # Customer view content
    └── status.html            # System status content
```

### Key Components

#### 1. DashboardStats Component

Located in `components/dashboard.js`, this component:
- Fetches stats from `/api/dashboard/stats`
- Displays 6 key metrics with icons
- Auto-refreshes every 30 seconds
- Shows last sync timestamp

**Usage:**
```javascript
const dashboardStats = new DashboardStats();
await dashboardStats.init();

// Manual refresh
dashboardStats.refresh();
```

#### 2. TabManager Component

Located in `components/dashboard.js`, manages all tab navigation:
- Register tabs dynamically
- Load tab content on-demand
- Call tab-specific initialization functions
- Support for icons and custom configurations

**Usage:**
```javascript
const tabManager = new TabManager();

// Register a new tab
tabManager.registerTab('myTab', {
  title: 'My Custom Tab',
  icon: '🎯',
  file: 'tabs/mytab.html',
  enabled: true,
  init: myTabInitFunction
});

// Render all tabs
tabManager.renderTabs();

// Switch to a tab
await tabManager.switchTo('myTab');
```

## How to Add a New Tab

### Step 1: Create the Tab HTML File

Create `src/tabs/mytab.html`:
```html
<div class="config-section">
  <div class="config-header">
    <h3>My Custom Tab</h3>
  </div>
  <div>
    <p>Tab content goes here</p>
    <button id="myCustomButton" class="btn primary">Do Something</button>
  </div>
</div>
```

### Step 2: Register the Tab

In `renderer-new.html`, add to the `DOMContentLoaded` section:
```javascript
tabManager.registerTab('mytab', {
  title: 'My Tab',
  icon: '🎯',
  file: 'tabs/mytab.html',
  enabled: true,
  init: initMyTab
});
```

### Step 3: Create Initialization Function

Add the initialization function:
```javascript
function initMyTab() {
  const btn = document.getElementById('myCustomButton');
  if (btn) {
    btn.addEventListener('click', async () => {
      showStatus('Button clicked!', 'success');
      // Your logic here
    });
  }

  // Load initial data
  loadMyTabData();
}

async function loadMyTabData() {
  try {
    const response = await fetch('/api/mytab/data');
    const data = await response.json();
    // Display data
  } catch (error) {
    showStatus('Error loading data: ' + error.message, 'error');
  }
}
```

### Step 4: Add Tab Content Container

In `renderer-new.html`, add the tab content container:
```html
<div class="tab-content" id="mytab">
  <!-- Content will be loaded dynamically -->
</div>
```

## API Integration

### Dashboard Stats Endpoint

The dashboard automatically calls `GET /api/dashboard/stats` which returns:
```json
{
  "success": true,
  "stats": {
    "stripe_customers": 150,
    "halopsa_clients": 120,
    "mapped_customers": 95,
    "stripe_transactions": 1234,
    "halopsa_invoices": 567,
    "purchase_orders": 89,
    "last_sync": "2025-10-29T10:30:00Z"
  }
}
```

This endpoint is implemented in:
- **Backend**: `server.js` line 880 - `app.get('/api/dashboard-stats', ...)`
- **Database**: `database.js` line 498 - `getDashboardStats()` method

### Refreshing Dashboard Stats

After any data import or sync operation:
```javascript
// Inside your tab's function
await importSomeData();

// Refresh the dashboard
dashboardStats.refresh();
```

## Design System

### Color Scheme

The application uses a modern gradient color scheme defined in `styles.css`:

- **Primary**: Indigo/Purple gradient (#4F46E5 → #7C3AED)
- **Secondary**: Emerald green (#10B981)
- **Accent**: Amber (#F59E0B)
- **Background**: Purple gradient body

### Component Classes

#### Cards
```html
<div class="config-section">
  <div class="config-header">
    <h3>Section Title</h3>
  </div>
  <div>
    <!-- Content -->
  </div>
</div>
```

#### Buttons
```html
<button class="btn primary">Primary Action</button>
<button class="btn secondary">Secondary</button>
<button class="btn success">Success</button>
<button class="btn warning">Warning</button>
<button class="btn danger">Danger</button>
<button class="btn info">Info</button>
```

#### Status Messages
```javascript
showStatus('Success message', 'success');
showStatus('Error message', 'error');
showStatus('Info message', 'info');
showStatus('Warning message', 'warning');
```

#### Badges
```html
<span class="badge success">Active</span>
<span class="badge warning">Pending</span>
<span class="badge danger">Error</span>
<span class="badge info">Info</span>
```

#### Stats Display
```html
<div class="stat-card">
  <div class="stat-value">1,234</div>
  <div class="stat-label">Total Records</div>
</div>
```

## Utility Functions

### Global Functions Available

- `showStatus(message, type)` - Display status notifications
- `dashboardStats.refresh()` - Refresh dashboard statistics
- `tabManager.switchTo(tabId)` - Switch to a specific tab
- `saveConfig(formData)` - Save configuration
- `loadConfiguration()` - Load configuration

## Best Practices

### 1. Always Refresh Dashboard After Data Changes
```javascript
await importData();
dashboardStats.refresh();
```

### 2. Use Error Handling
```javascript
try {
  const result = await someOperation();
  showStatus('Success!', 'success');
} catch (error) {
  showStatus('Error: ' + error.message, 'error');
}
```

### 3. Disable Buttons During Operations
```javascript
async function myOperation() {
  const btn = document.getElementById('myBtn');
  btn.disabled = true;
  btn.textContent = 'Processing...';

  try {
    await doWork();
    showStatus('Done!', 'success');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Original Text';
  }
}
```

### 4. Use Semantic HTML and CSS Classes
- Use descriptive IDs for elements that need JavaScript interaction
- Use existing CSS classes from the design system
- Keep styling in CSS, not inline

## Migration from Old UI

To switch to the new modular UI system:

1. Rename `renderer.html` to `renderer-legacy.html`
2. Rename `renderer-new.html` to `renderer.html`
3. Test all functionality
4. Update any custom tab files to work with new initialization pattern

## Example: Complete Tab Implementation

Here's a complete example of adding a "Reports" tab:

**tabs/reports.html:**
```html
<div class="config-section">
  <div class="config-header">
    <h3>Financial Reports</h3>
  </div>
  <div>
    <div class="form-group">
      <label for="reportType">Report Type</label>
      <select id="reportType" class="customer-filter">
        <option value="revenue">Revenue Report</option>
        <option value="expenses">Expenses Report</option>
        <option value="transactions">All Transactions</option>
      </select>
    </div>

    <div class="actions">
      <button id="generateReport" class="btn primary">Generate Report</button>
      <button id="exportReport" class="btn secondary">Export CSV</button>
    </div>

    <div id="reportResults" style="margin-top: 2rem;">
      <!-- Results will be displayed here -->
    </div>
  </div>
</div>
```

**In renderer-new.html (registration):**
```javascript
tabManager.registerTab('reports', {
  title: 'Reports',
  icon: '📊',
  file: 'tabs/reports.html',
  enabled: true,
  init: initReportsTab
});
```

**In renderer-new.html (initialization):**
```javascript
function initReportsTab() {
  const generateBtn = document.getElementById('generateReport');
  const exportBtn = document.getElementById('exportReport');
  const reportType = document.getElementById('reportType');
  const resultsDiv = document.getElementById('reportResults');

  if (generateBtn) {
    generateBtn.addEventListener('click', async () => {
      const type = reportType.value;
      generateBtn.disabled = true;
      generateBtn.textContent = 'Generating...';

      try {
        const response = await fetch(`/api/reports/generate?type=${type}`);
        const data = await response.json();

        if (data.success) {
          // Display results
          resultsDiv.innerHTML = `
            <table class="table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Description</th>
                  <th>Amount</th>
                </tr>
              </thead>
              <tbody>
                ${data.rows.map(row => `
                  <tr>
                    <td>${row.date}</td>
                    <td>${row.description}</td>
                    <td>$${row.amount.toFixed(2)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          `;
          showStatus('Report generated successfully', 'success');
        }
      } catch (error) {
        showStatus('Error generating report: ' + error.message, 'error');
      } finally {
        generateBtn.disabled = false;
        generateBtn.textContent = 'Generate Report';
      }
    });
  }

  if (exportBtn) {
    exportBtn.addEventListener('click', () => {
      // Export logic
      showStatus('Export functionality coming soon', 'info');
    });
  }
}
```

**Add content container in renderer-new.html:**
```html
<div class="tab-content" id="reports">
  <!-- Content loaded dynamically -->
</div>
```

That's it! Your new tab is fully integrated with the modular system.

## Testing

To test the new UI:

1. Start the application: `node src/server.js`
2. Open Electron app or navigate to `http://localhost:3000`
3. Verify dashboard stats load correctly
4. Test tab navigation
5. Test data import operations and verify dashboard refresh
6. Test responsive design on different screen sizes

## Support

For issues or questions about the modular UI system, check:
- `CLAUDE.md` - Project documentation
- `src/components/dashboard.js` - Component source code
- `src/styles.css` - Design system reference
