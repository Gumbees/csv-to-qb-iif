/**
 * Dashboard Component System
 * Centralized stats and modular UI components
 */

// Dashboard Stats Component
class DashboardStats {
  constructor() {
    this.stats = {
      stripe_customers: 0,
      halopsa_clients: 0,
      mapped_customers: 0,
      stripe_transactions: 0,
      halopsa_invoices: 0,
      purchase_orders: 0,
      last_sync: null
    };
    this.element = null;
  }

  /**
   * Initialize the dashboard stats component
   */
  async init() {
    await this.fetchStats();
    this.render();
    // Auto-refresh every 30 seconds
    setInterval(() => this.refresh(), 30000);
  }

  /**
   * Fetch stats from API
   */
  async fetchStats() {
    try {
      const response = await fetch('/api/dashboard/stats');
      const data = await response.json();
      if (data.success) {
        this.stats = data.stats;
      }
    } catch (error) {
      console.error('Error fetching dashboard stats:', error);
    }
  }

  /**
   * Refresh stats
   */
  async refresh() {
    await this.fetchStats();
    this.render();
  }

  /**
   * Render the dashboard stats
   */
  render() {
    const container = document.getElementById('dashboardStats');
    if (!container) return;

    const mappingPercentage = this.stats.halopsa_clients > 0
      ? Math.round((this.stats.mapped_customers / this.stats.halopsa_clients) * 100)
      : 0;

    container.innerHTML = `
      <div class="dashboard-stats">
        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #4F46E5 0%, #7C3AED 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path>
              <circle cx="9" cy="7" r="4"></circle>
              <path d="M23 21v-2a4 4 0 0 0-3-3.87"></path>
              <path d="M16 3.13a4 4 0 0 1 0 7.75"></path>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.stripe_customers}</div>
            <div class="stat-label">Stripe Customers</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #10B981 0%, #059669 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
              <circle cx="12" cy="7" r="4"></circle>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.halopsa_clients}</div>
            <div class="stat-label">HaloPSA Clients</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #F59E0B 0%, #D97706 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.mapped_customers}</div>
            <div class="stat-label">Mapped Customers</div>
            <div class="stat-sublabel">${mappingPercentage}% of clients</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <rect x="1" y="4" width="22" height="16" rx="2" ry="2"></rect>
              <line x1="1" y1="10" x2="23" y2="10"></line>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.stripe_transactions}</div>
            <div class="stat-label">Stripe Transactions</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
              <polyline points="14 2 14 8 20 8"></polyline>
              <line x1="16" y1="13" x2="8" y2="13"></line>
              <line x1="16" y1="17" x2="8" y2="17"></line>
              <polyline points="10 9 9 9 8 9"></polyline>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.halopsa_invoices}</div>
            <div class="stat-label">HaloPSA Invoices</div>
          </div>
        </div>

        <div class="stat-card">
          <div class="stat-icon" style="background: linear-gradient(135deg, #EC4899 0%, #DB2777 100%);">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <path d="M6 2L3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z"></path>
              <line x1="3" y1="6" x2="21" y2="6"></line>
              <path d="M16 10a4 4 0 0 1-8 0"></path>
            </svg>
          </div>
          <div class="stat-content">
            <div class="stat-value">${this.stats.purchase_orders}</div>
            <div class="stat-label">Purchase Orders</div>
          </div>
        </div>
      </div>
      ${this.stats.last_sync ? `
        <div class="dashboard-sync-status">
          <span class="sync-indicator">●</span>
          Last synced: ${this.formatTimestamp(this.stats.last_sync)}
        </div>
      ` : ''}
    `;
  }

  /**
   * Format timestamp for display
   */
  formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} minute${diffMins > 1 ? 's' : ''} ago`;

    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;

    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
  }
}

// Tab Manager Component
class TabManager {
  constructor() {
    this.tabs = new Map();
    this.currentTab = null;
  }

  /**
   * Register a new tab
   * @param {string} id - Tab identifier
   * @param {Object} config - Tab configuration
   */
  registerTab(id, config) {
    this.tabs.set(id, {
      id,
      title: config.title,
      icon: config.icon || null,
      file: config.file || `tabs/${id}.html`,
      init: config.init || null,
      loaded: false,
      enabled: config.enabled !== false
    });
  }

  /**
   * Switch to a tab
   * @param {string} tabId - Tab identifier
   */
  async switchTo(tabId) {
    const tab = this.tabs.get(tabId);
    if (!tab || !tab.enabled) {
      console.error(`Tab ${tabId} not found or disabled`);
      return;
    }

    // Update UI
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    const tabElement = document.querySelector(`[data-tab="${tabId}"]`);
    const contentElement = document.getElementById(tabId);

    if (tabElement) tabElement.classList.add('active');
    if (contentElement) contentElement.classList.add('active');

    // Load tab content if not already loaded
    if (!tab.loaded && contentElement) {
      try {
        const response = await fetch(tab.file);
        if (response.ok) {
          const content = await response.text();
          contentElement.innerHTML = content;
          tab.loaded = true;

          // Call tab-specific initialization
          if (tab.init && typeof tab.init === 'function') {
            tab.init();
          }
        }
      } catch (error) {
        console.error(`Error loading tab ${tabId}:`, error);
        contentElement.innerHTML = `<div class="status error">Failed to load tab content: ${error.message}</div>`;
      }
    }

    this.currentTab = tabId;

    // Trigger dashboard stats refresh when switching tabs
    if (window.dashboardStats) {
      window.dashboardStats.refresh();
    }
  }

  /**
   * Get all registered tabs
   */
  getTabs() {
    return Array.from(this.tabs.values());
  }

  /**
   * Render tabs to the tab bar
   */
  renderTabs() {
    const tabsContainer = document.querySelector('.tabs');
    if (!tabsContainer) return;

    const tabsHTML = this.getTabs()
      .filter(tab => tab.enabled)
      .map(tab => `
        <div class="tab ${tab.id === this.currentTab ? 'active' : ''}" data-tab="${tab.id}">
          ${tab.icon ? `<span class="tab-icon">${tab.icon}</span>` : ''}
          ${tab.title}
        </div>
      `).join('');

    tabsContainer.innerHTML = tabsHTML;

    // Attach click handlers
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        this.switchTo(tab.dataset.tab);
      });
    });
  }
}

// Export for global access
window.DashboardStats = DashboardStats;
window.TabManager = TabManager;
