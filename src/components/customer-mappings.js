/**
 * Customer Mappings Component
 *
 * A modular, drag-and-drop customer mapping interface with three columns:
 * - Stripe Customers (draggable)
 * - HaloPSA Clients (drop zones and display center)
 * - QuickBooks Customers (draggable)
 *
 * Features:
 * - HTML5 Drag and Drop API
 * - Real-time search filtering with debouncing
 * - Infinite scroll for all three columns
 * - Visual feedback for drag operations
 * - Auto-mapping functionality
 * - Unmap functionality
 */

export class CustomerMappingsComponent {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        if (!this.container) {
            throw new Error(`Container element with id "${containerId}" not found`);
        }

        // Data arrays
        this.stripeCustomers = [];
        this.haloPsaClients = [];
        this.qbCustomers = [];
        this.mappings = [];

        // Search state
        this.searchQueries = {
            stripe: '',
            halopsa: '',
            qb: ''
        };
        this.searchDebounceTimers = {};

        // Infinite scroll state
        this.scrollState = {
            stripe: { page: 0, hasMore: true, isLoading: false },
            halopsa: { page: 0, hasMore: true, isLoading: false },
            qb: { page: 0, hasMore: true, isLoading: false }
        };
        this.pageSize = 50;

        // Observer for infinite scroll
        this.observers = {};
    }

    /**
     * Initialize the component
     */
    async init() {
        this.renderLayout();
        this.attachEventListeners();
        this.setupInfiniteScroll();
        await this.loadAllData();
    }

    /**
     * Render the three-column layout
     */
    renderLayout() {
        this.container.innerHTML = `
            <div class="customer-mappings-grid">
                <!-- Stripe Customers Column -->
                <div class="mapping-column stripe-column">
                    <div class="column-header">
                        <h3>💳 Stripe Customers</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="stripe-search"
                            placeholder="Search Stripe customers..."
                        >
                    </div>
                    <div class="column-body" id="stripe-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="stripe-sentinel"></div>
                </div>

                <!-- HaloPSA Clients Column (Center - Drop Zone) -->
                <div class="mapping-column halopsa-column">
                    <div class="column-header">
                        <h3>🏢 HaloPSA Clients</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="halopsa-search"
                            placeholder="Search HaloPSA clients..."
                        >
                    </div>
                    <div class="column-body" id="halopsa-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="halopsa-sentinel"></div>
                </div>

                <!-- QuickBooks Customers Column -->
                <div class="mapping-column qb-column">
                    <div class="column-header">
                        <h3>📊 QuickBooks Customers</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="qb-search"
                            placeholder="Search QB customers..."
                        >
                    </div>
                    <div class="column-body" id="qb-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="qb-sentinel"></div>
                </div>
            </div>
        `;
    }

    /**
     * Attach all event listeners
     */
    attachEventListeners() {
        // Search input listeners with debouncing
        ['stripe', 'halopsa', 'qb'].forEach(column => {
            const searchInput = document.getElementById(`${column}-search`);
            if (searchInput) {
                searchInput.addEventListener('input', (e) => {
                    this.handleSearch(column, e.target.value);
                });
            }
        });
    }

    /**
     * Setup infinite scroll observers for all three columns
     */
    setupInfiniteScroll() {
        ['stripe', 'halopsa', 'qb'].forEach(column => {
            const sentinel = document.getElementById(`${column}-sentinel`);
            if (sentinel) {
                this.observers[column] = new IntersectionObserver(
                    (entries) => {
                        entries.forEach(entry => {
                            if (entry.isIntersecting &&
                                this.scrollState[column].hasMore &&
                                !this.scrollState[column].isLoading) {
                                this.loadMoreData(column);
                            }
                        });
                    },
                    { threshold: 0.1, rootMargin: '100px' }
                );
                this.observers[column].observe(sentinel);
            }
        });
    }

    /**
     * Handle search with debouncing
     */
    handleSearch(column, query) {
        this.searchQueries[column] = query.toLowerCase();

        // Clear existing timer
        if (this.searchDebounceTimers[column]) {
            clearTimeout(this.searchDebounceTimers[column]);
        }

        // Set new debounced search
        this.searchDebounceTimers[column] = setTimeout(() => {
            this.filterColumn(column);
        }, 300);
    }

    /**
     * Filter a column based on search query
     */
    filterColumn(column) {
        const query = this.searchQueries[column];
        const columnBody = document.getElementById(`${column}-column-body`);
        const cards = columnBody.querySelectorAll('.mapping-card');

        cards.forEach(card => {
            const text = card.textContent.toLowerCase();
            card.style.display = text.includes(query) ? '' : 'none';
        });
    }

    /**
     * Load all data for all columns
     */
    async loadAllData() {
        try {
            await Promise.all([
                this.loadMoreData('stripe'),
                this.loadMoreData('halopsa'),
                this.loadMoreData('qb')
            ]);
        } catch (error) {
            console.error('Error loading data:', error);
            this.showToast('Error loading data', 'error');
        }
    }

    /**
     * Load more data for a specific column (infinite scroll)
     */
    async loadMoreData(column) {
        const state = this.scrollState[column];
        if (state.isLoading || !state.hasMore) return;

        state.isLoading = true;
        const columnBody = document.getElementById(`${column}-column-body`);

        try {
            let data = [];
            const offset = state.page * this.pageSize;

            switch (column) {
                case 'stripe':
                    data = await this.fetchStripeCustomers(this.pageSize, offset);
                    break;
                case 'halopsa':
                    data = await this.fetchHaloPSAClients(this.pageSize, offset);
                    break;
                case 'qb':
                    data = await this.fetchQBCustomers(this.pageSize, offset);
                    break;
            }

            if (!data || data.length === 0) {
                state.hasMore = false;
                this.showNoMoreData(column);
                return;
            }

            // Remove loading spinner on first load
            if (state.page === 0) {
                const spinner = columnBody.querySelector('.loading-spinner');
                if (spinner) spinner.remove();
            }

            // Append new cards
            this.appendCards(column, data);

            state.page++;
            if (data.length < this.pageSize) {
                state.hasMore = false;
                this.showNoMoreData(column);
            }

        } catch (error) {
            console.error(`Error loading ${column} data:`, error);
            state.hasMore = false;
        } finally {
            state.isLoading = false;
        }
    }

    /**
     * Fetch Stripe customers from API
     */
    async fetchStripeCustomers(limit, offset) {
        const response = await fetch(`/api/stripe/customers?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch Stripe customers');
        const data = await response.json();
        return data.customers || data || [];
    }

    /**
     * Fetch HaloPSA clients from API
     */
    async fetchHaloPSAClients(limit, offset) {
        const response = await fetch(`/api/customers/all?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch HaloPSA clients');
        return response.json();
    }

    /**
     * Fetch QuickBooks customers from API
     */
    async fetchQBCustomers(limit, offset) {
        const response = await fetch(`/api/qbd/sync/customers?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch QB customers');
        const data = await response.json();
        return data.customers || data || [];
    }

    /**
     * Append cards to a column
     */
    appendCards(column, data) {
        const columnBody = document.getElementById(`${column}-column-body`);
        const fragment = document.createDocumentFragment();

        data.forEach(item => {
            const card = this.createCard(column, item);
            fragment.appendChild(card);
        });

        columnBody.appendChild(fragment);
    }

    /**
     * Create a mapping card for a customer
     */
    createCard(column, data) {
        const card = document.createElement('div');
        card.className = 'mapping-card';

        // Check if customer is already mapped
        const isMapped = !!(data.mapping_id || data.halopsa_client_id);

        // Set draggable for Stripe and QB columns (only if not already mapped)
        if ((column === 'stripe' || column === 'qb') && !isMapped) {
            card.draggable = true;
            card.classList.add('draggable');
            card.addEventListener('dragstart', (e) => this.handleDragStart(e, column, data));
            card.addEventListener('dragend', (e) => this.handleDragEnd(e));
        } else if ((column === 'stripe' || column === 'qb') && isMapped) {
            // Mark as locked if already mapped
            card.classList.add('locked');
        }

        // HaloPSA column cards are drop zones
        if (column === 'halopsa') {
            card.classList.add('drop-zone');
            card.addEventListener('dragover', (e) => this.handleDragOver(e));
            card.addEventListener('dragleave', (e) => this.handleDragLeave(e));
            card.addEventListener('drop', (e) => this.handleDrop(e, data));
        }

        // Render card content based on column
        switch (column) {
            case 'stripe':
                card.innerHTML = this.renderStripeCard(data);
                break;
            case 'halopsa':
                card.innerHTML = this.renderHaloPSACard(data);
                card.dataset.halopsaId = data.halopsa_id || data.id;
                break;
            case 'qb':
                card.innerHTML = this.renderQBCard(data);
                break;
        }

        return card;
    }

    /**
     * Render Stripe customer card
     */
    renderStripeCard(customer) {
        const email = customer.email || 'No email';
        const name = customer.name || 'Unnamed Customer';
        const isMapped = !!(customer.mapping_id || customer.halopsa_client_id);

        return `
            <div class="card-header">
                <strong>${name}</strong>
                ${isMapped ? '<span class="lock-badge" title="Locked - Already mapped">🔒</span>' : ''}
            </div>
            <div class="card-body">
                <div class="card-detail">✉️ ${email}</div>
                <div class="card-id">ID: ${customer.stripe_id || customer.id}</div>
                ${isMapped ? `<div class="card-detail mapping-info">→ Mapped to: ${customer.halopsa_client_name}</div>` : ''}
            </div>
            ${isMapped ? `<div class="card-actions"><button class="btn-unlock" onclick="mappingsComponent.unlockStripe('${customer.stripe_id || customer.id}', ${customer.halopsa_client_id})">🔓 Unlock</button></div>` : ''}
        `;
    }

    /**
     * Render HaloPSA client card
     */
    renderHaloPSACard(client) {
        const email = client.email || 'No email';
        const name = client.name || 'Unnamed Client';
        const hasStripe = !!client.stripe_customer_id;
        const hasQB = !!client.qb_customer_id;
        const halopsaId = client.halopsa_id || client.id;

        let mappingBadges = '';
        if (hasStripe) {
            mappingBadges += `<span class="mapping-badge stripe-badge" title="${client.stripe_customer_name}">💳 Stripe</span>`;
        }
        if (hasQB) {
            mappingBadges += `<span class="mapping-badge qb-badge" title="${client.qb_customer_name}">📊 QBD</span>`;
        }
        if (!hasStripe && !hasQB) {
            mappingBadges = '<span class="mapping-badge unmapped-badge">⚠️ Unmapped</span>';
        }

        return `
            <div class="card-header">
                <strong>${name}</strong>
                ${mappingBadges}
            </div>
            <div class="card-body">
                <div class="card-detail">✉️ ${email}</div>
                ${client.phone ? `<div class="card-detail">📞 ${client.phone}</div>` : ''}
                ${hasStripe ? `<div class="card-detail">💳 ${client.stripe_customer_name}</div>` : ''}
                ${hasQB ? `<div class="card-detail">📊 ${client.qb_customer_name}</div>` : ''}
            </div>
            <div class="card-actions">
                ${hasStripe ? `<button class="btn-unmap" onclick="mappingsComponent.unmapStripe(${halopsaId})">✖ Unmap Stripe</button>` : ''}
                ${hasQB ? `<button class="btn-unmap" onclick="mappingsComponent.unmapQB(${halopsaId})">✖ Unmap QBD</button>` : ''}
            </div>
        `;
    }

    /**
     * Render QuickBooks customer card
     */
    renderQBCard(customer) {
        const email = customer.email || 'No email';
        const name = customer.qb_full_name || customer.name || 'Unnamed Customer';
        const isMapped = !!(customer.mapping_id || customer.halopsa_client_id);
        const qbId = customer.qb_list_id || customer.id;

        return `
            <div class="card-header">
                <strong>${name}</strong>
                ${isMapped ? '<span class="lock-badge" title="Locked - Already mapped">🔒</span>' : ''}
            </div>
            <div class="card-body">
                <div class="card-detail">✉️ ${email}</div>
                ${customer.company_name ? `<div class="card-detail">🏢 ${customer.company_name}</div>` : ''}
                ${isMapped ? `<div class="card-detail mapping-info">→ Mapped to: ${customer.halopsa_client_name}</div>` : ''}
            </div>
            ${isMapped ? `<div class="card-actions"><button class="btn-unlock" onclick="mappingsComponent.unlockQB('${qbId}', ${customer.halopsa_client_id})">🔓 Unlock</button></div>` : ''}
        `;
    }

    /**
     * Handle drag start
     */
    handleDragStart(e, source, data) {
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('application/json', JSON.stringify({ source, data }));
        e.target.classList.add('dragging');
    }

    /**
     * Handle drag end
     */
    handleDragEnd(e) {
        e.target.classList.remove('dragging');
        // Remove all drag-over classes
        document.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
    }

    /**
     * Handle drag over (allow drop)
     */
    handleDragOver(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        e.currentTarget.classList.add('drag-over');
    }

    /**
     * Handle drag leave
     */
    handleDragLeave(e) {
        e.currentTarget.classList.remove('drag-over');
    }

    /**
     * Handle drop on HaloPSA client
     */
    async handleDrop(e, haloPsaClient) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');

        try {
            const dragData = JSON.parse(e.dataTransfer.getData('application/json'));
            const { source, data } = dragData;

            await this.createMapping(source, data, haloPsaClient);
        } catch (error) {
            console.error('Error handling drop:', error);
            this.showToast('Error creating mapping', 'error');
        }
    }

    /**
     * Create a mapping between source and HaloPSA client
     */
    async createMapping(source, sourceData, haloPsaClient) {
        this.showToast(`Creating ${source} mapping...`, 'info');

        try {
            let endpoint = '';
            let body = {};
            const halopsaId = haloPsaClient.halopsa_id || haloPsaClient.id;

            if (source === 'stripe') {
                endpoint = '/api/customers/mappings/stripe';
                body = {
                    halopsa_client_id: halopsaId,
                    stripe_customer_id: sourceData.stripe_id || sourceData.id
                };
            } else if (source === 'qb') {
                endpoint = '/api/customers/mappings/qb';
                body = {
                    halopsa_client_id: halopsaId,
                    qb_customer_id: sourceData.qb_list_id || sourceData.id
                };
            }

            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            if (!response.ok) throw new Error('Mapping failed');

            this.showToast(`Mapping created successfully!`, 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error creating mapping:', error);
            this.showToast('Error creating mapping', 'error');
        }
    }

    /**
     * Auto-map customers using backend algorithm
     */
    async autoMap() {
        this.showToast('Running auto-match algorithm...', 'info');

        try {
            const response = await fetch('/api/customers/automatch', { method: 'POST' });
            const result = await response.json();

            this.showToast(`Auto-mapped ${result.matches?.length || 0} customers!`, 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error auto-mapping:', error);
            this.showToast('Error running auto-match', 'error');
        }
    }

    /**
     * Unmap Stripe customer from HaloPSA client
     */
    async unmapStripe(haloPsaClientId) {
        if (!confirm('Remove Stripe mapping for this client?')) return;

        this.showToast('Unmapping Stripe customer...', 'info');

        try {
            const response = await fetch(`/api/customers/mappings/stripe/${haloPsaClientId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unmap failed');

            this.showToast('Stripe mapping removed', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unmapping Stripe:', error);
            this.showToast('Error unmapping Stripe customer', 'error');
        }
    }

    /**
     * Unmap QuickBooks customer from HaloPSA client
     */
    async unmapQB(haloPsaClientId) {
        if (!confirm('Remove QuickBooks mapping for this client?')) return;

        this.showToast('Unmapping QB customer...', 'info');

        try {
            const response = await fetch(`/api/customers/mappings/qb/${haloPsaClientId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unmap failed');

            this.showToast('QB mapping removed', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unmapping QB:', error);
            this.showToast('Error unmapping QB customer', 'error');
        }
    }

    /**
     * Unlock (unmap) a Stripe customer from HaloPSA client
     */
    async unlockStripe(stripeCustomerId, haloPsaClientId) {
        if (!confirm('Unlock this Stripe customer? This will remove the mapping.')) return;

        this.showToast('Unlocking Stripe customer...', 'info');

        try {
            const response = await fetch(`/api/customers/mappings/stripe/${haloPsaClientId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unlock failed');

            this.showToast('Stripe customer unlocked', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unlocking Stripe customer:', error);
            this.showToast('Error unlocking Stripe customer', 'error');
        }
    }

    /**
     * Unlock (unmap) a QuickBooks customer from HaloPSA client
     */
    async unlockQB(qbCustomerId, haloPsaClientId) {
        if (!confirm('Unlock this QuickBooks customer? This will remove the mapping.')) return;

        this.showToast('Unlocking QB customer...', 'info');

        try {
            const response = await fetch(`/api/customers/mappings/qb/${haloPsaClientId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unlock failed');

            this.showToast('QB customer unlocked', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unlocking QB customer:', error);
            this.showToast('Error unlocking QB customer', 'error');
        }
    }

    /**
     * Refresh all data
     */
    async refresh() {
        // Reset scroll state
        Object.keys(this.scrollState).forEach(column => {
            this.scrollState[column] = { page: 0, hasMore: true, isLoading: false };
        });

        // Clear column bodies
        ['stripe', 'halopsa', 'qb'].forEach(column => {
            const columnBody = document.getElementById(`${column}-column-body`);
            columnBody.innerHTML = '<div class="loading-spinner">Loading...</div>';
        });

        // Reload data
        await this.loadAllData();
    }

    /**
     * Show "no more data" message
     */
    showNoMoreData(column) {
        const columnBody = document.getElementById(`${column}-column-body`);
        const existing = columnBody.querySelector('.no-more-data');
        if (!existing) {
            const msg = document.createElement('div');
            msg.className = 'no-more-data';
            msg.textContent = `✓ All ${column} data loaded`;
            columnBody.appendChild(msg);
        }
    }

    /**
     * Show toast notification
     */
    showToast(message, type = 'info') {
        const toast = document.getElementById('toast');
        const toastMessage = document.getElementById('toast-message');

        if (toast && toastMessage) {
            toastMessage.textContent = message;
            toast.className = `toast ${type} show`;
            setTimeout(() => toast.classList.remove('show'), 5000);
        }
    }

    /**
     * Destroy component and clean up
     */
    destroy() {
        // Disconnect all observers
        Object.values(this.observers).forEach(observer => observer.disconnect());

        // Clear timers
        Object.values(this.searchDebounceTimers).forEach(timer => clearTimeout(timer));

        // Clear container
        this.container.innerHTML = '';
    }
}

// Export as default for ES6 module import
export default CustomerMappingsComponent;
