/**
 * Transaction to Invoice Mappings Component
 *
 * A modular, drag-and-drop transaction mapping interface with three columns:
 * - Stripe Transactions (draggable)
 * - HaloPSA Invoices (drop zones and display center)
 * - Mapped Transactions (read-only view)
 *
 * Features:
 * - HTML5 Drag and Drop API
 * - Real-time search filtering with debouncing
 * - Infinite scroll for all three columns
 * - Visual feedback for drag operations
 * - Auto-matching functionality (invoice # extraction from description)
 * - Unmap functionality
 */

export class TransactionMappingsComponent {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        if (!this.container) {
            throw new Error(`Container element with id "${containerId}" not found`);
        }

        // Data arrays
        this.stripeTransactions = [];
        this.haloPsaInvoices = [];
        this.mappedTransactions = [];

        // Search state
        this.searchQueries = {
            transactions: '',
            invoices: '',
            mapped: ''
        };
        this.searchDebounceTimers = {};

        // Infinite scroll state
        this.scrollState = {
            transactions: { page: 0, hasMore: true, isLoading: false },
            invoices: { page: 0, hasMore: true, isLoading: false },
            mapped: { page: 0, hasMore: true, isLoading: false }
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
            <div class="transaction-mappings-grid">
                <!-- Stripe Transactions Column -->
                <div class="mapping-column transactions-column">
                    <div class="column-header">
                        <h3>💳 Stripe Transactions</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="transactions-search"
                            placeholder="Search transactions..."
                        >
                        <div class="column-stats" id="transactions-stats">Loading...</div>
                    </div>
                    <div class="column-body" id="transactions-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="transactions-sentinel"></div>
                </div>

                <!-- HaloPSA Invoices Column (Center - Drop Zone) -->
                <div class="mapping-column invoices-column">
                    <div class="column-header">
                        <h3>📄 HaloPSA Invoices</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="invoices-search"
                            placeholder="Search invoices..."
                        >
                        <div class="column-stats" id="invoices-stats">Loading...</div>
                    </div>
                    <div class="column-body" id="invoices-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="invoices-sentinel"></div>
                </div>

                <!-- Mapped Transactions Column -->
                <div class="mapping-column mapped-column">
                    <div class="column-header">
                        <h3>✅ Mapped Transactions</h3>
                        <input
                            type="text"
                            class="column-search"
                            id="mapped-search"
                            placeholder="Search mapped..."
                        >
                        <div class="column-stats" id="mapped-stats">Loading...</div>
                    </div>
                    <div class="column-body" id="mapped-column-body">
                        <div class="loading-spinner">Loading...</div>
                    </div>
                    <div class="scroll-sentinel" id="mapped-sentinel"></div>
                </div>
            </div>
        `;
    }

    /**
     * Attach all event listeners
     */
    attachEventListeners() {
        // Search input listeners with debouncing
        ['transactions', 'invoices', 'mapped'].forEach(column => {
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
        ['transactions', 'invoices', 'mapped'].forEach(column => {
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
                this.loadMoreData('transactions'),
                this.loadMoreData('invoices'),
                this.loadMoreData('mapped')
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
                case 'transactions':
                    data = await this.fetchStripeTransactions(this.pageSize, offset);
                    break;
                case 'invoices':
                    data = await this.fetchHaloPSAInvoices(this.pageSize, offset);
                    break;
                case 'mapped':
                    data = await this.fetchMappedTransactions(this.pageSize, offset);
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

            // Update stats
            this.updateColumnStats(column);

        } catch (error) {
            console.error(`Error loading ${column} data:`, error);
            state.hasMore = false;
        } finally {
            state.isLoading = false;
        }
    }

    /**
     * Fetch unmapped Stripe transactions from API
     */
    async fetchStripeTransactions(limit, offset) {
        const response = await fetch(`/api/transaction-mappings/stripe?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch Stripe transactions');
        const data = await response.json();
        return data.transactions || data || [];
    }

    /**
     * Fetch HaloPSA invoices with mapping info from API
     */
    async fetchHaloPSAInvoices(limit, offset) {
        const response = await fetch(`/api/transaction-mappings/invoices?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch HaloPSA invoices');
        const data = await response.json();
        return data.invoices || data || [];
    }

    /**
     * Fetch mapped transaction-invoice pairs from API
     */
    async fetchMappedTransactions(limit, offset) {
        const response = await fetch(`/api/transaction-mappings?limit=${limit}&offset=${offset}`);
        if (!response.ok) throw new Error('Failed to fetch mapped transactions');
        const data = await response.json();
        return data.mappings || data || [];
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
     * Create a mapping card for a transaction/invoice
     */
    createCard(column, data) {
        const card = document.createElement('div');
        card.className = 'mapping-card';

        // Check if already mapped
        const isMapped = !!(data.mapping_id || data.mapped);

        // Set draggable for Stripe transactions (only if not already mapped)
        if (column === 'transactions' && !isMapped) {
            card.draggable = true;
            card.classList.add('draggable');
            card.addEventListener('dragstart', (e) => this.handleDragStart(e, column, data));
            card.addEventListener('dragend', (e) => this.handleDragEnd(e));
        } else if (column === 'transactions' && isMapped) {
            // Mark as locked if already mapped
            card.classList.add('locked');
        }

        // HaloPSA invoice cards are drop zones
        if (column === 'invoices') {
            card.classList.add('drop-zone');
            card.addEventListener('dragover', (e) => this.handleDragOver(e));
            card.addEventListener('dragleave', (e) => this.handleDragLeave(e));
            card.addEventListener('drop', (e) => this.handleDrop(e, data));
        }

        // Render card content based on column
        switch (column) {
            case 'transactions':
                card.innerHTML = this.renderTransactionCard(data);
                break;
            case 'invoices':
                card.innerHTML = this.renderInvoiceCard(data);
                card.dataset.invoiceId = data.halopsa_id || data.id;
                break;
            case 'mapped':
                card.innerHTML = this.renderMappedCard(data);
                break;
        }

        return card;
    }

    /**
     * Extract invoice number from Stripe transaction description
     */
    extractInvoiceNumber(description) {
        if (!description) return null;

        // Common patterns: "Invoice #INV-1234", "INV-1234", "Invoice 1234", etc.
        const patterns = [
            /invoice\s*#?\s*([A-Z0-9-]+)/i,
            /inv\s*#?\s*([A-Z0-9-]+)/i,
            /#\s*([A-Z0-9-]+)/i,
            /\b([A-Z]{2,5}-\d{3,})\b/i  // Matches patterns like INV-1234, INVOICE-001
        ];

        for (const pattern of patterns) {
            const match = description.match(pattern);
            if (match && match[1]) {
                return match[1].toUpperCase();
            }
        }

        return null;
    }

    /**
     * Render Stripe transaction card
     */
    renderTransactionCard(transaction) {
        const amount = ((transaction.amount || 0) / 100).toFixed(2);
        const currency = (transaction.currency || 'USD').toUpperCase();
        const date = transaction.created ? new Date(transaction.created * 1000).toLocaleDateString() : 'N/A';
        const customer = transaction.customer_name || transaction.customer_email || 'Unknown Customer';
        const description = transaction.description || 'No description';
        const isMapped = !!(transaction.mapping_id || transaction.mapped);
        const txId = transaction.stripe_id || transaction.id;

        // Extract potential invoice number
        const extractedInvoice = this.extractInvoiceNumber(description);

        return `
            <div class="card-header">
                <strong class="transaction-amount">$${amount} ${currency}</strong>
                ${isMapped ? '<span class="lock-badge" title="Locked - Already mapped">🔒</span>' : ''}
            </div>
            <div class="card-body">
                <div class="card-detail">📅 ${date}</div>
                <div class="card-detail">👤 ${customer}</div>
                <div class="card-detail description">${description}</div>
                ${extractedInvoice ? `<div class="card-detail highlight">🔍 Detected: ${extractedInvoice}</div>` : ''}
                <div class="card-id">ID: ${txId.substring(0, 20)}...</div>
                ${isMapped ? `<div class="card-detail mapping-info">→ Mapped to: ${transaction.invoice_number}</div>` : ''}
            </div>
            ${isMapped ? `<div class="card-actions"><button class="btn-unlock" onclick="transactionMappingsComponent && transactionMappingsComponent.unlockTransaction(${transaction.id})">🔓 Unlock</button></div>` : ''}
        `;
    }

    /**
     * Render HaloPSA invoice card
     */
    renderInvoiceCard(invoice) {
        const invoiceNumber = invoice.invoice_number || 'N/A';
        const clientName = invoice.client_name || 'Unknown Client';
        const amount = (invoice.total_amount || 0).toFixed(2);
        const date = invoice.invoice_date ? new Date(invoice.invoice_date).toLocaleDateString() : 'N/A';
        const status = invoice.status || 'Unknown';
        const hasMappings = invoice.mapped_transaction_count > 0;
        const mappingCount = invoice.mapped_transaction_count || 0;

        let mappingBadge = '';
        if (hasMappings) {
            mappingBadge = `<span class="mapping-badge mapped-badge" title="${mappingCount} transaction(s) mapped">💳 ${mappingCount} tx</span>`;
        } else {
            mappingBadge = '<span class="mapping-badge unmapped-badge">⚠️ Unmapped</span>';
        }

        return `
            <div class="card-header">
                <strong>${invoiceNumber}</strong>
                ${mappingBadge}
            </div>
            <div class="card-body">
                <div class="card-detail">🏢 ${clientName}</div>
                <div class="card-detail">💰 $${amount}</div>
                <div class="card-detail">📅 ${date}</div>
                <div class="card-detail"><span class="status ${status.toLowerCase()}">${status}</span></div>
                ${hasMappings ? `<div class="card-detail mapping-info">${mappingCount} Stripe transaction(s)</div>` : ''}
            </div>
            ${hasMappings ? `<div class="card-actions"><button class="btn-view" onclick="transactionMappingsComponent && transactionMappingsComponent.viewMappings(${invoice.halopsa_id || invoice.id})">👁️ View Mappings</button></div>` : ''}
        `;
    }

    /**
     * Render mapped transaction-invoice card
     */
    renderMappedCard(mapping) {
        const txAmount = ((mapping.transaction_amount || 0) / 100).toFixed(2);
        const txCurrency = (mapping.transaction_currency || 'USD').toUpperCase();
        const invoiceAmount = (mapping.invoice_amount || 0).toFixed(2);
        const invoiceNumber = mapping.invoice_number || 'N/A';
        const date = mapping.created_at ? new Date(mapping.created_at).toLocaleDateString() : 'N/A';
        const autoMapped = mapping.auto_mapped ? '🤖 Auto' : '👆 Manual';
        const confidence = mapping.mapping_confidence ? `${(mapping.mapping_confidence * 100).toFixed(0)}%` : 'N/A';

        return `
            <div class="card-header">
                <strong>${invoiceNumber}</strong>
                <span class="mapping-badge auto-badge" title="${autoMapped}">${autoMapped}</span>
            </div>
            <div class="card-body">
                <div class="card-detail">💳 Stripe: $${txAmount} ${txCurrency}</div>
                <div class="card-detail">📄 Invoice: $${invoiceAmount}</div>
                <div class="card-detail">📅 ${date}</div>
                ${mapping.auto_mapped ? `<div class="card-detail">🎯 Confidence: ${confidence}</div>` : ''}
            </div>
            <div class="card-actions">
                <button class="btn-unmap" onclick="transactionMappingsComponent && transactionMappingsComponent.unmapTransaction(${mapping.id})">✖ Unmap</button>
            </div>
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
     * Handle drop on HaloPSA invoice
     */
    async handleDrop(e, haloPsaInvoice) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');

        try {
            const dragData = JSON.parse(e.dataTransfer.getData('application/json'));
            const { source, data } = dragData;

            await this.createMapping(data, haloPsaInvoice);
        } catch (error) {
            console.error('Error handling drop:', error);
            this.showToast('Error creating mapping', 'error');
        }
    }

    /**
     * Create a mapping between Stripe transaction and HaloPSA invoice
     */
    async createMapping(transaction, invoice) {
        this.showToast('Creating transaction mapping...', 'info');

        try {
            const endpoint = '/api/transaction-mappings';
            const body = {
                stripe_transaction_id: transaction.id,
                halopsa_invoice_id: invoice.halopsa_id || invoice.id,
                invoice_amount: invoice.total_amount
            };

            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            if (!response.ok) throw new Error('Mapping failed');

            this.showToast('Mapping created successfully!', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error creating mapping:', error);
            this.showToast('Error creating mapping', 'error');
        }
    }

    /**
     * Auto-match transactions to invoices using backend algorithm
     */
    async autoMap() {
        this.showToast('Running auto-match algorithm...', 'info');

        try {
            const response = await fetch('/api/transaction-mappings/auto-match', { method: 'POST' });
            const result = await response.json();

            this.showToast(`Auto-matched ${result.matches?.length || 0} transactions!`, 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error auto-mapping:', error);
            this.showToast('Error running auto-match', 'error');
        }
    }

    /**
     * Generate AI-powered transaction mapping suggestions
     */
    async generateAISuggestions() {
        this.showToast('Generating AI mapping suggestions...', 'info');

        try {
            const response = await fetch('/api/ai/suggest-transaction-mappings', { method: 'POST' });
            const result = await response.json();

            if (!result.success) {
                throw new Error(result.message || 'Failed to generate AI suggestions');
            }

            this.showToast(`Generated ${result.count} AI suggestions!`, 'success');

            // Show suggestions modal
            await this.showAISuggestions();

        } catch (error) {
            console.error('Error generating AI suggestions:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        }
    }

    /**
     * Show AI mapping suggestions in a modal
     */
    async showAISuggestions() {
        try {
            // Fetch pending suggestions
            const response = await fetch('/api/ai/transaction-suggestions');
            const result = await response.json();

            if (!result.success || !result.suggestions || result.suggestions.length === 0) {
                this.showToast('No AI suggestions available', 'info');
                return;
            }

            // Create and show modal
            this.renderSuggestionsModal(result.suggestions);

        } catch (error) {
            console.error('Error fetching AI suggestions:', error);
            this.showToast('Error loading AI suggestions', 'error');
        }
    }

    /**
     * Render AI suggestions modal
     */
    renderSuggestionsModal(suggestions) {
        // Create modal container
        const modal = document.createElement('div');
        modal.id = 'ai-suggestions-modal';
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content ai-suggestions-content">
                <div class="modal-header">
                    <h2>🤖 AI Transaction Mapping Suggestions</h2>
                    <button class="modal-close" onclick="document.getElementById('ai-suggestions-modal').remove()">✖</button>
                </div>
                <div class="modal-body">
                    <p class="suggestions-intro">
                        Found ${suggestions.length} intelligent mapping suggestions based on invoice number extraction,
                        amount matching, date proximity, and customer relationships.
                    </p>
                    <div class="suggestions-list" id="suggestions-list">
                        ${suggestions.map(s => this.renderSuggestionCard(s)).join('')}
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="document.getElementById('ai-suggestions-modal').remove()">
                        Close
                    </button>
                </div>
            </div>
        `;

        // Attach to body
        document.body.appendChild(modal);

        // Show modal
        setTimeout(() => modal.classList.add('show'), 10);
    }

    /**
     * Render a single AI suggestion card
     */
    renderSuggestionCard(suggestion) {
        const txAmount = ((suggestion.transaction_amount || 0) / 100).toFixed(2);
        const txCurrency = (suggestion.transaction_currency || 'USD').toUpperCase();
        const invAmount = (suggestion.invoice_amount || 0).toFixed(2);
        const confidence = (suggestion.confidence * 100).toFixed(0);
        const confidenceClass = confidence >= 90 ? 'high' : confidence >= 70 ? 'medium' : 'low';
        const txDate = suggestion.transaction_created ?
            new Date(suggestion.transaction_created * 1000).toLocaleDateString() : 'N/A';
        const invDate = suggestion.invoice_date ?
            new Date(suggestion.invoice_date).toLocaleDateString() : 'N/A';

        return `
            <div class="suggestion-card" data-suggestion-id="${suggestion.id}">
                <div class="suggestion-header">
                    <div class="suggestion-confidence confidence-${confidenceClass}">
                        ${confidence}% Match
                    </div>
                    <div class="suggestion-actions">
                        <button class="btn-approve" onclick="transactionMappingsComponent.approveSuggestion(${suggestion.id})">
                            ✓ Approve
                        </button>
                        <button class="btn-reject" onclick="transactionMappingsComponent.rejectSuggestion(${suggestion.id})">
                            ✖ Reject
                        </button>
                    </div>
                </div>
                <div class="suggestion-body">
                    <div class="suggestion-section">
                        <h4>💳 Stripe Transaction</h4>
                        <div class="suggestion-detail"><strong>Amount:</strong> $${txAmount} ${txCurrency}</div>
                        <div class="suggestion-detail"><strong>Date:</strong> ${txDate}</div>
                        <div class="suggestion-detail"><strong>Description:</strong> ${suggestion.transaction_description || 'N/A'}</div>
                        <div class="suggestion-detail"><strong>Customer:</strong> ${suggestion.customer_name || suggestion.customer_email || 'Unknown'}</div>
                    </div>
                    <div class="suggestion-arrow">→</div>
                    <div class="suggestion-section">
                        <h4>📄 HaloPSA Invoice</h4>
                        <div class="suggestion-detail"><strong>Invoice:</strong> ${suggestion.invoice_number}</div>
                        <div class="suggestion-detail"><strong>Amount:</strong> $${invAmount}</div>
                        <div class="suggestion-detail"><strong>Date:</strong> ${invDate}</div>
                        <div class="suggestion-detail"><strong>Client:</strong> ${suggestion.client_name || 'Unknown'}</div>
                    </div>
                </div>
                <div class="suggestion-reasoning">
                    <strong>🎯 Reasoning:</strong> ${suggestion.reasoning || 'No reasoning provided'}
                </div>
            </div>
        `;
    }

    /**
     * Approve an AI suggestion
     */
    async approveSuggestion(suggestionId) {
        try {
            const response = await fetch(`/api/ai/approve-transaction-mapping/${suggestionId}`, {
                method: 'POST'
            });

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.message || 'Failed to approve suggestion');
            }

            // Remove suggestion card from modal
            const card = document.querySelector(`[data-suggestion-id="${suggestionId}"]`);
            if (card) {
                card.style.opacity = '0';
                setTimeout(() => card.remove(), 300);
            }

            this.showToast('Mapping approved and created!', 'success');

            // Check if all suggestions are processed
            setTimeout(() => {
                const remainingSuggestions = document.querySelectorAll('.suggestion-card').length;
                if (remainingSuggestions === 0) {
                    document.getElementById('ai-suggestions-modal')?.remove();
                    this.refresh();
                }
            }, 500);

        } catch (error) {
            console.error('Error approving suggestion:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        }
    }

    /**
     * Reject an AI suggestion
     */
    async rejectSuggestion(suggestionId) {
        try {
            const response = await fetch(`/api/ai/reject-transaction-mapping/${suggestionId}`, {
                method: 'POST'
            });

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.message || 'Failed to reject suggestion');
            }

            // Remove suggestion card from modal
            const card = document.querySelector(`[data-suggestion-id="${suggestionId}"]`);
            if (card) {
                card.style.opacity = '0';
                setTimeout(() => card.remove(), 300);
            }

            this.showToast('Suggestion rejected', 'info');

            // Check if all suggestions are processed
            setTimeout(() => {
                const remainingSuggestions = document.querySelectorAll('.suggestion-card').length;
                if (remainingSuggestions === 0) {
                    document.getElementById('ai-suggestions-modal')?.remove();
                }
            }, 500);

        } catch (error) {
            console.error('Error rejecting suggestion:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        }
    }

    /**
     * Unmap a transaction-invoice mapping
     */
    async unmapTransaction(mappingId) {
        if (!confirm('Remove this transaction mapping?')) return;

        this.showToast('Unmapping transaction...', 'info');

        try {
            const response = await fetch(`/api/transaction-mappings/${mappingId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unmap failed');

            this.showToast('Mapping removed', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unmapping transaction:', error);
            this.showToast('Error unmapping transaction', 'error');
        }
    }

    /**
     * Unlock (unmap) a transaction
     */
    async unlockTransaction(mappingId) {
        if (!confirm('Unlock this transaction? This will remove the mapping.')) return;

        this.showToast('Unlocking transaction...', 'info');

        try {
            const response = await fetch(`/api/transaction-mappings/${mappingId}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Unlock failed');

            this.showToast('Transaction unlocked', 'success');
            await this.refresh();

        } catch (error) {
            console.error('Error unlocking transaction:', error);
            this.showToast('Error unlocking transaction', 'error');
        }
    }

    /**
     * View mappings for a specific invoice
     */
    viewMappings(invoiceId) {
        // TODO: Implement modal or detail view
        this.showToast(`Viewing mappings for invoice ${invoiceId}`, 'info');
        console.log('View mappings for invoice:', invoiceId);
    }

    /**
     * Update column statistics
     */
    updateColumnStats(column) {
        const statsEl = document.getElementById(`${column}-stats`);
        if (!statsEl) return;

        const columnBody = document.getElementById(`${column}-column-body`);
        const cards = columnBody.querySelectorAll('.mapping-card');

        statsEl.textContent = `${cards.length} items`;
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
        ['transactions', 'invoices', 'mapped'].forEach(column => {
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
export default TransactionMappingsComponent;
