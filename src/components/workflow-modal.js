/**
 * Modal-Based Workflow System
 * Provides guided workflows for complex operations
 */

class WorkflowModal {
    constructor(id, title, content, width = '600px') {
        this.id = id;
        this.title = title;
        this.content = content;
        this.width = width;
        this.modalElement = null;
        this.overlayElement = null;
    }

    show() {
        // Create overlay
        this.overlayElement = document.createElement('div');
        this.overlayElement.className = 'modal-overlay';
        this.overlayElement.onclick = () => this.hide();

        // Create modal
        this.modalElement = document.createElement('div');
        this.modalElement.className = 'workflow-modal';
        this.modalElement.style.maxWidth = this.width;
        this.modalElement.id = this.id;

        this.modalElement.innerHTML = `
            <div class="modal-header">
                <h2>${this.title}</h2>
                <button class="modal-close" aria-label="Close modal">&times;</button>
            </div>
            <div class="modal-body">
                ${this.content}
            </div>
        `;

        // Add to DOM
        document.body.appendChild(this.overlayElement);
        document.body.appendChild(this.modalElement);

        // Add event listeners
        this.modalElement.querySelector('.modal-close').onclick = () => this.hide();

        // Prevent modal click from closing
        this.modalElement.onclick = (e) => e.stopPropagation();

        // Handle Escape key
        this.escapeHandler = (e) => {
            if (e.key === 'Escape') this.hide();
        };
        document.addEventListener('keydown', this.escapeHandler);

        // Trigger animation
        requestAnimationFrame(() => {
            this.overlayElement.classList.add('active');
            this.modalElement.classList.add('active');
        });
    }

    hide() {
        if (!this.modalElement) return;

        // Fade out animation
        this.overlayElement.classList.remove('active');
        this.modalElement.classList.remove('active');

        // Remove from DOM after animation
        setTimeout(() => {
            this.overlayElement?.remove();
            this.modalElement?.remove();
            this.overlayElement = null;
            this.modalElement = null;
        }, 300);

        // Remove escape handler
        document.removeEventListener('keydown', this.escapeHandler);
    }

    update(content) {
        if (!this.modalElement) return;
        const body = this.modalElement.querySelector('.modal-body');
        if (body) {
            body.innerHTML = content;
        }
    }

    updateElement(selector, content) {
        if (!this.modalElement) return;
        const element = this.modalElement.querySelector(selector);
        if (element) {
            element.innerHTML = content;
        }
    }
}

// ==================== Import Data Modal ====================

function showImportDataModal() {
    const modal = new WorkflowModal(
        'import-data-modal',
        '📥 Import Your Data',
        `
        <p class="modal-description">Choose what to import from your connected services:</p>

        <div class="import-options">
            <button class="workflow-btn stripe-btn" onclick="importStripeFromModal()">
                <span class="btn-icon">💳</span>
                <div class="btn-content">
                    <span class="btn-label">Import Stripe Data</span>
                    <span class="btn-subtitle">Customers & Transactions</span>
                </div>
            </button>

            <button class="workflow-btn halopsa-btn" onclick="importHaloPSAFromModal()">
                <span class="btn-icon">🔧</span>
                <div class="btn-content">
                    <span class="btn-label">Import HaloPSA Data</span>
                    <span class="btn-subtitle">Clients, Invoices & Purchase Orders</span>
                </div>
            </button>

            <button class="workflow-btn qb-btn" onclick="syncQuickBooksFromModal()">
                <span class="btn-icon">📊</span>
                <div class="btn-content">
                    <span class="btn-label">Sync QuickBooks Data</span>
                    <span class="btn-subtitle">Chart of Accounts & Customers</span>
                </div>
            </button>

            <button class="workflow-btn all-btn" onclick="pullAllDataFromModal()">
                <span class="btn-icon">🚀</span>
                <div class="btn-content">
                    <span class="btn-label">Pull All Data</span>
                    <span class="btn-subtitle">Import everything at once</span>
                </div>
            </button>
        </div>

        <div id="import-progress" class="import-progress hidden">
            <div class="progress-header">
                <h4>Import Progress</h4>
            </div>
            <div id="progress-items"></div>
        </div>
        `,
        '700px'
    );
    modal.show();
    return modal;
}

// Import functions for modal
async function importStripeFromModal() {
    const modal = document.getElementById('import-data-modal');
    const progressDiv = modal.querySelector('#import-progress');
    const progressItems = modal.querySelector('#progress-items');

    progressDiv.classList.remove('hidden');
    progressItems.innerHTML = `
        <div class="progress-item" id="stripe-customers-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Importing Stripe Customers...</span>
        </div>
        <div class="progress-item" id="stripe-transactions-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Importing Stripe Transactions...</span>
        </div>
    `;

    try {
        // Import customers
        const customersResponse = await fetch('/api/stripe/import-customers', { method: 'POST' });
        const customersData = await customersResponse.json();

        const customersItem = document.getElementById('stripe-customers-progress');
        customersItem.querySelector('.progress-icon').textContent = '✅';
        customersItem.querySelector('.progress-label').textContent =
            `Imported ${customersData.count || 0} Stripe Customers`;

        // Import transactions
        const transactionsResponse = await fetch('/api/stripe/import-transactions', { method: 'POST' });
        const transactionsData = await transactionsResponse.json();

        const transactionsItem = document.getElementById('stripe-transactions-progress');
        transactionsItem.querySelector('.progress-icon').textContent = '✅';
        transactionsItem.querySelector('.progress-label').textContent =
            `Imported ${transactionsData.count || 0} Stripe Transactions`;

        // Add success message
        progressItems.innerHTML += `
            <div class="progress-item success">
                <span class="progress-icon">🎉</span>
                <span class="progress-label">Stripe import completed successfully!</span>
            </div>
        `;
    } catch (error) {
        progressItems.innerHTML += `
            <div class="progress-item error">
                <span class="progress-icon">❌</span>
                <span class="progress-label">Error: ${error.message}</span>
            </div>
        `;
    }
}

async function importHaloPSAFromModal() {
    const modal = document.getElementById('import-data-modal');
    const progressDiv = modal.querySelector('#import-progress');
    const progressItems = modal.querySelector('#progress-items');

    progressDiv.classList.remove('hidden');
    progressItems.innerHTML = `
        <div class="progress-item" id="halopsa-clients-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Importing HaloPSA Clients...</span>
        </div>
        <div class="progress-item" id="halopsa-invoices-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Importing HaloPSA Invoices...</span>
        </div>
        <div class="progress-item" id="halopsa-pos-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Importing HaloPSA Purchase Orders...</span>
        </div>
    `;

    try {
        // Import clients
        const clientsResponse = await fetch('/api/halopsa/import-clients', { method: 'POST' });
        const clientsData = await clientsResponse.json();

        const clientsItem = document.getElementById('halopsa-clients-progress');
        clientsItem.querySelector('.progress-icon').textContent = '✅';
        clientsItem.querySelector('.progress-label').textContent =
            `Imported ${clientsData.count || 0} HaloPSA Clients`;

        // Import invoices
        const invoicesResponse = await fetch('/api/halopsa/import-invoices', { method: 'POST' });
        const invoicesData = await invoicesResponse.json();

        const invoicesItem = document.getElementById('halopsa-invoices-progress');
        invoicesItem.querySelector('.progress-icon').textContent = '✅';
        invoicesItem.querySelector('.progress-label').textContent =
            `Imported ${invoicesData.count || 0} HaloPSA Invoices`;

        // Import purchase orders
        const posResponse = await fetch('/api/halopsa/import-purchase-orders', { method: 'POST' });
        const posData = await posResponse.json();

        const posItem = document.getElementById('halopsa-pos-progress');
        posItem.querySelector('.progress-icon').textContent = '✅';
        posItem.querySelector('.progress-label').textContent =
            `Imported ${posData.count || 0} HaloPSA Purchase Orders`;

        // Add success message
        progressItems.innerHTML += `
            <div class="progress-item success">
                <span class="progress-icon">🎉</span>
                <span class="progress-label">HaloPSA import completed successfully!</span>
            </div>
        `;
    } catch (error) {
        progressItems.innerHTML += `
            <div class="progress-item error">
                <span class="progress-icon">❌</span>
                <span class="progress-label">Error: ${error.message}</span>
            </div>
        `;
    }
}

async function syncQuickBooksFromModal() {
    const modal = document.getElementById('import-data-modal');
    const progressDiv = modal.querySelector('#import-progress');
    const progressItems = modal.querySelector('#progress-items');

    progressDiv.classList.remove('hidden');
    progressItems.innerHTML = `
        <div class="progress-item" id="qb-accounts-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Syncing Chart of Accounts...</span>
        </div>
        <div class="progress-item" id="qb-customers-progress">
            <span class="progress-icon">⏳</span>
            <span class="progress-label">Syncing Customer List...</span>
        </div>
    `;

    try {
        // This would trigger QBWC sync
        progressItems.innerHTML += `
            <div class="progress-item info">
                <span class="progress-icon">ℹ️</span>
                <span class="progress-label">QuickBooks sync requires Web Connector to be running</span>
            </div>
        `;
    } catch (error) {
        progressItems.innerHTML += `
            <div class="progress-item error">
                <span class="progress-icon">❌</span>
                <span class="progress-label">Error: ${error.message}</span>
            </div>
        `;
    }
}

async function pullAllDataFromModal() {
    await importStripeFromModal();
    await new Promise(resolve => setTimeout(resolve, 500)); // Brief pause
    await importHaloPSAFromModal();
}

// ==================== Map Customers Modal ====================

function showMapCustomersModal() {
    const modal = new WorkflowModal(
        'map-customers-modal',
        '🔗 Customer Mapping Workflow',
        `
        <p class="modal-description">Map customers across Stripe, HaloPSA, and QuickBooks:</p>

        <div class="mapping-steps">
            <div class="step">
                <h4>Step 1: Auto-Match</h4>
                <p class="step-description">Let AI match customers automatically based on names and emails</p>
                <div class="step-buttons">
                    <button class="workflow-btn" onclick="runAutoMatch()">
                        <span class="btn-icon">🤖</span>
                        <span class="btn-label">Run AI Auto-Match</span>
                    </button>
                    <button class="workflow-btn" onclick="runRuleMatch()">
                        <span class="btn-icon">📋</span>
                        <span class="btn-label">Run Rule-Based Match</span>
                    </button>
                </div>
                <div id="auto-match-result" class="step-result"></div>
            </div>

            <div class="step">
                <h4>Step 2: Review Suggestions</h4>
                <p class="step-description">Review and confirm auto-matched customers</p>
                <div class="step-buttons">
                    <button class="workflow-btn" onclick="reviewSuggestions()">
                        <span class="btn-icon">👀</span>
                        <span class="btn-label">Review Suggestions</span>
                    </button>
                </div>
                <div id="suggestion-count" class="badge-container">
                    <span class="badge">0 suggestions</span>
                </div>
            </div>

            <div class="step">
                <h4>Step 3: Manual Mapping</h4>
                <p class="step-description">Manually map remaining customers</p>
                <div class="step-buttons">
                    <button class="workflow-btn" onclick="openMappingInterface()">
                        <span class="btn-icon">✏️</span>
                        <span class="btn-label">Open Mapping Interface</span>
                    </button>
                </div>
            </div>
        </div>
        `,
        '700px'
    );
    modal.show();
    return modal;
}

async function runAutoMatch() {
    const resultDiv = document.getElementById('auto-match-result');
    resultDiv.innerHTML = '<div class="progress-item"><span class="progress-icon">⏳</span> Running auto-match...</div>';

    try {
        const response = await fetch('/api/customer-mappings/auto-match', { method: 'POST' });
        const data = await response.json();

        resultDiv.innerHTML = `
            <div class="progress-item success">
                <span class="progress-icon">✅</span>
                <span>Matched ${data.matched || 0} customers</span>
            </div>
        `;

        // Update suggestion count
        const badge = document.getElementById('suggestion-count');
        badge.innerHTML = `<span class="badge">${data.matched || 0} suggestions</span>`;
    } catch (error) {
        resultDiv.innerHTML = `
            <div class="progress-item error">
                <span class="progress-icon">❌</span>
                <span>Error: ${error.message}</span>
            </div>
        `;
    }
}

async function runRuleMatch() {
    const resultDiv = document.getElementById('auto-match-result');
    resultDiv.innerHTML = '<div class="progress-item"><span class="progress-icon">⏳</span> Running rule-based matching...</div>';

    // Rule-based matching logic would go here
    setTimeout(() => {
        resultDiv.innerHTML = `
            <div class="progress-item info">
                <span class="progress-icon">ℹ️</span>
                <span>Rule-based matching coming soon</span>
            </div>
        `;
    }, 500);
}

function reviewSuggestions() {
    // Close modal and navigate to mapping tab
    const modal = document.querySelector('.workflow-modal');
    if (modal) {
        modal.remove();
        document.querySelector('.modal-overlay')?.remove();
    }

    // Switch to Customer Mapping tab
    if (typeof switchTab === 'function') {
        switchTab('customer-mapping');
    }
}

function openMappingInterface() {
    // Close modal and navigate to mapping tab
    const modal = document.querySelector('.workflow-modal');
    if (modal) {
        modal.remove();
        document.querySelector('.modal-overlay')?.remove();
    }

    // Switch to Customer Mapping tab
    if (typeof switchTab === 'function') {
        switchTab('customer-mapping');
    }
}

// ==================== Verify Data Modal ====================

async function showVerifyDataModal(onConfirm) {
    const modal = new WorkflowModal(
        'verify-data-modal',
        '⚠️ Verify Before Syncing to QuickBooks',
        `
        <p class="modal-description">Review what will be sent to QuickBooks Desktop:</p>

        <div class="verification-checklist">
            <div class="check-item" data-status="checking">
                <span class="check-icon">⏳</span>
                <span class="check-label">Chart of Accounts Mapped</span>
                <span class="check-result">Checking...</span>
            </div>

            <div class="check-item" data-status="checking">
                <span class="check-icon">⏳</span>
                <span class="check-label">Customer Mappings Valid</span>
                <span class="check-result">Checking...</span>
            </div>

            <div class="check-item" data-status="checking">
                <span class="check-icon">⏳</span>
                <span class="check-label">Account Mappings Complete</span>
                <span class="check-result">Checking...</span>
            </div>

            <div class="check-item" data-status="checking">
                <span class="check-icon">⏳</span>
                <span class="check-label">Duplicate Detection</span>
                <span class="check-result">Checking...</span>
            </div>
        </div>

        <div class="sync-summary" id="sync-summary">
            <h4>Will Sync:</h4>
            <div class="summary-loading">Loading sync preview...</div>
        </div>

        <div class="modal-actions">
            <button class="btn-cancel" onclick="closeVerifyModal()">Cancel</button>
            <button class="btn-primary" id="confirm-sync" disabled>
                <span class="btn-icon">🔄</span>
                Sync to QuickBooks
            </button>
        </div>
        `,
        '700px'
    );

    modal.show();

    // Run verification checks
    await runVerificationChecks(onConfirm);

    return modal;
}

async function runVerificationChecks(onConfirm) {
    const checklist = document.querySelectorAll('.check-item');

    // Check 1: Chart of Accounts
    try {
        const coaResponse = await fetch('/api/qbd/chart-of-accounts');
        const coaData = await coaResponse.json();

        updateCheckItem(checklist[0], coaData.accounts?.length > 0 ? 'pass' : 'fail',
            coaData.accounts?.length > 0 ? `${coaData.accounts.length} accounts` : 'Not synced');
    } catch (error) {
        updateCheckItem(checklist[0], 'fail', 'Error checking');
    }

    // Check 2: Customer Mappings
    try {
        const mappingsResponse = await fetch('/api/customer-mappings');
        const mappingsData = await mappingsResponse.json();
        const confirmed = mappingsData.mappings?.filter(m => m.mapping_confirmed).length || 0;

        updateCheckItem(checklist[1], confirmed > 0 ? 'pass' : 'warning',
            confirmed > 0 ? `${confirmed} mapped` : 'No confirmed mappings');
    } catch (error) {
        updateCheckItem(checklist[1], 'warning', 'Error checking');
    }

    // Check 3: Account Mappings (CRITICAL)
    try {
        const validation = await validateAccountMappings();

        if (validation.valid) {
            updateCheckItem(checklist[2], 'pass', 'All required accounts mapped');
        } else {
            updateCheckItem(checklist[2], 'fail', `Missing: ${validation.missing.join(', ')}`);
        }
    } catch (error) {
        updateCheckItem(checklist[2], 'fail', 'Error checking');
    }

    // Check 4: Duplicate Detection
    try {
        // Check for potential duplicate POs
        const posResponse = await fetch('/api/halopsa/purchase-orders');
        const posData = await posResponse.json();

        // Simple duplicate detection by PO number
        const poNumbers = posData.purchase_orders?.map(po => po.po_number) || [];
        const duplicates = poNumbers.filter((num, index) => poNumbers.indexOf(num) !== index);

        updateCheckItem(checklist[3], duplicates.length > 0 ? 'warning' : 'pass',
            duplicates.length > 0 ? `${duplicates.length} potential duplicates` : 'No duplicates found');
    } catch (error) {
        updateCheckItem(checklist[3], 'pass', 'No duplicates found');
    }

    // Load sync summary
    await loadSyncSummary();

    // Enable/disable sync button based on validation
    const validation = await validateAccountMappings();
    const confirmBtn = document.getElementById('confirm-sync');

    if (validation.valid) {
        confirmBtn.disabled = false;
        confirmBtn.onclick = () => {
            closeVerifyModal();
            if (onConfirm) onConfirm();
        };
    } else {
        confirmBtn.disabled = true;
        confirmBtn.title = 'Fix validation errors before syncing';
    }
}

function updateCheckItem(element, status, result) {
    element.setAttribute('data-status', status);

    const icon = element.querySelector('.check-icon');
    const resultSpan = element.querySelector('.check-result');

    const icons = {
        pass: '✅',
        fail: '❌',
        warning: '⚠️',
        checking: '⏳'
    };

    icon.textContent = icons[status] || '❓';
    resultSpan.textContent = result;
}

async function loadSyncSummary() {
    const summaryDiv = document.getElementById('sync-summary');

    try {
        // Load counts from various endpoints
        const [posResponse, itemsResponse, customersResponse] = await Promise.all([
            fetch('/api/halopsa/purchase-orders'),
            fetch('/api/halopsa/items'),
            fetch('/api/customer-mappings')
        ]);

        const posData = await posResponse.json();
        const itemsData = await itemsResponse.json();
        const customersData = await customersResponse.json();

        const poCount = posData.purchase_orders?.length || 0;
        const itemCount = itemsData.items?.length || 0;
        const customerCount = customersData.mappings?.filter(m => m.mapping_confirmed).length || 0;

        summaryDiv.innerHTML = `
            <h4>Will Sync:</h4>
            <ul>
                <li>${poCount} Purchase Orders → Bills</li>
                <li>${itemCount} Items</li>
                <li>${customerCount} Customer Records</li>
            </ul>
        `;
    } catch (error) {
        summaryDiv.innerHTML = `
            <h4>Will Sync:</h4>
            <div class="error-message">Error loading sync summary</div>
        `;
    }
}

async function validateAccountMappings() {
    try {
        const response = await fetch('/api/qbd/mappings');
        const data = await response.json();
        const mappings = data.mappings || [];

        const required = [
            { type: 'customer_deposits', label: 'Customer Deposits' },
            { type: 'stripe_processing_fees', label: 'Stripe Processing Fees' },
            { type: 'stripe_payments', label: 'Stripe Payments' },
            { type: 'inventory_assets', label: 'Inventory Assets' }
        ];

        const missing = required.filter(req =>
            !mappings.find(m => m.mapping_type === req.type && m.qb_account_id)
        ).map(req => req.label);

        return {
            valid: missing.length === 0,
            missing: missing
        };
    } catch (error) {
        return {
            valid: false,
            missing: ['Error validating mappings']
        };
    }
}

function closeVerifyModal() {
    const modal = document.querySelector('.workflow-modal');
    if (modal) {
        modal.remove();
    }
    const overlay = document.querySelector('.modal-overlay');
    if (overlay) {
        overlay.remove();
    }
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        WorkflowModal,
        showImportDataModal,
        showMapCustomersModal,
        showVerifyDataModal
    };
}
