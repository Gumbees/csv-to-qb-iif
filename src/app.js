// Simple Application JavaScript
console.log('CSV to QuickBooks IIF Sync - Application Starting');

// Simple tab switching functionality
function setupTabSwitching() {
    const tabs = document.querySelectorAll('.tab');
    
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const tabId = tab.dataset.tab;
            
            // Remove active class from all tabs and contents
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            
            // Add active class to selected tab and content
            tab.classList.add('active');
            const tabContent = document.getElementById(tabId);
            if (tabContent) {
                tabContent.classList.add('active');
            }
            
            console.log(`Switched to tab: ${tabId}`);
            
            // Initialize tab-specific functionality
            initializeTabContent(tabId);
        });
    });
    
    console.log('Tab switching initialized');
}

// Initialize tab-specific functionality
function initializeTabContent(tabId) {
    console.log(`Initializing tab: ${tabId}`);
    
    switch(tabId) {
        case 'config':
            document.getElementById('qbConfigForm')?.addEventListener('submit', saveQBConfig);
            break;
        case 'stripe':
            document.getElementById('stripeConfigForm')?.addEventListener('submit', saveStripeConfig);
            document.getElementById('testStripe')?.addEventListener('click', testStripeConnection);
            document.getElementById('importCustomers')?.addEventListener('click', importStripeCustomers);
            document.getElementById('refreshCustomers')?.addEventListener('click', loadImportedCustomers);
            document.getElementById('refreshTransactions')?.addEventListener('click', loadImportedTransactions);
            break;
        case 'mapping':
            initializeCustomerMapping();
            document.getElementById('autoMatch')?.addEventListener('click', autoMatchCustomers);
            break;
        case 'halopsa':
            const halopsaForm = document.getElementById('halopsaConfigForm');
            if (halopsaForm) {
                console.log('Registering HaloPSA form submit handler');
                halopsaForm.addEventListener('submit', saveHaloPSAConfig);
            } else {
                console.warn('HaloPSA config form not found during initialization');
            }
            document.getElementById('testHalopsa')?.addEventListener('click', testHaloPSAConnection);
            document.getElementById('importHalopsaClients')?.addEventListener('click', importHalopsaClients);
            document.getElementById('importHalopsaPurchaseOrders')?.addEventListener('click', importHalopsaPurchaseOrders);
            document.getElementById('importHalopsaInvoices')?.addEventListener('click', importHalopsaInvoices);
            
            // Initialize HaloPSA subtabs
            setupHalopsaSubtabs();
            
            // Automatically load existing HaloPSA data when tab is activated
            setTimeout(() => {
                console.log('Auto-loading HaloPSA data for tab display');
                switchToHalopsaSubtab('invoices'); // Default to invoices tab
            }, 500);
            break;
        case 'customer':
            initializeCustomerView();
            break;
        case 'status':
            loadSystemStatus();
            break;
    }
}

// Application initialization
function initializeApp() {
    console.log('Initializing application...');
    setupTabSwitching();
    initializeTabContent('config');
    loadConfiguration();
    
    // Load HaloPSA data if the tab is active
    setTimeout(() => {
        if (document.getElementById('halopsa-invoices')) {
            loadHalopsaInvoices();
            setupHalopsaInfiniteScroll();
        }
    }, 1000);
    
    console.log('Application initialized successfully');
}

// Wait for DOM to be ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeApp);
} else {
    initializeApp();
}

// Status message utility
function showStatus(message, type = 'info') {
    const statusElement = document.getElementById('statusMessage');
    if (statusElement) {
        statusElement.textContent = message;
        statusElement.className = `status ${type}`;
        statusElement.style.display = 'block';
        setTimeout(() => { statusElement.style.display = 'none'; }, type === 'error' ? 10000 : 5000);
    }
}

// Configuration loading
async function loadConfiguration() {
    try {
        const response = await fetch('/api/config');
        const config = await response.json();
        Object.keys(config).forEach(key => {
            const element = document.getElementById(key);
            if (element) element.value = config[key];
        });
        console.log('Configuration loaded successfully');
    } catch (error) {
        console.error('Error loading configuration:', error);
        showStatus('Error loading configuration: ' + error.message, 'error');
    }
}

// System status functions
async function loadSystemStatus() {
    console.log('Loading system status...');
    
    try {
        const response = await fetch('/api/status');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const status = await response.json();
        console.log('System status data:', status);
        
        // Update status indicators
        updateStatusElement('qbwcStatus', status.qbwc?.status || 'Unknown');
        updateStatusElement('stripeStatus', status.stripe?.status || 'Unknown');
        
        // Fix HaloPSA status - "error" means "not configured"
        const halopsaStatus = status.halopsa?.status === 'error' ? 'Not Configured' : (status.halopsa?.status || 'Unknown');
        updateStatusElement('halopsaStatus', halopsaStatus);
        
        updateStatusElement('dbStatus', status.database?.status || 'Unknown');
        
    } catch (error) {
        console.error('Error loading system status:', error);
        updateStatusElement('qbwcStatus', 'Error loading status', 'error');
        updateStatusElement('stripeStatus', 'Error loading status', 'error');
        updateStatusElement('halopsaStatus', 'Error loading status', 'error');
        updateStatusElement('dbStatus', 'Error loading status', 'error');
    }
}

function updateStatusElement(elementId, status, statusType = null) {
    const element = document.getElementById(elementId);
    if (element) {
        let type = statusType;
        let displayText = status || 'Unknown';
        
        if (!type) {
            if (status === 'healthy') { type = 'success'; displayText = 'Healthy'; }
            else if (status === 'error') { type = 'error'; displayText = 'Error'; }
            else if (status === 'Not Configured') { type = 'info'; displayText = 'Not Configured'; }
            else { type = 'info'; displayText = 'Unknown'; }
        }
        
        element.textContent = displayText;
        element.className = `status ${type}`;
    }
}

// Customer mapping with infinite scroll
let stripeCustomers = [];
let halopsaClients = [];
let stripePage = 1;
let halopsaPage = 1;
const pageSize = 10;

function initializeCustomerMapping() {
    console.log('Initializing customer mapping tool...');
    loadCustomerMappings();
    loadStripeCustomersBatch();
    loadHaloCustomersBatch();
    setupInfiniteScroll();
}

async function loadStripeCustomersBatch() {
    try {
        const response = await fetch('/api/stripe/customers/imported');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const customers = await response.json();
        stripeCustomers = customers;
        
        const container = document.getElementById('stripeCustomers');
        if (container && customers.length > 0) {
            container.innerHTML = customers.slice(0, pageSize).map(customer => `
                <div class="customer-item" onclick="selectStripeCustomer(this)" data-customer-id="${customer.id || customer.stripe_id}">
                    <strong>${customer.name || 'Unnamed Customer'}</strong><br>
                    ${customer.email || 'No email'}<br>
                    <small>Created: ${customer.created ? new Date(customer.created).toLocaleDateString() : 'Unknown'}</small>
                </div>
            `).join('');
        }
    } catch (error) {
        console.error('Error loading Stripe customers:', error);
        showStatus('Error loading Stripe customers', 'error');
    }
}

async function loadHaloCustomersBatch() {
    try {
        const response = await fetch('/api/halopsa/clients');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const clients = await response.json();
        halopsaClients = clients;
        
        const container = document.getElementById('haloCustomers');
        if (container && clients.length > 0) {
            container.innerHTML = clients.slice(0, pageSize).map(client => `
                <div class="customer-item" onclick="selectHaloCustomer(this)" data-client-id="${client.id || client.client_id}">
                    <strong>${client.name || 'Unnamed Client'}</strong><br>
                    ${client.email || 'No email'}<br>
                    <small>ID: ${client.id || 'N/A'}</small>
                </div>
            `).join('');
        }
    } catch (error) {
        console.error('Error loading HaloPSA clients:', error);
        showStatus('Error loading HaloPSA clients', 'error');
    }
}

async function loadCustomerMappings() {
    try {
        const response = await fetch('/api/customers/mappings');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const mappings = await response.json();
        const table = document.getElementById('mappingsTable');
        
        if (table && mappings.length > 0) {
            table.innerHTML = mappings.map(mapping => `
                <tr>
                    <td>${mapping.stripe_customer_name || mapping.stripe_name || 'Unknown'}</td>
                    <td>${mapping.halopsa_client_name || mapping.halo_name || 'Unknown'}</td>
                    <td>${mapping.match_type || 'Manual'}</td>
                    <td>
                        <button class="action-btn warning" onclick="editCustomerMapping('${mapping.halopsa_client_id}')">Edit</button>
                        <button class="action-btn danger" onclick="removeCustomerMapping('${mapping.halopsa_client_id}')">Remove</button>
                    </td>
                </tr>
            `).join('');
        }
    } catch (error) {
        console.error('Error loading customer mappings:', error);
    }
}

function setupInfiniteScroll() {
    const stripeContainer = document.getElementById('stripeCustomers');
    const haloContainer = document.getElementById('haloCustomers');
    
    if (stripeContainer) {
        stripeContainer.addEventListener('scroll', () => {
            if (stripeContainer.scrollTop + stripeContainer.clientHeight >= stripeContainer.scrollHeight - 100) {
                // Load more if needed
            }
        });
    }
    
    if (haloContainer) {
        haloContainer.addEventListener('scroll', () => {
            if (haloContainer.scrollTop + haloContainer.clientHeight >= haloContainer.scrollHeight - 100) {
                // Load more if needed
            }
        });
    }
}

// Customer selection functions
function selectStripeCustomer(element) {
    document.querySelectorAll('#stripeCustomers .customer-item').forEach(item => {
        item.classList.remove('selected');
    });
    element.classList.add('selected');
}

function selectHaloCustomer(element) {
    document.querySelectorAll('#haloCustomers .customer-item').forEach(item => {
        item.classList.remove('selected');
    });
    element.classList.add('selected');
}

// Customer view functions
function initializeCustomerView() {
    console.log('Initializing customer view...');
    
    // Setup event listeners
    document.getElementById('refreshCustomers')?.addEventListener('click', () => loadCustomerList());
    document.getElementById('customerSearch')?.addEventListener('input', handleCustomerSearch);
    document.getElementById('customerFilter')?.addEventListener('change', handleCustomerFilter);
    document.getElementById('backToList')?.addEventListener('click', showCustomerList);
    document.getElementById('refreshCustomerData')?.addEventListener('click', refreshCustomerDetail);
    
    // Setup customer tabs
    document.querySelectorAll('[data-customer-tab]').forEach(tab => {
        tab.addEventListener('click', () => switchCustomerTab(tab.dataset.customerTab));
    });
    
    loadCustomerList();
}

async function loadCustomerList(page = 1) {
    try {
        const response = await fetch(`/api/customers/overview?page=${page}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        const container = document.getElementById('customerListContent');
        const paginationInfo = document.getElementById('customerPaginationInfo');
        
        if (container && data.customers) {
            container.innerHTML = data.customers.map(client => `
                <div class="customer-card" data-client-id="${client.halopsa_id}">
                    <div class="customer-info">
                        <h3>${client.name || 'Unknown Client'}</h3>
                        <div class="customer-meta">
                            <span>Email: ${client.email || 'None'}</span>
                            <span>Phone: ${client.phone || 'None'}</span>
                        </div>
                        <div class="customer-stats-mini">
                            <span class="stat-badge">Stripe Invoices: ${client.stripe_invoice_count || 0}</span>
                            <span class="stat-badge">HaloPSA Txns: ${client.halopsa_transaction_count || 0}</span>
                        </div>
                    </div>
                    <div class="customer-actions">
                        <button class="btn primary" onclick="showCustomerDetail('${client.halopsa_id}')">View Details</button>
                        <span class="mapping-status ${client.stripe_customer_id ? 'mapped' : 'unmapped'}">
                            ${client.stripe_customer_id ? '✓ Mapped' : '✗ Unmapped'}
                        </span>
                    </div>
                </div>
            `).join('');
        }
        
        if (paginationInfo) {
            paginationInfo.textContent = `Page ${page} of ${data.pagination.totalPages || 1} (${data.pagination.total || 0} customers)`;
        }
        
        // Update pagination buttons
        const prevBtn = document.getElementById('prevPage');
        const nextBtn = document.getElementById('nextPage');
        if (prevBtn) prevBtn.disabled = page <= 1;
        if (nextBtn) nextBtn.disabled = page >= (data.pagination.totalPages || 1);
        
        if (prevBtn) prevBtn.onclick = () => loadCustomerList(page - 1);
        if (nextBtn) nextBtn.onclick = () => loadCustomerList(page + 1);
        
    } catch (error) {
        console.error('Error loading customer list:', error);
        showStatus('Error loading customer list', 'error');
    }
}

async function showCustomerDetail(clientId) {
    try {
        const response = await fetch(`/api/customers/view/${clientId}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const result = await response.json();
        
        if (!result.success || !result.customer) {
            throw new Error('Invalid customer data received');
        }
        
        const customer = result.customer.halo_client;
        
        // Show detail view, hide list view
        document.getElementById('customerList').style.display = 'none';
        document.getElementById('customerDetail').style.display = 'block';
        
        // Populate basic customer info
        document.getElementById('customerName').textContent = customer.name || 'Unknown Customer';
        document.getElementById('customerEmail').textContent = customer.email || 'Not provided';
        document.getElementById('customerPhone').textContent = customer.phone || 'Not provided';
        document.getElementById('customerSince').textContent = customer.created_at ? new Date(customer.created_at).toLocaleDateString() : 'Unknown';
        document.getElementById('customerAddress').textContent = customer.address || 'Not provided';
        
        // Update mapping status badge
        const mappingBadge = document.getElementById('customerMappedBadge');
        if (result.customer.stripe_mapping) {
            mappingBadge.style.display = 'inline';
            mappingBadge.textContent = `✓ Mapped to ${result.customer.stripe_mapping.stripe_name || 'Stripe'}`;
        } else {
            mappingBadge.style.display = 'none';
        }
        
        // Populate stats
        document.getElementById('statInvoices').textContent = result.customer.summary?.total_invoices || 0;
        document.getElementById('statTransactions').textContent = result.customer.summary?.total_transactions || 0;
        document.getElementById('statPurchaseOrders').textContent = result.customer.summary?.total_purchase_orders || 0;
        document.getElementById('statItems').textContent = result.customer.summary?.total_items || 0;
        
        // Load tab data
        loadCustomerTabData('overview', { id: clientId });
        
    } catch (error) {
        console.error('Error loading customer detail:', error);
        showStatus('Error loading customer details', 'error');
    }
}

function showCustomerList() {
    document.getElementById('customerList').style.display = 'block';
    document.getElementById('customerDetail').style.display = 'none';
}

function refreshCustomerDetail() {
    const currentTab = document.querySelector('[data-customer-tab].active')?.dataset.customerTab || 'overview';
    const clientId = document.querySelector('.customer-card[data-client-id]')?.dataset.clientId;
    if (clientId) {
        loadCustomerTabData(currentTab, { id: clientId });
        showStatus('Customer data refreshed', 'success');
    }
}

function switchCustomerTab(tabName) {
    // Update active tab
    document.querySelectorAll('[data-customer-tab]').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.customerTab === tabName);
    });
    
    // Show active tab content
    document.querySelectorAll('.customer-tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `customer-${tabName}`);
    });
    
    // Load tab data if needed
    const clientId = document.querySelector('.customer-card[data-client-id]')?.dataset.clientId;
    if (clientId) {
        loadCustomerTabData(tabName, { id: clientId });
    }
}

async function loadCustomerTabData(tabName, customer) {
    try {
        switch(tabName) {
            case 'overview':
                await loadCustomerOverview(customer.id);
                break;
            case 'stripe':
                await loadCustomerStripeData(customer.id);
                break;
            case 'halopsa':
                await loadCustomerHaloPSAData(customer.id);
                break;
            case 'purchase-orders':
                await loadCustomerPurchaseOrders(customer.id);
                break;
            case 'items':
                await loadCustomerServiceItems(customer.id);
                break;
            case 'mapping':
                await loadCustomerMapping(customer.id);
                break;
        }
    } catch (error) {
        console.error(`Error loading ${tabName} data:`, error);
    }
}

async function loadCustomerOverview(clientId) {
    try {
        const response = await fetch(`/api/customers/overview/${clientId}`);
        const data = await response.json();
        
        const activityContainer = document.getElementById('recentActivity');
        const financialContainer = document.getElementById('financialSummary');
        const serviceContainer = document.getElementById('serviceHistory');
        
        // Recent Activity
        if (activityContainer) {
            activityContainer.innerHTML = data.recent_activity?.length ? 
                data.recent_activity.map(activity => `
                    <div class="activity-item">
                        <span class="activity-date">${new Date(activity.date).toLocaleDateString()}</span>
                        <span class="activity-type ${activity.type}">${activity.type}</span>
                        <span class="activity-desc">${activity.description}</span>
                    </div>
                `).join('') : '<p class="no-data">No recent activity</p>';
        }
        
        // Financial Summary
        if (financialContainer) {
            financialContainer.innerHTML = data.financial_summary ? `
                <div class="financial-stats">
                    <div class="financial-item">
                        <label>Total Invoiced:</label>
                        <span>$${data.financial_summary.total_invoiced || 0}</span>
                    </div>
                    <div class="financial-item">
                        <label>Total Paid:</label>
                        <span>$${data.financial_summary.total_paid || 0}</span>
                    </div>
                    <div class="financial-item">
                        <label>Outstanding:</label>
                        <span>$${data.financial_summary.outstanding || 0}</span>
                    </div>
                    <div class="financial-item">
                        <label>Avg. Invoice:</label>
                        <span>$${data.financial_summary.average_invoice || 0}</span>
                    </div>
                </div>
            ` : '<p class="no-data">No financial data available</p>';
        }
        
        // Service History
        if (serviceContainer) {
            serviceContainer.innerHTML = data.service_history?.length ? 
                data.service_history.map(service => `
                    <div class="service-item">
                        <span class="service-date">${new Date(service.date).toLocaleDateString()}</span>
                        <span class="service-type">${service.type}</span>
                        <span class="service-desc">${service.description}</span>
                    </div>
                `).join('') : '<p class="no-data">No service history</p>';
        }
        
    } catch (error) {
        console.error('Error loading customer overview:', error);
    }
}

async function loadCustomerStripeData(clientId) {
    try {
        const response = await fetch(`/api/stripe/customers/${clientId}/transactions`);
        const data = await response.json();
        
        const container = document.getElementById('stripeTransactions');
        const customerInfo = document.getElementById('stripeCustomerInfo');
        
        if (customerInfo) {
            customerInfo.textContent = data.stripe_customer ? 
                `Mapped to: ${data.stripe_customer.name}` : 'Not mapped to Stripe';
        }
        
        if (container) {
            container.innerHTML = data.transactions?.length ? 
                data.transactions.map(tx => `
                    <div class="transaction-card">
                        <div class="transaction-header">
                            <span class="transaction-date">${new Date(tx.created * 1000).toLocaleDateString()}</span>
                            <span class="transaction-amount $${tx.amount > 0 ? 'positive' : 'negative'}">
                                $${Math.abs(tx.amount / 100).toFixed(2)}
                            </span>
                        </div>
                        <div class="transaction-details">
                            <span class="transaction-description">${tx.description || 'Payment'}</span>
                            <span class="transaction-status ${tx.status}">${tx.status}</span>
                        </div>
                    </div>
                `).join('') : '<p class="no-data">No Stripe transactions found</p>';
        }
        
    } catch (error) {
        console.error('Error loading Stripe data:', error);
    }
}

async function loadCustomerHaloPSAData(clientId) {
    try {
        const response = await fetch(`/api/halopsa/clients/${clientId}/transactions`);
        const data = await response.json();
        
        const container = document.getElementById('halopsaTransactions');
        
        if (container) {
            container.innerHTML = data.transactions?.length ? 
                data.transactions.map(tx => `
                    <div class="transaction-card">
                        <div class="transaction-header">
                            <span class="transaction-date">${new Date(tx.date).toLocaleDateString()}</span>
                            <span class="transaction-type">${tx.type}</span>
                            <span class="transaction-amount $${tx.amount > 0 ? 'positive' : 'negative'}">
                                $${Math.abs(tx.amount).toFixed(2)}
                            </span>
                        </div>
                        <div class="transaction-details">
                            <span class="transaction-description">${tx.description}</span>
                            <span class="transaction-reference">Ref: ${tx.reference || 'N/A'}</span>
                        </div>
                    </div>
                `).join('') : '<p class="no-data">No HaloPSA transactions found</p>';
        }
        
    } catch (error) {
        console.error('Error loading HaloPSA data:', error);
    }
}

async function loadCustomerPurchaseOrders(clientId) {
    try {
        const response = await fetch(`/api/halopsa/clients/${clientId}/purchase-orders`);
        const data = await response.json();
        
        const container = document.getElementById('purchaseOrders');
        
        if (container) {
            container.innerHTML = data.purchase_orders?.length ? 
                data.purchase_orders.map(po => `
                    <div class="purchase-order-card">
                        <div class="po-header">
                            <h5>${po.number || 'Unknown PO'}</h5>
                            <span class="po-status ${po.status?.toLowerCase()}">${po.status}</span>
                        </div>
                        <div class="po-details">
                            <span>Date: ${new Date(po.date).toLocaleDateString()}</span>
                            <span>Total: $${po.total || 0}</span>
                            <span>Supplier: ${po.supplier || 'Unknown'}</span>
                        </div>
                        <div class="po-items">
                            ${po.items?.map(item => `
                                <div class="po-item">${item.quantity}x ${item.description} @ $${item.unit_price}</div>
                            `).join('') || ''}
                        </div>
                    </div>
                `).join('') : '<p class="no-data">No purchase orders found</p>';
        }
        
    } catch (error) {
        console.error('Error loading purchase orders:', error);
    }
}

async function loadCustomerServiceItems(clientId) {
    try {
        const response = await fetch(`/api/halopsa/clients/${clientId}/service-items`);
        const data = await response.json();
        
        const container = document.getElementById('serviceItems');
        
        if (container) {
            container.innerHTML = data.service_items?.length ? 
                data.service_items.map(item => `
                    <div class="service-item-card">
                        <div class="item-header">
                            <h5>${item.name || 'Unknown Item'}</h5>
                            <span class="item-status ${item.status?.toLowerCase()}">${item.status}</span>
                        </div>
                        <div class="item-details">
                            <span>Type: ${item.type || 'Unknown'}</span>
                            <span>Cost: $${item.cost || 0}</span>
                            <span>Price: $${item.price || 0}</span>
                        </div>
                        <div class="item-description">
                            ${item.description || 'No description available'}
                        </div>
                    </div>
                `).join('') : '<p class="no-data">No service items found</p>';
        }
        
    } catch (error) {
        console.error('Error loading service items:', error);
    }
}

async function loadCustomerMapping(clientId) {
    try {
        const response = await fetch(`/api/customers/mappings?halopsa_client_id=${clientId}`);
        const data = await response.json();
        
        const container = document.getElementById('mappingSettings');
        
        if (container) {
            if (data.mapping) {
                container.innerHTML = `
                    <div class="mapping-card">
                        <h5>Current Mapping</h5>
                        <div class="mapping-details">
                            <div class="mapping-pair">
                                <div class="mapping-source">
                                    <strong>HaloPSA Client:</strong>
                                    <span>${data.mapping.halopsa_client_name}</span>
                                </div>
                                <div class="mapping-arrow">→</div>
                                <div class="mapping-target">
                                    <strong>Stripe Customer:</strong>
                                    <span>${data.mapping.stripe_customer_name}</span>
                                </div>
                            </div>
                            <div class="mapping-meta">
                                <span>Created: ${new Date(data.mapping.created_at).toLocaleDateString()}</span>
                            </div>
                        </div>
                        <div class="mapping-actions">
                            <button class="btn warning" onclick="unmapCustomer('${data.mapping.stripe_customer_id}')">Unmap</button>
                            <button class="btn primary" onclick="editCustomerMapping('${clientId}')">Edit</button>
                        </div>
                    </div>
                `;
            } else {
                container.innerHTML = `
                    <div class="mapping-card unmapped">
                        <h5>No Mapping Found</h5>
                        <p>This HaloPSA client is not mapped to any Stripe customer.</p>
                        <div class="mapping-actions">
                            <button class="btn primary" onclick="createCustomerMapping('${clientId}')">Create Mapping</button>
                            <button class="btn secondary" onclick="autoMatchCustomer('${clientId}')">Auto-Match</button>
                        </div>
                    </div>
                `;
            }
        }
        
    } catch (error) {
        console.error('Error loading customer mapping:', error);
    }
}

function handleCustomerSearch(event) {
    const searchTerm = event.target.value.toLowerCase();
    const customerCards = document.querySelectorAll('.customer-card');
    
    customerCards.forEach(card => {
        const customerName = card.querySelector('h3').textContent.toLowerCase();
        const customerEmail = card.querySelector('.customer-meta span:first-child').textContent.toLowerCase();
        const customerId = card.dataset.clientId;
        
        const matches = customerName.includes(searchTerm) || 
                       customerEmail.includes(searchTerm) || 
                       customerId.includes(searchTerm);
        
        card.style.display = matches ? 'block' : 'none';
    });
}

function handleCustomerFilter(event) {
    const filterValue = event.target.value;
    const customerCards = document.querySelectorAll('.customer-card');
    
    customerCards.forEach(card => {
        const mappingStatus = card.querySelector('.mapping-status').classList.contains('mapped');
        const statsText = card.querySelector('.customer-stats-mini').textContent;
        const hasInvoices = statsText.includes('Invoices:') && parseInt(statsText.match(/Invoices: (\d+)/)?.[1]) > 0;
        const hasOrders = statsText.includes('Orders:') && parseInt(statsText.match(/Orders: (\d+)/)?.[1]) > 0;
        
        let shouldShow = true;
        
        switch(filterValue) {
            case 'mapped':
                shouldShow = mappingStatus;
                break;
            case 'unmapped':
                shouldShow = !mappingStatus;
                break;
            case 'with-invoices':
                shouldShow = hasInvoices;
                break;
            case 'with-orders':
                shouldShow = hasOrders;
                break;
            case 'all':
            default:
                shouldShow = true;
        }
        
        card.style.display = shouldShow ? 'block' : 'none';
    });
}

// Placeholder functions for mapping actions
function unmapCustomer(stripeCustomerId) { 
    showStatus('Unmapping functionality coming soon', 'info'); 
}
function editCustomerMapping(clientId) { 
    showStatus('Edit mapping functionality coming soon', 'info'); 
}
function createCustomerMapping(clientId) { 
    showStatus('Create mapping functionality coming soon', 'info'); 
}
function autoMatchCustomer(clientId) { 
    showStatus('Auto-match functionality coming soon', 'info'); 
}
async function saveQBConfig(e) { 
    e.preventDefault(); 
    try {
        const formData = new FormData(document.getElementById('qbConfigForm'));
        const config = Object.fromEntries(formData.entries());
        
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        await response.json();
        showStatus('QuickBooks config saved', 'success');
    } catch (error) {
        console.error('Error saving QB config:', error);
        showStatus('Error saving QB config', 'error');
    }
}
async function saveStripeConfig(e) { 
    e.preventDefault(); 
    try {
        const formData = new FormData(document.getElementById('stripeConfigForm'));
        const config = Object.fromEntries(formData.entries());
        
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        await response.json();
        showStatus('Stripe config saved', 'success');
    } catch (error) {
        console.error('Error saving Stripe config:', error);
        showStatus('Error saving Stripe config', 'error');
    }
}
async function saveHaloPSAConfig(e) { 
    e.preventDefault(); 
    try {
        console.log('Saving HaloPSA configuration...');
        const form = document.getElementById('halopsaConfigForm');
        if (!form) {
            console.error('HaloPSA config form not found');
            showStatus('Error: Config form not found', 'error');
            return;
        }
        
        const formData = new FormData(form);
        const config = Object.fromEntries(formData.entries());
        console.log('Form data:', config);
        
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        
        console.log('Response status:', response.status);
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        
        const result = await response.json();
        console.log('Save result:', result);
        
        if (result.success) {
            showStatus('HaloPSA config saved successfully', 'success');
        } else {
            showStatus('HaloPSA config save failed: ' + (result.message || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('Error saving HaloPSA config:', error);
        showStatus('Error saving HaloPSA config', 'error');
    }
}
async function testStripeConnection() { 
    try {
        showStatus('Testing Stripe connection...', 'info');
        const response = await fetch('/api/stripe/test');
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        const result = await response.json();
        
        if (result.success) {
            showStatus('Stripe connection successful', 'success');
        } else {
            showStatus("Stripe test failed: " + result.message, 'error');
        }
    } catch (error) {
        console.error('Error testing Stripe connection:', error);
        showStatus('Error testing Stripe connection', 'error');
    }
}
async function testHaloPSAConnection() { 
    try {
        showStatus('Testing HaloPSA connection...', 'info');
        const response = await fetch('/api/halopsa/test');
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        const result = await response.json();
        
        if (result.success) {
            showStatus('HaloPSA connection successful', 'success');
        } else {
            showStatus("HaloPSA test failed: " + result.message, 'error');
        }
    } catch (error) {
        console.error('Error testing HaloPSA connection:', error);
        showStatus('Error testing HaloPSA connection', 'error');
    }
}
async function importStripeCustomers() { showStatus('Importing customers', 'info'); }
async function loadImportedCustomers() { showStatus('Loading customers', 'info'); }
async function loadImportedTransactions() { showStatus('Loading transactions', 'info'); }
async function autoMatchCustomers() { showStatus('Auto-matching customers', 'info'); }
function editCustomerMapping(id) { showStatus('Edit mapping coming soon', 'info'); }
function removeCustomerMapping(id) { showStatus('Remove mapping coming soon', 'info'); }

// HaloPSA Import Functions
async function importHalopsaClients() {
    try {
        showStatus('Importing HaloPSA clients...', 'info');
        const progress = document.getElementById('halopsaClientProgress');
        const progressText = document.getElementById('halopsaClientProgressText');
        if (progress) progress.style.display = 'block';
        if (progressText) progressText.textContent = 'Starting client import...';
        
        const response = await fetch('/api/halopsa/import/clients', { method: 'POST' });
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        const result = await response.json();
        
        if (result.success) {
            showStatus(`HaloPSA client import successful: ${result.message}`, 'success');
            if (progressText) progressText.textContent = 'Import completed successfully';
        } else {
            showStatus(`HaloPSA client import failed: ${result.message}`, 'error');
            if (progressText) progressText.textContent = 'Import failed';
        }
    } catch (error) {
        console.error('Error importing HaloPSA clients:', error);
        showStatus('Error importing HaloPSA clients', 'error');
    } finally {
        // Hide progress after a delay
        setTimeout(() => {
            const progress = document.getElementById('halopsaClientProgress');
            if (progress) progress.style.display = 'none';
        }, 3000);
    }
}

async function importHalopsaPurchaseOrders() {
    try {
        showStatus('Importing HaloPSA purchase orders...', 'info');
        const progress = document.getElementById('halopsaPurchaseOrderProgress');
        const progressText = document.getElementById('halopsaPurchaseOrderProgressText');
        if (progress) progress.style.display = 'block';
        if (progressText) progressText.textContent = 'Starting purchase order import...';
        
        const response = await fetch('/api/halopsa/import/purchase-orders', { method: 'POST' });
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        const result = await response.json();
        
        if (result.success) {
            showStatus(`HaloPSA purchase order import successful: ${result.message}`, 'success');
            if (progressText) progressText.textContent = 'Import completed successfully';
        } else {
            showStatus(`HaloPSA purchase order import failed: ${result.message}`, 'error');
            if (progressText) progressText.textContent = 'Import failed';
        }
    } catch (error) {
        console.error('Error importing HaloPSA purchase orders:', error);
        showStatus('Error importing HaloPSA purchase orders', 'error');
    } finally {
        // Hide progress after a delay
        setTimeout(() => {
            const progress = document.getElementById('halopsaPurchaseOrderProgress');
            if (progress) progress.style.display = 'none';
        }, 3000);
    }
}

async function importHalopsaInvoices() {
    try {
        showStatus('Importing HaloPSA invoices...', 'info');
        const progress = document.getElementById('halopsaInvoiceProgress');
        const progressText = document.getElementById('halopsaInvoiceProgressText');
        if (progress) progress.style.display = 'block';
        if (progressText) progressText.textContent = 'Starting invoice import...';
        
        const response = await fetch('/api/halopsa/import/invoices', { method: 'POST' });
        
        if (!response.ok) throw new Error("HTTP " + response.status);
        const result = await response.json();
        
        if (result.success) {
            showStatus(`HaloPSA invoice import successful: ${result.message}`, 'success');
            if (progressText) progressText.textContent = 'Import completed successfully';
            
            // Refresh the invoices table
            loadHalopsaInvoices();
            setupHalopsaInfiniteScroll();
        } else {
            showStatus(`HaloPSA invoice import failed: ${result.message}`, 'error');
            if (progressText) progressText.textContent = 'Import failed';
        }
    } catch (error) {
        console.error('Error importing HaloPSA invoices:', error);
        showStatus('Error importing HaloPSA invoices', 'error');
    } finally {
        // Hide progress after a delay
        setTimeout(() => {
            const progress = document.getElementById('halopsaInvoiceProgress');
            if (progress) progress.style.display = 'none';
        }, 3000);
    }
}

// HaloPSA Data Loading Functions
// HaloPSA Data Loading Functions - Infinite Scrolling
let currentHalopsaPage = 1;
let isLoadingHalopsa = false;
let hasMoreHalopsa = true;
let currentHalopsaSearch = '';

async function loadHalopsaInvoices(append = false) {
    if (isLoadingHalopsa || (!append && !hasMoreHalopsa)) return;
    
    isLoadingHalopsa = true;
    
    try {
        const loadingElement = document.getElementById('halopsaLoading');
        if (loadingElement) {
            loadingElement.style.display = 'block';
        }
        
        const params = new URLSearchParams({
            page: currentHalopsaPage,
            limit: 50,
            search: currentHalopsaSearch
        });
        
        const url = `/api/halopsa/invoices?${params}`;
        console.log('Fetching HaloPSA invoices from:', url);
        
        // Test if this endpoint exists by trying a simple version first
        const testUrl = `/api/halopsa/invoices?limit=3`;
        console.log('Testing endpoint:', testUrl);
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('HaloPSA API response received:', { 
            hasInvoices: !!data.invoices, 
            isArray: Array.isArray(data),
            totalRecords: data.pagination?.total,
            dataKeys: Object.keys(data)
        });
        
        // Handle both new paginated response and old flat array response
        let invoices = [];
        let pagination = { total: 0, pages: 1 };
        
        if (data.invoices && Array.isArray(data.invoices)) {
            // New paginated response format
            invoices = data.invoices;
            pagination = data.pagination || { total: invoices.length, pages: 1 };
        } else if (Array.isArray(data)) {
            // Old flat array format (fallback)
            invoices = data;
            pagination = { total: invoices.length, pages: 1 };
        } else {
            console.error('Unexpected response format:', data);
            throw new Error('Invalid response format from server');
        }
        
        const table = document.getElementById('halopsaInvoicesTable');
        if (table) {
            if (!append) {
                table.innerHTML = '';
            }
            
            // Check if we actually have invoices to display
            if (invoices.length === 0 && !append) {
                table.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 20px;">No invoices found</td></tr>';
            } else if (invoices.length > 0) {
            table.innerHTML += invoices.map(invoice => `
                <tr data-invoice-id="${invoice.id}">
                        <td>${invoice.invoice_number || 'N/A'}</td>
                        <td>${invoice.client_name || 'Unknown Client'}</td>
                        <td>${invoice.invoice_date ? new Date(invoice.invoice_date).toLocaleDateString() : 'N/A'}</td>
                        <td>$${(invoice.total_amount || 0).toFixed(2)}</td>
                        <td><span class="status-badge ${invoice.status?.toLowerCase() || 'unknown'}">${invoice.status || 'Unknown'}</span></td>
                        <td class="mapping-status">${invoice.stripe_transaction_id ? 'Mapped' : 'Not Mapped'}</td>
                        <td>
                            <button class="action-btn" onclick="viewHalopsaInvoice(${invoice.id})">View</button>
                            <button class="action-btn ${invoice.stripe_transaction_id ? 'danger' : 'success'}" 
                                    onclick="toggleMapping(${invoice.id})">
                                ${invoice.stripe_transaction_id ? 'Unmap' : 'Map'}
                            </button>
                        </td>
                    </tr>
                `).join('');
            }
            
            // Update pagination info
            const currentCount = document.getElementById('halopsaCurrentCount');
            const totalCount = document.getElementById('halopsaTotalCount');
            if (currentCount && totalCount) {
                const displayedCount = append ? (currentHalopsaPage - 1) * 50 + invoices.length : invoices.length;
                currentCount.textContent = displayedCount;
                totalCount.textContent = pagination.total || invoices.length;
            }
        }
        
        // Check if there are more pages
        hasMoreHalopsa = currentHalopsaPage < (pagination.pages || 1);
        
    } catch (error) {
        console.error('Error loading HaloPSA invoices:', error);
        showStatus('Error loading invoices: ' + error.message, 'error');
    } finally {
        isLoadingHalopsa = false;
        const loadingElement = document.getElementById('halopsaLoading');
        if (loadingElement) {
            loadingElement.style.display = 'none';
        }
    }
}

// Infinite scroll handler
function setupHalopsaInfiniteScroll() {
    const tableContainer = document.querySelector('#halopsa-invoices .table-container');
    if (tableContainer) {
        tableContainer.addEventListener('scroll', () => {
            const { scrollTop, scrollHeight, clientHeight } = tableContainer;
            if (scrollHeight - scrollTop <= clientHeight + 100 && hasMoreHalopsa && !isLoadingHalopsa) {
                currentHalopsaPage++;
                loadHalopsaInvoices(true);
            }
        });
    }
}

// Search functionality
function searchHalopsaInvoices() {
    const searchInput = document.getElementById('halopsaSearch');
    if (searchInput) {
        currentHalopsaSearch = searchInput.value.trim();
        currentHalopsaPage = 1;
        hasMoreHalopsa = true;
        loadHalopsaInvoices(false);
    }
}

function clearHalopsaSearch() {
    const searchInput = document.getElementById('halopsaSearch');
    if (searchInput) {
        searchInput.value = '';
        currentHalopsaSearch = '';
        currentHalopsaPage = 1;
        hasMoreHalopsa = true;
        loadHalopsaInvoices(false);
    }
}

// HaloPSA Subtabs functionality
function setupHalopsaSubtabs() {
    const subtabButtons = document.querySelectorAll('[data-halopsa-tab]');
    subtabButtons.forEach(button => {
        button.addEventListener('click', (e) => {
            e.preventDefault();
            const tabId = button.getAttribute('data-halopsa-tab');
            switchToHalopsaSubtab(tabId);
        });
    });
}

function switchToHalopsaSubtab(tabId) {
    console.log('Switching to HaloPSA subtab:', tabId);
    
    // Hide all subtab contents
    const subtabContents = document.querySelectorAll('.halopsa-tab-content');
    subtabContents.forEach(content => {
        content.style.display = 'none';
    });
    
    // Show the selected subtab content
    const activeContent = document.getElementById(`halopsa-${tabId}`);
    if (activeContent) {
        activeContent.style.display = 'block';
    }
    
    // Update active subtab button styling
    const subtabButtons = document.querySelectorAll('[data-halopsa-tab]');
    subtabButtons.forEach(button => {
        button.classList.remove('active');
        if (button.getAttribute('data-halopsa-tab') === tabId) {
            button.classList.add('active');
        }
    });
    
    // Load data for the selected subtab
    switch (tabId) {
        case 'clients':
            loadHalopsaClients();
            break;
        case 'purchase-orders':
            loadHalopsaPurchaseOrders();
            break;
        case 'invoices':
            loadHalopsaInvoices();
            setupHalopsaInfiniteScroll();
            break;
    }
}

// Placeholder functions for clients and purchase orders (to be implemented)
function loadHalopsaClients() {
    console.log('Loading HaloPSA clients...');
    // TODO: Implement clients loading
    const table = document.getElementById('halopsaClientsTable');
    if (table) {
        table.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px;">Clients data loading not yet implemented</td></tr>';
    }
}

function loadHalopsaPurchaseOrders() {
    console.log('Loading HaloPSA purchase orders...');
    // TODO: Implement purchase orders loading
    const table = document.getElementById('halopsaPurchaseOrdersTable');
    if (table) {
        table.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px;">Purchase orders data loading not yet implemented</td></tr>';
    }
}

// Helper functions for HaloPSA UI
function viewHalopsaInvoice(id) {
    showStatus('Invoice view functionality coming soon', 'info');
}

// Mapping functionality
let currentMappingInvoice = null;
let selectedStripeTransaction = null;

function toggleMapping(invoiceId) {
    console.log('Toggle mapping for invoice:', invoiceId);
    
    // Get the current invoice details to see if it's already mapped
    const invoiceRow = document.querySelector(`tr[data-invoice-id="${invoiceId}"]`);
    if (!invoiceRow) {
        showStatus('Invoice not found in current view', 'error');
        return;
    }
    
    const isMapped = invoiceRow.querySelector('.mapping-status').textContent === 'Mapped';
    
    if (isMapped) {
        // Unmap invoice
        if (confirm('Are you sure you want to unmap this invoice from its Stripe payment?')) {
            unmapInvoice(invoiceId);
        }
    } else {
        // Map invoice - open mapping modal
        openMappingModal(invoiceId);
    }
}

function openMappingModal(invoiceId) {
    currentMappingInvoice = invoiceId;
    selectedStripeTransaction = null;
    
    // Load invoice details
    loadInvoiceDetails(invoiceId);
    
    // Load available stripe transactions
    loadStripeTransactions(invoiceId);
    
    // Show modal
    document.getElementById('mappingModal').style.display = 'block';
}

function closeMappingModal() {
    document.getElementById('mappingModal').style.display = 'none';
    currentMappingInvoice = null;
    selectedStripeTransaction = null;
}

async function loadInvoiceDetails(invoiceId) {
    try {
        const response = await fetch(`/api/halopsa/invoice/${invoiceId}`);
        if (!response.ok) throw new Error('Failed to fetch invoice details');
        
        const invoice = await response.json();
        
        const detailsDiv = document.getElementById('invoiceDetails');
        detailsDiv.innerHTML = `
            <div><strong>Invoice #:</strong> ${invoice.invoice_number || 'N/A'}</div>
            <div><strong>Client:</strong> ${invoice.client_name || 'Unknown'}</div>
            <div><strong>Date:</strong> ${invoice.invoice_date ? new Date(invoice.invoice_date).toLocaleDateString() : 'N/A'}</div>
            <div><strong>Total Amount:</strong> $${(invoice.total_amount || 0).toFixed(2)}</div>
            <div><strong>Status:</strong> ${invoice.status || 'Unknown'}</div>
            ${invoice.stripe_transaction_id ? `<div><strong>Currently Mapped to:</strong> ${invoice.stripe_transaction_id}</div>` : ''}
        `;
    } catch (error) {
        console.error('Error loading invoice details:', error);
        showStatus('Error loading invoice details', 'error');
    }
}

async function loadStripeTransactions(invoiceId) {
    try {
        // First get the invoice details to use for matching
        const invoiceResponse = await fetch(`/api/halopsa/invoice/${invoiceId}`);
        if (!invoiceResponse.ok) throw new Error('Failed to fetch invoice details');
        const invoice = await invoiceResponse.json();
        
        // Get stripe transactions with matching suggestions
        const response = await fetch(`/api/stripe/transactions?invoiceAmount=${invoice.total_amount}&clientName=${encodeURIComponent(invoice.client_name || '')}`);
        if (!response.ok) throw new Error('Failed to fetch Stripe transactions');
        
        const transactions = await response.json();
        
        const transactionsList = document.getElementById('stripeTransactionsList');
        if (transactions.length === 0) {
            transactionsList.innerHTML = '<tr><td colspan="7" style="text-align: center;">No Stripe transactions found</td></tr>';
            return;
        }
        
        transactionsList.innerHTML = transactions.map(transaction => {
            const matchScore = calculateMatchScore(invoice, transaction);
            const isSelected = selectedStripeTransaction === transaction.stripe_id;
            
            return `
                <tr class="${isSelected ? 'selected' : ''}">
                    <td>
                        <input type="radio" name="stripeTransaction" value="${transaction.stripe_id}" 
                               ${isSelected ? 'checked' : ''} 
                               onchange="selectStripeTransaction('${transaction.stripe_id}')">
                    </td>
                    <td>${transaction.stripe_id || 'N/A'}</td>
                    <td>$${(transaction.amount / 100).toFixed(2)}</td>
                    <td>${transaction.customer_name || transaction.customer_email || 'Unknown'}</td>
                    <td>${transaction.created ? new Date(transaction.created * 1000).toLocaleDateString() : 'N/A'}</td>
                    <td>${transaction.description ? transaction.description.substring(0, 50) : 'N/A'}</td>
                    <td><span class="match-score ${matchScore > 80 ? 'high' : matchScore > 60 ? 'medium' : 'low'}">${matchScore}%</span></td>
                </tr>
            `;
        }).join('');
        
    } catch (error) {
        console.error('Error loading Stripe transactions:', error);
        showStatus('Error loading Stripe transactions', 'error');
    }
}

function selectStripeTransaction(stripeId) {
    selectedStripeTransaction = stripeId;
    
    // Update UI to show selection
    document.querySelectorAll('#stripeTransactionsList tr').forEach(row => {
        row.classList.remove('selected');
    });
    
    const selectedRow = document.querySelector(`tr input[value="${stripeId}"]`)?.closest('tr');
    if (selectedRow) {
        selectedRow.classList.add('selected');
    }
    
    // Enable confirm button
    document.getElementById('confirmMapping').disabled = false;
}

function calculateMatchScore(invoice, transaction) {
    let score = 0;
    
    // Amount match (40% weight)
    const invoiceAmount = parseFloat(invoice.total_amount) || 0;
    const transactionAmount = parseFloat(transaction.amount) / 100 || 0;
    const amountDiff = Math.abs(invoiceAmount - transactionAmount);
    const amountMatch = Math.max(0, 100 - (amountDiff / Math.max(invoiceAmount, transactionAmount)) * 100);
    score += amountMatch * 0.4;
    
    // Date match (30% weight) - if dates are close
    if (invoice.invoice_date && transaction.created) {
        const invoiceDate = new Date(invoice.invoice_date);
        const transactionDate = new Date(transaction.created * 1000);
        const dateDiff = Math.abs(invoiceDate - transactionDate) / (1000 * 60 * 60 * 24); // Difference in days
        const dateMatch = Math.max(0, 100 - dateDiff);
        score += dateMatch * 0.3;
    } else {
        score += 30; // Partial credit if dates missing
    }
    
    // Client name match (30% weight) - check if any customer mappings exist
    // This would need customer mapping data to be more accurate
    
    return Math.min(100, Math.round(score));
}

async function confirmStripeMapping() {
    if (!currentMappingInvoice || !selectedStripeTransaction) {
        showStatus('Please select a Stripe transaction to map', 'error');
        return;
    }
    
    try {
        const response = await fetch('/api/map-invoice-to-stripe', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                invoiceId: currentMappingInvoice,
                stripeTransactionId: selectedStripeTransaction
            })
        });
        
        if (!response.ok) throw new Error('Failed to map invoice to Stripe');
        
        const result = await response.json();
        
        if (result.success) {
            showStatus('Invoice successfully mapped to Stripe transaction', 'success');
            closeMappingModal();
            
            // Refresh the invoices table
            loadHalopsaInvoices();
        } else {
            showStatus('Mapping failed: ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error mapping invoice to Stripe:', error);
        showStatus('Error mapping invoice to Stripe', 'error');
    }
}

async function unmapInvoice(invoiceId) {
    try {
        const response = await fetch('/api/unmap-invoice', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ invoiceId })
        });
        
        if (!response.ok) throw new Error('Failed to unmap invoice');
        
        const result = await response.json();
        
        if (result.success) {
            showStatus('Invoice successfully unmapped', 'success');
            
            // Refresh the invoices table
            loadHalopsaInvoices();
        } else {
            showStatus('Unmapping failed: ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error unmapping invoice:', error);
        showStatus('Error unmapping invoice', 'error');
    }
}

function searchStripeTransactions() {
    const searchTerm = document.getElementById('stripeSearch').value;
    // Implement search functionality
    console.log('Searching Stripe transactions for:', searchTerm);
    // This would need additional API endpoints
}

// Close modal when clicking X
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('mappingModal');
    const closeBtn = modal?.querySelector('.close');
    
    if (closeBtn) {
        closeBtn.addEventListener('click', closeMappingModal);
    }
    
    // Close modal when clicking outside
    window.addEventListener('click', function(event) {
        if (event.target === modal) {
            closeMappingModal();
        }
    });
});

console.log('Application JavaScript loaded');
