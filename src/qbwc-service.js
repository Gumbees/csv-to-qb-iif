const { Builder, parseString } = require('xml2js');
const dayjs = require('dayjs');
const DatabaseManager = require('./database');

class QBWCService {
    constructor() {
        this.sessions = new Map();
        this.qbxmlBuilder = new Builder({
            // IMPORTANT: No XML declaration for QBXML sent via QBWC
            // QBXML should start with <QBXML>, not <?xml?>
            xmldec: null,  // Suppress XML declaration
            headless: false,  // Keep root element
            renderOpts: { pretty: false }  // No formatting
        });
        this.database = null;

        // Persistent sync request queue (survives session reconnects)
        this.pendingSyncRequests = [];

        // Initialize database connection
        try {
            this.database = new DatabaseManager();
        } catch (error) {
            console.error('Failed to initialize database for QBWC:', error);
        }
    }

    /**
     * Add a sync request to the persistent queue
     * @param {string} type - Type of sync: 'accounts', 'customers', 'items'
     * @param {number} priority - Priority level (lower = higher priority)
     */
    queueSyncRequest(type, priority = 100) {
        // Remove any existing request of this type
        this.pendingSyncRequests = this.pendingSyncRequests.filter(req => req.type !== type);

        // Add new request
        this.pendingSyncRequests.push({ type, priority, timestamp: Date.now() });

        // Sort by priority (lower priority value = higher priority)
        this.pendingSyncRequests.sort((a, b) => a.priority - b.priority);

        console.log(`QBWC: Queued sync request for '${type}' (priority: ${priority}). Queue:`,
            this.pendingSyncRequests.map(r => r.type));
    }

    /**
     * Get the next pending sync request
     * @returns {Object|null} - Next sync request or null if queue is empty
     */
    getNextSyncRequest() {
        return this.pendingSyncRequests.shift();
    }

    /**
     * Check if there are pending sync requests
     * @returns {boolean}
     */
    hasPendingSyncRequests() {
        return this.pendingSyncRequests.length > 0;
    }

    /**
     * Clear all pending sync requests
     */
    clearSyncQueue() {
        this.pendingSyncRequests = [];
        console.log('QBWC: Sync queue cleared');
    }

    // Generate a proper GUID format with UPPERCASE HEX chars and curly braces for QuickBooks compatibility
    generateSimpleGUID() {
        const guid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16).toUpperCase();
        });
        return `{${guid}}`;
    }

    // Generate .qwc configuration file for QuickBooks Web Connector
    generateQWCFile(config) {
        // Use Simple GUID format without dashes for QuickBooks compatibility
        // QuickBooks sometimes has issues with standard GUID format
        const ownerID = this.generateSimpleGUID();
        const fileID = this.generateSimpleGUID();
        
        const qwcConfig = {
            QBWCXML: {
                AppName: config.appName || 'CSV to QuickBooks IIF Sync',
                AppID: '',
                AppURL: config.appUrl || 'http://localhost:3000/qbwc',
                AppDescription: config.description || 'Sync CSV purchase orders with QuickBooks',
                AppSupport: config.supportUrl || 'http://localhost:3000/support',
                UserName: config.username || 'qbwc_user',
                OwnerID: ownerID,
                FileID: fileID,
                QBType: 'QBFS',
                Style: 'Document',
                Scheduler: {
                    RunEveryNMinutes: config.interval || 60
                },
                AppSupport: {
                    IsReadOnly: false,
                    CertificateURL: config.appUrl.replace('/qbwc', '/ssl-certificate.crt')
                },
                IsReadOnly: false
            }
        };

        return this.qbxmlBuilder.buildObject(qwcConfig);
    }

    // Generate GUID in QuickBooks format: {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX} in uppercase
    generateGUID() {
        // QuickBooks REQUIRES GUIDs with braces in uppercase
        const guid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16).toUpperCase(); // MUST be uppercase
        });
        return `{${guid}}`; // MUST be wrapped in braces
    }

    // Generate GUID for QWC file - QuickBooks requires specific GUID format
    generateGUID() {
        // Use static GUIDs for better QuickBooks compatibility
        const staticGUIDs = [
            '90A44FB5-33D9-4815-AC85-AC62A5F2CB6C',
            '57F3B9B6-5013-42E8-8FEF-B77E1946E6B7'
        ];
        
        return staticGUIDs[Math.floor(Math.random() * staticGUIDs.length)];
    }

    // Generate GUID for QWC file - QuickBooks requires specific GUID format
    generateGUID() {
        // Use static GUIDs for better QuickBooks compatibility
        // QuickBooks can be sensitive to GUID format - these are properly formatted
        const staticGUIDs = [
            '90A44FB5-33D9-4815-AC85-AC62A5F2CB6C',
            '57F3B9B6-5013-42E8-8FEF-B77E1946E6B7',
            'EF92D2EE-45B2-490A-A5EB-4FD7A39B3C76' // Previous GUID that failed
        ];
        
        // Return a static GUID (not random) for consistency
        return staticGUIDs[Math.floor(Math.random() * staticGUIDs.length)];
    }

    // Generate QBXML for purchase orders
    generatePurchaseOrderQBXML(purchaseOrders) {
        const billRequests = purchaseOrders.map(po => ({
            BillAddRq: {
                BillAdd: {
                    VendorRef: {
                        FullName: this.sanitizeQBXML(po.vendor)
                    },
                    APAccountRef: {
                        FullName: 'Accounts Payable'
                    },
                    TxnDate: this.formatQBDate(po.date),
                    RefNumber: this.sanitizeQBXML(po.ref_num),
                    DueDate: this.formatQBDate(po.due_date || po.date),
                    Memo: `HaloPSA Purchase Order - ${po.ref_num}`,
                    BillLineAdd: po.lines.map(line => ({
                        ItemRef: {
                            FullName: this.sanitizeQBXML(line.item)
                        },
                        Desc: this.sanitizeQBXML(line.description || line.item),
                        Quantity: line.quantity,
                        Cost: line.unit_cost,
                        Amount: line.line_amount,
                        AccountRef: {
                            FullName: 'Inventory Asset'
                        }
                    }))
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'stopOnError' }
                }
            }
        };

        // Add all bill requests
        qbxml.QBXML.QBXMLMsgsRq.BillAddRq = billRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    /**
     * Enhanced: Generate QBXML for Purchase Orders → QuickBooks Bills (Unpaid)
     * Integrates with account mappings for proper GL account assignment
     * ONLY generates QBXML for POs that haven't been synced yet (synced_to_qb = FALSE)
     * @param {Array} purchaseOrders - Purchase order records from database
     * @param {Object} accountMappings - Account mappings from account_mappings table
     * @returns {String} - Generated QBXML
     */
    generatePurchaseOrderBillsQBXML(purchaseOrders, accountMappings = {}) {
        if (!purchaseOrders || purchaseOrders.length === 0) {
            return null;
        }

        const billRequests = [];

        for (const po of purchaseOrders) {
            // Skip POs that have already been synced to QuickBooks
            if (po.synced_to_qb === true || po.synced_to_qb === 1) {
                console.log(`Skipping PO ${po.po_number}: already synced to QB (TxnID: ${po.qb_txn_id})`);
                continue;
            }
            // Parse line items from JSON if stored as string
            let lineItems = [];
            if (typeof po.line_items === 'string') {
                try {
                    lineItems = JSON.parse(po.line_items);
                } catch (e) {
                    console.error(`Failed to parse line_items for PO ${po.po_number}:`, e);
                    continue;
                }
            } else if (Array.isArray(po.line_items)) {
                lineItems = po.line_items;
            } else {
                console.warn(`No line items found for PO ${po.po_number}`);
                continue;
            }

            // Skip if no line items
            if (lineItems.length === 0) {
                console.warn(`PO ${po.po_number} has no line items, skipping`);
                continue;
            }

            // Get accounts payable account from mappings
            const apAccount = accountMappings.accounts_payable || 'Accounts Payable';
            const inventoryAssetAccount = accountMappings.inventory_asset || 'Inventory Asset';

            // Calculate due date from payment terms if available
            let dueDate = po.due_date || po.expected_delivery || po.po_date;

            const billAdd = {
                VendorRef: {
                    FullName: this.sanitizeQBXML(po.vendor_name || 'Unknown Vendor')
                },
                APAccountRef: {
                    FullName: apAccount
                },
                TxnDate: this.formatQBDate(po.po_date),
                RefNumber: this.sanitizeQBXML(po.po_number),
                DueDate: this.formatQBDate(dueDate),
                Memo: `HaloPSA PO ${po.po_number}${po.client_name ? ' - ' + this.sanitizeQBXML(po.client_name) : ''}`,
                IsPaid: false, // Mark as unpaid
                ItemLineAdd: []
            };

            // Add each line item
            for (const item of lineItems) {
                const lineAdd = {
                    ItemRef: {
                        FullName: this.sanitizeQBXML(item.item_name || item.name || item.description || 'Misc Item')
                    },
                    Desc: this.sanitizeQBXML(item.description || item.item_name || ''),
                    Quantity: this.safeFloat(item.quantity || 1),
                    Cost: this.safeFloat(item.unit_cost || item.cost || 0),
                    Amount: this.safeFloat(item.total || item.amount || (item.quantity * item.unit_cost) || 0)
                };

                // Add account reference for inventory items
                if (item.is_inventory || item.item_type === 'inventory') {
                    lineAdd.AccountRef = {
                        FullName: inventoryAssetAccount
                    };
                }

                billAdd.ItemLineAdd.push(lineAdd);
            }

            billRequests.push({
                BillAddRq: {
                    BillAdd: billAdd
                }
            });
        }

        if (billRequests.length === 0) {
            console.warn('No valid bills generated from purchase orders');
            return null;
        }

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        // Add all bill requests
        Object.assign(qbxml.QBXML.QBXMLMsgsRq, ...billRequests);

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate QBXML for inventory items
    generateInventoryQBXML(purchaseOrders) {
        // Extract unique inventory items
        const inventoryMap = new Map();

        purchaseOrders.forEach(po => {
            po.lines.forEach(line => {
                if (!inventoryMap.has(line.item)) {
                    inventoryMap.set(line.item, {
                        name: line.item,
                        description: line.description || line.item,
                        cost: line.unit_cost,
                        salesPrice: Math.round(line.unit_cost * 1.5 * 100) / 100, // 50% markup
                        quantityOnHand: 0
                    });
                }
            });
        });

        const itemRequests = Array.from(inventoryMap.values()).map(item => ({
            ItemInventoryAddRq: {
                ItemInventoryAdd: {
                    Name: this.sanitizeQBXML(item.name),
                    SalesDesc: this.sanitizeQBXML(item.description),
                    SalesPrice: item.salesPrice,
                    PurchaseDesc: this.sanitizeQBXML(item.description),
                    PurchaseCost: item.cost,
                    COGSAccountRef: {
                        FullName: 'Cost of Goods Sold'
                    },
                    IncomeAccountRef: {
                        FullName: 'Sales'
                    },
                    AssetAccountRef: {
                        FullName: 'Inventory Asset'
                    }
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.ItemInventoryAddRq = itemRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate QBXML for service items from database
    generateServiceItemsQBXML(items) {
        const serviceItems = items.filter(item => item.item_type === 'ItemService');

        if (serviceItems.length === 0) {
            return null;
        }

        const itemRequests = serviceItems.map(item => ({
            ItemServiceAddRq: {
                ItemServiceAdd: {
                    Name: this.sanitizeQBXML(item.name),
                    SalesOrPurchase: {
                        Desc: this.sanitizeQBXML(item.sales_description || item.description),
                        Price: item.sales_price || 0,
                        AccountRef: {
                            FullName: item.income_account || 'Sales Income'
                        }
                    }
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.ItemServiceAddRq = itemRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate QBXML for non-inventory items from database
    generateNonInventoryItemsQBXML(items) {
        const nonInvItems = items.filter(item => item.item_type === 'ItemNonInventory');

        if (nonInvItems.length === 0) {
            return null;
        }

        const itemRequests = nonInvItems.map(item => ({
            ItemNonInventoryAddRq: {
                ItemNonInventoryAdd: {
                    Name: this.sanitizeQBXML(item.name),
                    SalesOrPurchase: {
                        Desc: this.sanitizeQBXML(item.sales_description || item.description),
                        Price: item.sales_price || 0,
                        AccountRef: {
                            FullName: item.income_account || 'Sales Income'
                        }
                    }
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.ItemNonInventoryAddRq = itemRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate QBXML for inventory items from database
    generateInventoryItemsQBXML(items) {
        const invItems = items.filter(item => item.item_type === 'ItemInventory');

        if (invItems.length === 0) {
            return null;
        }

        const itemRequests = invItems.map(item => ({
            ItemInventoryAddRq: {
                ItemInventoryAdd: {
                    Name: this.sanitizeQBXML(item.name),
                    SalesDesc: this.sanitizeQBXML(item.sales_description || item.description),
                    SalesPrice: item.sales_price || 0,
                    PurchaseDesc: this.sanitizeQBXML(item.purchase_description || item.description),
                    PurchaseCost: item.purchase_cost || 0,
                    COGSAccountRef: {
                        FullName: item.cogs_account || 'Cost of Goods Sold'
                    },
                    IncomeAccountRef: {
                        FullName: item.income_account || 'Sales Income'
                    },
                    AssetAccountRef: {
                        FullName: item.asset_account || 'Inventory Asset'
                    }
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.ItemInventoryAddRq = itemRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    /**
     * Enhanced: Generate QBXML for Items with Account Mapping Integration
     * Supports inventory, service, and non-inventory items
     * Integrates with account_mappings table for proper GL accounts
     * @param {Array} items - Item records from qb_items table
     * @param {Object} accountMappings - Account mappings from account_mappings table
     * @returns {String} - Generated QBXML
     */
    generateItemsWithMappingsQBXML(items, accountMappings = {}) {
        if (!items || items.length === 0) {
            return null;
        }

        const requests = [];

        // Default account names from mappings
        const inventoryAssetAccount = accountMappings.inventory_asset || 'Inventory Asset';
        const cogsAccount = accountMappings.cost_of_goods || 'Cost of Goods Sold';
        const incomeAccount = accountMappings.halopsa_invoice_income || 'Sales Income';
        const expenseAccount = accountMappings.purchase_order_expense || 'Expenses';

        // Group by type
        const serviceItems = items.filter(item => item.item_type === 'ItemService');
        const nonInvItems = items.filter(item => item.item_type === 'ItemNonInventory');
        const invItems = items.filter(item => item.item_type === 'ItemInventory');

        // Add service items
        serviceItems.forEach(item => {
            requests.push({
                ItemServiceAddRq: {
                    ItemServiceAdd: {
                        Name: this.sanitizeQBXML(item.name),
                        IsActive: item.is_active !== false,
                        SalesOrPurchase: {
                            Desc: this.sanitizeQBXML(item.sales_description || item.description || ''),
                            Price: this.safeFloat(item.sales_price || 0),
                            AccountRef: {
                                FullName: item.income_account || incomeAccount
                            }
                        }
                    }
                }
            });
        });

        // Add non-inventory items
        nonInvItems.forEach(item => {
            requests.push({
                ItemNonInventoryAddRq: {
                    ItemNonInventoryAdd: {
                        Name: this.sanitizeQBXML(item.name),
                        IsActive: item.is_active !== false,
                        SalesOrPurchase: {
                            Desc: this.sanitizeQBXML(item.sales_description || item.description || ''),
                            Price: this.safeFloat(item.sales_price || 0),
                            AccountRef: {
                                FullName: item.income_account || incomeAccount
                            }
                        }
                    }
                }
            });
        });

        // Add inventory items
        invItems.forEach(item => {
            const inventoryAdd = {
                Name: this.sanitizeQBXML(item.name),
                IsActive: item.is_active !== false,
                SalesDesc: this.sanitizeQBXML(item.sales_description || item.description || ''),
                SalesPrice: this.safeFloat(item.sales_price || 0),
                IncomeAccountRef: {
                    FullName: item.income_account || incomeAccount
                },
                PurchaseDesc: this.sanitizeQBXML(item.purchase_description || item.description || ''),
                PurchaseCost: this.safeFloat(item.purchase_cost || 0),
                COGSAccountRef: {
                    FullName: item.cogs_account || cogsAccount
                },
                AssetAccountRef: {
                    FullName: item.asset_account || inventoryAssetAccount
                }
            };

            // Add quantity on hand if specified
            if (item.quantity_on_hand !== undefined && item.quantity_on_hand !== null) {
                inventoryAdd.QuantityOnHand = this.safeFloat(item.quantity_on_hand);
            }

            requests.push({
                ItemInventoryAddRq: {
                    ItemInventoryAdd: inventoryAdd
                }
            });
        });

        if (requests.length === 0) {
            return null;
        }

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        // Add all requests as a flat structure
        Object.assign(qbxml.QBXML.QBXMLMsgsRq, ...requests);

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate combined QBXML for all item types
    generateAllItemsQBXML(items) {
        if (!items || items.length === 0) {
            return null;
        }

        const requests = [];

        // Group by type
        const serviceItems = items.filter(item => item.item_type === 'ItemService');
        const nonInvItems = items.filter(item => item.item_type === 'ItemNonInventory');
        const invItems = items.filter(item => item.item_type === 'ItemInventory');

        // Add service items
        serviceItems.forEach(item => {
            requests.push({
                ItemServiceAddRq: {
                    ItemServiceAdd: {
                        Name: this.sanitizeQBXML(item.name),
                        SalesOrPurchase: {
                            Desc: this.sanitizeQBXML(item.sales_description || item.description),
                            Price: item.sales_price || 0,
                            AccountRef: {
                                FullName: item.income_account || 'Sales Income'
                            }
                        }
                    }
                }
            });
        });

        // Add non-inventory items
        nonInvItems.forEach(item => {
            requests.push({
                ItemNonInventoryAddRq: {
                    ItemNonInventoryAdd: {
                        Name: this.sanitizeQBXML(item.name),
                        SalesOrPurchase: {
                            Desc: this.sanitizeQBXML(item.sales_description || item.description),
                            Price: item.sales_price || 0,
                            AccountRef: {
                                FullName: item.income_account || 'Sales Income'
                            }
                        }
                    }
                }
            });
        });

        // Add inventory items
        invItems.forEach(item => {
            requests.push({
                ItemInventoryAddRq: {
                    ItemInventoryAdd: {
                        Name: this.sanitizeQBXML(item.name),
                        SalesDesc: this.sanitizeQBXML(item.sales_description || item.description),
                        SalesPrice: item.sales_price || 0,
                        PurchaseDesc: this.sanitizeQBXML(item.purchase_description || item.description),
                        PurchaseCost: item.purchase_cost || 0,
                        COGSAccountRef: {
                            FullName: item.cogs_account || 'Cost of Goods Sold'
                        },
                        IncomeAccountRef: {
                            FullName: item.income_account || 'Sales Income'
                        },
                        AssetAccountRef: {
                            FullName: item.asset_account || 'Inventory Asset'
                        }
                    }
                }
            });
        });

        if (requests.length === 0) {
            return null;
        }

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        // Add all requests as a flat array
        Object.assign(qbxml.QBXML.QBXMLMsgsRq, ...requests);

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Generate vendor QBXML
    generateVendorQBXML(purchaseOrders) {
        const vendorMap = new Map();
        
        purchaseOrders.forEach(po => {
            if (!vendorMap.has(po.vendor)) {
                vendorMap.set(po.vendor, {
                    name: po.vendor,
                    isActive: true
                });
            }
        });

        const vendorRequests = Array.from(vendorMap.values()).map(vendor => ({
            VendorAddRq: {
                VendorAdd: {
                    Name: this.sanitizeQBXML(vendor.name),
                    IsActive: true
                }
            }
        }));

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'stopOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.VendorAddRq = vendorRequests;

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Parse QBXML response
    async parseQBXMLResponse(xmlResponse) {
        return new Promise((resolve, reject) => {
            parseString(xmlResponse, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    // Helper: Safely parse integer (returns 0 if NaN)
    safeInt(value) {
        if (value === null || value === undefined) return 0;
        const num = parseInt(value);
        return isNaN(num) ? 0 : num;
    }

    // Helper: Safely parse float (returns 0.0 if NaN)
    safeFloat(value) {
        if (value === null || value === undefined) return 0.0;
        const num = parseFloat(value);
        return isNaN(num) ? 0.0 : num;
    }

    // Format date for QBXML (YYYY-MM-DD)
    formatQBDate(dateString) {
        if (!dateString) return dayjs().format('YYYY-MM-DD');
        const date = dayjs(dateString);
        return date.isValid() ? date.format('YYYY-MM-DD') : dayjs().format('YYYY-MM-DD');
    }

    // Sanitize strings for QBXML
    sanitizeQBXML(text) {
        if (!text) return '';
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;')
            .substring(0, 100); // QB field length limit
    }

    // Session management for QBWC
    createSession(ticket) {
        const session = {
            ticket: ticket,
            created: new Date(),
            lastActivity: new Date(),
            companyFile: '',
            currentRequest: null,
            pendingRequests: []
        };
        
        this.sessions.set(ticket, session);
        return session;
    }

    getSession(ticket) {
        const session = this.sessions.get(ticket);
        if (session) {
            session.lastActivity = new Date();
        }
        return session;
    }

    cleanupSessions() {
        const now = new Date();
        const cutoff = new Date(now.getTime() - 30 * 60 * 1000); // 30 minutes
        
        for (const [ticket, session] of this.sessions.entries()) {
            if (session.lastActivity < cutoff) {
                this.sessions.delete(ticket);
            }
        }
    }

    // Authentication for QBWC
    authenticate(username, password) {
        console.log('QBWC Service authentication called:', { username, password: password ? '***' : 'not provided' });

        // Get credentials from database config
        let validUsername = 'qbwc_user'; // fallback default
        let validPassword = 'password123'; // fallback default

        if (this.database && this.database.db) {
            try {
                const usernameConfig = this.database.db.prepare('SELECT value FROM config WHERE key = ?').get('qbwc_username');
                const passwordConfig = this.database.db.prepare('SELECT value FROM config WHERE key = ?').get('qbwc_password');

                if (usernameConfig && usernameConfig.value) {
                    validUsername = usernameConfig.value;
                }
                if (passwordConfig && passwordConfig.value) {
                    validPassword = passwordConfig.value;
                }

                console.log('QBWC: Using credentials from config:', { username: validUsername, password: '***' });
            } catch (error) {
                console.error('QBWC: Error reading credentials from config, using defaults:', error);
            }
        } else {
            console.warn('QBWC: Database not available, using default credentials');
        }

        if (username === validUsername && password === validPassword) {
            const ticket = Math.random().toString(36).substring(7);
            this.createSession(ticket);
            console.log('QBWC Authentication SUCCESS:', { username, ticket });
            // Return ticket string only for success
            return { ticket, errorCode: '' };
        }

        console.log('QBWC Authentication FAILED: Invalid credentials for user:', username);
        // Return empty ticket and error code for failure
        return { ticket: '', errorCode: 'nvu' }; // nvu = non-valid username
    }

    // Helper method to get configuration values safely
    getConfigValue(key) {
        if (!this.database) {
            console.error('Database not initialized for QBWC configuration');
            // Return hardcoded defaults if database not available
            const defaults = {
                'qbwc_username': 'qbwc_user',
                'qbwc_password': 'password123',
                'qbwc_app_name': 'CSV to QuickBooks IIF Sync',
                'qbwc_sync_interval': '30'
            };
            return defaults[key] || null;
        }
        
        try {
            // Use the database method properly - it's async
            return new Promise((resolve) => {
                this.database.getConfig(null, (err, config) => {
                    if (err || !config) {
                        console.error('Error getting config value:', err);
                        resolve(null);
                    } else if (config[key]) {
                        resolve(config[key].value);
                    } else {
                        resolve(null);
                    }
                });
            });
        } catch (error) {
            console.error('Error getting config value:', error);
            return null;
        }
    }

    // Generate status report
    generateSyncReport(purchaseOrders, qbResponse) {
        const report = {
            timestamp: new Date().toISOString(),
            totalOrders: purchaseOrders.length,
            totalLineItems: purchaseOrders.reduce((sum, po) => sum + po.lines.length, 0),
            uniqueVendors: new Set(purchaseOrders.map(po => po.vendor)).size,
            uniqueItems: new Set(purchaseOrders.flatMap(po => po.lines.map(line => line.item))).size,
            totalAmount: purchaseOrders.reduce((sum, po) => sum + po.total_amount, 0),
            qbResponse: qbResponse || null,
            status: qbResponse ? 'completed' : 'pending'
        };

        return report;
    }

    // Generate QBXML for Chart of Accounts query
    generateAccountQueryQBXML() {
        const qbxml = {
            QBXML: {
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' },
                    AccountQueryRq: {
                        // Query all accounts - no filters for comprehensive list
                        // MaxReturned: 1000, // Optional: limit results if needed
                        IncludeRetElement: [
                            'ListID',
                            'TimeCreated',
                            'TimeModified',
                            'EditSequence',
                            'Name',
                            'FullName',
                            'IsActive',
                            'ParentRef',
                            'Sublevel',
                            'AccountType',
                            'SpecialAccountType',
                            'AccountNumber',
                            'BankNumber',
                            'Desc',
                            'Balance',
                            'TotalBalance'
                        ]
                    }
                }
            }
        };

        let xml = this.qbxmlBuilder.buildObject(qbxml);

        // CRITICAL: Remove XML declaration and add qbxml processing instruction
        // QuickBooks requires: <?qbxml version="13.0"?><QBXML>...
        xml = xml.replace(/<\?xml[^?]*\?>\s*/g, '');
        xml = '<?qbxml version="13.0"?>' + xml;

        return xml;
    }

    // Parse AccountQueryRs response and store in database
    async parseAccountQueryResponse(responseXml) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            const accountQueryRs = msgsRs.AccountQueryRs;

            if (!accountQueryRs || accountQueryRs.length === 0) {
                console.log('No AccountQueryRs found in response');
                return { success: false, count: 0 };
            }

            // Check for errors
            const statusCode = accountQueryRs[0].$?.statusCode;
            const statusMessage = accountQueryRs[0].$?.statusMessage;

            if (statusCode && statusCode !== '0') {
                console.error('AccountQueryRs error:', statusCode, statusMessage);
                return { success: false, error: statusMessage, statusCode };
            }

            const accountRets = accountQueryRs[0].AccountRet;
            if (!accountRets || accountRets.length === 0) {
                console.log('No accounts returned in response');
                return { success: true, count: 0 };
            }

            console.log(`Parsing ${accountRets.length} accounts from QuickBooks`);

            // Store accounts in database
            const accounts = [];
            for (const acct of accountRets) {
                const account = {
                    qb_list_id: acct.ListID?.[0] || null,
                    qb_edit_sequence: acct.EditSequence?.[0] || null,
                    account_name: acct.Name?.[0] || '',
                    fully_qualified_name: acct.FullName?.[0] || '',
                    account_type: acct.AccountType?.[0] || '',
                    account_number: acct.AccountNumber?.[0] || null,
                    description: acct.Desc?.[0] || null,
                    is_active: acct.IsActive?.[0] === 'true' ? 1 : 0,
                    balance: this.safeFloat(acct.Balance?.[0]),
                    special_account_type: acct.SpecialAccountType?.[0] || null,
                    bank_number: acct.BankNumber?.[0] || null,
                    parent_ref_list_id: acct.ParentRef?.[0]?.ListID?.[0] || null,
                    parent_ref_full_name: acct.ParentRef?.[0]?.FullName?.[0] || null,
                    sublevel: this.safeInt(acct.Sublevel?.[0]),
                    raw_qbxml: JSON.stringify(acct)
                };
                accounts.push(account);
            }

            // Insert or update accounts in database
            if (this.database) {
                const stmt = this.database.db.prepare(`
                    INSERT OR REPLACE INTO qb_accounts (
                        qb_list_id, qb_edit_sequence, account_name, fully_qualified_name,
                        account_type, account_number, description, is_active, balance,
                        special_account_type, bank_number, parent_ref_list_id,
                        parent_ref_full_name, sublevel, raw_qbxml, last_sync
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                `);

                for (const account of accounts) {
                    stmt.run([
                        account.qb_list_id,
                        account.qb_edit_sequence,
                        account.account_name,
                        account.fully_qualified_name,
                        account.account_type,
                        account.account_number,
                        account.description,
                        account.is_active,
                        account.balance,
                        account.special_account_type,
                        account.bank_number,
                        account.parent_ref_list_id,
                        account.parent_ref_full_name,
                        account.sublevel,
                        account.raw_qbxml
                    ]);
                }

                console.log(`Stored ${accounts.length} accounts in database`);
            }

            return { success: true, count: accounts.length };
        } catch (error) {
            console.error('Error parsing AccountQueryRs:', error);
            return { success: false, error: error.message };
        }
    }

    // Generate QBXML for Customer query
    generateCustomerQueryQBXML() {
        const qbxml = {
            QBXML: {
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' },
                    CustomerQueryRq: {
                        // Query all active customers
                        ActiveStatus: 'All', // Get both active and inactive
                        IncludeRetElement: [
                            'ListID',
                            'TimeCreated',
                            'TimeModified',
                            'EditSequence',
                            'Name',
                            'FullName',
                            'IsActive',
                            'CompanyName',
                            'Salutation',
                            'FirstName',
                            'MiddleName',
                            'LastName',
                            'BillAddress',
                            'ShipAddress',
                            'Phone',
                            'AltPhone',
                            'Fax',
                            'Email',
                            'Contact',
                            'AltContact',
                            'CustomerTypeRef',
                            'TermsRef',
                            'SalesRepRef',
                            'Balance',
                            'TotalBalance',
                            'SalesTaxCodeRef',
                            'ItemSalesTaxRef',
                            'ResaleNumber',
                            'AccountNumber',
                            'CreditLimit',
                            'CreditCardInfo',
                            'JobStatus',
                            'JobStartDate',
                            'JobEndDate',
                            'JobDesc',
                            'JobTypeRef',
                            'Notes',
                            'PreferredPaymentMethodRef',
                            'PriceLevelRef'
                        ]
                    }
                }
            }
        };

        let xml = this.qbxmlBuilder.buildObject(qbxml);

        // CRITICAL: Remove XML declaration and add qbxml processing instruction
        // QuickBooks requires: <?qbxml version="13.0"?><QBXML>...
        xml = xml.replace(/<\?xml[^?]*\?>\s*/g, '');
        xml = '<?qbxml version="13.0"?>' + xml;

        return xml;
    }

    // Parse CustomerQueryRs response and store in database
    async parseCustomerQueryResponse(responseXml) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            const customerQueryRs = msgsRs.CustomerQueryRs;

            if (!customerQueryRs || customerQueryRs.length === 0) {
                console.log('No CustomerQueryRs found in response');
                return { success: false, count: 0 };
            }

            // Check for errors
            const statusCode = customerQueryRs[0].$?.statusCode;
            const statusMessage = customerQueryRs[0].$?.statusMessage;

            if (statusCode && statusCode !== '0') {
                console.error('CustomerQueryRs error:', statusCode, statusMessage);
                return { success: false, error: statusMessage, statusCode };
            }

            const customerRets = customerQueryRs[0].CustomerRet;
            if (!customerRets || customerRets.length === 0) {
                console.log('No customers returned in response');
                return { success: true, count: 0 };
            }

            console.log(`Parsing ${customerRets.length} customers from QuickBooks`);

            // Store customers in database
            const customers = [];
            for (const cust of customerRets) {
                const billAddr = cust.BillAddress?.[0] || {};
                const shipAddr = cust.ShipAddress?.[0] || {};

                const customer = {
                    qb_list_id: cust.ListID?.[0] || null,
                    qb_edit_sequence: cust.EditSequence?.[0] || null,
                    qb_full_name: cust.FullName?.[0] || '',
                    company_name: cust.CompanyName?.[0] || null,
                    first_name: cust.FirstName?.[0] || null,
                    last_name: cust.LastName?.[0] || null,
                    email: cust.Email?.[0] || null,
                    phone: cust.Phone?.[0] || null,
                    fax: cust.Fax?.[0] || null,
                    billing_address_line1: billAddr.Addr1?.[0] || null,
                    billing_address_line2: billAddr.Addr2?.[0] || null,
                    billing_address_city: billAddr.City?.[0] || null,
                    billing_address_state: billAddr.State?.[0] || null,
                    billing_address_postal_code: billAddr.PostalCode?.[0] || null,
                    billing_address_country: billAddr.Country?.[0] || null,
                    shipping_address_line1: shipAddr.Addr1?.[0] || null,
                    shipping_address_line2: shipAddr.Addr2?.[0] || null,
                    shipping_address_city: shipAddr.City?.[0] || null,
                    shipping_address_state: shipAddr.State?.[0] || null,
                    shipping_address_postal_code: shipAddr.PostalCode?.[0] || null,
                    shipping_address_country: shipAddr.Country?.[0] || null,
                    contact_name: cust.Contact?.[0] || null,
                    account_number: cust.AccountNumber?.[0] || null,
                    is_active: cust.IsActive?.[0] !== 'false' ? 1 : 0,
                    balance: this.safeFloat(cust.Balance?.[0]),
                    total_balance: this.safeFloat(cust.TotalBalance?.[0]),
                    sales_tax_code: cust.SalesTaxCodeRef?.[0]?.FullName?.[0] || null,
                    payment_terms: cust.TermsRef?.[0]?.FullName?.[0] || null,
                    credit_limit: this.safeFloat(cust.CreditLimit?.[0]),
                    notes: cust.Notes?.[0] || null,
                    raw_qbxml: JSON.stringify(cust)
                };
                customers.push(customer);
            }

            // Insert or update customers in database
            if (this.database) {
                const stmt = this.database.db.prepare(`
                    INSERT OR REPLACE INTO qb_customers (
                        qb_list_id, qb_edit_sequence, qb_full_name, company_name,
                        first_name, last_name, email, phone, fax,
                        billing_address_line1, billing_address_line2, billing_address_city,
                        billing_address_state, billing_address_postal_code, billing_address_country,
                        shipping_address_line1, shipping_address_line2, shipping_address_city,
                        shipping_address_state, shipping_address_postal_code, shipping_address_country,
                        contact_name, account_number, is_active, balance, total_balance,
                        sales_tax_code, payment_terms, credit_limit, notes, raw_qbxml, last_sync
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                `);

                for (const customer of customers) {
                    stmt.run([
                        customer.qb_list_id,
                        customer.qb_edit_sequence,
                        customer.qb_full_name,
                        customer.company_name,
                        customer.first_name,
                        customer.last_name,
                        customer.email,
                        customer.phone,
                        customer.fax,
                        customer.billing_address_line1,
                        customer.billing_address_line2,
                        customer.billing_address_city,
                        customer.billing_address_state,
                        customer.billing_address_postal_code,
                        customer.billing_address_country,
                        customer.shipping_address_line1,
                        customer.shipping_address_line2,
                        customer.shipping_address_city,
                        customer.shipping_address_state,
                        customer.shipping_address_postal_code,
                        customer.shipping_address_country,
                        customer.contact_name,
                        customer.account_number,
                        customer.is_active,
                        customer.balance,
                        customer.total_balance,
                        customer.sales_tax_code,
                        customer.payment_terms,
                        customer.credit_limit,
                        customer.notes,
                        customer.raw_qbxml
                    ]);
                }

                console.log(`Stored ${customers.length} customers in database`);
            }

            return { success: true, count: customers.length };
        } catch (error) {
            console.error('Error parsing CustomerQueryRs:', error);
            return { success: false, error: error.message };
        }
    }

    // Generate QBXML for Item query (all item types)
    generateItemQueryQBXML() {
        const qbxml = {
            QBXML: {
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' },
                    ItemQueryRq: {
                        // Query all item types
                        ActiveStatus: 'All', // Get both active and inactive
                        IncludeRetElement: [
                            'ListID',
                            'TimeCreated',
                            'TimeModified',
                            'EditSequence',
                            'Name',
                            'FullName',
                            'IsActive',
                            'Sublevel',
                            'Type',
                            'SalesDesc',
                            'SalesPrice',
                            'PurchaseDesc',
                            'PurchaseCost',
                            'QuantityOnHand',
                            'AverageCost',
                            'QuantityOnOrder',
                            'QuantityOnSalesOrder',
                            'IncomeAccountRef',
                            'COGSAccountRef',
                            'AssetAccountRef',
                            'ExpenseAccountRef',
                            'SalesTaxCodeRef'
                        ]
                    }
                }
            }
        };

        let xml = this.qbxmlBuilder.buildObject(qbxml);

        // CRITICAL: Remove XML declaration and add qbxml processing instruction
        // QuickBooks requires: <?qbxml version="13.0"?><QBXML>...
        xml = xml.replace(/<\?xml[^?]*\?>\s*/g, '');
        xml = '<?qbxml version="13.0"?>' + xml;

        return xml;
    }

    // Parse ItemQueryRs response and store in database
    async parseItemQueryResponse(responseXml) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            const itemQueryRs = msgsRs.ItemQueryRs;

            if (!itemQueryRs || itemQueryRs.length === 0) {
                console.log('No ItemQueryRs found in response');
                return { success: false, count: 0 };
            }

            // Check for errors
            const statusCode = itemQueryRs[0].$?.statusCode;
            const statusMessage = itemQueryRs[0].$?.statusMessage;

            if (statusCode && statusCode !== '0') {
                console.error('ItemQueryRs error:', statusCode, statusMessage);
                return { success: false, error: statusMessage, statusCode };
            }

            // Parse different item types (ItemServiceRet, ItemInventoryRet, ItemNonInventoryRet, etc.)
            const items = [];
            const itemRet = itemQueryRs[0].ItemServiceRet || [];
            const invRet = itemQueryRs[0].ItemInventoryRet || [];
            const nonInvRet = itemQueryRs[0].ItemNonInventoryRet || [];
            const otherItemRet = itemQueryRs[0].ItemOtherChargeRet || [];

            console.log(`Found ${itemRet.length} service items, ${invRet.length} inventory items, ${nonInvRet.length} non-inventory items`);

            // Parse Service Items
            for (const item of itemRet) {
                const salesOrPurchase = item.SalesOrPurchase?.[0] || item.SalesAndPurchase?.[0] || {};
                items.push({
                    qb_list_id: item.ListID?.[0] || null,
                    qb_edit_sequence: item.EditSequence?.[0] || null,
                    item_type: 'ItemService',
                    name: item.Name?.[0] || '',
                    full_name: item.FullName?.[0] || '',
                    is_active: item.IsActive?.[0] !== 'false' ? 1 : 0,
                    description: salesOrPurchase.Desc?.[0] || '',
                    sales_description: salesOrPurchase.Desc?.[0] || '',
                    sales_price: this.safeFloat(salesOrPurchase.Price?.[0]),
                    income_account: salesOrPurchase.AccountRef?.[0]?.FullName?.[0] || null,
                    quantity_on_hand: 0,
                    raw_qbxml: JSON.stringify(item)
                });
            }

            // Parse Inventory Items
            for (const item of invRet) {
                items.push({
                    qb_list_id: item.ListID?.[0] || null,
                    qb_edit_sequence: item.EditSequence?.[0] || null,
                    item_type: 'ItemInventory',
                    name: item.Name?.[0] || '',
                    full_name: item.FullName?.[0] || '',
                    is_active: item.IsActive?.[0] !== 'false' ? 1 : 0,
                    description: item.SalesDesc?.[0] || '',
                    sales_description: item.SalesDesc?.[0] || '',
                    purchase_description: item.PurchaseDesc?.[0] || '',
                    sales_price: this.safeFloat(item.SalesPrice?.[0]),
                    purchase_cost: this.safeFloat(item.PurchaseCost?.[0]),
                    quantity_on_hand: this.safeFloat(item.QuantityOnHand?.[0]),
                    income_account: item.IncomeAccountRef?.[0]?.FullName?.[0] || null,
                    cogs_account: item.COGSAccountRef?.[0]?.FullName?.[0] || null,
                    asset_account: item.AssetAccountRef?.[0]?.FullName?.[0] || null,
                    raw_qbxml: JSON.stringify(item)
                });
            }

            // Parse Non-Inventory Items
            for (const item of nonInvRet) {
                const salesOrPurchase = item.SalesOrPurchase?.[0] || item.SalesAndPurchase?.[0] || {};
                items.push({
                    qb_list_id: item.ListID?.[0] || null,
                    qb_edit_sequence: item.EditSequence?.[0] || null,
                    item_type: 'ItemNonInventory',
                    name: item.Name?.[0] || '',
                    full_name: item.FullName?.[0] || '',
                    is_active: item.IsActive?.[0] !== 'false' ? 1 : 0,
                    description: salesOrPurchase.Desc?.[0] || '',
                    sales_description: salesOrPurchase.Desc?.[0] || '',
                    sales_price: this.safeFloat(salesOrPurchase.Price?.[0]),
                    income_account: salesOrPurchase.AccountRef?.[0]?.FullName?.[0] || null,
                    expense_account: salesOrPurchase.AccountRef?.[0]?.FullName?.[0] || null,
                    quantity_on_hand: 0,
                    raw_qbxml: JSON.stringify(item)
                });
            }

            console.log(`Parsed ${items.length} total items from QuickBooks`);

            // Store items in database
            if (this.database && items.length > 0) {
                const stmt = this.database.db.prepare(`
                    INSERT OR REPLACE INTO qb_items (
                        qb_list_id, qb_edit_sequence, item_type, name, full_name,
                        description, sales_description, purchase_description,
                        sales_price, purchase_cost, quantity_on_hand,
                        income_account, cogs_account, asset_account, expense_account,
                        is_active, synced_to_qb, raw_qbxml, last_sync
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
                `);

                for (const item of items) {
                    stmt.run([
                        item.qb_list_id,
                        item.qb_edit_sequence,
                        item.item_type,
                        item.name,
                        item.full_name,
                        item.description,
                        item.sales_description,
                        item.purchase_description || null,
                        item.sales_price,
                        item.purchase_cost || null,
                        item.quantity_on_hand,
                        item.income_account,
                        item.cogs_account || null,
                        item.asset_account || null,
                        item.expense_account || null,
                        item.is_active,
                        item.raw_qbxml
                    ]);
                }

                console.log(`Stored ${items.length} items in database`);
            }

            return { success: true, count: items.length };
        } catch (error) {
            console.error('Error parsing ItemQueryRs:', error);
            return { success: false, error: error.message };
        }
    }

    // Generate QBXML for updating existing items (ItemMod)
    generateItemModQBXML(items) {
        if (!items || items.length === 0) {
            return null;
        }

        const requests = [];

        // Group by type and generate appropriate Mod requests
        for (const item of items) {
            if (!item.qb_list_id || !item.qb_edit_sequence) {
                console.warn(`Skipping item ${item.name}: missing QB ListID or EditSequence`);
                continue;
            }

            if (item.item_type === 'ItemService') {
                requests.push({
                    ItemServiceModRq: {
                        ItemServiceMod: {
                            ListID: item.qb_list_id,
                            EditSequence: item.qb_edit_sequence,
                            Name: this.sanitizeQBXML(item.name),
                            IsActive: item.is_active !== false,
                            SalesOrPurchase: {
                                Desc: this.sanitizeQBXML(item.sales_description || item.description),
                                Price: item.sales_price || 0,
                                AccountRef: {
                                    FullName: item.income_account || 'Sales Income'
                                }
                            }
                        }
                    }
                });
            } else if (item.item_type === 'ItemInventory') {
                requests.push({
                    ItemInventoryModRq: {
                        ItemInventoryMod: {
                            ListID: item.qb_list_id,
                            EditSequence: item.qb_edit_sequence,
                            Name: this.sanitizeQBXML(item.name),
                            IsActive: item.is_active !== false,
                            SalesDesc: this.sanitizeQBXML(item.sales_description || item.description),
                            SalesPrice: item.sales_price || 0,
                            PurchaseDesc: this.sanitizeQBXML(item.purchase_description || item.description),
                            PurchaseCost: item.purchase_cost || 0
                        }
                    }
                });
            } else if (item.item_type === 'ItemNonInventory') {
                requests.push({
                    ItemNonInventoryModRq: {
                        ItemNonInventoryMod: {
                            ListID: item.qb_list_id,
                            EditSequence: item.qb_edit_sequence,
                            Name: this.sanitizeQBXML(item.name),
                            IsActive: item.is_active !== false,
                            SalesOrPurchase: {
                                Desc: this.sanitizeQBXML(item.sales_description || item.description),
                                Price: item.sales_price || 0,
                                AccountRef: {
                                    FullName: item.income_account || 'Sales Income'
                                }
                            }
                        }
                    }
                });
            }
        }

        if (requests.length === 0) {
            return null;
        }

        const qbxml = {
            QBXML: {
                $: { version: '13.0' },
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' }
                }
            }
        };

        // Add all requests
        Object.assign(qbxml.QBXML.QBXMLMsgsRq, ...requests);

        return this.qbxmlBuilder.buildObject(qbxml);
    }

    // Parse ItemMod response
    async parseItemModResponse(responseXml) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            let successCount = 0;
            let errorCount = 0;

            // Check for different mod response types
            const serviceModRs = msgsRs.ItemServiceModRs || [];
            const inventoryModRs = msgsRs.ItemInventoryModRs || [];
            const nonInvModRs = msgsRs.ItemNonInventoryModRs || [];

            const allResponses = [...serviceModRs, ...inventoryModRs, ...nonInvModRs];

            for (const response of allResponses) {
                const statusCode = response.$?.statusCode;
                if (statusCode === '0') {
                    successCount++;
                } else {
                    errorCount++;
                    console.error('Item mod error:', response.$?.statusMessage);
                }
            }

            console.log(`Item update complete: ${successCount} success, ${errorCount} errors`);

            return { success: true, successCount, errorCount };
        } catch (error) {
            console.error('Error parsing ItemModRs:', error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Generate QBXML to check if a Bill already exists in QuickBooks
     * Searches by Vendor name, Reference number (PO number), and Amount
     * @param {Object} po - Purchase order object with vendor_name, po_number, total_amount
     * @returns {String} - QBXML for BillQueryRq
     */
    generateBillCheckQBXML(po) {
        if (!po || !po.vendor_name || !po.po_number) {
            console.warn('Cannot generate BillCheckQBXML: Missing vendor or PO number');
            return null;
        }

        const qbxml = {
            QBXML: {
                QBXMLMsgsRq: {
                    $: { onError: 'continueOnError' },
                    BillQueryRq: {
                        // Filter by vendor
                        EntityFilter: {
                            FullName: this.sanitizeQBXML(po.vendor_name)
                        },
                        // Filter by reference number (PO number)
                        RefNumberFilter: {
                            MatchCriterion: 'Contains',
                            RefNumber: this.sanitizeQBXML(po.po_number)
                        },
                        IncludeRetElement: [
                            'TxnID',
                            'TxnNumber',
                            'VendorRef',
                            'RefNumber',
                            'TxnDate',
                            'DueDate',
                            'AmountDue',
                            'IsPaid'
                        ]
                    }
                }
            }
        };

        let xml = this.qbxmlBuilder.buildObject(qbxml);

        // Remove XML declaration and add qbxml processing instruction
        xml = xml.replace(/<\?xml[^?]*\?>\s*/g, '');
        xml = '<?qbxml version="13.0"?>' + xml;

        return xml;
    }

    /**
     * Parse BillQueryRs response to check for duplicate Bills
     * @param {String} responseXml - QBXML response from QuickBooks
     * @param {Object} po - Purchase order object to match against
     * @returns {Object} - { exists: boolean, txnId: string|null, matchDetails: Object }
     */
    async parseBillCheckResponse(responseXml, po) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            const billQueryRs = msgsRs.BillQueryRs;

            if (!billQueryRs || billQueryRs.length === 0) {
                console.log('No BillQueryRs found - Bill does not exist');
                return { exists: false, txnId: null, matchDetails: null };
            }

            const statusCode = billQueryRs[0].$?.statusCode;
            const statusMessage = billQueryRs[0].$?.statusMessage;

            // Status code 1 means "No records found" - Bill doesn't exist
            if (statusCode === '1') {
                console.log(`No existing Bill found for PO ${po.po_number}`);
                return { exists: false, txnId: null, matchDetails: null };
            }

            // Status code 0 means success - check if matching Bill exists
            if (statusCode === '0') {
                const billRets = billQueryRs[0].BillRet;

                if (!billRets || billRets.length === 0) {
                    return { exists: false, txnId: null, matchDetails: null };
                }

                // Check each returned Bill for amount match
                for (const bill of billRets) {
                    const txnId = bill.TxnID?.[0];
                    const refNumber = bill.RefNumber?.[0];
                    const amountDue = this.safeFloat(bill.AmountDue?.[0]);
                    const isPaid = bill.IsPaid?.[0] === 'true';

                    // Match criteria:
                    // 1. Reference number contains PO number
                    // 2. Amount matches within tolerance ($0.01)
                    const refNumberMatch = refNumber && refNumber.includes(po.po_number);
                    const amountTolerance = 0.01;
                    const amountMatch = Math.abs(amountDue - po.total_amount) <= amountTolerance;

                    if (refNumberMatch && amountMatch) {
                        console.log(`Found matching Bill: TxnID=${txnId}, RefNumber=${refNumber}, Amount=${amountDue}`);
                        return {
                            exists: true,
                            txnId: txnId,
                            matchDetails: {
                                refNumber: refNumber,
                                amount: amountDue,
                                isPaid: isPaid,
                                matchReason: 'Reference number and amount match'
                            }
                        };
                    }
                }

                // No exact match found
                console.log(`Bills found for vendor/ref but no amount match for PO ${po.po_number}`);
                return { exists: false, txnId: null, matchDetails: null };
            }

            // Other error codes
            console.error('BillQueryRs error:', statusCode, statusMessage);
            return { exists: false, txnId: null, matchDetails: null, error: statusMessage };

        } catch (error) {
            console.error('Error parsing BillCheckResponse:', error);
            return { exists: false, txnId: null, matchDetails: null, error: error.message };
        }
    }

    /**
     * Process BillAddRs response after creating Bills in QuickBooks
     * Extracts TxnID and updates database with sync status
     * @param {String} responseXml - QBXML response from QuickBooks
     * @param {Array} purchaseOrders - Original PO records that were synced
     * @returns {Object} - { success: boolean, results: Array }
     */
    async processBillAddResponse(responseXml, purchaseOrders) {
        try {
            const result = await this.parseQBXMLResponse(responseXml);

            if (!result.QBXML || !result.QBXML.QBXMLMsgsRs || !result.QBXML.QBXMLMsgsRs[0]) {
                throw new Error('Invalid QBXML response structure');
            }

            const msgsRs = result.QBXML.QBXMLMsgsRs[0];
            const billAddResponses = msgsRs.BillAddRs || [];

            if (billAddResponses.length === 0) {
                console.log('No BillAddRs responses found');
                return { success: false, results: [] };
            }

            const results = [];
            let successCount = 0;
            let errorCount = 0;

            // Process each BillAddRs response
            for (let i = 0; i < billAddResponses.length; i++) {
                const response = billAddResponses[i];
                const statusCode = response.$?.statusCode;
                const statusMessage = response.$?.statusMessage;

                // Match response to PO by index (responses are in same order as requests)
                const po = purchaseOrders[i];
                if (!po) {
                    console.warn(`No matching PO for response index ${i}`);
                    continue;
                }

                const resultRecord = {
                    po_id: po.id,
                    po_number: po.po_number,
                    statusCode: statusCode,
                    statusMessage: statusMessage
                };

                // Success - extract TxnID and update database
                if (statusCode === '0' && response.BillRet && response.BillRet[0]) {
                    const billRet = response.BillRet[0];
                    const txnId = billRet.TxnID?.[0];
                    const refNumber = billRet.RefNumber?.[0];

                    if (txnId) {
                        // Update database: mark as synced
                        if (this.database) {
                            try {
                                this.database.run(`
                                    UPDATE halopsa_purchase_orders
                                    SET qb_txn_id = ?,
                                        synced_to_qb = 1,
                                        sync_error = NULL,
                                        last_sync = CURRENT_TIMESTAMP
                                    WHERE id = ?
                                `, [txnId, po.id]);

                                console.log(`✓ PO ${po.po_number} synced successfully: TxnID=${txnId}`);
                                successCount++;
                                resultRecord.success = true;
                                resultRecord.txnId = txnId;
                            } catch (dbError) {
                                console.error(`Database update error for PO ${po.po_number}:`, dbError);
                                resultRecord.success = false;
                                resultRecord.error = `Database error: ${dbError.message}`;
                                errorCount++;
                            }
                        }
                    } else {
                        console.warn(`No TxnID returned for PO ${po.po_number}`);
                        resultRecord.success = false;
                        resultRecord.error = 'No TxnID in response';
                        errorCount++;
                    }
                } else {
                    // Error - update database with error message
                    const errorMessage = statusMessage || `QB Error ${statusCode}`;
                    console.error(`✗ Failed to sync PO ${po.po_number}: ${errorMessage}`);

                    if (this.database) {
                        try {
                            this.database.run(`
                                UPDATE halopsa_purchase_orders
                                SET sync_error = ?,
                                    synced_to_qb = 0,
                                    last_sync_attempt = CURRENT_TIMESTAMP
                                WHERE id = ?
                            `, [errorMessage, po.id]);
                        } catch (dbError) {
                            console.error(`Database error recording sync failure for PO ${po.po_number}:`, dbError);
                        }
                    }

                    resultRecord.success = false;
                    resultRecord.error = errorMessage;
                    errorCount++;
                }

                results.push(resultRecord);
            }

            console.log(`Bill sync complete: ${successCount} success, ${errorCount} errors`);

            return {
                success: true,
                results: results,
                summary: {
                    total: results.length,
                    success: successCount,
                    errors: errorCount
                }
            };

        } catch (error) {
            console.error('Error processing BillAddResponse:', error);
            return { success: false, error: error.message, results: [] };
        }
    }

    /**
     * Update database to add last_sync_attempt column if missing (migration helper)
     */
    async ensureSyncTrackingColumns() {
        if (!this.database) {
            console.warn('Database not available for migration');
            return;
        }

        try {
            // Check if last_sync_attempt column exists
            const tableInfo = this.database.all(`PRAGMA table_info(halopsa_purchase_orders)`);
            const hasLastSyncAttempt = tableInfo.some(col => col.name === 'last_sync_attempt');

            if (!hasLastSyncAttempt) {
                console.log('Adding last_sync_attempt column to halopsa_purchase_orders');
                this.database.run(`
                    ALTER TABLE halopsa_purchase_orders
                    ADD COLUMN last_sync_attempt DATETIME
                `);
            }
        } catch (error) {
            // Column might already exist - ignore duplicate errors
            if (!error.message.includes('duplicate column')) {
                console.error('Migration error:', error);
            }
        }
    }
}

module.exports = QBWCService;