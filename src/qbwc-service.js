const { Builder, parseString } = require('xml2js');
const dayjs = require('dayjs');

class QBWCService {
    constructor() {
        this.sessions = new Map();
        this.qbxmlBuilder = new Builder({ 
            xmldec: { version: '1.0', encoding: 'utf-8' },
            headless: true 
        });
    }

    // Generate .qwc configuration file for QuickBooks Web Connector
    generateQWCFile(config) {
        const qwcConfig = {
            QBWCXML: {
                AppName: config.appName || 'HaloPSA QuickBooks Sync',
                AppID: '',
                AppURL: config.appUrl || 'http://localhost:3000/qbwc',
                AppDescription: config.description || 'Sync HaloPSA purchase orders with QuickBooks',
                AppSupport: config.supportUrl || 'http://localhost:3000/support',
                UserName: config.username || 'halopsa_user',
                OwnerID: this.generateGUID(),
                FileID: this.generateGUID(),
                QBType: 'QBFS',
                Style: 'Document',
                Scheduler: {
                    RunEveryNMinutes: config.interval || 60
                }
            }
        };

        return this.qbxmlBuilder.buildObject(qwcConfig);
    }

    // Generate GUID for QWC file
    generateGUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
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
                    $: { onError: 'stopOnError' }
                }
            }
        };

        qbxml.QBXML.QBXMLMsgsRq.ItemInventoryAddRq = itemRequests;

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
        // Simple authentication - in production, use proper auth
        const validUsers = {
            'halopsa_user': 'password123',
            'admin': 'admin123'
        };

        if (validUsers[username] && validUsers[username] === password) {
            const ticket = Math.random().toString(36).substring(7);
            this.createSession(ticket);
            return { ticket, authType: 'none' };
        }

        return { ticket: 'n', authType: 'Invalid credentials' };
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
}

module.exports = QBWCService;