/**
 * Item Sync Service
 * Extracts items from HaloPSA invoices and purchase orders
 * Prepares them for QuickBooks synchronization
 */

class ItemSyncService {
    constructor(db) {
        this.db = db;
    }

    /**
     * Extract items from HaloPSA invoices
     */
    async extractItemsFromInvoices() {
        try {
            console.log('Extracting items from HaloPSA invoices...');

            const invoices = await this.db.all(
                'SELECT id, halopsa_id, invoice_number, line_items, raw_data FROM halopsa_invoices WHERE line_items IS NOT NULL'
            );

            const items = new Map();
            let processedCount = 0;
            let errorCount = 0;

            for (const invoice of invoices) {
                try {
                    const lineItems = JSON.parse(invoice.line_items);

                    if (Array.isArray(lineItems)) {
                        lineItems.forEach(line => {
                            const item = this.extractItemFromLine(line, 'invoice');
                            if (item && item.name) {
                                const key = `${item.name}|${item.type}`;
                                if (!items.has(key)) {
                                    items.set(key, item);
                                } else {
                                    // Update with better pricing if available
                                    const existing = items.get(key);
                                    if (item.sales_price && !existing.sales_price) {
                                        existing.sales_price = item.sales_price;
                                    }
                                    if (item.purchase_cost && !existing.purchase_cost) {
                                        existing.purchase_cost = item.purchase_cost;
                                    }
                                }
                            }
                        });
                        processedCount++;
                    }
                } catch (error) {
                    console.error(`Error processing invoice ${invoice.invoice_number}:`, error.message);
                    errorCount++;
                }
            }

            console.log(`Extracted ${items.size} unique items from ${processedCount} invoices (${errorCount} errors)`);
            return Array.from(items.values());
        } catch (error) {
            console.error('Error extracting items from invoices:', error);
            throw error;
        }
    }

    /**
     * Extract items from HaloPSA purchase orders
     */
    async extractItemsFromPurchaseOrders() {
        try {
            console.log('Extracting items from HaloPSA purchase orders...');

            const purchaseOrders = await this.db.all(
                'SELECT id, halopsa_id, po_number, line_items, raw_data FROM halopsa_purchase_orders WHERE line_items IS NOT NULL'
            );

            const items = new Map();
            let processedCount = 0;
            let errorCount = 0;

            for (const po of purchaseOrders) {
                try {
                    const lineItems = JSON.parse(po.line_items);

                    if (Array.isArray(lineItems)) {
                        lineItems.forEach(line => {
                            const item = this.extractItemFromLine(line, 'purchase_order');
                            if (item && item.name) {
                                const key = `${item.name}|${item.type}`;
                                if (!items.has(key)) {
                                    items.set(key, item);
                                } else {
                                    // Update with better pricing if available
                                    const existing = items.get(key);
                                    if (item.purchase_cost && !existing.purchase_cost) {
                                        existing.purchase_cost = item.purchase_cost;
                                    }
                                }
                            }
                        });
                        processedCount++;
                    }
                } catch (error) {
                    console.error(`Error processing PO ${po.po_number}:`, error.message);
                    errorCount++;
                }
            }

            console.log(`Extracted ${items.size} unique items from ${processedCount} purchase orders (${errorCount} errors)`);
            return Array.from(items.values());
        } catch (error) {
            console.error('Error extracting items from purchase orders:', error);
            throw error;
        }
    }

    /**
     * Extract item data from a line item
     */
    extractItemFromLine(line, sourceType) {
        // Handle various field name formats from HaloPSA
        const name = line.item || line.name || line.description || line.item_name || line.itemName || null;
        const description = line.description || line.desc || line.item_description || line.details || name;
        const quantity = parseFloat(line.quantity || line.qty || line.amount || 0);
        const unitPrice = parseFloat(line.unit_price || line.unitPrice || line.price || line.rate || line.unitCost || 0);
        const cost = parseFloat(line.cost || line.unit_cost || line.unitCost || unitPrice || 0);

        if (!name) return null;

        // Determine item type based on characteristics
        const itemType = this.determineItemType(line, sourceType);

        return {
            name: this.sanitizeItemName(name),
            full_name: name,
            description: description ? String(description).substring(0, 4000) : name,
            sales_description: description ? String(description).substring(0, 4000) : name,
            purchase_description: description ? String(description).substring(0, 4000) : name,
            sales_price: sourceType === 'invoice' ? unitPrice : (unitPrice * 1.3), // 30% markup if from PO
            purchase_cost: cost,
            item_type: itemType,
            quantity_on_hand: 0,
            income_account: 'Sales Income',
            cogs_account: 'Cost of Goods Sold',
            asset_account: itemType === 'ItemInventory' ? 'Inventory Asset' : null,
            expense_account: itemType === 'ItemNonInventory' ? 'Supplies' : null,
            tax_code: 'Tax',
            is_active: true,
            source_system: 'halopsa',
            halopsa_item_id: line.id || line.item_id || null,
            synced_to_qb: false
        };
    }

    /**
     * Determine QuickBooks item type
     */
    determineItemType(line, sourceType) {
        const name = (line.item || line.name || line.description || '').toLowerCase();
        const desc = (line.description || '').toLowerCase();

        // Service items - common service keywords
        const serviceKeywords = ['service', 'support', 'consulting', 'hourly', 'labor', 'maintenance', 'setup', 'installation', 'training', 'management'];
        if (serviceKeywords.some(keyword => name.includes(keyword) || desc.includes(keyword))) {
            return 'ItemService';
        }

        // Inventory items - physical products
        const inventoryKeywords = ['hardware', 'device', 'computer', 'server', 'laptop', 'phone', 'tablet', 'equipment', 'machine', 'unit'];
        if (inventoryKeywords.some(keyword => name.includes(keyword) || desc.includes(keyword))) {
            return 'ItemInventory';
        }

        // Software/Licenses - typically non-inventory
        const nonInventoryKeywords = ['license', 'software', 'subscription', 'cloud', 'saas', 'office 365', 'microsoft 365', 'adobe'];
        if (nonInventoryKeywords.some(keyword => name.includes(keyword) || desc.includes(keyword))) {
            return 'ItemNonInventory';
        }

        // Default: Service for invoices, Non-Inventory for purchase orders
        return sourceType === 'invoice' ? 'ItemService' : 'ItemNonInventory';
    }

    /**
     * Sanitize item name for QuickBooks (max 31 chars)
     */
    sanitizeItemName(name) {
        if (!name) return 'Unknown Item';

        return String(name)
            .replace(/[<>&"']/g, '') // Remove XML special chars
            .replace(/[:/\\|]/g, '-') // Replace QB illegal chars
            .trim()
            .substring(0, 31); // QB item name limit
    }

    /**
     * Save items to database
     */
    async saveItems(items) {
        try {
            console.log(`Saving ${items.length} items to database...`);

            let newCount = 0;
            let updateCount = 0;
            let skipCount = 0;
            let errorCount = 0;

            for (const item of items) {
                try {
                    // Check if item exists
                    const existing = await this.db.get(
                        'SELECT id, synced_to_qb FROM qb_items WHERE name = ? AND item_type = ?',
                        [item.name, item.item_type]
                    );

                    if (existing) {
                        // Only update if not yet synced to QB
                        if (!existing.synced_to_qb) {
                            await this.db.run(
                                `UPDATE qb_items SET
                                 description = ?, sales_description = ?, purchase_description = ?,
                                 sales_price = ?, purchase_cost = ?,
                                 income_account = ?, cogs_account = ?, asset_account = ?, expense_account = ?,
                                 halopsa_item_id = ?, source_system = ?,
                                 last_sync = CURRENT_TIMESTAMP
                                 WHERE id = ?`,
                                [
                                    item.description, item.sales_description, item.purchase_description,
                                    item.sales_price, item.purchase_cost,
                                    item.income_account, item.cogs_account, item.asset_account, item.expense_account,
                                    item.halopsa_item_id, item.source_system,
                                    existing.id
                                ]
                            );
                            updateCount++;
                        } else {
                            skipCount++;
                        }
                    } else {
                        // Insert new item
                        await this.db.run(
                            `INSERT INTO qb_items
                             (item_type, name, full_name, description, sales_description, purchase_description,
                              sales_price, purchase_cost, quantity_on_hand,
                              income_account, cogs_account, asset_account, expense_account, tax_code,
                              is_active, halopsa_item_id, source_system, synced_to_qb, last_sync)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                            [
                                item.item_type, item.name, item.full_name, item.description,
                                item.sales_description, item.purchase_description,
                                item.sales_price, item.purchase_cost, item.quantity_on_hand,
                                item.income_account, item.cogs_account, item.asset_account,
                                item.expense_account, item.tax_code,
                                item.is_active, item.halopsa_item_id, item.source_system, item.synced_to_qb
                            ]
                        );
                        newCount++;
                    }
                } catch (error) {
                    console.error(`Error saving item ${item.name}:`, error.message);
                    errorCount++;
                }
            }

            console.log(`Item save complete: ${newCount} new, ${updateCount} updated, ${skipCount} skipped, ${errorCount} errors`);

            return {
                success: true,
                new: newCount,
                updated: updateCount,
                skipped: skipCount,
                errors: errorCount,
                total: items.length
            };
        } catch (error) {
            console.error('Error saving items:', error);
            throw error;
        }
    }

    /**
     * Extract and save all items from HaloPSA data
     */
    async extractAndSaveAllItems() {
        try {
            console.log('Starting comprehensive item extraction from HaloPSA data...');

            // Extract from both sources
            const invoiceItems = await this.extractItemsFromInvoices();
            const poItems = await this.extractItemsFromPurchaseOrders();

            // Merge and deduplicate
            const allItems = new Map();

            [...invoiceItems, ...poItems].forEach(item => {
                const key = `${item.name}|${item.item_type}`;
                if (!allItems.has(key)) {
                    allItems.set(key, item);
                } else {
                    // Merge pricing data
                    const existing = allItems.get(key);
                    if (item.sales_price && (!existing.sales_price || item.sales_price > existing.sales_price)) {
                        existing.sales_price = item.sales_price;
                    }
                    if (item.purchase_cost && (!existing.purchase_cost || item.purchase_cost < existing.purchase_cost)) {
                        existing.purchase_cost = item.purchase_cost;
                    }
                }
            });

            const uniqueItems = Array.from(allItems.values());
            console.log(`Total unique items after merge: ${uniqueItems.length}`);

            // Save to database
            const result = await this.saveItems(uniqueItems);

            return {
                success: true,
                message: `Extracted ${uniqueItems.length} unique items from HaloPSA data`,
                ...result
            };
        } catch (error) {
            console.error('Error in extractAndSaveAllItems:', error);
            return {
                success: false,
                message: `Error extracting items: ${error.message}`
            };
        }
    }

    /**
     * Get unsynced items ready for QuickBooks
     */
    async getUnsyncedItems() {
        try {
            const items = await this.db.all(
                'SELECT * FROM qb_items WHERE synced_to_qb = 0 AND is_active = 1 ORDER BY item_type, name'
            );

            return items;
        } catch (error) {
            console.error('Error getting unsynced items:', error);
            throw error;
        }
    }

    /**
     * Mark items as synced
     */
    async markItemsSynced(itemIds, qbListIds = []) {
        try {
            for (let i = 0; i < itemIds.length; i++) {
                const itemId = itemIds[i];
                const qbListId = qbListIds[i] || null;

                await this.db.run(
                    `UPDATE qb_items SET
                     synced_to_qb = 1,
                     qb_list_id = ?,
                     sync_error = NULL,
                     last_sync = CURRENT_TIMESTAMP
                     WHERE id = ?`,
                    [qbListId, itemId]
                );
            }

            console.log(`Marked ${itemIds.length} items as synced`);
        } catch (error) {
            console.error('Error marking items as synced:', error);
            throw error;
        }
    }

    /**
     * Log sync operation
     */
    async logSync(itemName, itemType, action, status, errorMessage = null, qbxmlRequest = null, qbxmlResponse = null) {
        try {
            await this.db.run(
                `INSERT INTO qb_item_sync_log
                 (item_name, item_type, sync_action, sync_status, error_message, qbxml_request, qbxml_response)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [itemName, itemType, action, status, errorMessage, qbxmlRequest, qbxmlResponse]
            );
        } catch (error) {
            console.error('Error logging sync:', error);
        }
    }
}

module.exports = ItemSyncService;
