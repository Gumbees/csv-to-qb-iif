/**
 * Test script for enhanced QBXML generation
 * Tests purchase order → bill conversion and item sync with account mappings
 */

const DatabaseManager = require('./src/database');
const QBWCService = require('./src/qbwc-service');

async function testQBXMLEnhancements() {
    console.log('=== Testing Enhanced QBXML Generation ===\n');

    const db = new DatabaseManager();
    await db.ready;

    const qbwcService = new QBWCService();

    // Test 1: Fetch account mappings
    console.log('Test 1: Fetching account mappings...');
    const accountMappings = {};
    const mappings = await db.all('SELECT mapping_type, qb_account_name FROM account_mappings WHERE is_active = 1');
    mappings.forEach(m => {
        accountMappings[m.mapping_type] = m.qb_account_name;
    });
    console.log('Account mappings:', accountMappings);
    console.log('');

    // Test 2: Generate QBXML for purchase orders
    console.log('Test 2: Generating QBXML for purchase orders → bills...');
    const purchaseOrders = await db.all(`
        SELECT * FROM halopsa_purchase_orders
        LIMIT 2
    `);
    console.log(`Found ${purchaseOrders.length} purchase orders`);

    if (purchaseOrders.length > 0) {
        console.log('Sample PO:', {
            po_number: purchaseOrders[0].po_number,
            vendor_name: purchaseOrders[0].vendor_name,
            total_amount: purchaseOrders[0].total_amount,
            line_items: purchaseOrders[0].line_items ?
                (typeof purchaseOrders[0].line_items === 'string' ?
                    JSON.parse(purchaseOrders[0].line_items).length + ' items' :
                    purchaseOrders[0].line_items.length + ' items'
                ) : 'none'
        });

        const billQBXML = qbwcService.generatePurchaseOrderBillsQBXML(purchaseOrders, accountMappings);

        if (billQBXML) {
            console.log('✅ Successfully generated bill QBXML');
            console.log('QBXML preview (first 500 chars):');
            console.log(billQBXML.substring(0, 500) + '...\n');
        } else {
            console.log('❌ Failed to generate bill QBXML\n');
        }
    } else {
        console.log('⚠️  No purchase orders found in database\n');
    }

    // Test 3: Generate QBXML for items
    console.log('Test 3: Generating QBXML for items with account mappings...');
    const items = await db.all(`
        SELECT * FROM qb_items
        LIMIT 5
    `);
    console.log(`Found ${items.length} items`);

    if (items.length > 0) {
        console.log('Sample items by type:');
        const itemsByType = items.reduce((acc, item) => {
            acc[item.item_type] = (acc[item.item_type] || 0) + 1;
            return acc;
        }, {});
        console.log(itemsByType);

        const itemsQBXML = qbwcService.generateItemsWithMappingsQBXML(items, accountMappings);

        if (itemsQBXML) {
            console.log('✅ Successfully generated items QBXML');
            console.log('QBXML preview (first 500 chars):');
            console.log(itemsQBXML.substring(0, 500) + '...\n');
        } else {
            console.log('❌ Failed to generate items QBXML\n');
        }
    } else {
        console.log('⚠️  No items found in database\n');
    }

    // Test 4: Verify database schema
    console.log('Test 4: Verifying database schema...');
    const poSchema = await db.all(`
        PRAGMA table_info(halopsa_purchase_orders)
    `);
    const requiredPoFields = ['po_number', 'vendor_name', 'total_amount', 'line_items', 'synced_to_qb'];
    const hasRequiredPOFields = requiredPoFields.every(field =>
        poSchema.some(col => col.name === field)
    );
    console.log(`PO table has required fields: ${hasRequiredPOFields ? '✅' : '❌'}`);

    const itemSchema = await db.all(`
        PRAGMA table_info(qb_items)
    `);
    const requiredItemFields = ['name', 'item_type', 'sales_price', 'purchase_cost', 'synced_to_qb'];
    const hasRequiredItemFields = requiredItemFields.every(field =>
        itemSchema.some(col => col.name === field)
    );
    console.log(`Items table has required fields: ${hasRequiredItemFields ? '✅' : '❌'}`);

    const mappingSchema = await db.all(`
        PRAGMA table_info(account_mappings)
    `);
    const hasMappingTable = mappingSchema.length > 0;
    console.log(`Account mappings table exists: ${hasMappingTable ? '✅' : '❌'}`);
    console.log('');

    // Test 5: Check for unsynced records
    console.log('Test 5: Checking for unsynced records...');
    const unsyncedPOs = await db.get(`
        SELECT COUNT(*) as count FROM halopsa_purchase_orders
        WHERE synced_to_qb = 0 OR synced_to_qb IS NULL
    `);
    console.log(`Unsynced purchase orders: ${unsyncedPOs.count}`);

    const unsyncedItems = await db.get(`
        SELECT COUNT(*) as count FROM qb_items
        WHERE synced_to_qb = 0 OR synced_to_qb IS NULL
    `);
    console.log(`Unsynced items: ${unsyncedItems.count}`);
    console.log('');

    console.log('=== Test Complete ===');
    db.close();
}

// Run the test
testQBXMLEnhancements().catch(error => {
    console.error('Test failed:', error);
    process.exit(1);
});
