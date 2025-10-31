const Database = require('./src/database.js');

async function checkPOItems() {
    const db = new Database();
    await db.ready;

    // Check total items
    const itemCount = await db.get('SELECT COUNT(*) as total FROM qb_items');
    console.log(`\n📊 Total items in qb_items table: ${itemCount.total}`);

    // Check total POs
    const poCount = await db.get('SELECT COUNT(*) as total FROM halopsa_purchase_orders');
    console.log(`📊 Total purchase orders: ${poCount.total}`);

    // Check POs with line_items
    const posWithItems = await db.get('SELECT COUNT(*) as total FROM halopsa_purchase_orders WHERE line_items IS NOT NULL');
    console.log(`📊 POs with line_items: ${posWithItems.total}`);

    // Get a sample PO with raw_data to see structure
    const samplePO = await db.get('SELECT halopsa_id, po_number, line_items, raw_data FROM halopsa_purchase_orders LIMIT 1');

    if (samplePO) {
        console.log(`\n📝 Sample PO #${samplePO.po_number}:`);
        console.log(`   line_items field: ${samplePO.line_items ? 'Present' : 'NULL'}`);

        // Parse raw data to see if 'lines' exists
        try {
            const rawData = JSON.parse(samplePO.raw_data);
            console.log(`\n🔍 Keys in raw_data:`, Object.keys(rawData).join(', '));

            if (rawData.lines) {
                console.log(`\n✅ 'lines' array found in raw_data!`);
                console.log(`   Number of line items: ${rawData.lines.length}`);
                if (rawData.lines.length > 0) {
                    console.log(`   First line item keys:`, Object.keys(rawData.lines[0]).join(', '));
                }
            } else {
                console.log(`\n❌ No 'lines' array in raw_data`);
                console.log(`\n🔍 Checking for other possible item fields...`);
                ['items', 'lineitems', 'lineItems', 'purchaseOrderLines', 'orderLines'].forEach(field => {
                    if (rawData[field]) {
                        console.log(`   Found '${field}': ${Array.isArray(rawData[field]) ? rawData[field].length + ' items' : 'not an array'}`);
                    }
                });
            }
        } catch (e) {
            console.error('Error parsing raw_data:', e.message);
        }
    }

    await db.close();
}

checkPOItems().catch(console.error);
