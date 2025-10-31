const Database = require('./src/database.js');

async function verifyImport() {
    const db = new Database();
    await db.ready;

    console.log('\n📊 Purchase Order Import Verification:\n');

    // Count total POs
    const poCount = await db.get('SELECT COUNT(*) as total FROM halopsa_purchase_orders');
    console.log(`✅ Total Purchase Orders: ${poCount.total}`);

    // Count total items
    const itemCount = await db.get('SELECT COUNT(*) as total FROM qb_items');
    console.log(`✅ Total Items: ${itemCount.total}`);

    // Show sample PO with line items
    const samplePO = await db.get('SELECT po_number, vendor_name, total_amount, line_items FROM halopsa_purchase_orders WHERE line_items IS NOT NULL LIMIT 1');

    if (samplePO) {
        console.log(`\n📝 Sample PO: ${samplePO.po_number}`);
        console.log(`   Vendor: ${samplePO.vendor_name}`);
        console.log(`   Total: $${samplePO.total_amount.toFixed(2)}`);

        const lineItems = JSON.parse(samplePO.line_items);
        console.log(`   Line Items (${lineItems.length}):`);
        lineItems.slice(0, 3).forEach((item, index) => {
            console.log(`      ${index + 1}. ${item.item} - Qty: ${item.qty}, Cost: $${item.cost}`);
        });
        if (lineItems.length > 3) {
            console.log(`      ... and ${lineItems.length - 3} more items`);
        }
    }

    // Show sample items
    const sampleItems = await db.query('SELECT name, description, purchase_cost, quantity_on_hand FROM qb_items LIMIT 5');
    console.log(`\n📦 Sample Items:`);
    sampleItems.forEach((item, index) => {
        console.log(`   ${index + 1}. ${item.name}`);
        console.log(`      Description: ${item.description}`);
        console.log(`      Cost: $${item.purchase_cost}, Qty: ${item.quantity_on_hand}`);
    });

    await db.close();
    console.log('\n✅ Verification complete!\n');
}

verifyImport().catch(console.error);
