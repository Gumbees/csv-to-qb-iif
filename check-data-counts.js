const Database = require('./src/database.js');

(async () => {
    try {
        const db = new Database();
        await db.ready;

        const invoices = db.get('SELECT COUNT(*) as count FROM halopsa_invoices');
        const pos = db.get('SELECT COUNT(*) as count FROM halopsa_purchase_orders');
        const items = db.get('SELECT COUNT(*) as count FROM qb_items');

        console.log('\n=== Current Data Counts ===');
        console.log('Invoices:', invoices ? invoices.count : 0);
        console.log('Purchase Orders:', pos ? pos.count : 0);
        console.log('Items:', items ? items.count : 0);
        console.log('===========================\n');

        db.close();
        process.exit(0);
    } catch (error) {
        console.error('Error:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
})();
