const Database = require('./src/database.js');

(async () => {
    try {
        const db = new Database();
        await db.ready;

        const stripeCount = db.get('SELECT COUNT(*) as count FROM stripe_transactions');

        console.log('\n=== Stripe Transactions Count ===');
        console.log('Stripe Transactions:', stripeCount ? stripeCount.count : 0);
        console.log('===========================\n');

        db.close();
        process.exit(0);
    } catch (error) {
        console.error('Error:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
})();
