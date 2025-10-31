const Database = require('./src/database');

async function checkAutomatchSetup() {
    try {
        const db = new Database();
        
        console.log('=== Checking Database Setup ===\n');
        
        // Check configuration
        console.log('1. Configuration:');
        const configRows = await db.query(`SELECT key, value FROM config WHERE key LIKE 'stripe%' OR key LIKE 'halopsa%'`);
        
        if (configRows.length === 0) {
            console.log('   No Stripe or HaloPSA configuration found');
        } else {
            configRows.forEach(row => {
                const valuePreview = row.value ? (row.value.substring(0, 10) + (row.value.length > 10 ? '...' : '')) : '(empty)';
                console.log(`   ${row.key}: ${valuePreview}`);
            });
        }
        
        // Check stripe_customers table
        console.log('\n2. Stripe Customers:');
        const stripeCustomers = await db.query('SELECT COUNT(*) as count FROM stripe_customers');
        console.log(`   Total stripe_customers: ${stripeCustomers[0].count}`);
        
        if (stripeCustomers[0].count > 0) {
            const sampleCustomers = await db.query('SELECT stripe_id, name, email FROM stripe_customers LIMIT 3');
            sampleCustomers.forEach(customer => {
                console.log(`   - ${customer.name} (${customer.email}) [${customer.stripe_id.substring(0, 8)}...]`);
            });
        }
        
        // Check halopsa_clients table
        console.log('\n3. HaloPSA Clients:');
        const halopsaClients = await db.query('SELECT COUNT(*) as count FROM halopsa_clients');
        console.log(`   Total halopsa_clients: ${halopsaClients[0].count}`);
        
        if (halopsaClients[0].count > 0) {
            const sampleClients = await db.query('SELECT halopsa_id, name, email FROM halopsa_clients LIMIT 3');
            sampleClients.forEach(client => {
                console.log(`   - ${client.name} (${client.email}) [ID: ${client.halopsa_id}]`);
            });
        }
        
        // Check if both have data for matching
        console.log('\n4. Automatch Analysis:');
        if (stripeCustomers[0].count === 0 && halopsaClients[0].count === 0) {
            console.log('   ❌ No data available for automatching - both tables are empty');
        } else if (stripeCustomers[0].count === 0) {
            console.log('   ❌ No Stripe customers found - please import Stripe customers first');
        } else if (halopsaClients[0].count === 0) {
            console.log('   ❌ No HaloPSA clients found - please sync HaloPSA clients first');
        } else {
            console.log('   ✅ Data available for automatching');
            console.log(`   - Stripe customers: ${stripeCustomers[0].count}`);
            console.log(`   - HaloPSA clients: ${halopsaClients[0].count}`);
        }
        
        console.log('\n=== Checking Match Threshold Settings ===');
        const matchConfig = await db.query(`SELECT key, value FROM config WHERE key IN ('customer_match_threshold', 'customer_auto_match_enabled')`);
        
        matchConfig.forEach(config => {
            console.log(`   ${config.key}: ${config.value}`);
        });
        
        db.close();
        
    } catch (error) {
        console.error('Error checking automatch setup:', error);
    }
}

checkAutomatchSetup();