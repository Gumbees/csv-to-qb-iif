const Database = require('./src/database');
const StripeAPI = require('./src/stripe-api');

async function debugServerError() {
    console.log('=== Debugging Server Error ===\n');
    
    try {
        const db = new Database();
        const stripeAPI = new StripeAPI(db);
        
        // Test the exact same queries the server uses
        console.log('1. Testing threshold query...');
        const thresholdRow = db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        const threshold = thresholdRow && thresholdRow.value ? parseFloat(thresholdRow.value) : 0.55;
        console.log('   Threshold:', threshold);
        
        console.log('\n2. Testing existing mappings query...');
        const existingMappings = db.all('SELECT stripe_customer_id, halopsa_client_id, mapping_confirmed FROM customer_mappings WHERE mapping_confirmed = 1');
        const stripeMapped = new Set(existingMappings.map(m => m.stripe_customer_id));
        const haloMapped = new Set(existingMappings.map(m => m.halopsa_client_id.toString()));
        console.log('   Existing mappings:', existingMappings.length);
        
        console.log('\n3. Testing paginated stripe customers query...');
        const stripeWhereClause = stripeMapped.size > 0 
            ? `AND stripe_id NOT IN (${Array.from(stripeMapped).map(() => '?').join(',')})`
            : '';
        const stripeQuery = `
            SELECT * FROM stripe_customers 
            WHERE name IS NOT NULL AND name != 'null' 
            ${stripeWhereClause}
            ORDER BY name
            LIMIT ? OFFSET ?
        `;
        const stripeParams = stripeMapped.size > 0 
            ? [...Array.from(stripeMapped), 50, 0]
            : [50, 0];
            
        console.log('   Query:', stripeQuery.replace(/\s+/g, ' '));
        console.log('   Params:', stripeParams);
        
        const stripeCustomers = db.all(stripeQuery, stripeParams);
        console.log('   Result count:', stripeCustomers.length);
        
        console.log('\n4. Testing halo clients query...');
        const haloWhereClause = haloMapped.size > 0 
            ? `WHERE halopsa_id NOT IN (${Array.from(haloMapped).map(() => '?').join(',')})`
            : '';
        const haloQuery = `
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
            ${haloWhereClause}
        `;
        const haloParams = haloMapped.size > 0 ? [...Array.from(haloMapped)] : [];
        
        console.log('   Query:', haloQuery.replace(/\s+/g, ' '));
        console.log('   Params:', haloParams);
        
        const haloClients = db.all(haloQuery, haloParams);
        console.log('   Result count:', haloClients.length);
        
        console.log('\n5. Testing total count query...');
        const totalStripeQuery = `
            SELECT COUNT(*) as total FROM stripe_customers 
            WHERE name IS NOT NULL AND name != 'null' 
            ${stripeWhereClause}
        `;
        const totalStripeParams = stripeMapped.size > 0 ? [...Array.from(stripeMapped)] : [];
        
        console.log('   Query:', totalStripeQuery.replace(/\s+/g, ' '));
        console.log('   Params:', totalStripeParams);
        
        const totalStripeCount = db.get(totalStripeQuery, totalStripeParams);
        console.log('   Total count:', totalStripeCount.total);
        
        console.log('\n=== All queries executed successfully ===');
        
    } catch (error) {
        console.error('ERROR FOUND:', error.message);
        console.error('Stack:', error.stack);
    }
}

debugServerError();