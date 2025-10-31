const Database = require('./src/database');
const StripeAPI = require('./src/stripe-api');

async function debugAutomatch() {
    console.log('=== DEBUGGING AUTOMATCH LOGIC ===\n');
    
    try {
        const db = new Database();
        const stripeAPI = new StripeAPI(db);
        
        // Test threshold query
        console.log('1. Testing threshold query:');
        const thresholdRow = db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        console.log('   Threshold row:', thresholdRow);
        const threshold = thresholdRow && thresholdRow.value ? parseFloat(thresholdRow.value) : 0.55;
        console.log('   Using threshold:', threshold, '\n');
        
        // Test stripe customers query
        console.log('2. Testing stripe customers:');
        const stripeCustomers = await stripeAPI.getImportedCustomers();
        console.log('   Total stripe customers:', stripeCustomers.length);
        
        if (stripeCustomers.length > 0) {
            console.log('   Sample stripe customers:');
            stripeCustomers.slice(0, 3).forEach((customer, i) => {
                console.log(`     ${i+1}. Name: "${customer.name}", Email: "${customer.email}", ID: ${customer.stripe_id.substring(0, 10)}...`);
            });
        }
        
        // Test halo clients query
        console.log('\n3. Testing halo clients:');
        const haloClients = db.all('SELECT id, halopsa_id, name, email FROM halopsa_clients');
        console.log('   Total halo clients:', haloClients.length);
        
        if (haloClients.length > 0) {
            console.log('   Sample halo clients:');
            haloClients.slice(0, 3).forEach((client, i) => {
                console.log(`     ${i+1}. Name: "${client.name}", Email: "${client.email}", ID: ${client.halopsa_id}`);
            });
        }
        
        // Test existing mappings
        console.log('\n4. Testing existing mappings:');
        const existingMappings = db.all('SELECT stripe_customer_id, halopsa_client_id FROM customer_mappings WHERE mapping_confirmed = 1');
        console.log('   Existing confirmed mappings:', existingMappings.length);
        
        const stripeMapped = new Set(existingMappings.map(m => m.stripe_customer_id));
        const haloMapped = new Set(existingMappings.map(m => m.halopsa_client_id.toString()));
        
        // Filter valid customers
        console.log('\n5. Testing customer filtering:');
        const validStripeCustomers = stripeCustomers.filter(customer => 
            customer.name && customer.name !== 'null' && !stripeMapped.has(customer.stripe_id)
        );
        const validHaloClients = haloClients.filter(client => !haloMapped.has(client.halopsa_id.toString()));
        
        console.log('   Valid stripe customers (with names, not mapped):', validStripeCustomers.length);
        console.log('   Valid halo clients (not mapped):', validHaloClients.length);
        
        // Test matching logic
        console.log('\n6. Testing matching logic:');
        if (validStripeCustomers.length > 0 && validHaloClients.length > 0) {
            const stripeCustomer = validStripeCustomers[0];
            console.log(`   Testing with first stripe customer: "${stripeCustomer.name}"`);
            
            let matchCount = 0;
            for (const haloClient of validHaloClients.slice(0, 5)) { // Test first 5
                // Simple test - check if names have any words in common
                const name1 = stripeCustomer.name.toLowerCase();
                const name2 = haloClient.name.toLowerCase();
                
                // Basic matching test
                const words1 = new Set(name1.split(/\s+/).filter(w => w.length > 2));
                const words2 = new Set(name2.split(/\s+/).filter(w => w.length > 2));
                
                const commonWords = [...words1].filter(word => words2.has(word));
                
                if (commonWords.length > 0) {
                    matchCount++;
                    console.log(`     Potential match: "${haloClient.name}" (common words: ${commonWords.join(', ')})`);
                }
            }
            
            console.log(`   Found ${matchCount} potential matches for this customer`);
        } else {
            console.log('   Not enough valid customers to test matching');
        }
        
        console.log('\n=== DEBUGGING COMPLETE ===');
        db.close();
        
    } catch (error) {
        console.error('Error during debugging:', error);
    }
}

debugAutomatch();