// Test the automatch endpoint exactly as the frontend would call it
const http = require('http');

function testAutomatchEndpoint() {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'localhost',
            port: 3000,
            path: '/api/customers/automatch',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';

            res.on('data', (chunk) => {
                data += chunk;
            });

            res.on('end', () => {
                try {
                    const result = JSON.parse(data);
                    resolve(result);
                } catch (error) {
                    reject(new Error('Failed to parse response: ' + error.message));
                }
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.end();
    });
}

async function testFrontendAutomatch() {
    console.log('=== Testing Auto-match Endpoint (Frontend Perspective) ===\n');
    
    try {
        console.log('1. Making POST request to /api/customers/automatch...');
        const result = await testAutomatchEndpoint();
        
        console.log('2. Response received:');
        console.log(`   - Success: ${result.success}`);
        console.log(`   - Matches found: ${result.matches ? result.matches.length : 0}`);
        
        if (result.success && result.matches && result.matches.length > 0) {
            console.log('\n3. Sample matches (frontend will display these):');
            result.matches.slice(0, 3).forEach((match, i) => {
                console.log(`   ${i+1}. "${match.stripe_customer.name}" → "${match.halo_client.name}"`);
                console.log(`      Score: ${(match.match_score * 100).toFixed(1)}%`);
                console.log(`      Stripe ID: ${match.stripe_customer.stripe_id}`);
                console.log(`      HaloPSA ID: ${match.halo_client.halopsa_id}`);
            });
            
            console.log('\n4. Frontend will call confirmMapping with:');
            console.log(`   confirmMapping('${result.matches[0].stripe_customer.stripe_id}', '${result.matches[0].halo_client.halopsa_id}')`);
        } else {
            console.log('\n3. No matches found - possible issues:');
            console.log('   - Threshold too high? Current:', result.threshold);
            console.log('   - Customers already mapped?');
            console.log('   - Data not loaded properly?');
        }
        
        if (result.suggestions && result.conflicts) {
            console.log('\n5. Detailed breakdown (new format):');
            console.log(`   - Suggestions: ${result.suggestions.length}`);
            console.log(`   - Conflicts: ${result.conflicts.length}`);
        }
        
        console.log('\n=== Test Complete ===');
        
    } catch (error) {
        console.error('Error testing automatch endpoint:', error.message);
        console.log('\nPossible issues:');
        console.log('1. Server not running on port 3000');
        console.log('2. Endpoint path incorrect');
        console.log('3. Server error');
    }
}

// If server is not running, we can simulate the response
async function simulateAutomatchResponse() {
    console.log('\n=== Simulating Auto-match Response (Server Not Running) ===\n');
    
    const Database = require('./src/database');
    const StripeAPI = require('./src/stripe-api');
    
    try {
        const db = new Database();
        const stripeAPI = new StripeAPI(db);
        
        // Get threshold
        const thresholdRow = await db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        const threshold = thresholdRow ? parseFloat(thresholdRow.value) : 0.55;
        
        // Get customers
        const stripeCustomers = await stripeAPI.getImportedCustomers();
        const haloClients = await db.all('SELECT id, halopsa_id, name FROM halopsa_clients');
        
        // Filter valid customers
        const validStripeCustomers = stripeCustomers.filter(customer => customer.name && customer.name !== 'null');
        
        // Test matching
        const matches = [];
        for (const stripeCustomer of validStripeCustomers.slice(0, 10)) {
            let bestScore = 0;
            let bestMatch = null;
            
            for (const haloClient of haloClients) {
                // Simple name matching for simulation
                if (stripeCustomer.name && haloClient.name) {
                    const name1 = stripeCustomer.name.toLowerCase();
                    const name2 = haloClient.name.toLowerCase();
                    const score = name1 === name2 ? 0.8 : 
                                 (name1.includes(name2) || name2.includes(name1)) ? 0.6 : 0;
                    
                    if (score > bestScore && score >= threshold) {
                        bestScore = score;
                        bestMatch = haloClient;
                    }
                }
            }
            
            if (bestMatch) {
                matches.push({
                    stripe_customer: stripeCustomer,
                    halo_client: bestMatch,
                    match_score: bestScore
                });
            }
        }
        
        console.log('Simulated response:');
        console.log(`- Matches found: ${matches.length}`);
        console.log(`- Threshold: ${threshold}`);
        
        if (matches.length > 0) {
            console.log('\nSample matches:');
            matches.slice(0, 3).forEach((match, i) => {
                console.log(`  ${i+1}. "${match.stripe_customer.name}" → "${match.halo_client.name}"`);
            });
        }
        
        db.close();
        
    } catch (error) {
        console.error('Error in simulation:', error);
    }
}

// Try the real test first, if it fails, run simulation
testFrontendAutomatch().catch(() => {
    simulateAutomatchResponse();
});