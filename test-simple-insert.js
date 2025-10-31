const Database = require('./src/database');

async function testSimpleInsert() {
    console.log('=== Testing Simple Database Insert ===\n');
    
    try {
        const db = new Database();
        
        // Test 1: Simple insert with basic values
        console.log('1. Testing simple insert with basic values...');
        try {
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_simple']);
            const result = db.run(
                'INSERT INTO customer_mappings (stripe_customer_id, stripe_customer_email, halopsa_client_id, auto_mapped, mapping_confirmed) VALUES (?, ?, ?, ?, ?)',
                ['test_simple', 'test@example.com', 999, 0, 1]
            );
            console.log('   SUCCESS: Simple insert worked');
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_simple']);
        } catch (error) {
            console.log('   FAILED:', error.message);
        }
        
        // Test 2: Insert with the exact values from our mapping
        console.log('\n2. Testing insert with mapping values...');
        try {
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['cus_SdyDj0TflO21YK']);
            const result = db.run(
                'INSERT INTO customer_mappings (stripe_customer_id, stripe_customer_email, halopsa_client_id, auto_mapped, mapping_confirmed) VALUES (?, ?, ?, ?, ?)',
                ['cus_SdyDj0TflO21YK', 'test@example.com', 733, 0, 1]
            );
            console.log('   SUCCESS: Mapping values insert worked');
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['cus_SdyDj0TflO21YK']);
        } catch (error) {
            console.log('   FAILED:', error.message);
        }
        
        // Test 3: Check if the issue is with empty strings
        console.log('\n3. Testing insert with empty strings...');
        try {
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_empty']);
            const result = db.run(
                'INSERT INTO customer_mappings (stripe_customer_id, stripe_customer_email, stripe_customer_name, halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed) VALUES (?, ?, ?, ?, ?, ?, ?)',
                ['test_empty', '', '', 888, '', 0, 1]
            );
            console.log('   SUCCESS: Empty strings insert worked');
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_empty']);
        } catch (error) {
            console.log('   FAILED:', error.message);
        }
        
        // Test 4: Test the exact query from our server
        console.log('\n4. Testing exact server query...');
        try {
            // Get the actual customer data
            const stripeCustomer = db.get('SELECT name, email FROM stripe_customers WHERE stripe_id = ?', ['cus_SdyDj0TflO21YK']);
            const haloClient = db.get('SELECT name FROM halopsa_clients WHERE halopsa_id = ?', [733]);
            
            console.log('   Stripe customer:', stripeCustomer);
            console.log('   Halo client:', haloClient);
            
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['cus_SdyDj0TflO21YK']);
            const result = db.run(
                `INSERT INTO customer_mappings 
                 (stripe_customer_id, stripe_customer_email, stripe_customer_name, 
                  halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [
                    'cus_SdyDj0TflO21YK',
                    stripeCustomer?.email || '',
                    stripeCustomer?.name || '',
                    733,
                    haloClient?.name || '',
                    0,
                    1
                ]
            );
            console.log('   SUCCESS: Exact server query worked');
            db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['cus_SdyDj0TflO21YK']);
        } catch (error) {
            console.log('   FAILED:', error.message);
        }
        
        console.log('\n=== Testing Complete ===');
        db.close();
        
    } catch (error) {
        console.error('Error during testing:', error);
    }
}

testSimpleInsert();