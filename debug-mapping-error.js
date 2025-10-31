const Database = require('./src/database');

async function debugMappingError() {
    console.log('=== Debugging Customer Mapping Error ===\n');
    
    try {
        const db = new Database();
        
        // Test the exact values from the error
        const stripeCustomerId = 'cus_SdyDj0TflO21YK';
        const halopsaClientId = '733';
        
        console.log('1. Testing data types...');
        console.log('   stripeCustomerId type:', typeof stripeCustomerId, 'value:', stripeCustomerId);
        console.log('   halopsaClientId type:', typeof halopsaClientId, 'value:', halopsaClientId);
        
        // Test different numeric conversions
        console.log('\n2. Testing numeric conversions...');
        const numParseInt = Number.parseInt(halopsaClientId);
        const numParseFloat = Number.parseFloat(halopsaClientId);
        const mathFloor = Math.floor(Number(halopsaClientId));
        const numberConstructor = Number(halopsaClientId);
        
        console.log('   Number.parseInt:', numParseInt, 'type:', typeof numParseInt);
        console.log('   Number.parseFloat:', numParseFloat, 'type:', typeof numParseFloat);
        console.log('   Math.floor(Number()):', mathFloor, 'type:', typeof mathFloor);
        console.log('   Number():', numberConstructor, 'type:', typeof numberConstructor);
        
        // Check database schema
        console.log('\n3. Checking database schema...');
        const schema = db.all("PRAGMA table_info(customer_mappings)");
        schema.forEach(col => {
            console.log(`   Column: ${col.name}, Type: ${col.type}, NotNull: ${col.notnull}`);
        });
        
        // Test with different value types
        console.log('\n4. Testing INSERT with different value types...');
        const testValues = [
            { name: 'String "733"', value: '733' },
            { name: 'Number 733', value: 733 },
            { name: 'Float 733.0', value: 733.0 },
            { name: 'Parsed Int', value: Number.parseInt('733') },
            { name: 'Math.floor', value: Math.floor(Number('733')) }
        ];
        
        for (const test of testValues) {
            try {
                // First delete any existing test record
                db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_' + test.name]);
                
                const result = db.run(
                    `INSERT INTO customer_mappings 
                     (stripe_customer_id, stripe_customer_email, stripe_customer_name, 
                      halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    [
                        'test_' + test.name,
                        'test@example.com',
                        'Test Customer',
                        test.value,
                        'Test Halo Client',
                        false,
                        true
                    ]
                );
                console.log(`   ${test.name}: SUCCESS`);
                
                // Clean up
                db.run('DELETE FROM customer_mappings WHERE stripe_customer_id = ?', ['test_' + test.name]);
            } catch (error) {
                console.log(`   ${test.name}: FAILED - ${error.message}`);
            }
        }
        
        console.log('\n=== Debugging Complete ===');
        db.close();
        
    } catch (error) {
        console.error('Error during debugging:', error);
    }
}

debugMappingError();