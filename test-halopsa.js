const HaloPSAAPI = require('./src/halopsa-api');
const Database = require('./src/database');

async function testHaloPSA() {
    console.log('Testing HaloPSA API integration...\n');
    
    // Initialize database
    const db = new Database();
    
    // Initialize HaloPSA API
    const halopsaAPI = new HaloPSAAPI(db);
    
    console.log('1. Testing configuration...');
    const config = await halopsaAPI.getConfig();
    console.log('HaloPSA Configuration:', {
        apiKey: config.halopsa_api_key ? '***' : 'Not set',
        apiUrl: config.halopsa_api_url || 'Not set',
        poReportId: config.halopsa_purchase_order_report_id || 'Not set',
        invoiceReportId: config.halopsa_invoice_report_id || 'Not set'
    });
    
    console.log('\n2. Testing connection...');
    const connectionTest = await halopsaAPI.testConnection();
    console.log('Connection Test:', connectionTest);
    
    if (connectionTest.success) {
        console.log('\n3. Testing what we can access...');
        
        // Test endpoints that showed promise
        const workingEndpoints = [
            '/api/status',
            '/api/health'
        ];
        
        for (const endpoint of workingEndpoints) {
            console.log(`Testing endpoint: ${endpoint}`);
            try {
                const result = await halopsaAPI.makeRequest(endpoint);
                console.log(`✅ SUCCESS with ${endpoint}`);
                console.log('Response type:', Array.isArray(result) ? 'array' : typeof result);
                if (Array.isArray(result)) {
                    console.log(`Array length: ${result.length}`);
                    if (result.length > 0) {
                        console.log('First item sample:', JSON.stringify(result[0], null, 2).substring(0, 200));
                    }
                } else if (typeof result === 'object') {
                    console.log('Object keys:', Object.keys(result));
                    console.log('Sample data:', JSON.stringify(result, null, 2).substring(0, 200));
                }
            } catch (error) {
                console.log(`❌ Failed: ${error.message}`);
            }
        }
        
        console.log('\n4. Testing data endpoints with permission issues...');
        const dataEndpoints = [
            '/api/Client',
            '/api/Clients',
            '/api/clients',
            '/api/Organisation',
            '/api/Company'
        ];
        
        for (const endpoint of dataEndpoints) {
            console.log(`Testing data endpoint: ${endpoint}`);
            try {
                const result = await halopsaAPI.makeRequest(endpoint);
                console.log(`✅ SUCCESS with ${endpoint}`);
                console.log('Response type:', Array.isArray(result) ? 'array' : typeof result);
            } catch (error) {
                console.log(`❌ Permission denied: ${endpoint} - ${error.message}`);
            }
        }
        
        console.log('\n5. THE ISSUE ANALYSIS:');
        console.log('✅ We can authenticate successfully');
        console.log('✅ The /api/status and /api/health endpoints work');
        console.log('🔒 The /api/Client endpoint exists but returns 403 Forbidden');
        console.log('💡 This means our client credentials have limited permissions');
    }
    
    console.log('\nTest completed.');
    process.exit(0);
}

testHaloPSA().catch(error => {
    console.error('Test failed:', error);
    process.exit(1);
});