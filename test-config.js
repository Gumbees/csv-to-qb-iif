const DatabaseManager = require('./src/database');
const ConfigAPI = require('./src/config-api');

async function testConfigAPI() {
    console.log('=== Testing Configuration API ===');
    
    try {
        // Create database instance
        const db = new DatabaseManager();
        await db.ready;
        
        // Create config API instance
        const configAPI = new ConfigAPI(db);
        
        console.log('✅ Database and Config API initialized successfully');
        
        // Test 1: Test system status
        console.log('\n=== Testing System Status ===');
        const status = await configAPI.getSystemStatus();
        console.log('System Status:', JSON.stringify(status, null, 2));
        
        // Test 2: Test configuration parsing
        console.log('\n=== Testing Configuration Value Parsing ===');
        
        // Test boolean parsing
        const boolResult = configAPI.parseConfigValue('true', 'boolean');
        console.log('Boolean parsing "true":', boolResult);
        
        // Test number parsing
        const numResult = configAPI.parseConfigValue('123.45', 'number');
        console.log('Number parsing "123.45":', numResult);
        
        // Test JSON parsing
        const jsonResult = configAPI.parseConfigValue('{"key": "value"}', 'json');
        console.log('JSON parsing:', jsonResult);
        
        // Test 3: Test configuration validation
        console.log('\n=== Testing Configuration Validation ===');
        
        // Test required field validation
        const requiredValidation = configAPI.validateConfig('test_key', '', 'text', null);
        console.log('Required field validation:', requiredValidation);
        
        // Test number validation
        const numberValidation = configAPI.validateConfig('test_key', 'not-a-number', 'number', null);
        console.log('Number validation:', numberValidation);
        
        // Test 4: Test feature configuration
        console.log('\n=== Testing Feature Configuration ===');
        
        const stripeConfig = await configAPI.getFeatureConfig('stripe');
        console.log('Stripe config keys:', stripeConfig.map(c => c.key));
        
        const qbConfig = await configAPI.getFeatureConfig('quickbooks');
        console.log('QuickBooks config keys:', qbConfig.map(c => c.key));
        
        console.log('\n✅ All Configuration API tests completed successfully!');
        
    } catch (error) {
        console.error('❌ Configuration API test failed:', error);
    }
}

// Run the test
testConfigAPI();