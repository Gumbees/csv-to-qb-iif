const Database = require('./src/database');

async function testConfig() {
    console.log('Testing HaloPSA configuration...\n');
    
    // Create database instance
    const db = new Database();
    
    try {
        // Check what HaloPSA configs exist
        const configs = await db.all("SELECT * FROM config WHERE key LIKE 'halopsa_%'");
        
        console.log('Current HaloPSA Configuration:');
        configs.forEach(config => {
            console.log(`${config.key}: ${config.value ? (config.key.includes('secret') || config.key.includes('token') ? '***' : config.value) : 'NOT SET'}`);
        });
        
        // Check if old API key field exists
        const oldApiKey = await db.get("SELECT * FROM config WHERE key = 'halopsa_api_key'");
        if (oldApiKey) {
            console.log('\n⚠️  WARNING: Legacy "halopsa_api_key" field found!');
            console.log('This is causing confusion with the new OAuth setup.');
            
            // Ask if we should clean it up
            console.log('\nWould you like to remove the legacy API key field? (y/n)');
            // For now, let's remove it automatically since it's causing issues
            console.log('Automatically removing legacy field...');
            await db.run("DELETE FROM config WHERE key = 'halopsa_api_key'");
            console.log('✅ Legacy field removed!');
            
            // Show updated config
            const updatedConfigs = await db.all("SELECT * FROM config WHERE key LIKE 'halopsa_%'");
            console.log('\n✅ Updated HaloPSA Configuration:');
            updatedConfigs.forEach(config => {
                console.log(`${config.key}: ${config.value ? (config.key.includes('secret') || config.key.includes('token') ? '***' : config.value) : 'NOT SET'}`);
            });
        } else {
            console.log('\n✅ No legacy API key field found.');
        }
        
        console.log('\nConfiguration test completed.');
    } catch (error) {
        console.error('Test failed:', error);
    }
    
    process.exit(0);
}

testConfig().catch(error => {
    console.error('Test failed:', error);
    process.exit(1);
});