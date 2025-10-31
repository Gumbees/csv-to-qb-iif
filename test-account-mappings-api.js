/**
 * Test script for QuickBooks Account Mappings API
 *
 * Tests all CRUD operations for account mappings:
 * - GET /api/qbd/mappings (list all)
 * - GET /api/qbd/mappings/:type (get specific)
 * - POST /api/qbd/mappings/:type (save/update)
 * - DELETE /api/qbd/mappings/:type (remove)
 */

const BASE_URL = 'http://localhost:3000';

async function testAPI() {
    console.log('\n=== Testing QuickBooks Account Mappings API ===\n');

    try {
        // Test 1: Get all account mappings
        console.log('1. GET /api/qbd/mappings (all mappings)');
        const allMappingsRes = await fetch(`${BASE_URL}/api/qbd/mappings`);
        const allMappings = await allMappingsRes.json();
        console.log(`   Status: ${allMappingsRes.status}`);
        console.log(`   Count: ${allMappings.count}`);
        console.log(`   Mappings: ${allMappings.mappings.map(m => m.mapping_type).join(', ')}`);
        console.log('');

        // Test 2: Get specific mapping (customer_deposits)
        console.log('2. GET /api/qbd/mappings/customer_deposits');
        const singleMappingRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`);
        const singleMapping = await singleMappingRes.json();
        console.log(`   Status: ${singleMappingRes.status}`);
        if (singleMapping.success) {
            console.log(`   Type: ${singleMapping.mapping.mapping_type}`);
            console.log(`   QB Account ID: ${singleMapping.mapping.qb_account_id || 'Not set'}`);
            console.log(`   QB Account Name: ${singleMapping.mapping.qb_account_name || 'Not set'}`);
            console.log(`   Description: ${singleMapping.mapping.description}`);
        } else {
            console.log(`   Error: ${singleMapping.message}`);
        }
        console.log('');

        // Test 3: Try to get non-existent mapping
        console.log('3. GET /api/qbd/mappings/nonexistent_type (should 404)');
        const notFoundRes = await fetch(`${BASE_URL}/api/qbd/mappings/nonexistent_type`);
        const notFound = await notFoundRes.json();
        console.log(`   Status: ${notFoundRes.status}`);
        console.log(`   Success: ${notFound.success}`);
        console.log(`   Message: ${notFound.message}`);
        console.log('');

        // Test 4: Get all QB accounts (to find a valid account ID for testing)
        console.log('4. GET /api/qbd/accounts (to get test account)');
        const accountsRes = await fetch(`${BASE_URL}/api/qbd/accounts`);
        const accountsData = await accountsRes.json();
        console.log(`   Status: ${accountsRes.status}`);

        if (accountsData.success && accountsData.accounts && accountsData.accounts.length > 0) {
            const testAccount = accountsData.accounts[0];
            console.log(`   Found test account: ${testAccount.account_name} (ID: ${testAccount.id}, Type: ${testAccount.account_type})`);
            console.log('');

            // Test 5: Save account mapping
            console.log(`5. POST /api/qbd/mappings/customer_deposits (set to account ID ${testAccount.id})`);
            const saveRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ qb_account_id: testAccount.id })
            });
            const saveResult = await saveRes.json();
            console.log(`   Status: ${saveRes.status}`);
            console.log(`   Success: ${saveResult.success}`);
            console.log(`   Message: ${saveResult.message}`);
            if (saveResult.mapping) {
                console.log(`   Mapped to: ${saveResult.mapping.qb_account_display_name} (${saveResult.mapping.account_type})`);
            }
            console.log('');

            // Test 6: Verify the mapping was saved
            console.log('6. GET /api/qbd/mappings/customer_deposits (verify save)');
            const verifyRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`);
            const verify = await verifyRes.json();
            console.log(`   Status: ${verifyRes.status}`);
            console.log(`   QB Account: ${verify.mapping.qb_account_display_name || 'Not set'}`);
            console.log(`   Account Type: ${verify.mapping.account_type || 'N/A'}`);
            console.log('');

            // Test 7: Delete account mapping
            console.log('7. DELETE /api/qbd/mappings/customer_deposits (clear mapping)');
            const deleteRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`, {
                method: 'DELETE'
            });
            const deleteResult = await deleteRes.json();
            console.log(`   Status: ${deleteRes.status}`);
            console.log(`   Success: ${deleteResult.success}`);
            console.log(`   Message: ${deleteResult.message}`);
            console.log('');

            // Test 8: Verify the mapping was cleared
            console.log('8. GET /api/qbd/mappings/customer_deposits (verify delete)');
            const verifyClearRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`);
            const verifyClear = await verifyClearRes.json();
            console.log(`   Status: ${verifyClearRes.status}`);
            console.log(`   QB Account ID: ${verifyClear.mapping.qb_account_id || 'Cleared!'}`);
            console.log('');
        } else {
            console.log(`   No QB accounts found in database. Skipping save/delete tests.`);
            console.log(`   Run QBWC sync to populate qb_accounts table first.`);
            console.log('');
        }

        // Test 9: Try to save with invalid account ID
        console.log('9. POST /api/qbd/mappings/customer_deposits (invalid account ID 99999)');
        const invalidSaveRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ qb_account_id: 99999 })
        });
        const invalidSave = await invalidSaveRes.json();
        console.log(`   Status: ${invalidSaveRes.status}`);
        console.log(`   Success: ${invalidSave.success}`);
        console.log(`   Message: ${invalidSave.message}`);
        console.log('');

        // Test 10: Try to save without account ID
        console.log('10. POST /api/qbd/mappings/customer_deposits (missing account ID)');
        const missingSaveRes = await fetch(`${BASE_URL}/api/qbd/mappings/customer_deposits`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });
        const missingSave = await missingSaveRes.json();
        console.log(`   Status: ${missingSaveRes.status}`);
        console.log(`   Success: ${missingSave.success}`);
        console.log(`   Message: ${missingSave.message}`);
        console.log('');

        console.log('=== All tests completed! ===\n');

    } catch (error) {
        console.error('Error running tests:', error);
        console.log('\nMake sure the server is running: node src/server.js');
    }
}

// Run tests
testAPI();
