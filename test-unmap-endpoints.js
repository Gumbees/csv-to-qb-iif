/**
 * Test script for unmap API endpoints
 * Tests the four new endpoints:
 * - DELETE /api/customers/mappings/stripe/:halopsa_client_id
 * - DELETE /api/customers/mappings/qb/:halopsa_client_id
 * - POST /api/customers/mappings/stripe
 * - POST /api/customers/mappings/qb
 */

const BASE_URL = 'http://localhost:3000';

async function testUnmapEndpoints() {
    console.log('======================================');
    console.log('Testing Unmap API Endpoints');
    console.log('======================================\n');

    try {
        // 1. Get a sample HaloPSA client for testing
        console.log('1. Fetching HaloPSA clients...');
        const clientsResponse = await fetch(`${BASE_URL}/api/halopsa/clients`);
        const clients = await clientsResponse.json();

        if (!clients || clients.length === 0) {
            console.error('❌ No HaloPSA clients found. Please import clients first.');
            return;
        }

        const testClient = clients[0];
        console.log(`✅ Found client: ${testClient.name} (ID: ${testClient.halopsa_id})\n`);

        // 2. Get a Stripe customer for testing
        console.log('2. Fetching Stripe customers...');
        const stripeResponse = await fetch(`${BASE_URL}/api/stripe/customers`);
        const stripeCustomers = await stripeResponse.json();

        if (!stripeCustomers || stripeCustomers.length === 0) {
            console.error('❌ No Stripe customers found. Please import Stripe data first.');
            return;
        }

        const testStripeCustomer = stripeCustomers[0];
        console.log(`✅ Found Stripe customer: ${testStripeCustomer.name} (ID: ${testStripeCustomer.stripe_id})\n`);

        // 3. Create a Stripe mapping
        console.log('3. Creating Stripe → HaloPSA mapping...');
        const createStripeResponse = await fetch(`${BASE_URL}/api/customers/mappings/stripe`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                stripe_customer_id: testStripeCustomer.stripe_id,
                halopsa_client_id: testClient.halopsa_id
            })
        });

        const createStripeResult = await createStripeResponse.json();

        if (createStripeResponse.ok) {
            console.log(`✅ Stripe mapping created: ${createStripeResult.message}\n`);
        } else {
            console.log(`ℹ️ Stripe mapping response: ${createStripeResult.error || createStripeResult.message}\n`);
        }

        // 4. Create a QB mapping (simulated QB customer)
        console.log('4. Creating QB → HaloPSA mapping...');
        const testQBCustomerId = `QB-${testClient.halopsa_id}`;

        const createQBResponse = await fetch(`${BASE_URL}/api/customers/mappings/qb`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                qb_customer_id: testQBCustomerId,
                halopsa_client_id: testClient.halopsa_id
            })
        });

        const createQBResult = await createQBResponse.json();

        if (createQBResponse.ok) {
            console.log(`✅ QB mapping created: ${createQBResult.message}\n`);
        } else {
            console.log(`ℹ️ QB mapping response: ${createQBResult.error || createQBResult.message}\n`);
        }

        // 5. Verify both mappings exist
        console.log('5. Verifying mappings exist...');
        const mappingsResponse = await fetch(`${BASE_URL}/api/customers/mappings?page=1&pageSize=100`);
        const mappingsData = await mappingsResponse.json();
        const ourMapping = mappingsData.mappings?.find(m => m.halopsa_client_id === testClient.halopsa_id);

        if (ourMapping) {
            console.log('✅ Found mapping:');
            console.log(`   - Stripe: ${ourMapping.stripe_customer_id ? '✓' : '✗'}`);
            console.log(`   - QB: ${ourMapping.qb_customer_id ? '✓' : '✗'}`);
            console.log(`   - HaloPSA: ${ourMapping.halopsa_client_name}\n`);
        } else {
            console.log('⚠️ Mapping not found in list (may be on different page)\n');
        }

        // 6. Test removing Stripe mapping
        console.log('6. Testing DELETE Stripe mapping...');
        const deleteStripeResponse = await fetch(
            `${BASE_URL}/api/customers/mappings/stripe/${testClient.halopsa_id}`,
            { method: 'DELETE' }
        );

        const deleteStripeResult = await deleteStripeResponse.json();

        if (deleteStripeResponse.ok) {
            console.log(`✅ ${deleteStripeResult.message}`);
        } else {
            console.log(`❌ Failed: ${deleteStripeResult.error}`);
        }

        // 7. Verify Stripe mapping removed but QB remains
        console.log('7. Verifying Stripe removed but QB remains...');
        const mappingsResponse2 = await fetch(`${BASE_URL}/api/customers/mappings?page=1&pageSize=100`);
        const mappingsData2 = await mappingsResponse2.json();
        const ourMapping2 = mappingsData2.mappings?.find(m => m.halopsa_client_id === testClient.halopsa_id);

        if (ourMapping2) {
            console.log('✅ Mapping row still exists:');
            console.log(`   - Stripe: ${ourMapping2.stripe_customer_id ? '✓ (SHOULD BE ✗)' : '✗ (CORRECT)'}`);
            console.log(`   - QB: ${ourMapping2.qb_customer_id ? '✓ (CORRECT)' : '✗ (SHOULD BE ✓)'}\n`);
        } else {
            console.log('❌ Mapping row deleted (should have kept QB mapping)\n');
        }

        // 8. Test removing QB mapping (should delete entire row now)
        console.log('8. Testing DELETE QB mapping...');
        const deleteQBResponse = await fetch(
            `${BASE_URL}/api/customers/mappings/qb/${testClient.halopsa_id}`,
            { method: 'DELETE' }
        );

        const deleteQBResult = await deleteQBResponse.json();

        if (deleteQBResponse.ok) {
            console.log(`✅ ${deleteQBResult.message}`);
        } else {
            console.log(`❌ Failed: ${deleteQBResult.error}`);
        }

        // 9. Verify entire row deleted
        console.log('9. Verifying entire row deleted...');
        const mappingsResponse3 = await fetch(`${BASE_URL}/api/customers/mappings?page=1&pageSize=100`);
        const mappingsData3 = await mappingsResponse3.json();
        const ourMapping3 = mappingsData3.mappings?.find(m => m.halopsa_client_id === testClient.halopsa_id);

        if (ourMapping3) {
            console.log('❌ Mapping row still exists (should be deleted)\n');
        } else {
            console.log('✅ Mapping row deleted (correct)\n');
        }

        console.log('======================================');
        console.log('Test Complete!');
        console.log('======================================');

    } catch (error) {
        console.error('❌ Test failed with error:', error.message);
        console.error(error.stack);
    }
}

// Run the tests
testUnmapEndpoints().catch(console.error);
