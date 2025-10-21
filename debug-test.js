// Quick debug script to test database and new features
const Database = require('./src/database');
const StripeOAuth = require('./src/stripe-oauth');
const StripeAPI = require('./src/stripe-api');

async function debugTest() {
  console.log('🔍 Starting debug tests...\n');

  // Test 1: Database initialization
  console.log('1. Testing database initialization...');
  try {
    const db = new Database();
    console.log('✅ Database initialized successfully');

    // Test new tables exist
    const tables = await new Promise((resolve, reject) => {
      db.db.all("SELECT name FROM sqlite_master WHERE type='table'", [], (err, rows) => {
        if (err) reject(err);
        else resolve(rows.map(r => r.name));
      });
    });

    console.log('📋 Available tables:', tables.join(', '));

    const expectedTables = [
      'customers', 'purchase_orders', 'invoices',
      'stripe_oauth_tokens', 'stripe_transactions',
      'inventory_items', 'sync_status'
    ];

    const missingTables = expectedTables.filter(t => !tables.includes(t));
    if (missingTables.length === 0) {
      console.log('✅ All new tables created successfully');
    } else {
      console.log('⚠️  Missing tables:', missingTables.join(', '));
    }

    // Test 2: Stripe OAuth initialization
    console.log('\n2. Testing Stripe OAuth initialization...');
    const stripeOAuth = new StripeOAuth(db);
    console.log('✅ Stripe OAuth initialized');

    // Test 3: Stripe API initialization
    console.log('\n3. Testing Stripe API initialization...');
    const stripeAPI = new StripeAPI(db);
    console.log('✅ Stripe API initialized');

    // Test 4: Check dashboard stats still work
    console.log('\n4. Testing dashboard stats...');
    const stats = await db.getDashboardStats();
    console.log('✅ Dashboard stats:', stats);

    // Test 5: OAuth URL generation (without opening browser)
    console.log('\n5. Testing OAuth URL generation...');
    const authUrl = stripeOAuth.generateAuthUrl(['read_only']);
    console.log('✅ OAuth URL generated:', authUrl.substring(0, 100) + '...');

    db.close();
    console.log('\n🎉 All debug tests passed!');

  } catch (error) {
    console.error('❌ Debug test failed:', error.message);
    console.error('Stack:', error.stack);
  }
}

// Run if called directly
if (require.main === module) {
  debugTest();
}

module.exports = debugTest;