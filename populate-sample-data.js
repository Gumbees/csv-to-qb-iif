const Database = require('./src/database');

async function populateSampleData() {
    console.log('=== Populating Sample Data for Customer View ===\n');
    
    try {
        const db = new Database();
        
        // Get some HaloPSA clients and Stripe customers to work with
        const haloClients = db.all('SELECT halopsa_id, name FROM halopsa_clients LIMIT 10');
        const stripeCustomers = db.all('SELECT stripe_id, name FROM stripe_customers WHERE name IS NOT NULL LIMIT 10');
        
        console.log(`Found ${haloClients.length} HaloPSA clients and ${stripeCustomers.length} Stripe customers`);
        
        // Clear existing sample data
        console.log('1. Clearing existing sample data...');
        db.run('DELETE FROM stripe_invoices WHERE invoice_id LIKE ?', ['sample_%']);
        db.run('DELETE FROM halopsa_transactions WHERE transaction_id LIKE ?', ['sample_%']);
        db.run('DELETE FROM purchase_orders WHERE po_number LIKE ?', ['sample_%']);
        db.run('DELETE FROM client_items WHERE item_id LIKE ?', ['sample_%']);
        
        // Populate stripe_invoices for mapped customers
        console.log('2. Populating sample Stripe invoices...');
        const stripeMappings = db.all('SELECT * FROM customer_mappings WHERE mapping_confirmed = 1 LIMIT 5');
        
        stripeMappings.forEach((mapping, index) => {
            for (let i = 1; i <= 5; i++) {
                const invoiceId = `sample_inv_${mapping.halopsa_client_id}_${i}`;
                const amount = Math.random() * 1000 + 100; // $100-$1100
                const status = i % 3 === 0 ? 'paid' : i % 3 === 1 ? 'pending' : 'failed';
                
                db.run(
                    `INSERT OR IGNORE INTO stripe_invoices 
                     (invoice_id, customer_id, amount, currency, status, created, due_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    [
                        invoiceId,
                        mapping.stripe_customer_id,
                        amount.toFixed(2),
                        'USD',
                        status,
                        new Date(Date.now() - i * 7 * 24 * 60 * 60 * 1000).toISOString(), // Weeks ago
                        new Date(Date.now() + (30 - i) * 24 * 60 * 60 * 1000).toISOString() // Future dates
                    ]
                );
            }
        });
        console.log(`   Added ${stripeMappings.length * 5} sample invoices`);
        
        // Populate halopsa_transactions for all clients
        console.log('3. Populating sample HaloPSA transactions...');
        haloClients.forEach((client, index) => {
            for (let i = 1; i <= 8; i++) {
                const transactionId = `sample_txn_${client.halopsa_id}_${i}`;
                const type = i % 4 === 0 ? 'invoice' : i % 4 === 1 ? 'payment' : i % 4 === 2 ? 'refund' : 'adjustment';
                const amount = Math.random() * 500 + 50; // $50-$550
                const status = i % 3 === 0 ? 'completed' : i % 3 === 1 ? 'pending' : 'cancelled';
                
                db.run(
                    `INSERT OR IGNORE INTO halopsa_transactions 
                     (transaction_id, client_id, type, amount, date, description, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    [
                        transactionId,
                        client.halopsa_id,
                        type,
                        amount.toFixed(2),
                        new Date(Date.now() - i * 3 * 24 * 60 * 60 * 1000).toISOString(), // Days ago
                        `${type.charAt(0).toUpperCase() + type.slice(1)} for services`,
                        status
                    ]
                );
            }
        });
        console.log(`   Added ${haloClients.length * 8} sample transactions`);
        
        // Populate purchase_orders
        console.log('4. Populating sample purchase orders...');
        haloClients.forEach((client, index) => {
            for (let i = 1; i <= 3; i++) {
                const poNumber = `sample_po_${client.halopsa_id}_${i}`;
                const vendors = ['Dental Supply Co', 'Office Depot', 'Tech Solutions Inc', 'Medical Equipment Ltd'];
                const amount = Math.random() * 2000 + 200; // $200-$2200
                const status = i === 1 ? 'delivered' : i === 2 ? 'shipped' : 'ordered';
                
                db.run(
                    `INSERT OR IGNORE INTO purchase_orders 
                     (po_number, client_id, vendor, amount, status, created_at, expected_delivery, items)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                    [
                        poNumber,
                        client.halopsa_id,
                        vendors[index % vendors.length],
                        amount.toFixed(2),
                        status,
                        new Date(Date.now() - i * 30 * 24 * 60 * 60 * 1000).toISOString(), // Months ago
                        new Date(Date.now() + (90 - i * 30) * 24 * 60 * 60 * 1000).toISOString(),
                        JSON.stringify([
                            { name: 'Dental Supplies', quantity: 10, price: (amount * 0.3).toFixed(2) },
                            { name: 'Office Materials', quantity: 5, price: (amount * 0.2).toFixed(2) }
                        ])
                    ]
                );
            }
        });
        console.log(`   Added ${haloClients.length * 3} sample purchase orders`);
        
        // Populate client_items
        console.log('5. Populating sample service items...');
        const serviceItems = [
            { name: 'Teeth Cleaning', type: 'preventive' },
            { name: 'Dental Exam', type: 'diagnostic' },
            { name: 'Filling', type: 'restorative' },
            { name: 'Crown', type: 'restorative' },
            { name: 'Root Canal', type: 'endodontic' },
            { name: 'Extraction', type: 'surgical' },
            { name: 'Whitening', type: 'cosmetic' },
            { name: 'Braces', type: 'orthodontic' }
        ];
        
        haloClients.forEach((client, clientIndex) => {
            serviceItems.forEach((item, itemIndex) => {
                if (Math.random() > 0.3) { // 70% chance each client has this item
                    const usageCount = Math.floor(Math.random() * 10) + 1;
                    const lastUsed = new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000);
                    
                    db.run(
                        `INSERT OR IGNORE INTO client_items 
                         (client_id, item_id, item_name, item_type, last_used, usage_count)
                         VALUES (?, ?, ?, ?, ?, ?)`,
                        [
                            client.halopsa_id,
                            `sample_item_${client.halopsa_id}_${itemIndex}`,
                            item.name,
                            item.type,
                            lastUsed.toISOString(),
                            usageCount
                        ]
                    );
                }
            });
        });
        console.log(`   Added sample service items for ${haloClients.length} clients`);
        
        // Verify the data
        console.log('\n6. Verifying sample data...');
        const invoiceCount = db.get('SELECT COUNT(*) as count FROM stripe_invoices WHERE invoice_id LIKE ?', ['sample_%']).count;
        const transactionCount = db.get('SELECT COUNT(*) as count FROM halopsa_transactions WHERE transaction_id LIKE ?', ['sample_%']).count;
        const poCount = db.get('SELECT COUNT(*) as count FROM purchase_orders WHERE po_number LIKE ?', ['sample_%']).count;
        const itemCount = db.get('SELECT COUNT(*) as count FROM client_items WHERE item_id LIKE ?', ['sample_%']).count;
        
        console.log(`   Stripe Invoices: ${invoiceCount}`);
        console.log(`   HaloPSA Transactions: ${transactionCount}`);
        console.log(`   Purchase Orders: ${poCount}`);
        console.log(`   Service Items: ${itemCount}`);
        
        console.log('\n=== Sample Data Population Complete ===');
        console.log('\nThe customer view should now display:');
        console.log('- Transaction history for mapped customers');
        console.log('- Purchase orders for all clients');
        console.log('- Service items with usage statistics');
        console.log('- Comprehensive financial overview');
        
        db.close();
        
    } catch (error) {
        console.error('Error populating sample data:', error);
    }
}

populateSampleData();