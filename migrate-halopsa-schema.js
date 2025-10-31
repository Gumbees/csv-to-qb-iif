/**
 * Database Schema Migration Script
 * Adds missing columns to halopsa_invoices and halopsa_purchase_orders tables
 *
 * Run with: node migrate-halopsa-schema.js
 */

const Database = require('./src/database.js');

(async () => {
    try {
        const db = new Database();
        await db.ready;

        console.log('\n=== Starting HaloPSA Schema Migration ===\n');

        // Migration for halopsa_invoices
        console.log('📋 Migrating halopsa_invoices table...');

        const invoiceColumns = [
            'ALTER TABLE halopsa_invoices ADD COLUMN balance_due REAL DEFAULT 0',
            'ALTER TABLE halopsa_invoices ADD COLUMN subtotal REAL DEFAULT 0',
            'ALTER TABLE halopsa_invoices ADD COLUMN tax_amount REAL DEFAULT 0',
            'ALTER TABLE halopsa_invoices ADD COLUMN tax_rate REAL DEFAULT 0',
            'ALTER TABLE halopsa_invoices ADD COLUMN discount_amount REAL DEFAULT 0',
            'ALTER TABLE halopsa_invoices ADD COLUMN payment_status TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN currency TEXT DEFAULT "USD"',
            'ALTER TABLE halopsa_invoices ADD COLUMN payment_terms TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN payment_method TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN last_payment_date DATETIME',
            'ALTER TABLE halopsa_invoices ADD COLUMN reference_number TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN po_number TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN notes TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN line_items TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN tax_details TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN payment_history TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN custom_fields TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN sent_date DATETIME',
            'ALTER TABLE halopsa_invoices ADD COLUMN approved_date DATETIME',
            'ALTER TABLE halopsa_invoices ADD COLUMN approved_by TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN created_by TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN modified_by TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN modified_date DATETIME',
            'ALTER TABLE halopsa_invoices ADD COLUMN qb_txn_id TEXT',
            'ALTER TABLE halopsa_invoices ADD COLUMN synced_to_qb BOOLEAN DEFAULT FALSE',
            'ALTER TABLE halopsa_invoices ADD COLUMN sync_error TEXT'
        ];

        let invoiceCount = 0;
        for (const sql of invoiceColumns) {
            try {
                db.run(sql);
                invoiceCount++;
                console.log(`  ✓ Added column: ${sql.match(/ADD COLUMN (\w+)/)[1]}`);
            } catch (error) {
                if (error.message.includes('duplicate column name')) {
                    // Column already exists, skip
                    console.log(`  ⊗ Column already exists: ${sql.match(/ADD COLUMN (\w+)/)[1]}`);
                } else {
                    throw error;
                }
            }
        }

        console.log(`✅ halopsa_invoices: Added ${invoiceCount} new columns\n`);

        // Migration for halopsa_purchase_orders
        console.log('📋 Migrating halopsa_purchase_orders table...');

        const poColumns = [
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN vendor_id INTEGER',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN vendor_email TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN vendor_phone TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN expected_delivery DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN actual_delivery DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN ordered_date DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN received_date DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN approved_date DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN modified_date DATETIME',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN paid_amount REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN balance_due REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN subtotal REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN tax_amount REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN tax_rate REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN shipping_cost REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN discount_amount REAL DEFAULT 0',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN approval_status TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN currency TEXT DEFAULT "USD"',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN payment_terms TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN payment_method TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN reference_number TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN requisition_number TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN contract_number TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN invoice_id TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN shipping_address TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN shipping_method TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN tracking_number TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN freight_terms TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN billing_address TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN notes TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN internal_notes TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN shipping_notes TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN line_items TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN custom_fields TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN approved_by TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN created_by TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN modified_by TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN ordered_by TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN received_by TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN qb_txn_id TEXT',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN synced_to_qb BOOLEAN DEFAULT FALSE',
            'ALTER TABLE halopsa_purchase_orders ADD COLUMN sync_error TEXT'
        ];

        let poCount = 0;
        for (const sql of poColumns) {
            try {
                db.run(sql);
                poCount++;
                console.log(`  ✓ Added column: ${sql.match(/ADD COLUMN (\w+)/)[1]}`);
            } catch (error) {
                if (error.message.includes('duplicate column name')) {
                    // Column already exists, skip
                    console.log(`  ⊗ Column already exists: ${sql.match(/ADD COLUMN (\w+)/)[1]}`);
                } else {
                    throw error;
                }
            }
        }

        console.log(`✅ halopsa_purchase_orders: Added ${poCount} new columns\n`);

        // Verify final schema
        console.log('📊 Verifying final schema...');
        const invoiceColsAfter = db.all('PRAGMA table_info(halopsa_invoices)');
        const poColsAfter = db.all('PRAGMA table_info(halopsa_purchase_orders)');

        console.log(`  • halopsa_invoices: ${invoiceColsAfter.length} total columns`);
        console.log(`  • halopsa_purchase_orders: ${poColsAfter.length} total columns`);

        db.close();

        console.log('\n✅ Migration completed successfully!\n');
        console.log('You can now retry the purchase order and invoice imports.\n');

        process.exit(0);
    } catch (error) {
        console.error('\n❌ Migration failed:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
})();
