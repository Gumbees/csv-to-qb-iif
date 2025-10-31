const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class DatabaseManager {
  constructor() {
    try {
      // Use environment variable for db path or default to container path
      const dataDir = process.env.DB_PATH || '/usr/src/app/data';
      if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true });
      }
      
      const dbPath = path.join(dataDir, 'csv-to-qb-iif.db');
      this.db = new Database(dbPath, { verbose: console.log });
      this.ready = this.init();
      console.log(`Connected to database at: ${dbPath}`);
      console.log(`Database file exists: ${fs.existsSync(dbPath)}`);
      if (fs.existsSync(dbPath)) {
        console.log(`Database file size: ${fs.statSync(dbPath).size} bytes`);
      }
    } catch (e) {
      console.error('DB connection error:', e);
      throw e;
    }
  }

  async init() {
    try {
      // Create tables if not exists
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS csv_imports (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'filename TEXT NOT NULL,' +
          'csv_content BLOB NOT NULL,' +
          'file_size INTEGER NOT NULL,' +
          'import_date DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'processed BOOLEAN DEFAULT FALSE,' +
          'processed_date DATETIME,' +
          'checksum TEXT UNIQUE NOT NULL' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS transactions (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'csv_import_id INTEGER NOT NULL REFERENCES csv_imports(id) ON DELETE CASCADE,' +
          'vendor TEXT NOT NULL,' +
          'ref_number TEXT NOT NULL,' +
          'transaction_date TEXT NOT NULL,' +
          'total_amount REAL NOT NULL,' +
          'payment_terms TEXT,' +
          'due_date TEXT,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Additional tables for comprehensive customer view
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS stripe_invoices (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'invoice_id TEXT UNIQUE NOT NULL,' +
          'customer_id TEXT NOT NULL,' +
          'amount REAL,' +
          'currency TEXT,' +
          'status TEXT,' +
          'created DATETIME,' +
          'due_date DATETIME,' +
          'invoice_pdf TEXT,' +
          'raw_data TEXT' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS halopsa_transactions (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'transaction_id TEXT UNIQUE NOT NULL,' +
          'client_id INTEGER NOT NULL,' +
          'type TEXT,' +
          'amount REAL,' +
          'date DATETIME,' +
          'description TEXT,' +
          'status TEXT,' +
          'raw_data TEXT' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS purchase_orders (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'po_number TEXT UNIQUE NOT NULL,' +
          'client_id INTEGER NOT NULL,' +
          'vendor TEXT,' +
          'amount REAL,' +
          'status TEXT,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'expected_delivery DATETIME,' +
          'items TEXT' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS client_items (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'client_id INTEGER NOT NULL,' +
          'item_id TEXT NOT NULL,' +
          'item_name TEXT NOT NULL,' +
          'item_type TEXT,' +
          'last_used DATETIME,' +
          'usage_count INTEGER DEFAULT 0' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS config (' +
          'key TEXT PRIMARY KEY,' +
          'value TEXT NOT NULL,' +
          'type TEXT DEFAULT "text",' +
          'category TEXT DEFAULT "general",' +
          'label TEXT,' +
          'description TEXT,' +
          'options TEXT,' +
          'is_required BOOLEAN DEFAULT FALSE,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS qbd_accounts (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'name TEXT NOT NULL,' +
          'account_type TEXT NOT NULL,' +
          'quickbooks_id TEXT,' +
          'last_sync DATETIME,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS stripe_transactions (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'stripe_id TEXT UNIQUE NOT NULL,' +
          'customer_id TEXT,' +
          'amount INTEGER NOT NULL,' +
          'currency TEXT NOT NULL,' +
          'description TEXT,' +
          'status TEXT NOT NULL,' +
          'created INTEGER NOT NULL,' +
          'invoice_id TEXT,' +
          'payment_intent_id TEXT,' +
          'refunded BOOLEAN DEFAULT FALSE,' +
          'raw_data TEXT NOT NULL,' +
          'mapped_to_halo BOOLEAN DEFAULT FALSE,' +
          'mapped_to_qb BOOLEAN DEFAULT FALSE,' +
          'halopsa_invoice_id INTEGER,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Migration: Add halopsa_invoice_id column if it doesn't exist
      try {
        this.db.exec('ALTER TABLE stripe_transactions ADD COLUMN halopsa_invoice_id INTEGER');
        console.log('Added halopsa_invoice_id column to stripe_transactions');
      } catch (e) {
        // Column already exists, ignore
        if (!e.message.includes('duplicate column name')) {
          console.error('Error adding halopsa_invoice_id column:', e.message);
        }
      }

      // Migration: Add transaction_type column if it doesn't exist
      try {
        this.db.exec('ALTER TABLE stripe_transactions ADD COLUMN transaction_type TEXT DEFAULT "payment"');
        console.log('Added transaction_type column to stripe_transactions');
      } catch (e) {
        // Column already exists, ignore
        if (!e.message.includes('duplicate column name')) {
          console.error('Error adding transaction_type column:', e.message);
        }
      }

      // Migration: Add related_transaction_id column if it doesn't exist (for linking deposits to final payments)
      try {
        this.db.exec('ALTER TABLE stripe_transactions ADD COLUMN related_transaction_id INTEGER');
        console.log('Added related_transaction_id column to stripe_transactions');
      } catch (e) {
        // Column already exists, ignore
        if (!e.message.includes('duplicate column name')) {
          console.error('Error adding related_transaction_id column:', e.message);
        }
      }

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS stripe_customers (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'stripe_id TEXT UNIQUE NOT NULL,' +
          'email TEXT,' +
          'name TEXT,' +
          'description TEXT,' +
          'phone TEXT,' +
          'address_line1 TEXT,' +
          'address_line2 TEXT,' +
          'address_city TEXT,' +
          'address_state TEXT,' +
          'address_postal_code TEXT,' +
          'address_country TEXT,' +
          'created INTEGER NOT NULL,' +
          'metadata TEXT,' +
          'raw_data TEXT NOT NULL,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS customer_mappings (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'stripe_customer_id TEXT,' +
          'stripe_customer_email TEXT,' +
          'stripe_customer_name TEXT,' +
          'halopsa_client_id INTEGER,' +
          'halopsa_client_name TEXT,' +
          'qb_customer_id TEXT,' +
          'qb_customer_name TEXT,' +
          'auto_mapped BOOLEAN DEFAULT FALSE,' +
          'mapping_confirmed BOOLEAN DEFAULT FALSE,' +
          'mapping_source TEXT DEFAULT "manual",' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'UNIQUE(stripe_customer_id, halopsa_client_id, qb_customer_id)' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS transaction_invoice_mappings (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'stripe_transaction_id INTEGER NOT NULL,' +
          'halopsa_invoice_id INTEGER NOT NULL,' +
          'invoice_amount REAL,' +
          'auto_mapped BOOLEAN DEFAULT FALSE,' +
          'mapping_confidence REAL DEFAULT 1.0,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'UNIQUE(stripe_transaction_id, halopsa_invoice_id),' +
          'FOREIGN KEY(stripe_transaction_id) REFERENCES stripe_transactions(id),' +
          'FOREIGN KEY(halopsa_invoice_id) REFERENCES halopsa_invoices(id)' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS halopsa_clients (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'halopsa_id INTEGER UNIQUE NOT NULL,' +
          'name TEXT NOT NULL,' +
          'toplevel_id INTEGER,' +
          'toplevel_name TEXT,' +
          'inactive BOOLEAN DEFAULT FALSE,' +
          'colour TEXT,' +
          'email TEXT,' +
          'phone TEXT,' +
          'website TEXT,' +
          'notes TEXT,' +
          'address_line1 TEXT,' +
          'address_line2 TEXT,' +
          'address_city TEXT,' +
          'address_state TEXT,' +
          'address_postal_code TEXT,' +
          'address_country TEXT,' +
          'primary_contact_name TEXT,' +
          'primary_contact_email TEXT,' +
          'account_manager_id INTEGER,' +
          'account_manager_name TEXT,' +
          'hourly_rate REAL,' +
          'monthly_charge REAL,' +
          'prepay_balance REAL,' +
          'credit_balance REAL,' +
          'invoice_enabled BOOLEAN DEFAULT TRUE,' +
          'date_created DATETIME,' +
          'start_date DATETIME,' +
          'end_date DATETIME,' +
          'customer_type TEXT,' +
          'payment_terms INTEGER,' +
          'billing_email TEXT,' +
          'accounts_ref TEXT,' +
          'vip_customer BOOLEAN DEFAULT FALSE,' +
          'custom_fields TEXT,' +
          'raw_data TEXT NOT NULL,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS halopsa_purchase_orders (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'halopsa_id INTEGER UNIQUE NOT NULL,' +
          'po_number TEXT UNIQUE NOT NULL,' +
          'halopsa_client_id INTEGER,' +
          'client_name TEXT,' +
          'vendor_id INTEGER,' +
          'vendor_name TEXT,' +
          'vendor_email TEXT,' +
          'vendor_phone TEXT,' +
          'po_date DATETIME,' +
          'expected_delivery DATETIME,' +
          'actual_delivery DATETIME,' +
          'ordered_date DATETIME,' +
          'received_date DATETIME,' +
          'approved_date DATETIME,' +
          'modified_date DATETIME,' +
          'total_amount REAL,' +
          'paid_amount REAL DEFAULT 0,' +
          'balance_due REAL DEFAULT 0,' +
          'subtotal REAL DEFAULT 0,' +
          'tax_amount REAL DEFAULT 0,' +
          'tax_rate REAL DEFAULT 0,' +
          'shipping_cost REAL DEFAULT 0,' +
          'discount_amount REAL DEFAULT 0,' +
          'status TEXT,' +
          'approval_status TEXT,' +
          'currency TEXT DEFAULT "USD",' +
          'payment_terms TEXT,' +
          'payment_method TEXT,' +
          'reference_number TEXT,' +
          'requisition_number TEXT,' +
          'contract_number TEXT,' +
          'invoice_id TEXT,' +
          'shipping_address TEXT,' +
          'shipping_method TEXT,' +
          'tracking_number TEXT,' +
          'freight_terms TEXT,' +
          'billing_address TEXT,' +
          'notes TEXT,' +
          'internal_notes TEXT,' +
          'shipping_notes TEXT,' +
          'line_items TEXT,' +
          'custom_fields TEXT,' +
          'approved_by TEXT,' +
          'created_by TEXT,' +
          'modified_by TEXT,' +
          'ordered_by TEXT,' +
          'received_by TEXT,' +
          'qb_txn_id TEXT,' +
          'synced_to_qb BOOLEAN DEFAULT FALSE,' +
          'sync_error TEXT,' +
          'last_sync_attempt DATETIME,' +
          'raw_data TEXT NOT NULL,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Migration: Add last_sync_attempt column if it doesn't exist
      try {
        this.db.exec('ALTER TABLE halopsa_purchase_orders ADD COLUMN last_sync_attempt DATETIME');
        console.log('Added last_sync_attempt column to halopsa_purchase_orders');
      } catch (e) {
        // Column already exists, ignore
        if (!e.message.includes('duplicate column name')) {
          console.error('Error adding last_sync_attempt column:', e.message);
        }
      }

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS halopsa_invoices (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'halopsa_id INTEGER UNIQUE NOT NULL,' +
          'invoice_number TEXT UNIQUE NOT NULL,' +
          'halopsa_client_id INTEGER,' +
          'client_name TEXT,' +
          'invoice_date DATETIME,' +
          'due_date DATETIME,' +
          'total_amount REAL,' +
          'paid_amount REAL DEFAULT 0,' +
          'balance_due REAL DEFAULT 0,' +
          'subtotal REAL DEFAULT 0,' +
          'tax_amount REAL DEFAULT 0,' +
          'tax_rate REAL DEFAULT 0,' +
          'discount_amount REAL DEFAULT 0,' +
          'status TEXT,' +
          'payment_status TEXT,' +
          'currency TEXT DEFAULT "USD",' +
          'payment_terms TEXT,' +
          'payment_method TEXT,' +
          'last_payment_date DATETIME,' +
          'reference_number TEXT,' +
          'po_number TEXT,' +
          'notes TEXT,' +
          'line_items TEXT,' +
          'tax_details TEXT,' +
          'payment_history TEXT,' +
          'custom_fields TEXT,' +
          'sent_date DATETIME,' +
          'approved_date DATETIME,' +
          'approved_by TEXT,' +
          'created_by TEXT,' +
          'modified_by TEXT,' +
          'modified_date DATETIME,' +
          'stripe_transaction_id TEXT,' +
          'qb_txn_id TEXT,' +
          'synced_to_qb BOOLEAN DEFAULT FALSE,' +
          'sync_error TEXT,' +
          'raw_data TEXT NOT NULL,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS qb_customers (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'qb_list_id TEXT UNIQUE,' +
          'qb_edit_sequence TEXT,' +
          'qb_full_name TEXT NOT NULL,' +
          'company_name TEXT,' +
          'first_name TEXT,' +
          'last_name TEXT,' +
          'email TEXT,' +
          'phone TEXT,' +
          'fax TEXT,' +
          'billing_address_line1 TEXT,' +
          'billing_address_line2 TEXT,' +
          'billing_address_city TEXT,' +
          'billing_address_state TEXT,' +
          'billing_address_postal_code TEXT,' +
          'billing_address_country TEXT,' +
          'shipping_address_line1 TEXT,' +
          'shipping_address_line2 TEXT,' +
          'shipping_address_city TEXT,' +
          'shipping_address_state TEXT,' +
          'shipping_address_postal_code TEXT,' +
          'shipping_address_country TEXT,' +
          'contact_name TEXT,' +
          'account_number TEXT,' +
          'is_active BOOLEAN DEFAULT TRUE,' +
          'balance REAL DEFAULT 0,' +
          'total_balance REAL DEFAULT 0,' +
          'sales_tax_code TEXT,' +
          'payment_terms TEXT,' +
          'credit_limit REAL,' +
          'notes TEXT,' +
          'raw_qbxml TEXT,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS qb_items (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'qb_list_id TEXT UNIQUE,' +
          'qb_edit_sequence TEXT,' +
          'item_type TEXT NOT NULL,' +
          'name TEXT NOT NULL,' +
          'full_name TEXT,' +
          'description TEXT,' +
          'sales_description TEXT,' +
          'purchase_description TEXT,' +
          'sales_price REAL,' +
          'purchase_cost REAL,' +
          'quantity_on_hand REAL DEFAULT 0,' +
          'income_account TEXT,' +
          'cogs_account TEXT,' +
          'asset_account TEXT,' +
          'expense_account TEXT,' +
          'tax_code TEXT,' +
          'is_active BOOLEAN DEFAULT TRUE,' +
          'halopsa_item_id TEXT,' +
          'source_system TEXT,' +
          'synced_to_qb BOOLEAN DEFAULT FALSE,' +
          'sync_error TEXT,' +
          'raw_qbxml TEXT,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'UNIQUE(name, item_type)' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS qb_item_sync_log (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'qb_item_id INTEGER,' +
          'item_name TEXT NOT NULL,' +
          'item_type TEXT NOT NULL,' +
          'sync_action TEXT NOT NULL,' +
          'sync_status TEXT NOT NULL,' +
          'error_message TEXT,' +
          'qbxml_request TEXT,' +
          'qbxml_response TEXT,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS qb_accounts (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'qb_list_id TEXT UNIQUE,' +
          'qb_edit_sequence TEXT,' +
          'account_name TEXT NOT NULL,' +
          'fully_qualified_name TEXT,' +
          'account_type TEXT NOT NULL,' +
          'account_number TEXT,' +
          'description TEXT,' +
          'is_active BOOLEAN DEFAULT TRUE,' +
          'balance REAL DEFAULT 0,' +
          'special_account_type TEXT,' +
          'bank_number TEXT,' +
          'parent_ref_list_id TEXT,' +
          'parent_ref_full_name TEXT,' +
          'sublevel INTEGER DEFAULT 0,' +
          'raw_qbxml TEXT,' +
          'last_sync DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS account_mappings (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'mapping_type TEXT NOT NULL,' +
          'qb_account_id INTEGER,' +
          'qb_account_name TEXT,' +
          'description TEXT,' +
          'is_active BOOLEAN DEFAULT TRUE,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'UNIQUE(mapping_type),' +
          'FOREIGN KEY(qb_account_id) REFERENCES qb_accounts(id)' +
        ')'
      );

      // Create AI settings table
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS ai_settings (' +
          'key TEXT PRIMARY KEY,' +
          'value TEXT,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Create AI mapping suggestions table
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS ai_mapping_suggestions (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'suggestion_type TEXT NOT NULL,' +
          'source_type TEXT NOT NULL,' +
          'source_id INTEGER NOT NULL,' +
          'target_type TEXT NOT NULL,' +
          'target_id INTEGER NOT NULL,' +
          'confidence REAL NOT NULL,' +
          'reasoning TEXT,' +
          'status TEXT DEFAULT "pending",' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Insert default configuration only if they don't exist
      const defaultConfigs = [
        ['auto_match_threshold', '0.8', 'number', 'general', 'Auto-match Threshold', 'Confidence threshold for automatic matching', null, 1],
        ['duplicate_check_enabled', 'true', 'boolean', 'general', 'Duplicate Check', 'Enable duplicate transaction detection', null, 1],
        ['quickbooks_sync_method', 'iif', 'select', 'quickbooks', 'Sync Method', 'Method for syncing with QuickBooks', '["iif","qbwc"]', 1],
        ['quickbooks_default_account', 'Accounts Payable', 'text', 'quickbooks', 'Default Account', 'Default accounts payable account', null, 1],
        ['accounting_currency', 'USD', 'text', 'accounting', 'Currency', 'Default accounting currency', null, 1],
        ['accounting_timezone', 'UTC', 'text', 'accounting', 'Timezone', 'Accounting timezone', null, 1],
        ['qbwc_username', 'qbwc_user', 'text', 'qbwc', 'QBWC Username', 'Username for QuickBooks Web Connector', null, 1],
        ['qbwc_password', 'password123', 'password', 'qbwc', 'QBWC Password', 'Password for QuickBooks Web Connector', null, 1],
        ['qbwc_app_name', 'CSV to QuickBooks IIF Sync', 'text', 'qbwc', 'QBWC App Name', 'Application name shown in QuickBooks', null, 1],
        ['qbwc_sync_interval', '30', 'number', 'qbwc', 'Sync Interval (minutes)', 'How often QBWC should check for updates', null, 1],
        ['stripe_secret_key', '', 'password', 'stripe', 'Stripe Secret Key', 'Your Stripe secret API key from the dashboard', null, 1],
        ['stripe_webhook_secret', '', 'password', 'stripe', 'Stripe Webhook Secret', 'Webhook secret for Stripe events', null, 1],
        ['stripe_sync_enabled', 'false', 'boolean', 'stripe', 'Enable Stripe Sync', 'Automatically sync Stripe transactions', null, 1],
        ['stripe_sync_interval', '60', 'number', 'stripe', 'Sync Interval (minutes)', 'How often to check for new Stripe transactions', null, 1],
        ['stripe_default_currency', 'USD', 'text', 'stripe', 'Default Currency', 'Default currency for Stripe transactions', null, 1],
        ['halopsa_api_url', 'https://halo.dtctoday.com', 'text', 'halopsa', 'HaloPSA API URL', 'Base URL for HaloPSA API', null, 1],
        ['halopsa_client_id', '', 'text', 'halopsa', 'OAuth Client ID', 'HaloPSA OAuth client ID for authentication', null, 1],
        ['halopsa_client_secret', '', 'password', 'halopsa', 'OAuth Client Secret', 'HaloPSA OAuth client secret for authentication', null, 1],
        ['halopsa_access_token', '', 'password', 'halopsa', 'OAuth Access Token', 'HaloPSA OAuth access token for API calls', null, 0],
        ['halopsa_purchase_order_report_id', '', 'number', 'halopsa', 'Purchase Order Report ID', 'HaloPSA report ID for purchase orders export', null, 0],
        ['halopsa_invoice_report_id', '', 'number', 'halopsa', 'Invoice Report ID', 'HaloPSA report ID for invoices export', null, 0],
        ['customer_auto_match_enabled', 'true', 'boolean', 'mapping', 'Auto-match Customers', 'Automatically match Stripe customers to HaloPSA clients', null, 1],
        ['customer_match_threshold', '0.85', 'number', 'mapping', 'Match Threshold', 'Confidence threshold for customer matching', null, 1]
      ];

      // Check if config table has any existing values
      const existingConfigs = this.db.prepare('SELECT COUNT(*) as count FROM config').get();
      
      // Only insert defaults if the table is empty
      if (existingConfigs.count === 0) {
        console.log('Inserting default configuration values');
        const stmt = this.db.prepare(`
          INSERT INTO config (key, value, type, category, label, description, options, is_required)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `);

        for (const config of defaultConfigs) {
          stmt.run(config);
        }
      } else {
        console.log('Config table already has values, skipping default insert');
      }

      // Insert default account mappings only if they don't exist
      const existingMappings = this.db.prepare('SELECT COUNT(*) as count FROM account_mappings').get();

      if (existingMappings.count === 0) {
        console.log('Inserting default account mapping configurations');
        const defaultMappings = [
          ['stripe_revenue', null, null, 'Revenue account for Stripe payments', 1],
          ['halopsa_invoice_income', null, null, 'Income account for HaloPSA invoices', 1],
          ['purchase_order_expense', null, null, 'Expense account for purchase orders', 1],
          ['cost_of_goods', null, null, 'Cost of goods sold account', 1],
          ['accounts_payable', null, null, 'Accounts payable account for bills', 1],
          ['accounts_receivable', null, null, 'Accounts receivable account for invoices', 1],
          ['inventory_asset', null, null, 'Inventory asset account', 1],
          ['sales_tax', null, null, 'Sales tax payable account', 1],
          ['customer_deposits', null, null, 'Customer deposits / unearned revenue (liability account)', 1],
          ['stripe_processing_fees', null, null, 'Stripe processing fees expense account', 1]
        ];

        const mappingStmt = this.db.prepare(`
          INSERT INTO account_mappings (mapping_type, qb_account_id, qb_account_name, description, is_active)
          VALUES (?, ?, ?, ?, ?)
        `);

        for (const mapping of defaultMappings) {
          mappingStmt.run(mapping);
        }
      } else {
        console.log('Account mappings table already has values, skipping default insert');

        // Migration: Add customer_deposits and stripe_processing_fees if they don't exist
        const customerDepositsExists = this.db.prepare(
          'SELECT COUNT(*) as count FROM account_mappings WHERE mapping_type = ?'
        ).get(['customer_deposits']);

        if (customerDepositsExists.count === 0) {
          console.log('Migrating: Adding customer_deposits account mapping');
          this.db.prepare(
            'INSERT INTO account_mappings (mapping_type, qb_account_id, qb_account_name, description, is_active) VALUES (?, ?, ?, ?, ?)'
          ).run(['customer_deposits', null, null, 'Customer deposits / unearned revenue (liability account)', 1]);
        }

        const stripeFeesExists = this.db.prepare(
          'SELECT COUNT(*) as count FROM account_mappings WHERE mapping_type = ?'
        ).get(['stripe_processing_fees']);

        if (stripeFeesExists.count === 0) {
          console.log('Migrating: Adding stripe_processing_fees account mapping');
          this.db.prepare(
            'INSERT INTO account_mappings (mapping_type, qb_account_id, qb_account_name, description, is_active) VALUES (?, ?, ?, ?, ?)'
          ).run(['stripe_processing_fees', null, null, 'Stripe processing fees expense account', 1]);
        }

        // Migration: Add stripe_payments if it doesn't exist
        const stripePaymentsExists = this.db.prepare(
          'SELECT COUNT(*) as count FROM account_mappings WHERE mapping_type = ?'
        ).get(['stripe_payments']);

        if (stripePaymentsExists.count === 0) {
          console.log('Migrating: Adding stripe_payments account mapping');
          this.db.prepare(
            'INSERT INTO account_mappings (mapping_type, qb_account_id, qb_account_name, description, is_active) VALUES (?, ?, ?, ?, ?)'
          ).run(['stripe_payments', null, null, 'Bank or asset account where Stripe payments are deposited', 1]);
        }
      }

      // Check if we need to insert sample customer data
      const existingClients = this.db.prepare('SELECT COUNT(*) as count FROM halopsa_clients').get();
      if (existingClients.count === 0) {
        console.log('Inserting sample customer data');
        await this.insertSampleCustomerData();
      } else {
        console.log('HaloPSA clients table already has data, skipping sample insert');
      }

      // Insert default AI settings
      const existingAISettings = this.db.prepare('SELECT COUNT(*) as count FROM ai_settings').get();
      if (existingAISettings.count === 0) {
        console.log('Inserting default AI settings');
        const defaultAISettings = [
          ['ai_provider', 'anthropic'],
          ['anthropic_model', 'claude-sonnet-4-20250514'],
          ['openrouter_model', 'anthropic/claude-3.5-sonnet']
        ];
        for (const [key, value] of defaultAISettings) {
          this.db.prepare('INSERT INTO ai_settings (key, value) VALUES (?, ?)').run([key, value]);
        }
      } else {
        // Migration: Add ai_provider if it doesn't exist
        const providerExists = this.db.prepare('SELECT COUNT(*) as count FROM ai_settings WHERE key = ?').get(['ai_provider']);
        if (providerExists.count === 0) {
          console.log('Migrating: Adding ai_provider setting');
          this.db.prepare('INSERT INTO ai_settings (key, value) VALUES (?, ?)').run(['ai_provider', 'anthropic']);
        }

        // Migration: Add openrouter_model default if it doesn't exist
        const openrouterModelExists = this.db.prepare('SELECT COUNT(*) as count FROM ai_settings WHERE key = ?').get(['openrouter_model']);
        if (openrouterModelExists.count === 0) {
          console.log('Migrating: Adding openrouter_model setting');
          this.db.prepare('INSERT INTO ai_settings (key, value) VALUES (?, ?)').run(['openrouter_model', 'anthropic/claude-3.5-sonnet']);
        }
      }

      console.log('Database initialized successfully');
      return true;
    } catch (e) {
      console.error('Database initialization error:', e);
      throw e;
    }
  }

  async insertSampleCustomerData() {
    try {
      // Insert sample HaloPSA client data
      const clients = [
        { id: 1001, name: 'ABC Company Inc.', email: 'contact@abccompany.com', phone: '(555) 123-4567', address: '123 Main St, Anytown, USA', created: '2024-01-15' },
        { id: 1002, name: 'XYZ Corporation', email: 'info@xyzcorp.com', phone: '(555) 987-6543', address: '456 Oak Ave, Somewhere, USA', created: '2024-02-20' },
        { id: 1003, name: 'Demo Client LLC', email: 'hello@democlient.com', phone: '(555) 456-7890', address: '789 Elm Blvd, Anywhere, USA', created: '2024-03-01' }
      ];
      
      for (const client of clients) {
        this.db.prepare(
          `INSERT OR IGNORE INTO halopsa_clients (halopsa_id, name, email, phone, address_line1, date_created, raw_data) VALUES (?, ?, ?, ?, ?, ?, ?)`
        ).run([client.id, client.name, client.email, client.phone, client.address, client.created, JSON.stringify(client)]);
      }
      
      // Insert sample purchase orders
      const purchaseOrders = [
        { number: 'PO-2024-001', clientId: 1001, vendor: 'Microsoft', amount: 1500.00, status: 'approved', created: '2024-10-01', delivery: '2024-10-15' },
        { number: 'PO-2024-002', clientId: 1001, vendor: 'Dell', amount: 3200.50, status: 'pending', created: '2024-10-05', delivery: '2024-11-01' },
        { number: 'PO-2024-003', clientId: 1002, vendor: 'HP', amount: 890.25, status: 'approved', created: '2024-09-20', delivery: '2024-10-10' },
        { number: 'PO-2024-004', clientId: 1003, vendor: 'Apple', amount: 2200.75, status: 'pending', created: '2024-10-08', delivery: '2024-10-25' }
      ];
      
      for (const po of purchaseOrders) {
        this.db.prepare(
          `INSERT OR IGNORE INTO purchase_orders (po_number, client_id, vendor, amount, status, created_at, expected_delivery) VALUES (?, ?, ?, ?, ?, ?, ?)`
        ).run([po.number, po.clientId, po.vendor, po.amount, po.status, po.created, po.delivery]);
      }
      
      // Insert sample client items
      const items = [
        { clientId: 1001, itemId: 'WIN-SRV-01', name: 'Windows Server License', type: 'software', used: '2024-10-01', count: 5 },
        { clientId: 1001, itemId: 'OFFICE-365', name: 'Office 365 Subscription', type: 'subscription', used: '2024-10-05', count: 25 },
        { clientId: 1002, itemId: 'NETWORK-SW', name: 'Network Switch', type: 'hardware', used: '2024-09-15', count: 12 },
        { clientId: 1003, itemId: 'MAC-PRO', name: 'Mac Pro Workstation', type: 'hardware', used: '2024-10-03', count: 3 }
      ];
      
      for (const item of items) {
        this.db.prepare(
          `INSERT OR IGNORE INTO client_items (client_id, item_id, item_name, item_type, last_used, usage_count) VALUES (?, ?, ?, ?, ?, ?)`
        ).run([item.clientId, item.itemId, item.name, item.type, item.used, item.count]);
      }
      
      // Insert sample HaloPSA transactions
      const transactions = [
        { id: 'TX-HALO-001', clientId: 1001, type: 'invoice', amount: 1500.00, date: '2024-10-01', desc: 'Monthly service fee - October', status: 'paid' },
        { id: 'TX-HALO-002', clientId: 1001, type: 'payment', amount: 1500.00, date: '2024-10-05', desc: 'Payment received for invoice TX-HALO-001', status: 'completed' },
        { id: 'TX-HALO-003', clientId: 1002, type: 'invoice', amount: 3200.50, date: '2024-10-02', desc: 'Project implementation fee', status: 'pending' },
        { id: 'TX-HALO-004', clientId: 1003, type: 'invoice', amount: 890.25, date: '2024-09-28', desc: 'Consulting services', status: 'paid' }
      ];
      
      for (const tx of transactions) {
        this.db.prepare(
          `INSERT OR IGNORE INTO halopsa_transactions (transaction_id, client_id, type, amount, date, description, status) VALUES (?, ?, ?, ?, ?, ?, ?)`
        ).run([tx.id, tx.clientId, tx.type, tx.amount, tx.date, tx.desc, tx.status]);
      }
      
      console.log('Sample customer data inserted successfully');
    } catch (error) {
      console.error('Error inserting sample customer data:', error);
    }
  }

  query(sql, params = []) {
    try {
      const stmt = this.db.prepare(sql);
      return stmt.all(params);
    } catch (e) {
      console.error('Database query error:', e);
      throw e;
    }
  }

  get(sql, params = []) {
    const stmt = this.db.prepare(sql);
    return stmt.get(params) || null;
  }

  all(sql, params = []) {
    return this.query(sql, params);
  }

  run(sql, params = []) {
    const stmt = this.db.prepare(sql);
    return stmt.run(params);
  }

  setConfig(key, value) {
    return this.run(
      'INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
      [key, value]
    );
  }

  close() {
    this.db.close();
  }

  // Support for callback-style getConfig usage (legacy)
  getConfig(category, callback) {
    try {
      const sql = category
        ? 'SELECT * FROM config WHERE category = ? ORDER BY category, key'
        : 'SELECT * FROM config ORDER BY category, key';
      const params = category ? [category] : [];

      const rows = this.query(sql, params);
      const result = {};

      rows.forEach(row => {
        result[row.key] = row.value;
      });

      if (callback) callback(null, result);
    } catch (e) {
      console.error('Error getting config:', e);
      if (callback) callback(e, {});
    }
  }

  // Get comprehensive dashboard statistics
  async getDashboardStats() {
    try {
      const stats = {
        stripe_customers: 0,
        halopsa_clients: 0,
        mapped_customers: 0,
        stripe_transactions: 0,
        halopsa_invoices: 0,
        purchase_orders: 0,
        last_sync: null
      };

      // Count Stripe customers
      const stripeCustomersResult = this.get('SELECT COUNT(*) as count FROM stripe_customers');
      stats.stripe_customers = stripeCustomersResult?.count || 0;

      // Count HaloPSA clients
      const haloPSAClientsResult = this.get('SELECT COUNT(*) as count FROM halopsa_clients');
      stats.halopsa_clients = haloPSAClientsResult?.count || 0;

      // Count mapped customers
      const mappedCustomersResult = this.get('SELECT COUNT(*) as count FROM customer_mappings WHERE mapping_confirmed = 1');
      stats.mapped_customers = mappedCustomersResult?.count || 0;

      // Count Stripe transactions
      const stripeTransactionsResult = this.get('SELECT COUNT(*) as count FROM stripe_transactions');
      stats.stripe_transactions = stripeTransactionsResult?.count || 0;

      // Count HaloPSA invoices
      const haloPSAInvoicesResult = this.get('SELECT COUNT(*) as count FROM halopsa_invoices');
      stats.halopsa_invoices = haloPSAInvoicesResult?.count || 0;

      // Count purchase orders
      const purchaseOrdersResult = this.get('SELECT COUNT(*) as count FROM halopsa_purchase_orders');
      stats.purchase_orders = purchaseOrdersResult?.count || 0;

      // Get last sync time from most recent update across all tables
      const lastSyncResult = this.get(`
        SELECT MAX(last_sync) as last_sync FROM (
          SELECT MAX(last_sync) as last_sync FROM stripe_customers
          UNION ALL
          SELECT MAX(last_sync) as last_sync FROM halopsa_clients
          UNION ALL
          SELECT MAX(last_sync) as last_sync FROM halopsa_invoices
          UNION ALL
          SELECT MAX(last_sync) as last_sync FROM halopsa_purchase_orders
        )
      `);
      stats.last_sync = lastSyncResult?.last_sync || null;

      return { success: true, stats };
    } catch (error) {
      console.error('Error getting dashboard stats:', error);
      return { success: false, error: error.message };
    }
  }
}

module.exports = DatabaseManager;
