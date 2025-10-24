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
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      this.db.exec(
        'CREATE TABLE IF NOT EXISTS customer_mappings (' +
          'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
          'stripe_customer_id TEXT UNIQUE NOT NULL,' +
          'stripe_customer_email TEXT,' +
          'stripe_customer_name TEXT,' +
          'halopsa_client_id INTEGER,' +
          'halopsa_client_name TEXT,' +
          'qb_customer_id TEXT,' +
          'qb_customer_name TEXT,' +
          'auto_mapped BOOLEAN DEFAULT FALSE,' +
          'mapping_confirmed BOOLEAN DEFAULT FALSE,' +
          'created_at DATETIME DEFAULT CURRENT_TIMESTAMP,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Insert default configuration
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
        ['halopsa_api_key', '', 'password', 'halopsa', 'HaloPSA API Key', 'API key for HaloPSA integration', null, 1],
        ['halopsa_api_url', 'https://halo.dtctoday.com', 'text', 'halopsa', 'HaloPSA API URL', 'Base URL for HaloPSA API', null, 1],
        ['customer_auto_match_enabled', 'true', 'boolean', 'mapping', 'Auto-match Customers', 'Automatically match Stripe customers to HaloPSA clients', null, 1],
        ['customer_match_threshold', '0.85', 'number', 'mapping', 'Match Threshold', 'Confidence threshold for customer matching', null, 1]
      ];

      const stmt = this.db.prepare(`
        INSERT OR IGNORE INTO config (key, value, type, category, label, description, options, is_required)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (const config of defaultConfigs) {
        stmt.run(config);
      }

      console.log('Database initialized successfully');
      return true;
    } catch (e) {
      console.error('Database initialization error:', e);
      throw e;
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
}

module.exports = DatabaseManager;
