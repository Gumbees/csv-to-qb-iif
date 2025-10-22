const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse/sync');

class DatabaseManager {
  constructor() {
    try {
      const dataDir = path.join(process.env.PUBLIC || process.env.USERPROFILE || '.', 'Documents', 'csv-to-qb-iif');
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

      // Create configuration table
      this.db.exec(
        'CREATE TABLE IF NOT EXISTS config (' +
          'key TEXT PRIMARY KEY,' +
          'value TEXT NOT NULL,' +
          'type TEXT DEFAULT \"text\",' +
          'category TEXT DEFAULT \"general\",' +
          'label TEXT,' +
          'description TEXT,' +
          'options TEXT,' +
          'is_required BOOLEAN DEFAULT 0,' +
          'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP' +
        ')'
      );

      // Create QBD accounts table
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

      this.initializeDefaultConfig();
      console.log('Database initialized successfully');
    } catch (e) {
      console.error('DB init error:', e);
      throw e;
    }
  }

  initializeDefaultConfig() {
    const defaultConfigs = [
      { key: 'auto_match_threshold', value: '0.8', type: 'number', category: 'general', label: 'Auto-match Threshold', description: 'Confidence threshold for automatic matching', is_required: 1 },
      { key: 'duplicate_check_enabled', value: 'true', type: 'boolean', category: 'general', label: 'Duplicate Check', description: 'Enable duplicate transaction detection', is_required: 1 },
      { key: 'quickbooks_sync_method', value: 'iif', type: 'select', category: 'quickbooks', label: 'Sync Method', description: 'Method for syncing with QuickBooks', options: '[\"iif\",\"qbwc\"]', is_required: 1 },
      { key: 'quickbooks_default_account', value: 'Accounts Payable', type: 'text', category: 'quickbooks', label: 'Default Account', description: 'Default accounts payable account', is_required: 1 },
      { key: 'accounting_currency', value: 'USD', type: 'text', category: 'accounting', label: 'Currency', description: 'Default accounting currency', is_required: 1 },
      { key: 'accounting_timezone', value: 'UTC', type: 'text', category: 'accounting', label: 'Timezone', description: 'Accounting timezone', is_required: 1 }
    ];

    const insertStmt = this.db.prepare(
      'INSERT OR IGNORE INTO config (key, value, type, category, label, description, options, is_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    );

    for (const config of defaultConfigs) {
      insertStmt.run(
        config.key,
        config.value,
        config.type,
        config.category,
        config.label,
        config.description,
        config.options || null,
        config.is_required ? 1 : 0
      );
    }
  }

  // Configuration methods
  async getConfig(category = null) {
    let query = 'SELECT * FROM config';
    const params = [];
    
    if (category) {
      query += ' WHERE category = ?';
      params.push(category);
    }
    
    query += ' ORDER BY category, key';
    
    const stmt = this.db.prepare(query);
    return stmt.all(...params);
  }

  async setConfig(key, value) {
    const stmt = this.db.prepare('INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)');
    return stmt.run(key, value);
  }

  // Dashboard stats method for health check
  async getDashboardStats() {
    const importsStmt = this.db.prepare('SELECT COUNT(*) as count FROM csv_imports');
    const transactionsStmt = this.db.prepare('SELECT COUNT(*) as count FROM transactions');
    
    const imports = importsStmt.get();
    const transactions = transactionsStmt.get();
    
    return {
      imports: imports.count,
      transactions: transactions.count
    };
  }

  // QBD accounts management
  async getQbdAccounts() {
    return this.db.prepare('SELECT * FROM qbd_accounts ORDER BY name').all();
  }

  async upsertQbdAccount(accountData) {
    const { id, name, account_type, quickbooks_id, last_sync } = accountData;
    
    if (id) {
      const stmt = this.db.prepare(
        'UPDATE qbd_accounts SET name = ?, account_type = ?, quickbooks_id = ?, last_sync = ? WHERE id = ?'
      );
      return stmt.run(name, account_type, quickbooks_id, last_sync, id);
    } else {
      const stmt = this.db.prepare(
        'INSERT INTO qbd_accounts (name, account_type, quickbooks_id, last_sync) VALUES (?, ?, ?, ?)'
      );
      return stmt.run(name, account_type, quickbooks_id, last_sync);
    }
  }

  // CSV import processing
  async processCsvImport(importId, options = {}) {
    // This would process a CSV import file and convert it to bills
    return { success: true, processed: true, importId };
  }

  // CSV type detection
  detectCsvType(rows, filename) {
    if (!rows || !rows.length) return 'unknown';
    
    const headers = Object.keys(rows[0]);
    const lowercaseHeaders = headers.map(h => h.toLowerCase());
    
    // Check for PO bills pattern
    const poBillIndicators = ['vendor', 'date', 'refnumber', 'ref number', 'ponumber', 'po number', 'total', 'amount'];
    const hasPoBillIndicators = poBillIndicators.some(indicator => 
      lowercaseHeaders.includes(indicator.toLowerCase())
    );
    
    if (hasPoBillIndicators) return 'po_bills';
    
    // Check for HaloPSA pattern
    const haloIndicators = ['halo', 'invoice', 'client', 'status', 'totalamount'];
    const hasHaloIndicators = haloIndicators.some(indicator => 
      lowercaseHeaders.includes(indicator.toLowerCase()) ||
      filename.toLowerCase().includes('halo')
    );
    
    if (hasHaloIndicators) return 'halo_invoices';
    
    // Check for bank transactions
    const bankIndicators = ['transaction', 'bank', 'account', 'debit', 'credit', 'balance'];
    const hasBankIndicators = bankIndicators.some(indicator => 
      lowercaseHeaders.includes(indicator.toLowerCase()) ||
      filename.toLowerCase().includes('bank') ||
      filename.toLowerCase().includes('fnb')
    );
    
    if (hasBankIndicators) return 'bank_transactions';
    
    return 'unknown';
  }

  // Parse bills from CSV content
  parseBillsFromCsv(content) {
    const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });
    
    const bills = [];
    const billMap = new Map();
    
    for (const row of rows) {
      const refNumber = row['RefNumber'] || row['Ref Number'] || row['PONumber'] || row['PO Number'] || 'Unknown';
      
      if (!billMap.has(refNumber)) {
        billMap.set(refNumber, {
          vendor: row['Vendor'] || row['Supplier'] || 'Unknown Vendor',
          date: row['Date'] || new Date().toISOString().split('T')[0],
          ref_num: refNumber,
          due_date: row['DueDate'] || row['Due Date'] || '',
          terms: row['Terms'] || row['PaymentTerms'] || '',
          total_amount: 0,
          lines: []
        });
      }
      
      const bill = billMap.get(refNumber);
      const lineAmount = parseFloat(row['Amount'] || row['LineAmount'] || row['Total'] || 0);
      const quantity = parseFloat(row['Qty'] || row['Quantity'] || 1);
      const unitCost = parseFloat(row['Cost'] || row['UnitCost'] || row['Price'] || lineAmount);
      
      bill.lines.push({
        item: row['Item'] || row['Description'] || 'Unnamed Item',
        description: row['Description'] || row['Item'] || '',
        quantity: quantity,
        unit_cost: unitCost,
        line_amount: lineAmount
      });
      
      bill.total_amount += lineAmount;
    }
    
    return Array.from(billMap.values());
  }

  // Get recent transactions
  async getRecentTransactions(limit = 50) {
    const stmt = this.db.prepare(
      'SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?'
    );
    return stmt.all(limit);
  }
}

module.exports = DatabaseManager;