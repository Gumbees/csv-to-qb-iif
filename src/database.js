const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
const os = require('os');

class Database {
  constructor() {
    this.dbPath = this.getDatabasePath();
    this.db = null;
    this.init();
  }

  getDatabasePath() {
    // Use platform-specific shared directory
    let appDir;
    
    if (process.platform === 'darwin') {
      // macOS: /Users/Shared/csv-to-qb-iif
      appDir = '/Users/Shared/csv-to-qb-iif';
    } else if (process.platform === 'win32') {
      // Windows: %PUBLIC%\Public Documents\csv-to-qb-iif
      const publicDocs = path.join(os.homedir(), 'Public', 'Documents');
      appDir = path.join(publicDocs, 'csv-to-qb-iif');
    } else {
      // Linux and other Unix-like systems: /var/lib/shared/csv-to-qb-iif
      appDir = '/var/lib/shared/csv-to-qb-iif';
    }
    
    // Create directory if it doesn't exist
    if (!fs.existsSync(appDir)) {
      try {
        fs.mkdirSync(appDir, { recursive: true });
      } catch (err) {
        console.error(`Error creating directory ${appDir}:`, err.message);
        // Fallback to user's home directory if shared directory creation fails
        appDir = path.join(os.homedir(), 'csv-to-qb-iif');
        if (!fs.existsSync(appDir)) {
          fs.mkdirSync(appDir, { recursive: true });
        }
      }
    }
    
    return path.join(appDir, 'csv-to-qb-iif.db');
  }

  init() {
    this.db = new sqlite3.Database(this.dbPath, (err) => {
      if (err) {
        console.error('Error opening database:', err.message);
        return;
      }
      console.log('Connected to database at:', this.dbPath);
      this.createTables();
    });
  }

  createTables() {
    // Table for storing raw CSV imports
    const createCsvImportsTable = `
      CREATE TABLE IF NOT EXISTS csv_imports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        csv_content BLOB NOT NULL,
        file_size INTEGER NOT NULL,
        import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        processed BOOLEAN DEFAULT FALSE,
        processed_date DATETIME,
        checksum TEXT UNIQUE NOT NULL
      )
    `;

    // Table for tracking processed transactions from CSV imports
    const createTransactionsTable = `
      CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        csv_import_id INTEGER NOT NULL,
        vendor TEXT NOT NULL,
        ref_number TEXT NOT NULL,
        transaction_date TEXT NOT NULL,
        total_amount REAL NOT NULL,
        payment_terms TEXT,
        due_date TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (csv_import_id) REFERENCES csv_imports (id)
      )
    `;

    // Table for individual line items from transactions
    const createLineItemsTable = `
      CREATE TABLE IF NOT EXISTS line_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id INTEGER NOT NULL,
        item_name TEXT NOT NULL,
        description TEXT,
        quantity REAL NOT NULL,
        unit_cost REAL NOT NULL,
        line_amount REAL NOT NULL,
        FOREIGN KEY (transaction_id) REFERENCES transactions (id)
      )
    `;

    // Table for maintaining inventory state
    const createInventoryTable = `
      CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_name TEXT UNIQUE NOT NULL,
        description TEXT,
        current_quantity REAL DEFAULT 0,
        total_received REAL DEFAULT 0,
        total_cost REAL DEFAULT 0,
        average_unit_cost REAL DEFAULT 0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_transaction_date TEXT,
        last_vendor TEXT
      )
    `;

    // Table for inventory transactions (receipts, adjustments, etc.)
    const createInventoryTransactionsTable = `
      CREATE TABLE IF NOT EXISTS inventory_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        transaction_id INTEGER NOT NULL,
        transaction_type TEXT NOT NULL,
        quantity REAL NOT NULL,
        unit_cost REAL NOT NULL,
        total_cost REAL NOT NULL,
        vendor TEXT NOT NULL,
        ref_number TEXT NOT NULL,
        transaction_date TEXT NOT NULL,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (item_id) REFERENCES inventory (id),
        FOREIGN KEY (transaction_id) REFERENCES transactions (id)
      )
    `;

    // Table for tracking IIF exports
    const createExportsTable = `
      CREATE TABLE IF NOT EXISTS exports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        export_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        filename TEXT NOT NULL,
        file_path TEXT NOT NULL,
        transaction_count INTEGER NOT NULL,
        total_amount REAL NOT NULL,
        export_type TEXT DEFAULT 'IIF',
        notes TEXT
      )
    `;

    // Table linking exports to transactions
    const createExportTransactionsTable = `
      CREATE TABLE IF NOT EXISTS export_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        export_id INTEGER NOT NULL,
        transaction_id INTEGER NOT NULL,
        FOREIGN KEY (export_id) REFERENCES exports (id),
        FOREIGN KEY (transaction_id) REFERENCES transactions (id)
      )
    `;

    this.db.serialize(() => {
      this.db.run(createCsvImportsTable);
      this.db.run(createTransactionsTable);
      this.db.run(createLineItemsTable);
      this.db.run(createInventoryTable);
      this.db.run(createInventoryTransactionsTable);
      this.db.run(createExportsTable);
      this.db.run(createExportTransactionsTable);
    });
  }

  // Generate checksum for CSV content to detect duplicates
  generateChecksum(content) {
    const crypto = require('crypto');
    return crypto.createHash('md5').update(content).digest('hex');
  }

  // Store CSV import with duplicate detection
  async storeCsvImport(filename, csvContent) {
    return new Promise((resolve, reject) => {
      const checksum = this.generateChecksum(csvContent);
      const fileSize = Buffer.byteLength(csvContent, 'utf8');
      
      // Check if this CSV has already been imported
      this.db.get(
        'SELECT id, processed FROM csv_imports WHERE checksum = ?',
        [checksum],
        (err, row) => {
          if (err) {
            reject(err);
            return;
          }
          
          if (row) {
            // CSV already exists
            resolve({ 
              id: row.id, 
              isDuplicate: true, 
              alreadyProcessed: row.processed,
              message: row.processed ? 'CSV already imported and processed' : 'CSV already imported but not processed'
            });
            return;
          }
          
          // Store new CSV import
          this.db.run(`
            INSERT INTO csv_imports (filename, csv_content, file_size, checksum)
            VALUES (?, ?, ?, ?)
          `, [filename, csvContent, fileSize, checksum], function(err) {
            if (err) {
              reject(err);
              return;
            }
            resolve({ 
              id: this.lastID, 
              isDuplicate: false, 
              alreadyProcessed: false,
              message: 'CSV imported successfully'
            });
          });
        }
      );
    });
  }

  // Process CSV import and extract transactions
  async processCsvImport(importId, bills) {
    return new Promise((resolve, reject) => {
      this.db.serialize(() => {
        try {
          for (const bill of bills) {
            // Insert transaction
            this.db.run(`
              INSERT INTO transactions 
              (csv_import_id, vendor, ref_number, transaction_date, total_amount, payment_terms, due_date)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `, [
              importId,
              bill.vendor,
              bill.ref_num,
              bill.date,
              bill.total_amount,
              bill.terms,
              bill.due_date
            ], function(err) {
              if (err) {
                console.error('Error inserting transaction:', err);
                return;
              }
              
              const transactionId = this.lastID;
              
              // Insert line items
              bill.lines.forEach(line => {
                this.db.run(`
                  INSERT INTO line_items 
                  (transaction_id, item_name, description, quantity, unit_cost, line_amount)
                  VALUES (?, ?, ?, ?, ?, ?)
                `, [
                  transactionId,
                  line.item,
                  line.description,
                  line.quantity,
                  line.unit_cost,
                  line.line_amount
                ]);
              });
              
              // Update inventory
              this.updateInventoryFromTransaction(transactionId, bill);
            });
          }
          
          // Mark import as processed
          this.db.run(`
            UPDATE csv_imports 
            SET processed = TRUE, processed_date = CURRENT_TIMESTAMP 
            WHERE id = ?
          `, [importId]);
          
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  // Update inventory from a transaction
  updateInventoryFromTransaction(transactionId, bill) {
    bill.lines.forEach(line => {
      // Check if item exists in inventory
      this.db.get(
        'SELECT * FROM inventory WHERE item_name = ?',
        [line.item],
        (err, row) => {
          if (err) {
            console.error('Error checking inventory:', err);
            return;
          }

          if (row) {
            // Update existing item
            const newQuantity = row.current_quantity + line.quantity;
            const newTotalCost = row.total_cost + line.line_amount;
            const newAverageCost = newTotalCost / newQuantity;

            this.db.run(`
              UPDATE inventory 
              SET current_quantity = ?, total_received = ?, total_cost = ?, 
                  average_unit_cost = ?, last_updated = CURRENT_TIMESTAMP,
                  last_transaction_date = ?, last_vendor = ?
              WHERE id = ?
            `, [newQuantity, row.total_received + line.quantity, newTotalCost, 
                 newAverageCost, bill.date, bill.vendor, row.id]);

            // Record inventory transaction
            this.db.run(`
              INSERT INTO inventory_transactions 
              (item_id, transaction_id, transaction_type, quantity, unit_cost, total_cost, 
               vendor, ref_number, transaction_date, notes)
              VALUES (?, ?, 'RECEIPT', ?, ?, ?, ?, ?, ?, ?)
            `, [row.id, transactionId, line.quantity, line.unit_cost, line.line_amount, 
                 bill.vendor, bill.ref_num, bill.date, 'CSV Import']);
          } else {
            // Insert new item
            this.db.run(`
              INSERT INTO inventory 
              (item_name, description, current_quantity, total_received, total_cost, average_unit_cost, last_transaction_date, last_vendor)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `, [line.item, line.description, line.quantity, line.quantity, 
                 line.line_amount, line.unit_cost, bill.date, bill.vendor], function(err) {
              if (err) {
                console.error('Error inserting new inventory item:', err);
                return;
              }

              // Record inventory transaction
              this.db.run(`
                INSERT INTO inventory_transactions 
                (item_id, transaction_id, transaction_type, quantity, unit_cost, total_cost, 
                 vendor, ref_number, transaction_date, notes)
                VALUES (?, ?, 'RECEIPT', ?, ?, ?, ?, ?, ?, ?)
              `, [this.lastID, transactionId, line.quantity, line.unit_cost, line.line_amount, 
                   bill.vendor, bill.ref_num, bill.date, 'CSV Import']);
            });
          }
        }
      );
    });
  }

  // Record IIF export
  async recordExport(filename, filePath, transactions, totalAmount) {
    return new Promise((resolve, reject) => {
      this.db.run(`
        INSERT INTO exports (filename, file_path, transaction_count, total_amount)
        VALUES (?, ?, ?, ?)
      `, [filename, filePath, transactions.length, totalAmount], function(err) {
        if (err) {
          reject(err);
          return;
        }
        
        const exportId = this.lastID;
        
        // Link transactions to export
        transactions.forEach(transactionId => {
          this.db.run(`
            INSERT INTO export_transactions (export_id, transaction_id)
            VALUES (?, ?)
          `, [exportId, transactionId]);
        });
        
        resolve(exportId);
      });
    });
  }

  // Get dashboard statistics
  async getDashboardStats() {
    return new Promise((resolve, reject) => {
      const stats = {};
      
      // Get total transactions count
      this.db.get('SELECT COUNT(*) as count FROM transactions', [], (err, row) => {
        if (err) {
          reject(err);
          return;
        }
        stats.totalTransactions = row.count;
        
        // Get total inventory items
        this.db.get('SELECT COUNT(*) as count FROM inventory', [], (err, row) => {
          if (err) {
            reject(err);
            return;
          }
          stats.totalInventoryItems = row.count;
          
          // Get total inventory value
          this.db.get('SELECT SUM(total_cost) as total FROM inventory', [], (err, row) => {
            if (err) {
              reject(err);
              return;
            }
            stats.totalInventoryValue = row.total || 0;
            
            // Get recent transactions (last 30 days)
            this.db.get(`
              SELECT COUNT(*) as count, SUM(total_amount) as total 
              FROM transactions 
              WHERE date(created_at) >= date('now', '-30 days')
            `, [], (err, row) => {
              if (err) {
                reject(err);
                return;
              }
              stats.recentTransactions = row.count || 0;
              stats.recentAmount = row.total || 0;
              
              resolve(stats);
            });
          });
        });
      });
    });
  }

  // Get transactions from last 24 hours
  async getRecentTransactions(hours = 24) {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT t.*, GROUP_CONCAT(li.item_name || ' x' || li.quantity) as items
        FROM transactions t
        LEFT JOIN line_items li ON t.id = li.transaction_id
        WHERE t.created_at >= datetime('now', '-${hours} hours')
        GROUP BY t.id
        ORDER BY t.created_at DESC
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get all transactions for export
  async getAllTransactions() {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT t.*, GROUP_CONCAT(li.item_name || ' x' || li.quantity) as items
        FROM transactions t
        LEFT JOIN line_items li ON t.id = li.transaction_id
        GROUP BY t.id
        ORDER BY t.created_at DESC
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get inventory summary
  async getInventorySummary() {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT * FROM inventory 
        ORDER BY item_name
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get inventory items with transaction history for export
  async getInventoryForExport() {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT i.*, 
               GROUP_CONCAT(DISTINCT t.vendor || ' (' || t.ref_number || ')') as vendors,
               GROUP_CONCAT(DISTINCT t.transaction_date) as transaction_dates,
               COUNT(DISTINCT t.id) as transaction_count
        FROM inventory i
        LEFT JOIN inventory_transactions it ON i.id = it.item_id
        LEFT JOIN transactions t ON it.transaction_id = t.id
        GROUP BY i.id
        ORDER BY i.item_name
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get import history
  async getImportHistory() {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT ci.*, 
               COUNT(t.id) as transaction_count,
               SUM(t.total_amount) as total_amount
        FROM csv_imports ci
        LEFT JOIN transactions t ON ci.id = t.csv_import_id
        GROUP BY ci.id
        ORDER BY ci.import_date DESC
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Get export history
  async getExportHistory() {
    return new Promise((resolve, reject) => {
      const sql = `
        SELECT * FROM exports 
        ORDER BY export_date DESC
      `;
      
      this.db.all(sql, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Close database connection
  close() {
    if (this.db) {
      this.db.close((err) => {
        if (err) {
          console.error('Error closing database:', err.message);
        } else {
          console.log('Database connection closed.');
        }
      });
    }
  }
}

module.exports = Database;
