const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class DatabaseManager {
  constructor() {
    // Create database directory if it doesn't exist
    const dbDir = path.join(process.env.PUBLIC || process.env.USERPROFILE, 'Documents', 'csv-to-qb-iif');
    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }
    
    const dbPath = path.join(dbDir, 'csv-to-qb-iif.db');
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.ready = this.init();
    console.log('Connected to database at: ' + dbPath);
  }

  async init() {
    try {
      // Create tables if not exists (SQLite dialect)
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

      console.log('Database initialized successfully');
    } catch (e) {
      console.error('DB init error:', e);
      throw e;
    }
  }
}

module.exports = DatabaseManager;
