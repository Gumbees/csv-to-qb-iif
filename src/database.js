const { Pool } = require('pg');

class Database {
  constructor() {
    const connectionString = process.env.DATABASE_URL || 'postgres://user:password@localhost:5432/mydb';
    this.pool = new Pool({ connectionString });
    this.ready = this.init();
  }

  async init() {
    // Create tables if not exists (PostgreSQL dialect)
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(`
        CREATE TABLE IF NOT EXISTS csv_imports (
          id SERIAL PRIMARY KEY,
          filename TEXT NOT NULL,
          csv_content BYTEA NOT NULL,
          file_size INTEGER NOT NULL,
          import_date TIMESTAMPTZ DEFAULT NOW(),
          processed BOOLEAN DEFAULT FALSE,
          processed_date TIMESTAMPTZ,
          checksum TEXT UNIQUE NOT NULL
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS transactions (
          id SERIAL PRIMARY KEY,
          csv_import_id INTEGER NOT NULL REFERENCES csv_imports(id) ON DELETE CASCADE,
          vendor TEXT NOT NULL,
          ref_number TEXT NOT NULL,
          transaction_date TEXT NOT NULL,
          total_amount NUMERIC(12,2) NOT NULL,
          payment_terms TEXT,
          due_date TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS line_items (
          id SERIAL PRIMARY KEY,
          transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
          item_name TEXT NOT NULL,
          description TEXT,
          quantity NUMERIC NOT NULL,
          unit_cost NUMERIC(12,4) NOT NULL,
          line_amount NUMERIC(12,2) NOT NULL
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS inventory (
          id SERIAL PRIMARY KEY,
          item_name TEXT UNIQUE NOT NULL,
          description TEXT,
          current_quantity NUMERIC DEFAULT 0,
          total_received NUMERIC DEFAULT 0,
          total_cost NUMERIC(12,2) DEFAULT 0,
          average_unit_cost NUMERIC(12,4) DEFAULT 0,
          last_updated TIMESTAMPTZ DEFAULT NOW(),
          last_transaction_date TEXT,
          last_vendor TEXT
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS inventory_transactions (
          id SERIAL PRIMARY KEY,
          item_id INTEGER NOT NULL REFERENCES inventory(id) ON DELETE CASCADE,
          transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
          transaction_type TEXT NOT NULL,
          quantity NUMERIC NOT NULL,
          unit_cost NUMERIC(12,4) NOT NULL,
          total_cost NUMERIC(12,2) NOT NULL,
          vendor TEXT NOT NULL,
          ref_number TEXT NOT NULL,
          transaction_date TEXT NOT NULL,
          notes TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS exports (
          id SERIAL PRIMARY KEY,
          export_date TIMESTAMPTZ DEFAULT NOW(),
          filename TEXT NOT NULL,
          file_path TEXT NOT NULL,
          transaction_count INTEGER NOT NULL,
          total_amount NUMERIC(12,2) NOT NULL,
          export_type TEXT DEFAULT 'IIF',
          notes TEXT
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS export_transactions (
          id SERIAL PRIMARY KEY,
          export_id INTEGER NOT NULL REFERENCES exports(id) ON DELETE CASCADE,
          transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        )
      `);

      /* Additional schema for unified ledger and external sources */
      await client.query(`
        CREATE TABLE IF NOT EXISTS ledger_sources (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          source_type TEXT NOT NULL DEFAULT 'csv',
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS ledger_transactions (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          txn_type TEXT NOT NULL,
          txn_date DATE NOT NULL,
          amount NUMERIC(14,2) NOT NULL,
          currency TEXT DEFAULT 'USD',
          description TEXT,
          counterparty TEXT,
          status TEXT,
          raw JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS ledger_links (
          id SERIAL PRIMARY KEY,
          ledger_transaction_id INTEGER NOT NULL REFERENCES ledger_transactions(id) ON DELETE CASCADE,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          match_status TEXT NOT NULL DEFAULT 'suggested',
          confidence NUMERIC(4,3) DEFAULT 0,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS customers (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT NOT NULL,
          email TEXT,
          tax_id TEXT,
          address TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS vendors (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT NOT NULL,
          address TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS catalog_items (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          item_code TEXT NOT NULL,
          name TEXT,
          description TEXT,
          category TEXT,
          unit_cost NUMERIC(12,4),
          unit_price NUMERIC(12,4),
          tax_code TEXT,
          is_active BOOLEAN DEFAULT TRUE,
          updated_at TIMESTAMPTZ DEFAULT NOW(),
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, item_code)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS invoices (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          number TEXT,
          customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
          invoice_date DATE,
          due_date DATE,
          status TEXT,
          currency TEXT DEFAULT 'USD',
          subtotal NUMERIC(14,2),
          tax_total NUMERIC(14,2),
          total NUMERIC(14,2),
          balance NUMERIC(14,2),
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS invoice_lines (
          id SERIAL PRIMARY KEY,
          invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
          item_id INTEGER REFERENCES catalog_items(id) ON DELETE SET NULL,
          description TEXT,
          quantity NUMERIC(12,4),
          unit_price NUMERIC(12,4),
          tax_code TEXT,
          line_total NUMERIC(14,2)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS payments (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
          payment_date DATE,
          amount_gross NUMERIC(14,2),
          fee_amount NUMERIC(14,2),
          amount_net NUMERIC(14,2),
          currency TEXT DEFAULT 'USD',
          method TEXT,
          status TEXT,
          raw JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS bank_transactions (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          txn_date DATE NOT NULL,
          amount NUMERIC(14,2) NOT NULL,
          currency TEXT DEFAULT 'USD',
          description TEXT,
          memo TEXT,
          balance_after NUMERIC(14,2),
          checksum TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (source_id, external_id)
        )
      `);

      /* MSP Tenants and related entities (legacy) */
      await client.query(`
        CREATE TABLE IF NOT EXISTS msp_tenants (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT NOT NULL,
          msp_tenant_uuid UUID NOT NULL,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (msp_tenant_uuid),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS tenant_id INTEGER REFERENCES msp_tenants(id) ON DELETE SET NULL`);
      await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS msp_tenant_uuid UUID`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_customers_msp_tenant_uuid ON customers (msp_tenant_uuid)`);

      /* New unified client-centric entities with MSP UUIDs */
      await client.query(`
        CREATE TABLE IF NOT EXISTS clients (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT NOT NULL,
          msp_customer_uuid UUID NOT NULL,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (msp_customer_uuid),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL`);
      await client.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS msp_customer_uuid UUID`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_customers_msp_customer_uuid ON customers (msp_customer_uuid)`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS locations (
          id SERIAL PRIMARY KEY,
          client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
          msp_location_uuid UUID NOT NULL,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT,
          address1 TEXT,
          address2 TEXT,
          city TEXT,
          state TEXT,
          postal_code TEXT,
          country TEXT,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (msp_location_uuid),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS contacts (
          id SERIAL PRIMARY KEY,
          client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
          location_id INTEGER REFERENCES locations(id) ON DELETE SET NULL,
          msp_contact_uuid UUID NOT NULL,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          first_name TEXT,
          last_name TEXT,
          email TEXT,
          phone TEXT,
          role TEXT,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (msp_contact_uuid),
          UNIQUE (source_id, external_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS contracts (
          id SERIAL PRIMARY KEY,
          client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
          location_id INTEGER REFERENCES locations(id) ON DELETE SET NULL,
          msp_contract_uuid UUID NOT NULL,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          external_id TEXT,
          name TEXT,
          contract_type TEXT,
          start_date DATE,
          end_date DATE,
          status TEXT,
          terms TEXT,
          amount NUMERIC(14,2),
          billing_cycle TEXT,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE (msp_contract_uuid),
          UNIQUE (source_id, external_id)
        )
      `);

      /* Import metadata capture */
      await client.query(`
        CREATE TABLE IF NOT EXISTS import_metadata (
          id SERIAL PRIMARY KEY,
          source_id INTEGER REFERENCES ledger_sources(id) ON DELETE SET NULL,
          import_type TEXT NOT NULL,
          original_filename TEXT,
          content_type TEXT,
          row_count INTEGER,
          raw_headers JSONB,
          sample JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS import_records (
          id SERIAL PRIMARY KEY,
          import_metadata_id INTEGER NOT NULL REFERENCES import_metadata(id) ON DELETE CASCADE,
          external_id TEXT,
          checksum TEXT,
          raw JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await client.query(`CREATE INDEX IF NOT EXISTS idx_import_records_external_id ON import_records (external_id)`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_import_records_checksum ON import_records (checksum)`);

      /* QuickBooks Desktop account configuration */
      await client.query(`
        CREATE TABLE IF NOT EXISTS qbd_accounts (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          role TEXT, -- e.g., inventory_asset, cogs, income, accounts_payable, accounts_receivable, bank
          account_type TEXT, -- optional informational
          is_default BOOLEAN DEFAULT FALSE,
          active BOOLEAN DEFAULT TRUE,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_qbd_accounts_role ON qbd_accounts (role)`);

      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('DB init error:', e);
      throw e;
    } finally {
      client.release();
    }
  }

  generateChecksum(content) {
    const crypto = require('crypto');
    return crypto.createHash('md5').update(content).digest('hex');
  }

  async storeCsvImport(filename, csvContent) {
    const checksum = this.generateChecksum(csvContent);
    const fileSize = Buffer.byteLength(csvContent, 'utf8');

    const client = await this.pool.connect();
    try {
      const existing = await client.query('SELECT id, processed FROM csv_imports WHERE checksum = $1', [checksum]);
      if (existing.rows.length) {
        const row = existing.rows[0];
        return {
          id: row.id,
          isDuplicate: true,
          alreadyProcessed: row.processed,
          message: row.processed ? 'CSV already imported and processed' : 'CSV already imported but not processed'
        };
      }

      const result = await client.query(
        'INSERT INTO csv_imports (filename, csv_content, file_size, checksum) VALUES ($1, $2, $3, $4) RETURNING id',
        [filename, Buffer.from(csvContent, 'utf8'), fileSize, checksum]
      );

      return {
        id: result.rows[0].id,
        isDuplicate: false,
        alreadyProcessed: false,
        message: 'CSV imported successfully'
      };
    } finally {
      client.release();
    }
  }

  async processCsvImport(importId, bills) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      for (const bill of bills) {
        const txn = await client.query(
          `INSERT INTO transactions (csv_import_id, vendor, ref_number, transaction_date, total_amount, payment_terms, due_date)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
          [importId, bill.vendor, bill.ref_num, bill.date, bill.total_amount, bill.terms, bill.due_date]
        );
        const transactionId = txn.rows[0].id;

        for (const line of bill.lines) {
          await client.query(
            `INSERT INTO line_items (transaction_id, item_name, description, quantity, unit_cost, line_amount)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [transactionId, line.item, line.description, line.quantity, line.unit_cost, line.line_amount]
          );

          // Inventory upsert
          const invRow = await client.query('SELECT * FROM inventory WHERE item_name = $1', [line.item]);
          if (invRow.rows.length) {
            const row = invRow.rows[0];
            const newQuantity = Number(row.current_quantity) + Number(line.quantity);
            const newTotalCost = Number(row.total_cost) + Number(line.line_amount);
            const newAverageCost = newQuantity ? newTotalCost / newQuantity : 0;

            await client.query(
              `UPDATE inventory SET current_quantity = $1, total_received = $2, total_cost = $3, average_unit_cost = $4, last_updated = NOW(), last_transaction_date = $5, last_vendor = $6 WHERE id = $7`,
              [newQuantity, Number(row.total_received) + Number(line.quantity), newTotalCost, newAverageCost, bill.date, bill.vendor, row.id]
            );

            await client.query(
              `INSERT INTO inventory_transactions (item_id, transaction_id, transaction_type, quantity, unit_cost, total_cost, vendor, ref_number, transaction_date, notes)
               VALUES ($1, $2, 'RECEIPT', $3, $4, $5, $6, $7, $8, $9)`,
              [row.id, transactionId, line.quantity, line.unit_cost, line.line_amount, bill.vendor, bill.ref_num, bill.date, 'CSV Import']
            );
          } else {
            const newItem = await client.query(
              `INSERT INTO inventory (item_name, description, current_quantity, total_received, total_cost, average_unit_cost, last_transaction_date, last_vendor)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
              [line.item, line.description, line.quantity, line.quantity, line.line_amount, line.unit_cost, bill.date, bill.vendor]
            );

            await client.query(
              `INSERT INTO inventory_transactions (item_id, transaction_id, transaction_type, quantity, unit_cost, total_cost, vendor, ref_number, transaction_date, notes)
               VALUES ($1, $2, 'RECEIPT', $3, $4, $5, $6, $7, $8, $9)`,
              [newItem.rows[0].id, transactionId, line.quantity, line.unit_cost, line.line_amount, bill.vendor, bill.ref_num, bill.date, 'CSV Import']
            );
          }
        }
      }

      await client.query('UPDATE csv_imports SET processed = TRUE, processed_date = NOW() WHERE id = $1', [importId]);

      await client.query('COMMIT');
      return;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async recordExport(filename, filePath, transactions, totalAmount) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const exp = await client.query(
        `INSERT INTO exports (filename, file_path, transaction_count, total_amount) VALUES ($1, $2, $3, $4) RETURNING id`,
        [filename, filePath, transactions.length, totalAmount]
      );
      const exportId = exp.rows[0].id;

      for (const transactionId of transactions) {
        await client.query(
          `INSERT INTO export_transactions (export_id, transaction_id) VALUES ($1, $2)`,
          [exportId, transactionId]
        );
      }

      await client.query('COMMIT');
      return exportId;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async getDashboardStats() {
    const client = await this.pool.connect();
    try {
      const stats = {};
      const totalTx = await client.query('SELECT COUNT(*)::int AS count FROM transactions');
      stats.totalTransactions = totalTx.rows[0].count || 0;

      const totalInv = await client.query('SELECT COUNT(*)::int AS count FROM inventory');
      stats.totalInventoryItems = totalInv.rows[0].count || 0;

      const totalValue = await client.query('SELECT COALESCE(SUM(total_cost),0)::numeric AS total FROM inventory');
      stats.totalInventoryValue = Number(totalValue.rows[0].total) || 0;

      const recent = await client.query(`
        SELECT COUNT(*)::int AS count, COALESCE(SUM(total_amount),0)::numeric AS total
        FROM transactions
        WHERE created_at >= NOW() - INTERVAL '30 days'
      `);
      stats.recentTransactions = recent.rows[0].count || 0;
      stats.recentAmount = Number(recent.rows[0].total) || 0;

      return stats;
    } finally {
      client.release();
    }
  }

  async getRecentTransactions(hours = 24) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT t.*, STRING_AGG(li.item_name || ' x' || (li.quantity)::text, ', ') AS items
         FROM transactions t
         LEFT JOIN line_items li ON t.id = li.transaction_id
         WHERE t.created_at >= NOW() - ($1 || ' hours')::interval
         GROUP BY t.id
         ORDER BY t.created_at DESC`,
        [String(hours)]
      );
      return rows;
    } finally {
      client.release();
    }
  }

  async getAllTransactions() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT t.*, STRING_AGG(li.item_name || ' x' || (li.quantity)::text, ', ') AS items
         FROM transactions t
         LEFT JOIN line_items li ON t.id = li.transaction_id
         GROUP BY t.id
         ORDER BY t.created_at DESC`
      );
      return rows;
    } finally {
      client.release();
    }
  }

  async getInventorySummary() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query('SELECT * FROM inventory ORDER BY item_name');
      return rows;
    } finally {
      client.release();
    }
  }

  async getInventoryForExport() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT i.*, 
                STRING_AGG(DISTINCT t.vendor || ' (' || t.ref_number || ')', ', ') AS vendors,
                STRING_AGG(DISTINCT t.transaction_date, ', ') AS transaction_dates,
                COUNT(DISTINCT t.id) AS transaction_count
         FROM inventory i
         LEFT JOIN inventory_transactions it ON i.id = it.item_id
         LEFT JOIN transactions t ON it.transaction_id = t.id
         GROUP BY i.id
         ORDER BY i.item_name`
      );
      return rows;
    } finally {
      client.release();
    }
  }

  async getImportHistory() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT ci.*, 
                COUNT(t.id) AS transaction_count,
                COALESCE(SUM(t.total_amount),0) AS total_amount
         FROM csv_imports ci
         LEFT JOIN transactions t ON ci.id = t.csv_import_id
         GROUP BY ci.id
         ORDER BY ci.import_date DESC`
      );
      return rows;
    } finally {
      client.release();
    }
  }

  async getExportHistory() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query('SELECT * FROM exports ORDER BY export_date DESC');
      return rows;
    } finally {
      client.release();
    }
  }

  async getItemTransactions(itemId) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT it.*, t.vendor, t.ref_number, t.transaction_date
         FROM inventory_transactions it
         JOIN transactions t ON it.transaction_id = t.id
         WHERE it.item_id = $1
         ORDER BY it.created_at DESC`,
        [itemId]
      );
      return rows;
    } finally {
      client.release();
    }
  }

  // Utility: get or create a source
  async getOrCreateSource(name, source_type = 'csv', metadata = {}) {
    const client = await this.pool.connect();
    try {
      const found = await client.query('SELECT id FROM ledger_sources WHERE name = $1', [name]);
      if (found.rows.length) return found.rows[0].id;
      const ins = await client.query(
        'INSERT INTO ledger_sources (name, source_type, metadata) VALUES ($1, $2, $3) RETURNING id',
        [name, source_type, metadata]
      );
      return ins.rows[0].id;
    } finally { client.release(); }
  }

  async upsertCatalogItems(sourceName, items) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      for (const it of items) {
        await client.query(
          `INSERT INTO catalog_items (source_id, external_id, item_code, name, description, category, unit_cost, unit_price, tax_code, is_active, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10,TRUE), NOW())
           ON CONFLICT (source_id, item_code) DO UPDATE SET
             external_id = EXCLUDED.external_id,
             name = EXCLUDED.name,
             description = EXCLUDED.description,
             category = EXCLUDED.category,
             unit_cost = EXCLUDED.unit_cost,
             unit_price = EXCLUDED.unit_price,
             tax_code = EXCLUDED.tax_code,
             is_active = EXCLUDED.is_active,
             updated_at = NOW()`,
          [sourceId, it.external_id || null, it.item_code, it.name || null, it.description || null, it.category || null, it.unit_cost || null, it.unit_price || null, it.tax_code || null, it.is_active]
        );
      }
      await client.query('COMMIT');
      return { count: items.length };
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
  }

  async upsertCustomer(sourceName, customer) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      // Derive or create MSP client (customer) and legacy tenant
      const clientRec = await this.getOrCreateClient(sourceName, {
        external_id: customer.client_external_id || customer.external_id || null,
        name: customer.client_name || customer.name
      });
      const tenant = await this.getOrCreateTenant(sourceName, {
        external_id: customer.tenant_external_id || customer.external_id || null,
        name: customer.tenant_name || customer.name
      });
      const res = await client.query(
        `INSERT INTO customers (source_id, external_id, name, email, tax_id, address, tenant_id, msp_tenant_uuid, client_id, msp_customer_uuid)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (source_id, external_id) DO UPDATE SET
           name = EXCLUDED.name,
           email = EXCLUDED.email,
           tax_id = EXCLUDED.tax_id,
           address = EXCLUDED.address,
           tenant_id = EXCLUDED.tenant_id,
           msp_tenant_uuid = EXCLUDED.msp_tenant_uuid,
           client_id = EXCLUDED.client_id,
           msp_customer_uuid = EXCLUDED.msp_customer_uuid
         RETURNING id`,
        [sourceId, customer.external_id || null, customer.name, customer.email || null, customer.tax_id || null, customer.address || null, tenant.tenantId, tenant.tenantUuid, clientRec.clientId, clientRec.clientUuid]
      );
      return res.rows[0].id;
    } finally { client.release(); }
  }

  async upsertInvoiceWithLines(sourceName, invoice, lines) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const customerId = invoice.customer ? await this.upsertCustomer(sourceName, invoice.customer) : null;
      const inv = await client.query(
        `INSERT INTO invoices (source_id, external_id, number, customer_id, invoice_date, due_date, status, currency, subtotal, tax_total, total, balance)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT (source_id, external_id) DO UPDATE SET
           number = EXCLUDED.number, customer_id = EXCLUDED.customer_id, invoice_date = EXCLUDED.invoice_date,
           due_date = EXCLUDED.due_date, status = EXCLUDED.status, currency = EXCLUDED.currency,
           subtotal = EXCLUDED.subtotal, tax_total = EXCLUDED.tax_total, total = EXCLUDED.total, balance = EXCLUDED.balance
         RETURNING id`,
        [sourceId, invoice.external_id || null, invoice.number || null, customerId, invoice.invoice_date || null, invoice.due_date || null, invoice.status || null, invoice.currency || 'USD', invoice.subtotal || null, invoice.tax_total || null, invoice.total || null, invoice.balance || null]
      );
      const invoiceId = inv.rows[0].id;

      // Replace lines for this invoice
      await client.query('DELETE FROM invoice_lines WHERE invoice_id = $1', [invoiceId]);
      for (const ln of lines || []) {
        let itemId = null;
        if (ln.item_code) {
          const item = await client.query('SELECT id FROM catalog_items WHERE source_id = $1 AND item_code = $2', [sourceId, ln.item_code]);
          if (item.rows.length) itemId = item.rows[0].id;
        }
        await client.query(
          `INSERT INTO invoice_lines (invoice_id, item_id, description, quantity, unit_price, tax_code, line_total)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
          [invoiceId, itemId, ln.description || null, ln.quantity || null, ln.unit_price || null, ln.tax_code || null, ln.line_total || null]
        );
      }

      // Record ledger transaction for invoice
      await client.query(
        `INSERT INTO ledger_transactions (source_id, external_id, txn_type, txn_date, amount, currency, description, counterparty, status, raw)
         VALUES ($1,$2,'invoice',$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (source_id, external_id) DO NOTHING`,
        [sourceId, invoice.external_id || invoice.number || null, invoice.invoice_date || invoice.due_date || new Date(), invoice.total || 0, invoice.currency || 'USD', `Invoice ${invoice.number || ''}`, invoice.customer?.name || null, invoice.status || null, invoice.raw || {}]
      );

      await client.query('COMMIT');
      return invoiceId;
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
  }

  async insertBankTransactions(sourceName, txns) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      for (const t of txns) {
        const externalId = t.external_id || null; // may be null; rely on checksum if null
        const checksum = t.checksum || null;
        const ins = await client.query(
          `INSERT INTO bank_transactions (source_id, external_id, txn_date, amount, currency, description, memo, balance_after, checksum)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT (source_id, external_id) DO NOTHING
           RETURNING id`,
          [sourceId, externalId, t.txn_date, t.amount, t.currency || 'USD', t.description || null, t.memo || null, t.balance_after || null, checksum]
        );
        const bankId = ins.rows[0]?.id;
        const lt = await client.query(
          `INSERT INTO ledger_transactions (source_id, external_id, txn_type, txn_date, amount, currency, description, status, raw)
           VALUES ($1,$2,'bank_txn',$3,$4,$5,$6,$7,$8)
           ON CONFLICT (source_id, external_id) DO NOTHING
           RETURNING id`,
          [sourceId, externalId, t.txn_date, t.amount, t.currency || 'USD', t.description || t.memo || null, t.status || null, t.raw || {}]
        );
        // Optional: could link bank_transaction to ledger via ledger_links
        if (bankId && lt.rows[0]?.id) {
          await client.query(
            `INSERT INTO ledger_links (ledger_transaction_id, entity_type, entity_id, match_status, confidence)
             VALUES ($1,'bank_txn',$2,'matched',1.0)`,
            [lt.rows[0].id, String(bankId)]
          );
        }
      }
      await client.query('COMMIT');
      return { count: txns.length };
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
  }

  async getOrCreateTenant(sourceName, { external_id = null, name }) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      if (external_id) {
        const byExt = await client.query('SELECT id, msp_tenant_uuid FROM msp_tenants WHERE source_id = $1 AND external_id = $2', [sourceId, external_id]);
        if (byExt.rows.length) return { tenantId: byExt.rows[0].id, tenantUuid: byExt.rows[0].msp_tenant_uuid };
      }
      const byName = await client.query('SELECT id, msp_tenant_uuid FROM msp_tenants WHERE LOWER(name) = LOWER($1) ORDER BY id DESC LIMIT 1', [name]);
      if (byName.rows.length) return { tenantId: byName.rows[0].id, tenantUuid: byName.rows[0].msp_tenant_uuid };

      const uuid = (require('crypto').randomUUID) ? require('crypto').randomUUID() : require('crypto').randomBytes(16).toString('hex');
      const ins = await client.query(
        `INSERT INTO msp_tenants (source_id, external_id, name, msp_tenant_uuid) VALUES ($1,$2,$3,$4) RETURNING id, msp_tenant_uuid`,
        [sourceId, external_id, name, uuid]
      );
      return { tenantId: ins.rows[0].id, tenantUuid: ins.rows[0].msp_tenant_uuid };
    } finally { client.release(); }
  }

  async getOrCreateClient(sourceName, { external_id = null, name }) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      if (external_id) {
        const byExt = await client.query('SELECT id, msp_customer_uuid FROM clients WHERE source_id = $1 AND external_id = $2', [sourceId, external_id]);
        if (byExt.rows.length) return { clientId: byExt.rows[0].id, clientUuid: byExt.rows[0].msp_customer_uuid };
      }
      const byName = await client.query('SELECT id, msp_customer_uuid FROM clients WHERE LOWER(name) = LOWER($1) ORDER BY id DESC LIMIT 1', [name]);
      if (byName.rows.length) return { clientId: byName.rows[0].id, clientUuid: byName.rows[0].msp_customer_uuid };

      const uuid = (require('crypto').randomUUID) ? require('crypto').randomUUID() : require('crypto').randomBytes(16).toString('hex');
      const ins = await client.query(
        `INSERT INTO clients (source_id, external_id, name, msp_customer_uuid) VALUES ($1,$2,$3,$4) RETURNING id, msp_customer_uuid`,
        [sourceId, external_id, name, uuid]
      );
      return { clientId: ins.rows[0].id, clientUuid: ins.rows[0].msp_customer_uuid };
    } finally { client.release(); }
  }

  async upsertLocation(sourceName, clientUuid, location) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      const c = await client.query('SELECT id FROM clients WHERE msp_customer_uuid = $1', [clientUuid]);
      if (!c.rows.length) throw new Error('Client not found for provided msp_customer_uuid');
      const clientId = c.rows[0].id;

      const existing = location.external_id ? await client.query('SELECT id FROM locations WHERE source_id = $1 AND external_id = $2', [sourceId, location.external_id]) : { rows: [] };
      let msp_location_uuid = location.msp_location_uuid;
      if (!msp_location_uuid) msp_location_uuid = (require('crypto').randomUUID) ? require('crypto').randomUUID() : require('crypto').randomBytes(16).toString('hex');

      if (existing.rows.length) {
        await client.query(
          `UPDATE locations SET name=$1, address1=$2, address2=$3, city=$4, state=$5, postal_code=$6, country=$7, metadata=$8 WHERE id=$9`,
          [location.name || null, location.address1 || null, location.address2 || null, location.city || null, location.state || null, location.postal_code || null, location.country || null, location.metadata || {}, existing.rows[0].id]
        );
        return existing.rows[0].id;
      } else {
        const ins = await client.query(
          `INSERT INTO locations (client_id, msp_location_uuid, source_id, external_id, name, address1, address2, city, state, postal_code, country, metadata)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
          [clientId, msp_location_uuid, sourceId, location.external_id || null, location.name || null, location.address1 || null, location.address2 || null, location.city || null, location.state || null, location.postal_code || null, location.country || null, location.metadata || {}]
        );
        return ins.rows[0].id;
      }
    } finally { client.release(); }
  }

  async upsertContract(sourceName, clientUuid, contract, locationUuid = null) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      const c = await client.query('SELECT id FROM clients WHERE msp_customer_uuid = $1', [clientUuid]);
      if (!c.rows.length) throw new Error('Client not found for provided msp_customer_uuid');
      const clientId = c.rows[0].id;
      let locationId = null;
      if (locationUuid) {
        const loc = await client.query('SELECT id FROM locations WHERE msp_location_uuid = $1', [locationUuid]);
        if (loc.rows.length) locationId = loc.rows[0].id;
      }

      const existing = contract.external_id ? await client.query('SELECT id FROM contracts WHERE source_id = $1 AND external_id = $2', [sourceId, contract.external_id]) : { rows: [] };
      let msp_contract_uuid = contract.msp_contract_uuid;
      if (!msp_contract_uuid) msp_contract_uuid = (require('crypto').randomUUID) ? require('crypto').randomUUID() : require('crypto').randomBytes(16).toString('hex');

      if (existing.rows.length) {
        await client.query(
          `UPDATE contracts SET name=$1, contract_type=$2, start_date=$3, end_date=$4, status=$5, terms=$6, amount=$7, billing_cycle=$8, metadata=$9, location_id=$10 WHERE id=$11`,
          [contract.name || null, contract.contract_type || null, contract.start_date || null, contract.end_date || null, contract.status || null, contract.terms || null, contract.amount || null, contract.billing_cycle || null, contract.metadata || {}, locationId, existing.rows[0].id]
        );
        return existing.rows[0].id;
      } else {
        const ins = await client.query(
          `INSERT INTO contracts (client_id, location_id, msp_contract_uuid, source_id, external_id, name, contract_type, start_date, end_date, status, terms, amount, billing_cycle, metadata)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id`,
          [clientId, locationId, msp_contract_uuid, sourceId, contract.external_id || null, contract.name || null, contract.contract_type || null, contract.start_date || null, contract.end_date || null, contract.status || null, contract.terms || null, contract.amount || null, contract.billing_cycle || null, contract.metadata || {}]
        );
        return ins.rows[0].id;
      }
    } finally { client.release(); }
  }

  async upsertContact(sourceName, clientUuid, contact, locationUuid = null) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      const c = await client.query('SELECT id FROM clients WHERE msp_customer_uuid = $1', [clientUuid]);
      if (!c.rows.length) throw new Error('Client not found for provided msp_customer_uuid');
      const clientId = c.rows[0].id;
      let locationId = null;
      if (locationUuid) {
        const loc = await client.query('SELECT id FROM locations WHERE msp_location_uuid = $1', [locationUuid]);
        if (loc.rows.length) locationId = loc.rows[0].id;
      }

      const existing = contact.external_id ? await client.query('SELECT id FROM contacts WHERE source_id = $1 AND external_id = $2', [sourceId, contact.external_id]) : { rows: [] };
      let msp_contact_uuid = contact.msp_contact_uuid;
      if (!msp_contact_uuid) msp_contact_uuid = (require('crypto').randomUUID) ? require('crypto').randomUUID() : require('crypto').randomBytes(16).toString('hex');

      if (existing.rows.length) {
        await client.query(
          `UPDATE contacts SET first_name=$1, last_name=$2, email=$3, phone=$4, role=$5, metadata=$6, client_id=$7, location_id=$8 WHERE id=$9`,
          [contact.first_name || null, contact.last_name || null, contact.email || null, contact.phone || null, contact.role || null, contact.metadata || {}, clientId, locationId, existing.rows[0].id]
        );
        return existing.rows[0].id;
      } else {
        const ins = await client.query(
          `INSERT INTO contacts (client_id, location_id, msp_contact_uuid, source_id, external_id, first_name, last_name, email, phone, role, metadata)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`,
          [clientId, locationId, msp_contact_uuid, sourceId, contact.external_id || null, contact.first_name || null, contact.last_name || null, contact.email || null, contact.phone || null, contact.role || null, contact.metadata || {}]
        );
        return ins.rows[0].id;
      }
    } finally { client.release(); }
  }

  async createImportMetadata(sourceName, importType, { original_filename = null, content_type = null, row_count = null, raw_headers = null, sample = null } = {}) {
    const sourceId = await this.getOrCreateSource(sourceName, 'csv');
    const client = await this.pool.connect();
    try {
      const res = await client.query(
        `INSERT INTO import_metadata (source_id, import_type, original_filename, content_type, row_count, raw_headers, sample)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
        [sourceId, importType, original_filename, content_type, row_count, raw_headers ? JSON.stringify(raw_headers) : null, sample ? JSON.stringify(sample) : null]
      );
      return res.rows[0].id;
    } finally { client.release(); }
  }

  async addImportRecords(importMetadataId, records) {
    if (!records || !records.length) return 0;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const text = `INSERT INTO import_records (import_metadata_id, external_id, checksum, raw) VALUES ($1,$2,$3,$4)`;
      for (const r of records) {
        await client.query(text, [importMetadataId, r.external_id || null, r.checksum || null, JSON.stringify(r.raw || {})]);
      }
      await client.query('COMMIT');
      return records.length;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally { client.release(); }
  }

  async getImportMetadata(limit = 100, offset = 0) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT im.*, ls.name as source_name
         FROM import_metadata im
         LEFT JOIN ledger_sources ls ON im.source_id = ls.id
         ORDER BY im.created_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
      );
      return rows;
    } finally { client.release(); }
  }

  async getImportMetadataById(id) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT im.*, ls.name as source_name
         FROM import_metadata im
         LEFT JOIN ledger_sources ls ON im.source_id = ls.id
         WHERE im.id = $1`,
        [id]
      );
      return rows[0] || null;
    } finally { client.release(); }
  }

  async getImportRecords(importMetadataId, limit = 50, offset = 0) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT id, external_id, checksum, raw, created_at
         FROM import_records
         WHERE import_metadata_id = $1
         ORDER BY id ASC
         LIMIT $2 OFFSET $3`,
        [importMetadataId, limit, offset]
      );
      return rows;
    } finally { client.release(); }
  }

  async getLedgerTransactions(limit = 100, offset = 0) {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query(
        `SELECT lt.*, ls.name AS source_name
         FROM ledger_transactions lt
         LEFT JOIN ledger_sources ls ON lt.source_id = ls.id
         ORDER BY lt.created_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
      );
      return rows;
    } finally { client.release(); }
  }

  async getQbdAccounts() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query('SELECT * FROM qbd_accounts WHERE active = TRUE ORDER BY role NULLS LAST, name');
      return rows;
    } finally { client.release(); }
  }

  async upsertQbdAccount(account) {
    const client = await this.pool.connect();
    try {
      const { name, role = null, account_type = null, is_default = false, active = true } = account;
      const existing = await client.query('SELECT id FROM qbd_accounts WHERE name = $1', [name]);
      if (existing.rows.length) {
        await client.query('UPDATE qbd_accounts SET role=$1, account_type=$2, is_default=$3, active=$4 WHERE id=$5', [role, account_type, !!is_default, !!active, existing.rows[0].id]);
        return existing.rows[0].id;
      } else {
        const ins = await client.query('INSERT INTO qbd_accounts (name, role, account_type, is_default, active) VALUES ($1,$2,$3,$4,$5) RETURNING id', [name, role, account_type, !!is_default, !!active]);
        return ins.rows[0].id;
      }
    } finally { client.release(); }
  }

  async setDefaultQbdAccount(role, accountId) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('UPDATE qbd_accounts SET is_default = FALSE WHERE role = $1', [role]);
      await client.query('UPDATE qbd_accounts SET is_default = TRUE, role = $1 WHERE id = $2', [role, accountId]);
      await client.query('COMMIT');
      return true;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally { client.release(); }
  }

  async getDefaultQbdAccounts() {
    const client = await this.pool.connect();
    try {
      const { rows } = await client.query('SELECT role, name FROM qbd_accounts WHERE is_default = TRUE');
      const map = {};
      for (const r of rows) { if (r.role) map[r.role] = r.name; }
      return map;
    } finally { client.release(); }
  }

  async close() {
    await this.pool.end();
  }
}

module.exports = Database;
