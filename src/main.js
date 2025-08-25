const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse/sync');
const dayjs = require('dayjs');
const Database = require('./database');

// Initialize database
const db = new Database();

function parseCliArgs(argv) {
  const args = argv.slice(1);
  let input = null;
  let output = null;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--') continue;
    if (a === '--input' || a === '-i') { input = args[i + 1]; i++; continue; }
    if (a === '--output' || a === '-o') { output = args[i + 1]; i++; continue; }
  }
  const bare = args.filter(a => !a.startsWith('--'));
  if (!input && bare.length >= 1) input = bare[0];
  if (!output && bare.length >= 2) output = bare[1];
  if (input && output) return { input, output };
  return null;
}

function formatDate(input) {
  if (!input) return dayjs().format('MM/DD/YYYY');
  const candidates = [
    'MM/DD/YYYY h:mm A', 'MM/DD/YYYY HH:mm', 'MM/DD/YYYY', 'M/D/YYYY', 'M/D/YY', 'MM-DD-YYYY', 'YYYY-MM-DD',
  ];
  for (const fmt of candidates) {
    const d = dayjs(input, fmt, true);
    if (d.isValid()) return d.format('MM/DD/YYYY');
  }
  const d2 = dayjs(input);
  return d2.isValid() ? d2.format('MM/DD/YYYY') : dayjs().format('MM/DD/YYYY');
}

function sanitize(value) {
  if (value === undefined || value === null) return '';
  return String(value).replace(/[\t\r\n]/g, ' ').replace(/"/g, '');
}

function computeDueDate(dateStr, terms) {
  if (!dateStr) return '';
  const t = String(terms || '').toLowerCase();
  const netMatch = t.match(/^net\s+(\d{1,3})/);
  if (netMatch) {
    const add = Number(netMatch[1]) || 0;
    const d = dayjs(dateStr, 'MM/DD/YYYY');
    if (d.isValid()) return d.add(add, 'day').format('MM/DD/YYYY');
  }
  return dateStr;
}

function parseBillsFromCsv(content) {
  const records = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });
  if (!records.length) {
    throw new Error('CSV is empty or could not be parsed.');
  }
  const headerKeys = Object.keys(records[0]).map((h) => String(h).trim().toLowerCase());
  const required = ['item', 'qty', 'cost', 'vendor', 'refnumber'];
  const missing = required.filter((h) => !headerKeys.includes(h));
  if (missing.length) {
    throw new Error(`Missing required columns: ${missing.join(', ')}`);
  }
  const groupMap = new Map();
  for (const row of records) {
    const vendor = sanitize(row['Vendor'] || '');
    const ref = sanitize(row['RefNumber'] || '');
    const date = formatDate(row['Date'] || row['TxnDate'] || row['Transaction Date'] || row['PO Date'] || row['DocDate'] || row['PODate'] || row['DATE']);
    if (!vendor || !ref) continue;
    const key = `${vendor}||${ref}||${date}`;
    if (!groupMap.has(key)) groupMap.set(key, []);
    groupMap.get(key).push(row);
  }

  const bills = [];
  for (const [key, rows] of groupMap.entries()) {
    const [vendor, ref, date] = key.split('||');
    // Try to infer payment terms from common CSV columns within the grouped rows
    let inferredTerms = '';
    for (const r of rows) {
      const t = sanitize(r['Terms'] || r['Payment Terms'] || r['Term'] || '');
      if (t) { inferredTerms = t; break; }
    }
    const lines = [];
    let total = 0;
    for (const r of rows) {
      const qty = Number((r['Qty'] || '').toString().trim()) || 0;
      const cost = Number((r['Cost'] || '').toString().trim()) || 0;
      const lineAmount = Math.round(qty * cost * 100) / 100;
      total += lineAmount;
      lines.push({
        item: sanitize(r['Item'] || ''),
        description: sanitize(r['Description'] || ''),
        quantity: qty,
        unit_cost: Math.round(cost * 100) / 100,
        line_amount: Math.round(lineAmount * 100) / 100,
      });
    }
    const terms = inferredTerms || 'Due upon receipt';
    const due_date = computeDueDate(date, terms);
    bills.push({ vendor, ref_num: ref, date, total_amount: Math.round(total * 100) / 100, due_date, terms, lines });
  }
  if (!bills.length) {
    throw new Error('No valid bills found (missing Vendor/RefNumber on rows).');
  }
  return bills;
}

function generateIif(bills) {
  const out = [];
  out.push('!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tTOPRINT\tADDR5\tDUEDATE\tTERMS');
  out.push('!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tQNTY\tPRICE\tINVITEM');
  out.push('!ENDTRNS');
  for (const bill of bills) {
    out.push(['TRNS','', 'BILL', bill.date, 'Accounts Payable', bill.vendor, '', `-${bill.total_amount.toFixed(2)}`, bill.ref_num, '', 'N', 'N', '', bill.due_date, bill.terms].join('\t'));
    for (const line of bill.lines) {
      out.push(['SPL','', 'BILL', bill.date, 'Inventory Asset', '', '', line.line_amount.toFixed(2), '', line.description, 'N', String(line.quantity || ''), line.unit_cost.toFixed(2), line.item].join('\t'));
    }
    out.push('ENDTRNS');
  }
  return out.join('\n') + '\n';
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1100,
    height: 760,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
    title: 'CSV ➜ QuickBooks Bills (IIF)'
  });

  win.loadFile(path.join(__dirname, 'renderer.html'));
}

ipcMain.handle('pick-file', async () => {
  try {
    const res = await dialog.showOpenDialog({ filters: [{ name: 'CSV', extensions: ['csv'] }], properties: ['openFile'] });
    if (res.canceled || !res.filePaths.length) return null;
    const filePath = res.filePaths[0];
    const content = fs.readFileSync(filePath, 'utf-8');
    
    // Check for duplicates and store CSV
    const filename = path.basename(filePath);
    const importResult = await db.storeCsvImport(filename, content);
    
    if (importResult.isDuplicate) {
      return { 
        error: importResult.message,
        isDuplicate: true,
        importId: importResult.id
      };
    }
    
    // Parse CSV and return preview
    const bills = parseBillsFromCsv(content);
    return { 
      bills, 
      filePath, 
      importId: importResult.id,
      isDuplicate: false 
    };
  } catch (err) {
    return { error: err.message || String(err) };
  }
});

ipcMain.handle('drop-csv', async (evt, content, filename = 'dropped.csv') => {
  try {
    // Check for duplicates and store CSV
    const importResult = await db.storeCsvImport(filename, content);
    
    if (importResult.isDuplicate) {
      return { 
        error: importResult.message,
        isDuplicate: true,
        importId: importResult.id
      };
    }
    
    // Parse CSV and return preview
    const bills = parseBillsFromCsv(content);
    return { 
      bills, 
      importId: importResult.id,
      isDuplicate: false 
    };
  } catch (err) {
    return { error: err.message || String(err) };
  }
});

ipcMain.handle('process-import', async (evt, importId, bills) => {
  try {
    await db.processCsvImport(importId, bills);
    return { success: true };
  } catch (err) {
    console.error('Error processing import:', err);
    return { error: err.message };
  }
});

ipcMain.handle('export-iif', async (evt, bills, suggestedName = 'bills_output.iif') => {
  const iif = generateIif(bills);
  const res = await dialog.showSaveDialog({ defaultPath: suggestedName, filters: [{ name: 'IIF', extensions: ['iif'] }] });
  if (res.canceled || !res.filePath) return null;
  fs.writeFileSync(res.filePath, iif, 'utf-8');
  
  // Record export in database
  try {
    const totalAmount = bills.reduce((sum, bill) => sum + bill.total_amount, 0);
    const transactionIds = bills.map(bill => bill.dbTransactionId || 0).filter(id => id > 0);
    
    if (transactionIds.length > 0) {
      await db.recordExport(
        path.basename(res.filePath),
        res.filePath,
        transactionIds,
        totalAmount
      );
    }
  } catch (err) {
    console.error('Error recording export to database:', err);
  }
  
  return { saved: true, path: res.filePath };
});

// Database operation handlers
ipcMain.handle('get-dashboard-stats', async () => {
  try {
    return await db.getDashboardStats();
  } catch (err) {
    console.error('Error getting dashboard stats:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-recent-transactions', async (evt, hours = 24) => {
  try {
    return await db.getRecentTransactions(hours);
  } catch (err) {
    console.error('Error getting recent transactions:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-all-transactions', async () => {
  try {
    return await db.getAllTransactions();
  } catch (err) {
    console.error('Error getting all transactions:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-inventory-summary', async () => {
  try {
    return await db.getInventorySummary();
  } catch (err) {
    console.error('Error getting inventory summary:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-inventory-for-export', async () => {
  try {
    return await db.getInventoryForExport();
  } catch (err) {
    console.error('Error getting inventory for export:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-import-history', async () => {
  try {
    return await db.getImportHistory();
  } catch (err) {
    console.error('Error getting import history:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-export-history', async () => {
  try {
    return await db.getExportHistory();
  } catch (err) {
    console.error('Error getting export history:', err);
    return { error: err.message };
  }
});

ipcMain.handle('get-item-transactions', async (evt, itemId) => {
  try {
    return await db.getItemTransactions(itemId);
  } catch (err) {
    console.error('Error getting item transactions:', err);
    return { error: err.message };
  }
});

app.whenReady().then(() => {
  const cli = parseCliArgs(process.argv);
  if (cli) {
    try {
      const csvContent = fs.readFileSync(cli.input, 'utf-8');
      const bills = parseBillsFromCsv(csvContent);
      const iif = generateIif(bills);
      fs.writeFileSync(cli.output, iif, 'utf-8');
      console.log(`Successfully converted ${cli.input} to ${cli.output}`);
      app.exit(0);
      return;
    } catch (err) {
      console.error(`Error: ${err.message || String(err)}`);
      app.exit(1);
      return;
    }
  }
  createWindow();
  app.on('activate', function () {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', function () {
  if (process.platform !== 'darwin') app.quit();
});

app.on('before-quit', () => {
  db.close();
});


