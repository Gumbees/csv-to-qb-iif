

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const dayjs = require('dayjs');
const Database = require('./database');
const StripeAPI = require('./stripe-api');
const HaloPSAAPI = require('./halopsa-api');
const ConfigAPI = require('./config-api');
const crypto = require('crypto');
const QBWCService = require('./qbwc-service');
const ItemSyncService = require('./item-sync-service');
const TransactionMapper = require('./transaction-mapper');
const ClientMapper = require('./client-mapper');
const AIService = require('./ai-service');
const registerAIEndpoints = require('./ai-endpoints');

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
const db = new Database();
const qbwcService = new QBWCService();
const configAPI = new ConfigAPI(db);
const aiService = new AIService(db);
// Stripe integration now handled by StripeAPI class

// Ensure DB initialization completes before serving traffic
(async () => {
  try { await db.ready; console.log('Database initialized'); }
  catch (e) { console.error('Database init failed:', e); process.exit(1); }
})();

app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '25mb' }));

// Webhook endpoints will be handled by StripeAPI class

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

    // Accept synonyms for required fields and map to actual header names
    const findActualCol = (syns) => {
        for (const syn of syns) {
            const actual = Object.keys(records[0]).find(k => k.toLowerCase().trim() === syn);
            if (actual) return actual;
        }
        return null;
    };
    const vendorCol = findActualCol(['vendor','supplier','vendor name']);
    const refCol = findActualCol(['refnumber','ref number','reference','reference number','docnum','document number']);
    const itemCol = findActualCol(['item','item code','sku','product','product code']);
    const qtyCol = findActualCol(['qty','quantity','qnty']);
    const costCol = findActualCol(['cost','unit cost','price','unit price','rate']);

    const missing = [];
    if (!vendorCol) missing.push('vendor');
    if (!refCol) missing.push('refnumber');
    if (!itemCol) missing.push('item');
    if (!qtyCol) missing.push('qty');
    if (!costCol) missing.push('cost');
    if (missing.length) {
        throw new Error(`Missing required columns: ${missing.join(', ')}`);
    }

    const groupMap = new Map();
    for (const row of records) {
        const vendor = sanitize(row[vendorCol] || '');
        const ref = sanitize(row[refCol] || '');
        const date = formatDate(row['Date'] || row['TxnDate'] || row['Transaction Date'] || row['PO Date'] || row['DocDate'] || row['PODate'] || row['DATE']);
        if (!vendor || !ref) continue;
        const key = `${vendor}||${ref}||${date}`;
        if (!groupMap.has(key)) groupMap.set(key, []);
        groupMap.get(key).push(row);
    }

    const bills = [];
    for (const [key, rows] of groupMap.entries()) {
        const [vendor, ref, date] = key.split('||');
        let inferredTerms = '';
        for (const r of rows) {
            const t = sanitize(r['Terms'] || r['Payment Terms'] || r['Term'] || '');
            if (t) { inferredTerms = t; break; }
        }
        const lines = [];
        let total = 0;
        for (const r of rows) {
            const qty = Number((r[qtyCol] || '').toString().replace(/[^0-9.-]/g, '')) || 0;
            const cost = Number((r[costCol] || '').toString().replace(/[^0-9.-]/g, '')) || 0;
            const lineAmount = Math.round(qty * cost * 100) / 100;
            total += lineAmount;
            lines.push({
                item: sanitize(r[itemCol] || ''),
                description: sanitize(r['Description'] || r['Item Description'] || ''),
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

function getHeadersLower(rows) {
    return rows.length ? Object.keys(rows[0]).map(h => String(h).trim().toLowerCase()) : [];
}

function detectCsvType(rows, filename = '') {
    const h = getHeadersLower(rows);
    const has = (name) => h.includes(name);
    const any = (...names) => names.some(n => has(n));
    const all = (...names) => names.every(n => has(n));
    const fname = (filename || '').toLowerCase();

    // Explicit Purchase Order (PO) detection by PO_ID per user requirement
    if (has('po_id')) {
        return 'po_bills';
    }

    // FNBPA batch summary
    if (all('batch number', 'transfer description', 'effective date') && any('dr amount', 'cr amount')) {
        return 'bank_batch';
    }

    // Halo invoices export (robust)
    const invoiceNumberSyn = ['invoice number','invoice no','invoice #','inv','invoice','document number'];
    const invoiceIdSyn = ['invoiceid','invoice id','id'];
    const invDateSyn = ['invoice date','date','invoicedate'];
    const customerSyn = ['customer','customer name','account name','client','client name'];
    const amountsSyn = ['total','amount due','balance','subtotal'];
    if (any(...invoiceNumberSyn) || any(...invoiceIdSyn) || (any(...customerSyn) && any(...invDateSyn) && any(...amountsSyn)) || fname.includes('invoice')) {
        return 'halo_invoices';
    }

    // Stripe CSV heuristic
    if (any('balance transaction id', 'type') && any('amount', 'net')) {
        return 'stripe_csv';
    }

    // PO bills (robust)
    if (any('vendor','supplier','vendor name') && any('refnumber','ref number','reference','reference number','docnum','document number') && any('item','item code','sku','product','product code','description') && any('qty','quantity','qnty') && any('cost','unit cost','price','unit price','rate')) {
        return 'po_bills';
    }

    // Generic bank CSV
    if (any('date','posting date','transaction date') && any('amount', 'credit', 'debit') && any('description', 'memo', 'details')) {
        return 'bank_generic';
    }
    return 'unknown';
}

function generateIif(bills) {
    const out = [];
    out.push('!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tTOPRINT\tADDR5\tDUEDATE\tTERMS');
    out.push('!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tQNTY\tPRICE\tINVITEM');
    out.push('!ENDTRNS');
    for (const bill of bills) {
        out.push(['TRNS', '', 'BILL', bill.date, 'Accounts Payable', bill.vendor, '', `-${bill.total_amount.toFixed(2)}`, bill.ref_num, '', 'N', 'N', '', bill.due_date, bill.terms].join('\t'));
        for (const line of bill.lines) {
            out.push(['SPL', '', 'BILL', bill.date, 'Inventory Asset', '', '', line.line_amount.toFixed(2), '', line.description, 'N', String(line.quantity || ''), line.unit_cost.toFixed(2), line.item].join('\t'));
        }
        out.push('ENDTRNS');
    }
    return out.join('\n') + '\n';
}

// Stripe webhook (optional if STRIPE_SECRET_KEY provided)
app.post('/api/webhooks/stripe', async (req, res) => {
    if (!stripe || !stripeWebhookSecret) return res.status(501).send('Stripe not configured');
    const sig = req.headers['stripe-signature'];
    let event;
    try {
        event = stripe.webhooks.constructEvent(req.body, sig, stripeWebhookSecret);
    } catch (err) {
        return res.status(400).send(`Webhook Error: ${err.message}`);
    }
    try {
        // Placeholder: we can expand handling later
        res.json({ received: true });
    } catch (err) {
        console.error('Stripe webhook error:', err);
        res.status(500).json({ error: err.message });
    }
});

// REST backfill from Stripe (simple version) — requires STRIPE_SECRET_KEY
app.post('/api/import/stripe/backfill', async (req, res) => {
    try {
        if (!stripe) return res.status(501).json({ error: 'Stripe not configured' });
        const limit = Math.min(Number(req.query.limit || 100), 100);
        const txns = [];
        const iterator = stripe.balanceTransactions.list({ limit });
        const page = iterator.autoPagingEach ? iterator.autoPagingEach() : iterator.data;
        const rawRecords = [];
        for await (const bt of page) {
            const net = (bt.net || 0) / 100;
            const created = new Date((bt.created || 0) * 1000);
            txns.push({
                external_id: bt.id,
                txn_date: created,
                amount: net,
                currency: (bt.currency || 'usd').toUpperCase(),
                description: `${bt.type} ${bt.source || ''}`.trim(),
                memo: bt.description || '',
                balance_after: null,
                status: bt.status || null,
                raw: bt
            });
            rawRecords.push({ external_id: bt.id, checksum: hashRow(bt), raw: bt });
        }

        // Capture import metadata for Stripe backfill
        const importMetaId = await db.createImportMetadata('Stripe', 'stripe_backfill', {
            original_filename: null,
            content_type: 'application/json',
            row_count: rawRecords.length,
            raw_headers: [],
            sample: rawRecords.slice(0, 5).map(r => r.raw)
        });
        await db.addImportRecords(importMetaId, rawRecords);

        const result = await db.insertBankTransactions('Stripe', txns);
        res.json({ imported: result.count, import_meta_id: importMetaId });
    } catch (err) {
        console.error('Stripe backfill error:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/healthz', async (req, res) => {
    try {
        // Simple DB availability check
        await db.ready;
        res.status(200).json({ status: 'ok' });
    } catch (e) {
        res.status(500).json({ status: 'error', message: e.message });
    }
});

// Serve static files from the src directory
app.use(express.static(__dirname));

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});




app.post('/api/pick-file', upload.single('file'), async (req, res) => {
    try {


        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const content = req.file.buffer.toString('utf-8');
        const filename = req.file.originalname || 'uploaded.csv';
        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });

        const detectedType = db.detectCsvType(rows, filename);

        if (detectedType === 'po_bills') {
            const metaId = await db.createImportMetadata('LocalCSV', 'po_bills', {

                original_filename: filename, content_type: req.file.mimetype || 'text/csv',
                row_count: rows.length, raw_headers: rows.length ? Object.keys(rows[0]) : [], sample: rows.slice(0, 5)
            });
            await db.addImportRecords(metaId, rows.map(r => ({ external_id: r['RefNumber'] || r['Ref Number'] || null, checksum: hashRow(r), raw: r })));

            const importResult = await db.storeCsvImport(filename, content);
            if (importResult.isDuplicate) return res.json({ error: importResult.message, isDuplicate: true, importId: importResult.id, detectedType });
            const bills = db.parseBillsFromCsv(content);

            return res.json({ bills, filePath: filename, importId: importResult.id, isDuplicate: false, import_meta_id: metaId, detectedType });
        }

        return res.status(501).json({ error: `File upload detected as ${detectedType}. Please use drag-and-drop for auto-routing.`, detectedType });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});





app.post('/api/process-import', async (req, res) => {
    const { importId, bills } = req.body;
    try {
        await db.processCsvImport(importId, bills);
        res.json({ success: true });
    } catch (err) {
        console.error('Error processing import:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/pick-file', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }
        const content = req.file.buffer.toString('utf-8');
        const filename = req.file.originalname || 'uploaded.csv';
        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });
        const detectedType = detectCsvType(rows, filename);

        if (detectedType === 'po_bills') {
            const poMetaId = await db.createImportMetadata('LocalCSV', 'po_bills', {
                original_filename: filename,
                content_type: req.file.mimetype || 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(poMetaId, rows.map(r => ({ external_id: r['RefNumber'] || r['Ref Number'] || null, checksum: hashRow(r), raw: r })));

            const importResult = await db.storeCsvImport(filename, content);
            if (importResult.isDuplicate) {
                return res.json({ error: importResult.message, isDuplicate: true, importId: importResult.id, detectedType });
            }
            const bills = parseBillsFromCsv(content);
            return res.json({ bills, filePath: filename, importId: importResult.id, isDuplicate: false, import_meta_id: poMetaId, detectedType });
        }
        if (detectedType === 'halo_invoices') {
            const importMetaId = await db.createImportMetadata('HaloPSA', 'halo_invoices', {
                original_filename: filename,
                content_type: req.file.mimetype || 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(importMetaId, rows.map(r => ({ external_id: r['InvoiceID'] || r['Invoice ID'] || r['ID'] || null, checksum: hashRow(r), raw: r })));

            // Group and upsert
            const map = new Map();
            for (const r of rows) {
                const num = String(r['Invoice Number'] || r['Number'] || r['Invoice'] || r['INV'] || r['Document Number'] || '').trim();
                const extId = String(r['InvoiceID'] || r['Invoice ID'] || r['ID'] || '').trim();
                const key = num || extId || hashRow({ r });
                if (!map.has(key)) map.set(key, []);
                map.get(key).push(r);
            }
            let imported = 0;
            const parseNum = (v) => v == null ? null : Number(String(v).replace(/[^0-9.-]/g, ''));
            const parseDateStr = (v) => dayjs(v).isValid() ? dayjs(v).format('YYYY-MM-DD') : null;
            for (const [key, group] of map.entries()) {
                const head = group[0];
                const invoice = {
                    external_id: String(head['InvoiceID'] || head['Invoice ID'] || head['ID'] || head['Number'] || head['Document Number'] || '').trim() || null,
                    number: String(head['Invoice Number'] || head['Number'] || head['Document Number'] || '').trim() || null,
                    invoice_date: parseDateStr(head['Invoice Date'] || head['Date'] || head['InvoiceDate']),
                    due_date: parseDateStr(head['Due Date'] || head['DueDate']),
                    status: head['Status'] || null,
                    currency: (head['Currency'] || head['Document Currency'] || 'USD').toString(),
                    subtotal: parseNum(head['Subtotal']),
                    tax_total: head['Tax'] ? parseNum(head['Tax']) : (head['Tax Total'] ? parseNum(head['Tax Total']) : null),
                    total: parseNum(head['Total']),
                    balance: head['Balance'] ? parseNum(head['Balance']) : (head['Amount Due'] ? parseNum(head['Amount Due']) : null),
                    customer: {
                        external_id: head['CustomerID'] || head['Customer Id'] || head['Customer ID'] || null,
                        name: head['Customer'] || head['Customer Name'] || head['Account Name'] || null,
                        email: head['Customer Email'] || null,
                        address: head['Billing Address'] || null
                    },
                    raw: head
                };
                const lines = group.map(r => {
                    const qtyN = parseNum(r['Qty'] || r['Quantity']);
                    const priceN = parseNum(r['Unit Price'] || r['Price']);
                    const lineTotal = r['Line Total'] != null ? parseNum(r['Line Total']) : (qtyN != null && priceN != null ? Number((qtyN * priceN).toFixed(2)) : null);
                    return { item_code: r['Item Code'] || r['Item'] || r['SKU'] || null, description: r['Description'] || r['Item Description'] || null, quantity: qtyN, unit_price: priceN, tax_code: r['Tax Code'] || null, line_total: lineTotal };
                });
                await db.upsertInvoiceWithLines('HaloPSA', invoice, lines);
                imported++;
            }
            return res.json({ detectedType, imported, import_meta_id: importMetaId });
        }
        if (detectedType === 'bank_batch' || detectedType === 'bank_generic') {
            const sourceName = detectedType === 'bank_batch' ? 'FNBPA' : 'BankCSV';
            const importMetaId = await db.createImportMetadata(sourceName, detectedType, {
                original_filename: filename,
                content_type: req.file.mimetype || 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(importMetaId, rows.map(r => ({ external_id: (r['Reference Number'] || r['reference number'] || r['Transaction ID'] || r['ID'] || null), checksum: hashRow(r), raw: r })));

            const parseNumber = (v) => { if (v==null) return 0; const n=Number(String(v).replace(/[^0-9.-]/g,'')); return isNaN(n)?0:n; };
            const txns = [];
            if (detectedType === 'bank_batch') {
                for (const r of rows) {
                    const effective = r['Effective Date'] || r['effective date'] || r['Date'] || r['date'];
                    const transferDesc = r['Transfer Description'] || r['transfer description'] || '';
                    const company = r['Company Name'] || r['company name'] || '';
                    const batchType = r['Batch Type'] || r['batch type'] || '';
                    const sec = r['SEC Code'] || r['sec code'] || '';
                    const itemCount = r['Item Count'] || r['item count'] || '';
                    const status = r['Batch Status'] || r['batch status'] || '';
                    const ref = r['Reference Number'] || r['reference number'] || '';
                    const dr = parseNumber(r['DR Amount'] || r['dr amount']);
                    const cr = parseNumber(r['CR Amount'] || r['cr amount']);
                    const baseDesc = `${transferDesc}`.trim();
                    const memoBase = `Company: ${company} | SEC: ${sec} | Items: ${itemCount} | Status: ${status} | Type: ${batchType}`;
                    const txnDate = effective ? new Date(effective) : new Date();
                    if (cr > 0) txns.push({ external_id: ref ? `${ref}-CR` : null, txn_date: txnDate, amount: cr, currency: 'USD', description: `${baseDesc} • CR`, memo: memoBase, balance_after: null, checksum: hashRow({effective,baseDesc,cr,type:'CR',ref}), raw: r });
                    if (dr > 0) txns.push({ external_id: ref ? `${ref}-DR` : null, txn_date: txnDate, amount: -Math.abs(dr), currency: 'USD', description: `${baseDesc} • DR`, memo: memoBase, balance_after: null, checksum: hashRow({effective,baseDesc,dr,type:'DR',ref}), raw: r });
                }
            } else {
                for (const r of rows) {
                    const date = r['Date'] || r['Transaction Date'] || r['Posting Date'] || r['TxnDate'];
                    const desc = r['Description'] || r['Memo'] || r['Details'] || '';
                    const memo = r['Memo'] || r['Notes'] || '';
                    const amount = r['Amount'] || r['Credit'] || r['Debit'] ? (r['Amount'] || r['Credit'] || `-${Math.abs(parseNumber(r['Debit']))}`) : (r['amount']);
                    const balance = r['Balance'] || r['Running Balance'] || null;
                    const parsedAmt = parseNumber(amount);
                    if (!parsedAmt) continue;
                    txns.push({ external_id: r['Transaction ID'] || r['ID'] || null, txn_date: date ? new Date(date) : new Date(), amount: parsedAmt, currency: 'USD', description: String(desc).trim(), memo: String(memo).trim(), balance_after: balance ? parseNumber(balance) : null, checksum: hashRow({date,desc,memo,amount,balance}), raw: r });
                }
            }
            const result = await db.insertBankTransactions(sourceName, txns);
            return res.json({ detectedType, imported: result.count, import_meta_id: importMetaId });
        }
        if (detectedType === 'stripe_csv') {
            return res.status(501).json({ error: 'Stripe CSV not supported yet. Use Stripe backfill API.', detectedType });
        }
        return res.status(400).json({ error: 'Unknown CSV structure. Please select the appropriate import route.', detectedType });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});

app.post('/api/drop-csv', async (req, res) => {
    try {
        const { content, filename } = req.body || {};
        if (!content) return res.status(400).json({ error: 'Missing CSV content' });

        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });
        const detectedType = detectCsvType(rows, filename || 'dropped.csv');

        if (detectedType === 'po_bills') {
            const poMetaId = await db.createImportMetadata('LocalCSV', 'po_bills', {
                original_filename: filename || 'dropped.csv',
                content_type: 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(poMetaId, rows.map(r => ({ external_id: r['RefNumber'] || r['Ref Number'] || null, checksum: hashRow(r), raw: r })));

            const importResult = await db.storeCsvImport(filename || 'dropped.csv', content);
            if (importResult.isDuplicate) {
                return res.json({ error: importResult.message, isDuplicate: true, importId: importResult.id, detectedType });
            }
            const bills = parseBillsFromCsv(content);
            return res.json({ bills, importId: importResult.id, isDuplicate: false, import_meta_id: poMetaId, detectedType });
        }
        if (detectedType === 'halo_invoices') {
            const importMetaId = await db.createImportMetadata('HaloPSA', 'halo_invoices', {
                original_filename: filename || 'dropped.csv',
                content_type: 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(importMetaId, rows.map(r => ({ external_id: r['InvoiceID'] || r['Invoice ID'] || r['ID'] || null, checksum: hashRow(r), raw: r })));

            const map = new Map();
            for (const r of rows) {
                const num = String(r['Invoice Number'] || r['Number'] || r['Invoice'] || r['INV'] || r['Document Number'] || '').trim();
                const extId = String(r['InvoiceID'] || r['Invoice ID'] || r['ID'] || '').trim();
                const key = num || extId || hashRow({ r });
                if (!map.has(key)) map.set(key, []);
                map.get(key).push(r);
            }
            let imported = 0;
            const parseNum = (v) => v == null ? null : Number(String(v).replace(/[^0-9.-]/g, ''));
            const parseDateStr = (v) => dayjs(v).isValid() ? dayjs(v).format('YYYY-MM-DD') : null;
            for (const [key, group] of map.entries()) {
                const head = group[0];
                const invoice = {
                    external_id: String(head['InvoiceID'] || head['Invoice ID'] || head['ID'] || head['Number'] || head['Document Number'] || '').trim() || null,
                    number: String(head['Invoice Number'] || head['Number'] || head['Document Number'] || '').trim() || null,
                    invoice_date: parseDateStr(head['Invoice Date'] || head['Date'] || head['InvoiceDate']),
                    due_date: parseDateStr(head['Due Date'] || head['DueDate']),
                    status: head['Status'] || null,
                    currency: (head['Currency'] || head['Document Currency'] || 'USD').toString(),
                    subtotal: parseNum(head['Subtotal']),
                    tax_total: head['Tax'] ? parseNum(head['Tax']) : (head['Tax Total'] ? parseNum(head['Tax Total']) : null),
                    total: parseNum(head['Total']),
                    balance: head['Balance'] ? parseNum(head['Balance']) : (head['Amount Due'] ? parseNum(head['Amount Due']) : null),
                    customer: {
                        external_id: head['CustomerID'] || head['Customer Id'] || head['Customer ID'] || null,
                        name: head['Customer'] || head['Customer Name'] || head['Account Name'] || null,
                        email: head['Customer Email'] || null,
                        address: head['Billing Address'] || null
                    },
                    raw: head
                };
                const lines = group.map(r => {
                    const qtyN = parseNum(r['Qty'] || r['Quantity']);
                    const priceN = parseNum(r['Unit Price'] || r['Price']);
                    const lineTotal = r['Line Total'] != null ? parseNum(r['Line Total']) : (qtyN != null && priceN != null ? Number((qtyN * priceN).toFixed(2)) : null);
                    return { item_code: r['Item Code'] || r['Item'] || r['SKU'] || null, description: r['Description'] || r['Item Description'] || null, quantity: qtyN, unit_price: priceN, tax_code: r['Tax Code'] || null, line_total: lineTotal };
                });
                await db.upsertInvoiceWithLines('HaloPSA', invoice, lines);
                imported++;
            }
            return res.json({ detectedType, imported, import_meta_id: importMetaId });
        }
        if (detectedType === 'bank_batch' || detectedType === 'bank_generic') {
            const sourceName = detectedType === 'bank_batch' ? 'FNBPA' : 'BankCSV';
            const importMetaId = await db.createImportMetadata(sourceName, detectedType, {
                original_filename: filename || 'dropped.csv',
                content_type: 'text/csv',
                row_count: rows.length,
                raw_headers: rows.length ? Object.keys(rows[0]) : [],
                sample: rows.slice(0, 5)
            });
            await db.addImportRecords(importMetaId, rows.map(r => ({ external_id: (r['Reference Number'] || r['reference number'] || r['Transaction ID'] || r['ID'] || null), checksum: hashRow(r), raw: r })));

            const parseNumber = (v) => { if (v==null) return 0; const n=Number(String(v).replace(/[^0-9.-]/g,'')); return isNaN(n)?0:n; };
            const txns = [];
            if (detectedType === 'bank_batch') {
                for (const r of rows) {
                    const effective = r['Effective Date'] || r['effective date'] || r['Date'] || r['date'];
                    const transferDesc = r['Transfer Description'] || r['transfer description'] || '';
                    const company = r['Company Name'] || r['company name'] || '';
                    const batchType = r['Batch Type'] || r['batch type'] || '';
                    const sec = r['SEC Code'] || r['sec code'] || '';
                    const itemCount = r['Item Count'] || r['item count'] || '';
                    const status = r['Batch Status'] || r['batch status'] || '';
                    const ref = r['Reference Number'] || r['reference number'] || '';
                    const dr = parseNumber(r['DR Amount'] || r['dr amount']);
                    const cr = parseNumber(r['CR Amount'] || r['cr amount']);
                    const baseDesc = `${transferDesc}`.trim();
                    const memoBase = `Company: ${company} | SEC: ${sec} | Items: ${itemCount} | Status: ${status} | Type: ${batchType}`;
                    const txnDate = effective ? new Date(effective) : new Date();
                    if (cr > 0) txns.push({ external_id: ref ? `${ref}-CR` : null, txn_date: txnDate, amount: cr, currency: 'USD', description: `${baseDesc} • CR`, memo: memoBase, balance_after: null, checksum: hashRow({effective,baseDesc,cr,type:'CR',ref}), raw: r });
                    if (dr > 0) txns.push({ external_id: ref ? `${ref}-DR` : null, txn_date: txnDate, amount: -Math.abs(dr), currency: 'USD', description: `${baseDesc} • DR`, memo: memoBase, balance_after: null, checksum: hashRow({effective,baseDesc,dr,type:'DR',ref}), raw: r });
                }
            } else {
                for (const r of rows) {
                    const date = r['Date'] || r['Transaction Date'] || r['Posting Date'] || r['TxnDate'];
                    const desc = r['Description'] || r['Memo'] || r['Details'] || '';
                    const memo = r['Memo'] || r['Notes'] || '';
                    const amount = r['Amount'] || r['Credit'] || r['Debit'] ? (r['Amount'] || r['Credit'] || `-${Math.abs(parseNumber(r['Debit']))}`) : (r['amount']);
                    const balance = r['Balance'] || r['Running Balance'] || null;
                    const parsedAmt = parseNumber(amount);
                    if (!parsedAmt) continue;
                    txns.push({ external_id: r['Transaction ID'] || r['ID'] || null, txn_date: date ? new Date(date) : new Date(), amount: parsedAmt, currency: 'USD', description: String(desc).trim(), memo: String(memo).trim(), balance_after: balance ? parseNumber(balance) : null, checksum: hashRow({date,desc,memo,amount,balance}), raw: r });
                }
            }
            const result = await db.insertBankTransactions(sourceName, txns);
            return res.json({ detectedType, imported: result.count, import_meta_id: importMetaId });
        }
        if (detectedType === 'stripe_csv') {
            return res.status(501).json({ error: 'Stripe CSV not supported yet. Use Stripe backfill API.', detectedType });
        }
        return res.status(400).json({ error: 'Unknown CSV structure. Please select the appropriate import route.', detectedType });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});
app.post('/api/process-import', async (req, res) => {
    const { importId, bills } = req.body;
    try {
        await db.processCsvImport(importId, bills);
        res.json({ success: true });
    } catch (err) {
        console.error('Error processing import:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/export-iif', async (req, res) => {
    const { bills, suggestedName = 'bills_output.iif', transactionIds = [] } = req.body;
    const iif = generateIif(bills);

    // Record export in DB if we have transaction IDs
    try {
        const totalAmount = bills.reduce((sum, bill) => sum + (Number(bill.total_amount) || 0), 0);
        if (Array.isArray(transactionIds) && transactionIds.length) {
            await db.recordExport(suggestedName, suggestedName, transactionIds, totalAmount);
        }
    } catch (err) {
        console.error('Error recording export:', err);
        // Continue anyway; exporting file should not fail due to DB
    }

    res.setHeader('Content-disposition', `attachment; filename=${suggestedName}`);
    res.setHeader('Content-type', 'text/plain');
    res.send(iif);
});

// =============== New Imports for Halo Items, Halo Invoices, and Bank CSV ===============

function hashRow(obj) {
    return crypto.createHash('md5').update(JSON.stringify(obj)).digest('hex');
}

app.post('/api/import/halo/items', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const content = req.file.buffer.toString('utf-8');
        const rows = parse(content, { columns: true, skip_empty_lines: true });

        // Capture import metadata and raw records
        const importMetaId = await db.createImportMetadata('HaloPSA', 'halo_items', {
            original_filename: req.file.originalname,
            content_type: req.file.mimetype,
            row_count: rows.length,
            raw_headers: rows.length ? Object.keys(rows[0]) : [],
            sample: rows.slice(0, 5)
        });
        await db.addImportRecords(importMetaId, rows.map(r => ({
            external_id: r['ID'] || r['ItemID'] || r['ExternalId'] || null,
            checksum: hashRow(r),
            raw: r
        })));

        // Flexible mapping: attempt to find common headers
        const items = rows.map(r => ({
            external_id: r['ID'] || r['ItemID'] || r['ExternalId'] || null,
            item_code: String(r['Item Code'] || r['Code'] || r['SKU'] || r['Item'] || '').trim(),
            name: r['Name'] || r['Item Name'] || r['Title'] || null,
            description: r['Description'] || null,
            category: r['Category'] || null,
            unit_cost: r['Cost'] ? Number(r['Cost']) : null,
            unit_price: r['Price'] ? Number(r['Price']) : null,
            tax_code: r['Tax Code'] || r['TaxCode'] || null,
            is_active: (String(r['Active'] || 'true').toLowerCase() !== 'false')
        })).filter(i => i.item_code);
        const result = await db.upsertCatalogItems('HaloPSA', items);
        res.json({ imported: result.count, import_meta_id: importMetaId });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});


app.post('/api/import/halo/invoices', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const content = req.file.buffer.toString('utf-8');
        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });

        // Capture import metadata and raw records
        const importMetaId = await db.createImportMetadata('HaloPSA', 'halo_invoices', {
            original_filename: req.file.originalname,
            content_type: req.file.mimetype,
            row_count: rows.length,
            raw_headers: rows.length ? Object.keys(rows[0]) : [],
            sample: rows.slice(0, 5)
        });
        await db.addImportRecords(importMetaId, rows.map(r => ({
            external_id: r['InvoiceID'] || r['Invoice ID'] || r['ID'] || null,
            checksum: hashRow(r),
            raw: r
        })));

        function parseNumber(v) {
            if (v === null || v === undefined) return null;
            const n = Number(String(v).replace(/[^0-9.-]/g, ''));
            return isNaN(n) ? null : n;
        }
        function parseDate(v) {
            if (!v) return null;
            const candidates = ['MM/DD/YYYY', 'M/D/YYYY', 'YYYY-MM-DD', 'MM/DD/YY'];
            for (const fmt of candidates) {
                const d = dayjs(v, fmt, true);
                if (d.isValid()) return d.format('YYYY-MM-DD');
            }
            const d2 = dayjs(v);
            return d2.isValid() ? d2.format('YYYY-MM-DD') : null;
        }

        // Group by invoice number or external id
        const map = new Map();
        for (const r of rows) {
            const num = String(r['Invoice Number'] || r['Number'] || r['Invoice'] || r['INV'] || '').trim();
            const extId = String(r['InvoiceID'] || r['Invoice ID'] || r['ID'] || '').trim();
            const key = num || extId || hashRow({ r });
            if (!map.has(key)) map.set(key, []);
            map.get(key).push(r);
        }

        let imported = 0;
        for (const [key, group] of map.entries()) {
            const head = group[0];
            const invoice = {
                external_id: String(head['InvoiceID'] || head['Invoice ID'] || head['ID'] || head['Number'] || '').trim() || null,
                number: String(head['Invoice Number'] || head['Number'] || '').trim() || null,
                invoice_date: parseDate(head['Invoice Date'] || head['Date'] || head['InvoiceDate']),
                due_date: parseDate(head['Due Date'] || head['DueDate']),
                status: head['Status'] || null,
                currency: (head['Currency'] || head['Document Currency'] || 'USD').toString(),
                subtotal: parseNumber(head['Subtotal']),
                tax_total: parseNumber(head['Tax'] || head['Tax Total']),
                total: parseNumber(head['Total']),
                balance: parseNumber(head['Balance'] || head['Amount Due']),
                customer: {
                    external_id: head['CustomerID'] || head['Customer Id'] || head['Customer ID'] || null,
                    name: head['Customer'] || head['Customer Name'] || null,
                    email: head['Customer Email'] || null,
                    address: head['Billing Address'] || null
                },
                raw: head
            };
            const lines = group.map(r => {
                const qty = parseNumber(r['Qty'] || r['Quantity']);
                const price = parseNumber(r['Unit Price'] || r['Price']);
                const lineTotal = parseNumber(r['Line Total']) ?? (qty !== null && price !== null ? Number((qty * price).toFixed(2)) : null);
                return {
                    item_code: r['Item Code'] || r['Item'] || r['SKU'] || null,
                    description: r['Description'] || r['Item Description'] || null,
                    quantity: qty,
                    unit_price: price,
                    tax_code: r['Tax Code'] || null,
                    line_total: lineTotal
                };
            });

            await db.upsertInvoiceWithLines('HaloPSA', invoice, lines);
            imported++;
        }
        res.json({ imported });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});

app.post('/api/import/bank/fnb', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const content = req.file.buffer.toString('utf-8');
        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });

        // Capture import metadata and raw records
        const importMetaId = await db.createImportMetadata('FNBPA', 'bank_batch', {
            original_filename: req.file.originalname,
            content_type: req.file.mimetype,
            row_count: rows.length,
            raw_headers: rows.length ? Object.keys(rows[0]) : [],
            sample: rows.slice(0, 5)
        });
        await db.addImportRecords(importMetaId, rows.map(r => ({
            external_id: r['Reference Number'] || r['reference number'] || null,
            checksum: hashRow(r),
            raw: r
        })));

        const headers = rows.length ? Object.keys(rows[0]).map(h => String(h).trim().toLowerCase()) : [];
        const hasBatchFormat = headers.includes('batch number') && headers.includes('transfer description') && headers.includes('effective date');

        function parseNumber(v) {
            if (v === null || v === undefined) return 0;
            const num = Number(String(v).replace(/[^0-9.-]/g, ''));
            return isNaN(num) ? 0 : num;
        }

        let txns = [];
        if (hasBatchFormat) {
            // ACH batch summary format (per your sample): create separate CR and DR entries per row when non-zero
            for (const r of rows) {
                const effective = r['Effective Date'] || r['effective date'] || r['Date'] || r['date'];
                const transferDesc = r['Transfer Description'] || r['transfer description'] || '';
                const company = r['Company Name'] || r['company name'] || '';
                const batchType = r['Batch Type'] || r['batch type'] || '';
                const sec = r['SEC Code'] || r['sec code'] || '';
                const itemCount = r['Item Count'] || r['item count'] || '';
                const status = r['Batch Status'] || r['batch status'] || '';
                const ref = r['Reference Number'] || r['reference number'] || '';
                const dr = parseNumber(r['DR Amount'] || r['dr amount']);
                const cr = parseNumber(r['CR Amount'] || r['cr amount']);

                const baseDesc = `${transferDesc}`.trim();
                const memoBase = `Company: ${company} | SEC: ${sec} | Items: ${itemCount} | Status: ${status} | Type: ${batchType}`;
                const txnDate = effective ? new Date(effective) : new Date();

                if (cr > 0) {
                    const desc = `${baseDesc} • CR`;
                    const checksum = hashRow({ effective, baseDesc, cr, type: 'CR', ref });
                    txns.push({
                        external_id: ref ? `${ref}-CR` : null,
                        txn_date: txnDate,
                        amount: cr, // credit positive
                        currency: 'USD',
                        description: desc,
                        memo: memoBase,
                        balance_after: null,
                        checksum,
                        raw: r
                    });
                }
                if (dr > 0) {
                    const desc = `${baseDesc} • DR`;
                    const checksum = hashRow({ effective, baseDesc, dr, type: 'DR', ref });
                    txns.push({
                        external_id: ref ? `${ref}-DR` : null,
                        txn_date: txnDate,
                        amount: -Math.abs(dr), // debit negative
                        currency: 'USD',
                        description: desc,
                        memo: memoBase,
                        balance_after: null,
                        checksum,
                        raw: r
                    });
                }
            }
        } else {
            // Generic bank CSV format
            txns = rows.map(r => {
                const date = r['Date'] || r['Transaction Date'] || r['Posting Date'] || r['TxnDate'];
                const desc = r['Description'] || r['Memo'] || r['Details'] || '';
                const memo = r['Memo'] || r['Notes'] || '';
                const amount = r['Amount'] || r['Credit'] || r['Debit'] ? parseNumber(r['Amount'] || r['Credit'] || `-${Math.abs(parseNumber(r['Debit']))}`) : parseNumber(r['amount']);
                const balance = r['Balance'] || r['Running Balance'] || null;
                const raw = r;
                return {
                    external_id: r['Transaction ID'] || r['ID'] || null,
                    txn_date: date ? new Date(date) : new Date(),
                    amount: Number(amount || 0),
                    currency: 'USD',
                    description: String(desc).trim(),
                    memo: String(memo).trim(),
                    balance_after: balance ? parseNumber(balance) : null,
                    checksum: hashRow({ date, desc, memo, amount, balance }),
                    raw
                };
            }).filter(t => t.amount !== 0);
        }

        if (!txns.length) return res.status(400).json({ error: 'No transactions parsed from CSV' });
        const result = await db.insertBankTransactions('FNBPA', txns);
        res.json({ imported: result.count });
    } catch (err) {
        res.status(400).json({ error: err.message || String(err) });
    }
});

app.get('/api/dashboard-stats', async (req, res) => {
    try {
        const stats = await db.getDashboardStats();
        res.json(stats);
    } catch (err) {
        console.error('Error getting dashboard stats:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/recent-transactions', async (req, res) => {
    const hours = req.query.hours || 24;
    try {
        const transactions = await db.getRecentTransactions(hours);
        res.json(transactions);
    } catch (err) {
        console.error('Error getting recent transactions:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/all-transactions', async (req, res) => {
    try {
        const transactions = await db.getAllTransactions();
        res.json(transactions);
    } catch (err) {
        console.error('Error getting all transactions:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/inventory-summary', async (req, res) => {
    try {
        const summary = await db.getInventorySummary();
        res.json(summary);
    } catch (err) {
        console.error('Error getting inventory summary:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/inventory-for-export', async (req, res) => {
    try {
        const inventory = await db.getInventoryForExport();
        res.json(inventory);
    } catch (err) {
        console.error('Error getting inventory for export:', err);
        res.status(500).json({ error: err.message });
    }
});
app.get('/api/import-history', async (req, res) => {
    try {
        const history = await db.getImportHistory();
        res.json(history);
    } catch (err) {
        console.error('Error getting import history:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/export-history', async (req, res) => {
    try {
        const history = await db.getExportHistory();
        res.json(history);
    } catch (err) {
        console.error('Error getting export history:', err);
        res.status(500).json({ error:err.message });
    }
});

// Imports metadata browsing
app.get('/api/imports-metadata', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
        const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
        const data = await db.getImportMetadata(limit, offset);
        res.json(data);
    } catch (err) {
        console.error('Error getting imports metadata:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/imports-metadata/:id', async (req, res) => {
    try {
        const meta = await db.getImportMetadataById(parseInt(req.params.id, 10));
        if (!meta) return res.status(404).json({ error: 'Not found' });
        res.json(meta);
    } catch (err) {
        console.error('Error getting import metadata by id:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/imports-metadata/:id/records', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit || '50', 10), 1000);
        const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
        const records = await db.getImportRecords(parseInt(req.params.id, 10), limit, offset);
        res.json(records);
    } catch (err) {
        console.error('Error getting import records:', err);
        res.status(500).json({ error: err.message });
    }
});

// Ledger browsing (basic)
app.get('/api/ledger', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
        const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
        const rows = await db.getLedgerTransactions(limit, offset);
        res.json(rows);
    } catch (err) {
        console.error('Error getting ledger:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/item-transactions/:itemId', async (req, res) => {
    const { itemId } = req.params;
    try {
        const transactions = await db.getItemTransactions(itemId);
        res.json(transactions);
    } catch (err) {
        console.error('Error getting item transactions:', err);
        res.status(500).json({ error: err.message });
    }
});

// QBD accounts configuration API
app.get('/api/qbd/accounts', async (req, res) => {
    try {
        const rows = await db.getQbdAccounts();
        res.json(rows);
    } catch (err) {
        console.error('Error getting QBD accounts:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/qbd/accounts', async (req, res) => {
    try {
        const id = await db.upsertQbdAccount(req.body || {});
        res.json({ id });
    } catch (err) {
        console.error('Error upserting QBD account:', err);
        res.status(400).json({ error: err.message });
    }
});

app.post('/api/qbd/accounts/default', async (req, res) => {
    try {
        const { role, accountId } = req.body || {};
        if (!role || !accountId) return res.status(400).json({ error: 'Missing role or accountId' });
        const ok = await db.setDefaultQbdAccount(role, Number(accountId));
        res.json({ success: ok });
    } catch (err) {
        console.error('Error setting default QBD account:', err);
        res.status(400).json({ error: err.message });
    }
});

// Trigger QuickBooks Chart of Accounts sync via QBWC
app.post('/api/qbd/sync/accounts', async (req, res) => {
    try {
        // Add accounts sync to persistent queue (priority 10 = high priority)
        qbwcService.queueSyncRequest('accounts', 10);

        res.json({
            success: true,
            message: 'Chart of Accounts sync queued successfully',
            note: 'Accounts will be synced when QuickBooks Web Connector connects. Click "Update Selected" in QBWC to start sync.'
        });
    } catch (err) {
        console.error('Error triggering account sync:', err);
        res.status(500).json({ error: err.message });
    }
});

// Trigger QuickBooks Customer list sync via QBWC
app.post('/api/qbd/sync/customers', async (req, res) => {
    try {
        // Add customers sync to persistent queue (priority 20 = medium priority)
        qbwcService.queueSyncRequest('customers', 20);

        res.json({
            success: true,
            message: 'Customer sync queued successfully',
            note: 'Customers will be synced when QuickBooks Web Connector connects. Click "Update Selected" in QBWC to start sync.'
        });
    } catch (err) {
        console.error('Error triggering customer sync:', err);
        res.status(500).json({ error: err.message });
    }
});

// Get synced QuickBooks accounts from database
app.get('/api/qbd/sync/accounts', async (req, res) => {
    try {
        const accounts = await db.all('SELECT * FROM qb_accounts ORDER BY account_name ASC');
        res.json({
            success: true,
            count: accounts.length,
            accounts: accounts
        });
    } catch (err) {
        console.error('Error getting synced QB accounts:', err);
        res.status(500).json({ error: err.message });
    }
});

// Get synced QuickBooks customers from database with optional filtering
app.get('/api/qbd/sync/customers', async (req, res) => {
    try {
        const { filter = 'all' } = req.query;

        // Build query based on filter
        let query = `
            SELECT
                qc.*,
                cm.id as mapping_id,
                cm.halopsa_client_id,
                cm.halopsa_client_name,
                cm.mapping_confirmed
            FROM qb_customers qc
            LEFT JOIN customer_mappings cm ON qc.qb_list_id = cm.qb_customer_id
        `;

        // Apply filter
        const conditions = [];
        if (filter === 'unmapped') {
            conditions.push('cm.id IS NULL');
        } else if (filter === 'mapped') {
            conditions.push('cm.id IS NOT NULL');
        }

        if (conditions.length > 0) {
            query += ' WHERE ' + conditions.join(' AND ');
        }

        query += ' ORDER BY qc.qb_full_name ASC';

        const customers = await db.all(query);
        res.json({
            success: true,
            count: customers.length,
            customers: customers
        });
    } catch (err) {
        console.error('Error getting synced QB customers:', err);
        res.status(500).json({ error: err.message });
    }
});

// QuickBooks Web Connector Endpoints
app.post('/qbwc', express.text({ type: '*/*', limit: '50mb' }), async (req, res) => {
    // Parse the QBWC SOAP request
    const soapRequest = req.body;
    console.log('QBWC Request received:', soapRequest.substring(0, 500) + '...');
    
    // Log the full request for debugging (commented out for production)
    // console.log('Full QBWC Request:', soapRequest);
    
    let responseXML = '';
    
    // Handle different QBWC operations
    if (soapRequest.includes('serverVersion')) {
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <serverVersionResponse xmlns="http://developer.intuit.com/">
            <serverVersionResult>2.0</serverVersionResult>
        </serverVersionResponse>
    </soap:Body>
</soap:Envelope>`;
    } 
    else if (soapRequest.includes('clientVersion')) {
        // QBWC requires response to start with W:, E:, or O:
        // W: = Warning (proceed anyway)
        // E: = Error (stop)
        // O: = OK
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <clientVersionResponse xmlns="http://developer.intuit.com/">
            <clientVersionResult>O:34.0</clientVersionResult>
        </clientVersionResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('authenticate')) {
        const usernameMatch = soapRequest.match(/<strUserName[^>]*>([^<]+)<\/strUserName>/);
        const passwordMatch = soapRequest.match(/<strPassword[^>]*>([^<]+)<\/strPassword>/);
        
        const username = usernameMatch ? usernameMatch[1] : '';
        const password = passwordMatch ? passwordMatch[1] : '';
        
        console.log('QBWC Authentication attempt:', { username, password: password ? '***' : 'not provided' });
        
        const authResult = qbwcService.authenticate(username, password);
        
        console.log('QBWC Authentication result:', { 
            ticket: authResult.ticket, 
            errorCode: authResult.errorCode,
            success: !authResult.errorCode
        });
        
        // Log authentication failure details
        if (authResult.errorCode) {
            console.error('QBWC Authentication FAILED for user:', username, 'Error code:', authResult.errorCode);
        }
        
        // QuickBooks Web Connector expects specific authentication response format
        // Must return array with exactly 2 strings: [ticket, companyFileName]
        // Success: <string>ticket</string><string></string>  (empty company file = use current)
        // Failure: <string></string><string>errorCode</string>
        let authResultXML;
        if (authResult.errorCode) {
            // Authentication failed
            authResultXML = `<string></string><string>${authResult.errorCode}</string>`;
        } else {
            // Authentication successful - empty string for company file means use current open company file
            authResultXML = `<string>${authResult.ticket}</string><string></string>`;
        }
        
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <authenticateResponse xmlns="http://developer.intuit.com/">
            <authenticateResult>
                ${authResultXML}
            </authenticateResult>
        </authenticateResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('sendRequestXML')) {
        const ticketMatch = soapRequest.match(/<ticket[^>]*>([^<]+)<\/ticket>/);
        const ticket = ticketMatch ? ticketMatch[1] : '';

        console.log('QBWC sendRequestXML for ticket:', ticket);

        // Get session to track what we're syncing
        let session = qbwcService.getSession(ticket);
        if (!session) {
            session = qbwcService.createSession(ticket);
        }

        let qbxmlData = '';

        try {
            // Check persistent sync request queue first (survives session reconnects)
            const nextRequest = qbwcService.getNextSyncRequest();

            if (nextRequest) {
                console.log(`QBWC: Processing queued sync request: ${nextRequest.type}`);

                if (nextRequest.type === 'accounts') {
                    console.log('QBWC: Generating AccountQuery request');
                    qbxmlData = qbwcService.generateAccountQueryQBXML();
                    session.currentRequest = 'accounts';
                    console.log('QBWC: Generated QBXML for Chart of Accounts query');
                }
                else if (nextRequest.type === 'customers') {
                    console.log('QBWC: Generating CustomerQuery request');
                    qbxmlData = qbwcService.generateCustomerQueryQBXML();
                    session.currentRequest = 'customers';
                    console.log('QBWC: Generated QBXML for Customer list query');
                }
                else if (nextRequest.type === 'items') {
                    console.log('QBWC: Generating ItemQuery request');
                    qbxmlData = qbwcService.generateItemQueryQBXML();
                    session.currentRequest = 'item_query';
                    console.log('QBWC: Generated QBXML for Item list query');
                }
            }
            // No queued requests - default to item sync workflow
            else {
                const itemSyncService = new ItemSyncService(db);
                const unsyncedItems = await itemSyncService.getUnsyncedItems();

                if (unsyncedItems.length > 0) {
                    console.log(`QBWC: Found ${unsyncedItems.length} unsynced items to process`);

                    // First check if we need to query QB items
                    const qbItemCount = db.db.prepare('SELECT COUNT(*) as count FROM qb_items WHERE qb_list_id IS NOT NULL').get();

                    if (qbItemCount.count === 0 && !session.itemsQueried) {
                        // No QB items in database yet, query first
                        console.log('QBWC: No QB items in database, querying QuickBooks first...');
                        qbxmlData = qbwcService.generateItemQueryQBXML();
                        session.currentRequest = 'item_query';
                        session.itemsQueried = true;
                    } else {
                        // Separate items into add vs modify based on QB data
                        const itemsToAdd = [];
                        const itemsToModify = [];

                        for (const item of unsyncedItems) {
                            const qbItem = db.db.prepare(
                                'SELECT qb_list_id, qb_edit_sequence FROM qb_items WHERE name = ? AND item_type = ? AND qb_list_id IS NOT NULL'
                            ).get(item.name, item.item_type);

                            if (qbItem) {
                                // Item exists in QB, prepare for modification
                                item.qb_list_id = qbItem.qb_list_id;
                                item.qb_edit_sequence = qbItem.qb_edit_sequence;
                                itemsToModify.push(item);
                            } else {
                                // New item, add it
                                itemsToAdd.push(item);
                            }
                        }

                        console.log(`QBWC: ${itemsToAdd.length} items to add, ${itemsToModify.length} items to modify`);

                        // Store item IDs in session for later marking as synced
                        session.pendingItemIds = unsyncedItems.map(item => item.id);

                        // Generate QBXML - prioritize modifications first, then additions
                        if (itemsToModify.length > 0) {
                            qbxmlData = qbwcService.generateItemModQBXML(itemsToModify);
                            session.currentRequest = 'item_mod';
                            session.pendingAdds = itemsToAdd; // Store for next request
                        } else if (itemsToAdd.length > 0) {
                            qbxmlData = qbwcService.generateAllItemsQBXML(itemsToAdd);
                            session.currentRequest = 'item_add';
                        }

                        console.log('QBWC: Generated QBXML for item sync');
                    }
                } else {
                    console.log('QBWC: No unsynced items found');

                    // Check for pending purchase orders (future implementation)
                    const pendingBills = []; // Placeholder - would be loaded from database

                    if (pendingBills.length > 0) {
                        qbxmlData = qbwcService.generatePurchaseOrderQBXML(pendingBills);
                        session.currentRequest = 'bills';
                    } else {
                        // No data to process
                        console.log('QBWC: No data to sync');
                        qbxmlData = '';
                        session.currentRequest = null;
                    }
                }
            }
        } catch (error) {
            console.error('QBWC: Error generating request:', error);
            qbxmlData = '';
        }

        // IMPORTANT: QBXML must be XML-escaped when embedded in SOAP response
        // Do NOT remove XML declaration - keep the full QBXML as-is
        // Then escape it for embedding in the SOAP envelope

        function escapeXml(unsafe) {
            if (!unsafe) return '';
            return unsafe
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&apos;');
        }

        const escapedQBXML = escapeXml(qbxmlData);

        // Log the QBXML being sent for debugging
        console.log('QBWC: Sending QBXML (first 500 chars):', qbxmlData ? qbxmlData.substring(0, 500) : '(empty)');
        console.log('QBWC: Escaped QBXML (first 200 chars):', escapedQBXML ? escapedQBXML.substring(0, 200) : '(empty)');

        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <sendRequestXMLResponse xmlns="http://developer.intuit.com/">
            <sendRequestXMLResult>${escapedQBXML}</sendRequestXMLResult>
        </sendRequestXMLResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('receiveResponseXML')) {
        const ticketMatch = soapRequest.match(/<ticket[^>]*>([^<]+)<\/ticket>/);
        // Use [\s\S]* to match any character including newlines between response tags
        const responseMatch = soapRequest.match(/<response[^>]*>([\s\S]*?)<\/response>/);
        const hresultMatch = soapRequest.match(/<hresult[^>]*>([^<]+)<\/hresult>/);
        const messageMatch = soapRequest.match(/<message[^>]*>([^<]+)<\/message>/);

        const ticket = ticketMatch ? ticketMatch[1] : '';
        // Unescape HTML entities in response (QBWC sends XML as HTML-escaped)
        let response = responseMatch ? responseMatch[1] : '';
        response = response
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'")
            .replace(/&amp;/g, '&');

        const hresult = hresultMatch ? hresultMatch[1] : '';
        const message = messageMatch ? messageMatch[1] : '';

        console.log('QBWC receiveResponseXML:', { ticket, hresult, message });

        // Get session to see what was being synced
        const session = qbwcService.getSession(ticket);

        // Process the response
        // In QBWC, empty hresult means success, non-empty means error
        if (!hresult || hresult === '' || hresult === '0') {
            console.log('QBWC operation completed successfully');

            try {
                // Handle account sync completion
                if (session && session.currentRequest === 'accounts') {
                    console.log('QBWC: Processing AccountQueryRs response');

                    // Parse and store accounts in database
                    const result = await qbwcService.parseAccountQueryResponse(response);

                    if (result.success) {
                        console.log(`QBWC: Successfully stored ${result.count} accounts from QuickBooks`);
                    } else {
                        console.error('QBWC: Error parsing AccountQueryRs:', result.error);
                    }

                    // Clear session data
                    session.currentRequest = null;
                }
                // Handle customer sync completion
                else if (session && session.currentRequest === 'customers') {
                    console.log('QBWC: Processing CustomerQueryRs response');

                    // Parse and store customers in database
                    const result = await qbwcService.parseCustomerQueryResponse(response);

                    if (result.success) {
                        console.log(`QBWC: Successfully stored ${result.count} customers from QuickBooks`);
                    } else {
                        console.error('QBWC: Error parsing CustomerQueryRs:', result.error);
                    }

                    // Clear session data
                    session.currentRequest = null;
                }
                // Handle item query completion
                else if (session && session.currentRequest === 'item_query') {
                    console.log('QBWC: Processing ItemQueryRs response');

                    // Parse and store items in database
                    const result = await qbwcService.parseItemQueryResponse(response);

                    if (result.success) {
                        console.log(`QBWC: Successfully stored ${result.count} items from QuickBooks`);
                    } else {
                        console.error('QBWC: Error parsing ItemQueryRs:', result.error);
                    }

                    // Clear session data
                    session.currentRequest = null;
                    // Note: On next sendRequestXML, we'll check for unsynced items and decide add vs modify
                }
                // Handle item modification completion
                else if (session && session.currentRequest === 'item_mod') {
                    console.log('QBWC: Processing ItemModRs response');

                    const result = await qbwcService.parseItemModResponse(response);

                    if (result.success) {
                        console.log(`QBWC: Successfully updated items: ${result.successCount} success, ${result.errorCount} errors`);
                    } else {
                        console.error('QBWC: Error parsing ItemModRs:', result.error);
                    }

                    // Check if we have pending adds
                    if (session.pendingAdds && session.pendingAdds.length > 0) {
                        console.log(`QBWC: ${session.pendingAdds.length} items still need to be added`);
                        // These will be processed on next sendRequestXML
                        session.currentRequest = null;
                    } else if (session.pendingItemIds) {
                        // All items processed, mark as synced
                        const itemSyncService = new ItemSyncService(db);
                        await itemSyncService.markItemsSynced(session.pendingItemIds);
                        console.log('QBWC: Modified items marked as synced');
                        session.pendingItemIds = null;
                        session.currentRequest = null;
                    }
                }
                // Handle item addition completion
                else if (session && session.currentRequest === 'item_add' && session.pendingItemIds) {
                    const itemSyncService = new ItemSyncService(db);

                    // Parse response to extract ListIDs
                    const listIds = [];
                    const listIdMatches = response.matchAll(/<ListID>([^<]+)<\/ListID>/g);
                    for (const match of listIdMatches) {
                        listIds.push(match[1]);
                    }

                    console.log(`QBWC: Marking ${session.pendingItemIds.length} new items as synced with ${listIds.length} ListIDs`);

                    // Mark items as synced
                    await itemSyncService.markItemsSynced(session.pendingItemIds, listIds);

                    console.log('QBWC: New items marked as synced successfully');

                    // Clear session data
                    session.pendingItemIds = null;
                    session.currentRequest = null;
                }
                // Handle old item sync completion (legacy)
                else if (session && session.currentRequest === 'items' && session.pendingItemIds) {
                    const itemSyncService = new ItemSyncService(db);

                    // Parse response to extract ListIDs
                    const listIds = [];
                    const listIdMatches = response.matchAll(/<ListID>([^<]+)<\/ListID>/g);
                    for (const match of listIdMatches) {
                        listIds.push(match[1]);
                    }

                    console.log(`QBWC: Marking ${session.pendingItemIds.length} items as synced with ${listIds.length} ListIDs`);

                    // Mark items as synced
                    await itemSyncService.markItemsSynced(session.pendingItemIds, listIds);

                    // Log successful sync
                    await itemSyncService.logSync(
                        'BATCH',
                        'MIXED',
                        'qbwc_sync',
                        'success',
                        null,
                        null,
                        response
                    );

                    console.log('QBWC: Items marked as synced successfully');

                    // Clear session data
                    session.pendingItemIds = null;
                    session.currentRequest = null;
                } else if (session && session.currentRequest === 'bills') {
                    // Future: handle bill sync completion
                    console.log('QBWC: Bills processed successfully');
                    session.currentRequest = null;
                }
            } catch (error) {
                console.error('QBWC: Error processing successful response:', error);
            }
        } else {
            console.log('QBWC operation failed:', message);

            try {
                // Log failed sync
                if (session && session.currentRequest === 'items') {
                    const itemSyncService = new ItemSyncService(db);
                    await itemSyncService.logSync(
                        'BATCH',
                        'MIXED',
                        'qbwc_sync',
                        'error',
                        message,
                        null,
                        response
                    );

                    // Clear session data
                    session.pendingItemIds = null;
                    session.currentRequest = null;
                }
            } catch (error) {
                console.error('QBWC: Error logging failed response:', error);
            }
        }

        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <receiveResponseXMLResponse xmlns="http://developer.intuit.com/">
            <receiveResponseXMLResult>100</receiveResponseXMLResult>
        </receiveResponseXMLResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('connectionError')) {
        console.log('QBWC connection error occurred');
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <connectionErrorResponse xmlns="http://developer.intuit.com/">
            <connectionErrorResult>done</connectionErrorResult>
        </connectionErrorResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('closeConnection')) {
        console.log('QBWC closeConnection requested');
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <closeConnectionResponse xmlns="http://developer.intuit.com/">
            <closeConnectionResult>OK</closeConnectionResult>
        </closeConnectionResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else {
        // Unknown operation
        console.log('Unknown QBWC operation requested');
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <unknownOperationResponse xmlns="http://developer.intuit.com/">
            <unknownOperationResult>Unknown operation</unknownOperationResult>
        </unknownOperationResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    
    res.set('Content-Type', 'text/xml');
    res.send(responseXML);
});

// Generate QWC configuration file endpoint - dynamically handles any FQDN
app.get('/qbwc/config', async (req, res) => {
    try {
        // Get the actual host from headers to support proxies
        const host = req.get('X-Forwarded-Host') || req.get('Host') || req.get('host') || 'localhost:3000';
        const protocol = req.get('X-Forwarded-Proto') || req.protocol;
        
        // Use direct database query for config to avoid async issues
        db.getConfig(null, async (err, qbwcConfig) => {
            if (err) {
                console.error('Error reading QBWC config:', err);
                // Fallback to defaults
                const config = {
                    appName: 'CSV to QuickBooks IIF Sync',
                    appUrl: `${protocol}://${host}/qbwc`,
                    description: 'Sync CSV purchase orders with QuickBooks via Web Connector',
                    supportUrl: `${protocol}://${host}/support`,
                    username: 'qbwc_user',
                    interval: 30
                };
                
                try {
                    const qwcFile = qbwcService.generateQWCFile(config);
                    res.set({
                        'Content-Type': 'application/xml',
                        'Content-Disposition': `attachment; filename="csv-to-qb-sync.qwc"`
                    });
                    res.send(qwcFile);
                } catch (fileError) {
                    console.error('Error generating QWC file:', fileError);
                    res.status(500).send('Error generating QWC configuration file');
                }
            } else {
                // Get config values with fallbacks
                const appName = (qbwcConfig && qbwcConfig.qbwc_app_name) ? qbwcConfig.qbwc_app_name.value : 'CSV to QuickBooks IIF Sync';
                const username = (qbwcConfig && qbwcConfig.qbwc_username) ? qbwcConfig.qbwc_username.value : 'qbwc_user';
                const interval = (qbwcConfig && qbwcConfig.qbwc_sync_interval) ? parseInt(qbwcConfig.qbwc_sync_interval.value) : 30;
                
                const config = {
                    appName: appName,
                    appUrl: `${protocol}://${host}/qbwc`,
                    description: 'Sync CSV purchase orders with QuickBooks via Web Connector',
                    supportUrl: `${protocol}://${host}/support`,
                    username: username,
                    interval: interval
                };
                
                try {
                    const qwcFile = qbwcService.generateQWCFile(config);
                    res.set({
                        'Content-Type': 'application/xml',
                        'Content-Disposition': `attachment; filename="csv-to-qb-sync.qwc"`
                    });
                    res.send(qwcFile);
                } catch (fileError) {
                    console.error('Error generating QWC file:', fileError);
                    res.status(500).send('Error generating QWC configuration file');
                }
            }
        });
    } catch (error) {
        console.error('Error in QWC config endpoint:', error);
        res.status(500).send('Error generating QWC configuration file');
    }
});

// Configuration API endpoints
app.post('/api/config', async (req, res) => {
    try {
        const configUpdates = req.body;
        
        // Validate required fields
        if (!configUpdates || typeof configUpdates !== 'object') {
            return res.status(400).json({ error: 'Invalid configuration data' });
        }
        
        // Update each configuration key
        for (const [key, value] of Object.entries(configUpdates)) {
            await db.run(
                'INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                [key, value]
            );
        }
        
        res.json({ 
            success: true, 
            message: 'Configuration updated successfully',
            updates: configUpdates 
        });
    } catch (error) {
        console.error('Error updating configuration:', error);
        res.status(500).json({ error: 'Failed to update configuration' });
    }
});

app.get('/api/config', async (req, res) => {
    try {
        const allConfigs = await db.all('SELECT * FROM config');
        const config = {};
        allConfigs.forEach(row => {
            config[row.key] = row.value;
        });
        res.json(config);
    } catch (error) {
        console.error('Error fetching configuration:', error);
        res.status(500).json({ error: 'Failed to fetch configuration' });
    }
});

// Category-based configuration endpoints
app.get('/api/config/:category', async (req, res) => {
    try {
        const category = req.params.category;
        const configs = await configAPI.getConfigByCategory(category);
        res.json(configs);
    } catch (error) {
        console.error(`Error fetching ${req.params.category} configuration:`, error);
        res.status(500).json({ error: `Failed to fetch ${req.params.category} configuration` });
    }
});

app.put('/api/config/:category', async (req, res) => {
    try {
        const category = req.params.category;
        const updates = req.body;
        
        if (!updates || typeof updates !== 'object') {
            return res.status(400).json({ error: 'Invalid configuration data' });
        }
        
        for (const [key, valueObj] of Object.entries(updates)) {
            if (typeof valueObj === 'object' && valueObj.hasOwnProperty('value') && valueObj.hasOwnProperty('type')) {
                await configAPI.updateConfig(key, valueObj.value, valueObj.type);
            }
        }
        
        const updatedConfigs = await configAPI.getConfigByCategory(category);
        res.json({ 
            success: true, 
            message: `${category} configuration updated successfully`,
            configs: updatedConfigs 
        });
    } catch (error) {
        console.error(`Error updating ${req.params.category} configuration:`, error);
        res.status(500).json({ error: `Failed to update ${req.params.category} configuration` });
    }
});

app.get('/api/stripe/test', async (req, res) => {
    console.log('🔌 Stripe test endpoint called');
    try {
        console.log('🔄 Creating StripeAPI instance...');
        const stripeAPI = new StripeAPI(db);
        console.log('✅ StripeAPI instance created, testing connection...');
        const result = await stripeAPI.testConnection();
        console.log('🎯 Stripe test result:', result);
        res.json(result);
    } catch (error) {
        console.error('❌ Error testing Stripe connection:', error);
        res.status(500).json({ success: false, message: 'Error testing Stripe connection: ' + error.message });
    }
});

app.post('/api/stripe/import/customers', async (req, res) => {
    try {
        const stripeAPI = new StripeAPI(db);
        const result = await stripeAPI.importCustomers();

        // Automatically run auto-mapping after successful import
        if (result.success) {
            console.log('✓ Stripe customers imported successfully, running auto-match...');
            try {
                await performLegacyAutomatch(null, stripeAPI, db, 0.85);
                console.log('✓ Auto-match completed');
            } catch (autoMatchError) {
                console.error('Warning: Auto-match failed after import:', autoMatchError);
                // Don't fail the import if auto-match fails
            }

            // Trigger AI mapping suggestions in background
            if (aiTriggers && aiTriggers.autoTriggerCustomerSuggestions) {
                setImmediate(() => aiTriggers.autoTriggerCustomerSuggestions());
            }
        }

        res.json(result);
    } catch (error) {
        console.error('Error importing Stripe customers:', error);
        res.status(500).json({ success: false, message: 'Error importing Stripe customers' });
    }
});

app.post('/api/stripe/import/transactions', async (req, res) => {
    try {
        const stripeAPI = new StripeAPI(db);
        const importAll = req.query.all === 'true';
        const result = await stripeAPI.importTransactions(null, importAll);

        // Trigger AI transaction mapping suggestions in background
        if (result.success && aiTriggers && aiTriggers.autoTriggerTransactionSuggestions) {
            setImmediate(() => aiTriggers.autoTriggerTransactionSuggestions());
        }

        res.json(result);
    } catch (error) {
        console.error('Error importing Stripe transactions:', error);
        res.status(500).json({ success: false, message: 'Error importing Stripe transactions' });
    }
});

app.get('/api/stripe/customers/imported', async (req, res) => {
    try {
        const stripeAPI = new StripeAPI(db);
        const customers = await stripeAPI.getImportedCustomers();
        res.json(customers);
    } catch (error) {
        console.error('Error fetching imported customers:', error);
        res.status(500).json({ error: 'Failed to fetch imported customers' });
    }
});

app.get('/api/stripe/transactions/imported', async (req, res) => {
    try {
        const limit = req.query.limit ? parseInt(req.query.limit) : null;
        const offset = req.query.offset ? parseInt(req.query.offset) : 0;

        const stripeAPI = new StripeAPI(db);
        const transactions = await stripeAPI.getImportedTransactions(limit, offset);
        const total = await stripeAPI.getTransactionsCount();

        res.json({
            transactions,
            pagination: {
                limit: limit || total,
                offset,
                total,
                hasMore: limit ? (offset + transactions.length < total) : false
            }
        });
    } catch (error) {
        console.error('Error fetching imported transactions:', error);
        res.status(500).json({ error: 'Failed to fetch imported transactions' });
    }
});

// Customer view API endpoints
app.get('/api/stripe/customers/:id', async (req, res) => {
    try {
        const customer = await db.get(
            'SELECT * FROM stripe_customers WHERE stripe_id = ?',
            [req.params.id]
        );
        
        if (!customer) {
            return res.status(404).json({ error: 'Customer not found' });
        }
        
        res.json(customer);
    } catch (error) {
        console.error('Error fetching customer:', error);
        res.status(500).json({ error: 'Failed to fetch customer' });
    }
});

app.get('/api/stripe/customers/:id/transactions', async (req, res) => {
    try {
        const transactions = await db.all(
            `SELECT t.*, c.name as customer_name, c.email as customer_email
             FROM stripe_transactions t
             LEFT JOIN stripe_customers c ON t.customer_id = c.stripe_id
             WHERE t.customer_id = ?
             ORDER BY t.created DESC LIMIT 100`,
            [req.params.id]
        );
        res.json(transactions);
    } catch (error) {
        console.error('Error fetching customer transactions:', error);
        res.status(500).json({ error: 'Failed to fetch customer transactions' });
    }
});

// HaloPSA API endpoints
app.get('/api/halopsa/clients', async (req, res) => {
    try {
        // Get clients from our local database (already synced)
        const clients = await db.all(`
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
            ORDER BY name
        `);
        
        // Parse raw_data JSON for each client
        const clientsWithData = clients.map(client => ({
            ...client,
            raw_data: client.raw_data ? JSON.parse(client.raw_data) : null
        }));
        
        res.json(clientsWithData);
    } catch (error) {
        console.error('Error fetching synced HaloPSA clients:', error);
        res.status(500).json({ error: 'Failed to fetch synced HaloPSA clients' });
    }
});

app.get('/api/halopsa/reports/purchase-orders', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        const result = await halopsaAPI.getPurchaseOrderReport();
        res.json(result);
    } catch (error) {
        console.error('Error fetching HaloPSA purchase order report:', error);
        res.status(500).json({ success: false, message: 'Error fetching HaloPSA purchase order report' });
    }
});

app.get('/api/halopsa/reports/invoices', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        const result = await halopsaAPI.getInvoiceReport();
        res.json(result);
    } catch (error) {
        console.error('Error fetching HaloPSA invoice report:', error);
        res.status(500).json({ success: false, message: 'Error fetching HaloPSA invoice report' });
    }
});

app.post('/api/halopsa/import/clients', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        // Wait for initialization to complete
        await new Promise(resolve => setTimeout(resolve, 100));
        const result = await halopsaAPI.importClients();

        // Automatically run auto-mapping after successful import
        if (result.success) {
            console.log('✓ HaloPSA clients imported successfully, running auto-match...');
            try {
                const stripeAPI = new StripeAPI(db);
                await performLegacyAutomatch(null, stripeAPI, db, 0.85);
                console.log('✓ Auto-match completed');
            } catch (autoMatchError) {
                console.error('Warning: Auto-match failed after import:', autoMatchError);
                // Don't fail the import if auto-match fails
            }
        }

        res.json(result);
    } catch (error) {
        console.error('Error importing HaloPSA clients:', error);
        res.status(500).json({ success: false, message: 'Error importing HaloPSA clients' });
    }
});

app.post('/api/halopsa/import/purchase-orders', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        const result = await halopsaAPI.importPurchaseOrders();
        res.json(result);
    } catch (error) {
        console.error('Error importing HaloPSA purchase orders:', error);
        res.status(500).json({ success: false, message: 'Error importing HaloPSA purchase orders' });
    }
});

app.post('/api/halopsa/import/invoices', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        const result = await halopsaAPI.importInvoices();
        res.json(result);
    } catch (error) {
        console.error('Error importing HaloPSA invoices:', error);
        res.status(500).json({ success: false, message: 'Error importing HaloPSA invoices' });
    }
});

app.get('/api/halopsa/invoices', async (req, res) => {
    console.log('⚠️  HaloPSA invoices endpoint hit with query:', req.query);
    console.log('⚠️  Request URL:', req.url);
    
    try {
        const { page = 1, limit = 50, search = '' } = req.query;
        const offset = (page - 1) * limit;
        
        let query = 'SELECT * FROM halopsa_invoices';
        let countQuery = 'SELECT COUNT(*) as total FROM halopsa_invoices';
        let params = [];
        
        if (search) {
            const searchCondition = ` WHERE invoice_number LIKE ? OR client_name LIKE ? OR raw_data LIKE ?`;
            query += searchCondition;
            countQuery += searchCondition;
            const searchTerm = `%${search}%`;
            params = [searchTerm, searchTerm, searchTerm];
        }
        
        query += ' ORDER BY invoice_date DESC LIMIT ? OFFSET ?';
        params.push(parseInt(limit), parseInt(offset));
        
        console.log('⚠️  Executing query:', query, 'with params:', params);
        
        const invoices = await db.query(query, params);
        console.log('⚠️  Found', invoices.length, 'invoices');
        
        // Get total count (with same search conditions but without LIMIT/OFFSET)
        const countParams = params.slice(0, params.length - 2); // Remove LIMIT/OFFSET params
        const countResult = await db.query(countQuery, countParams);
        const total = countResult[0]?.total || 0;
        console.log('⚠️  Total count:', total);
        
        // Return paginated format
        const response = {
            invoices,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total,
                pages: Math.ceil(total / limit)
            }
        };
        
        console.log('⚠️  Returning paginated response with invoices array and pagination object');
        res.json(response);
        
    } catch (error) {
        console.error('⚠️  Error fetching HaloPSA invoices:', error);
        res.status(500).json({ success: false, message: 'Error fetching HaloPSA invoices' });
    }
});

// =============================================================================
// ITEM SYNC ENDPOINTS
// =============================================================================

/**
 * POST /api/items/extract
 * Extract items from HaloPSA invoices and purchase orders
 */
app.post('/api/items/extract', async (req, res) => {
    try {
        console.log('Starting item extraction from HaloPSA data...');
        const itemSyncService = new ItemSyncService(db);
        const result = await itemSyncService.extractAndSaveAllItems();

        console.log('Item extraction complete:', result);
        res.json(result);
    } catch (error) {
        console.error('Error extracting items:', error);
        res.status(500).json({
            success: false,
            message: `Error extracting items: ${error.message}`
        });
    }
});

/**
 * GET /api/items/unsynced
 * Get items that haven't been synced to QuickBooks yet
 */
app.get('/api/items/unsynced', async (req, res) => {
    try {
        const itemSyncService = new ItemSyncService(db);
        const items = await itemSyncService.getUnsyncedItems();

        // Group by item type for easier display
        const grouped = {
            service: items.filter(item => item.item_type === 'ItemService'),
            inventory: items.filter(item => item.item_type === 'ItemInventory'),
            nonInventory: items.filter(item => item.item_type === 'ItemNonInventory')
        };

        res.json({
            success: true,
            items,
            grouped,
            counts: {
                total: items.length,
                service: grouped.service.length,
                inventory: grouped.inventory.length,
                nonInventory: grouped.nonInventory.length
            }
        });
    } catch (error) {
        console.error('Error fetching unsynced items:', error);
        res.status(500).json({
            success: false,
            message: `Error fetching unsynced items: ${error.message}`
        });
    }
});

/**
 * POST /api/items/sync-to-qb
 * Prepare items for QuickBooks Web Connector sync
 * This endpoint generates QBXML for unsynced items
 */
app.post('/api/items/sync-to-qb', async (req, res) => {
    try {
        const itemSyncService = new ItemSyncService(db);
        const items = await itemSyncService.getUnsyncedItems();

        if (items.length === 0) {
            return res.json({
                success: true,
                message: 'No items to sync',
                qbxml: null,
                itemCount: 0
            });
        }

        // Generate QBXML for all items
        const qbxml = qbwcService.generateAllItemsQBXML(items);

        // Log the sync operation
        await itemSyncService.logSync(
            'BATCH',
            'MIXED',
            'generate_qbxml',
            'pending',
            null,
            qbxml,
            null
        );

        console.log(`Generated QBXML for ${items.length} items`);

        res.json({
            success: true,
            message: `Generated QBXML for ${items.length} items. Use QuickBooks Web Connector to complete the sync.`,
            qbxml,
            itemCount: items.length,
            itemTypes: {
                service: items.filter(i => i.item_type === 'ItemService').length,
                inventory: items.filter(i => i.item_type === 'ItemInventory').length,
                nonInventory: items.filter(i => i.item_type === 'ItemNonInventory').length
            }
        });
    } catch (error) {
        console.error('Error preparing items for QB sync:', error);
        res.status(500).json({
            success: false,
            message: `Error preparing items for QB sync: ${error.message}`
        });
    }
});

/**
 * POST /api/qbd/sync/purchase-orders
 * Queue purchase orders for QuickBooks sync as Bills
 * Integrates with account mappings for proper GL account assignment
 */
app.post('/api/qbd/sync/purchase-orders', async (req, res) => {
    try {
        // Fetch unsynced purchase orders
        const purchaseOrders = await db.all(`
            SELECT * FROM halopsa_purchase_orders
            WHERE synced_to_qb = 0 OR synced_to_qb IS NULL
            ORDER BY po_date DESC
        `);

        if (purchaseOrders.length === 0) {
            return res.json({
                success: true,
                message: 'No purchase orders to sync',
                qbxml: null,
                poCount: 0
            });
        }

        // Fetch account mappings
        const accountMappings = {};
        const mappings = await db.all('SELECT mapping_type, qb_account_name FROM account_mappings WHERE is_active = 1');
        mappings.forEach(m => {
            accountMappings[m.mapping_type] = m.qb_account_name;
        });

        // Generate QBXML for purchase orders → bills
        const qbxml = qbwcService.generatePurchaseOrderBillsQBXML(purchaseOrders, accountMappings);

        if (!qbxml) {
            return res.json({
                success: false,
                message: 'Failed to generate QBXML for purchase orders',
                poCount: purchaseOrders.length
            });
        }

        // Queue the sync request in QBWC service
        qbwcService.queueSyncRequest('purchase_orders', 50);

        console.log(`Generated QBXML for ${purchaseOrders.length} purchase orders as bills`);

        res.json({
            success: true,
            message: `Queued ${purchaseOrders.length} purchase orders for QB sync. Use QuickBooks Web Connector to complete the sync.`,
            qbxml,
            poCount: purchaseOrders.length,
            totalAmount: purchaseOrders.reduce((sum, po) => sum + (po.total_amount || 0), 0)
        });
    } catch (error) {
        console.error('Error preparing purchase orders for QB sync:', error);
        res.status(500).json({
            success: false,
            message: `Error preparing purchase orders for QB sync: ${error.message}`
        });
    }
});

/**
 * POST /api/qbd/sync/items
 * Queue items for QuickBooks sync with account mapping integration
 * Supports inventory, service, and non-inventory items
 */
app.post('/api/qbd/sync/items', async (req, res) => {
    try {
        // Fetch unsynced items
        const items = await db.all(`
            SELECT * FROM qb_items
            WHERE synced_to_qb = 0 OR synced_to_qb IS NULL
            ORDER BY item_type, name
        `);

        if (items.length === 0) {
            return res.json({
                success: true,
                message: 'No items to sync',
                qbxml: null,
                itemCount: 0
            });
        }

        // Fetch account mappings
        const accountMappings = {};
        const mappings = await db.all('SELECT mapping_type, qb_account_name FROM account_mappings WHERE is_active = 1');
        mappings.forEach(m => {
            accountMappings[m.mapping_type] = m.qb_account_name;
        });

        // Generate QBXML for items with account mappings
        const qbxml = qbwcService.generateItemsWithMappingsQBXML(items, accountMappings);

        if (!qbxml) {
            return res.json({
                success: false,
                message: 'Failed to generate QBXML for items',
                itemCount: items.length
            });
        }

        // Queue the sync request in QBWC service
        qbwcService.queueSyncRequest('items', 40);

        console.log(`Generated QBXML for ${items.length} items with account mappings`);

        res.json({
            success: true,
            message: `Queued ${items.length} items for QB sync. Use QuickBooks Web Connector to complete the sync.`,
            qbxml,
            itemCount: items.length,
            itemTypes: {
                service: items.filter(i => i.item_type === 'ItemService').length,
                inventory: items.filter(i => i.item_type === 'ItemInventory').length,
                nonInventory: items.filter(i => i.item_type === 'ItemNonInventory').length
            }
        });
    } catch (error) {
        console.error('Error preparing items for QB sync:', error);
        res.status(500).json({
            success: false,
            message: `Error preparing items for QB sync: ${error.message}`
        });
    }
});

/**
 * POST /api/qbd/sync/bills
 * Convert unsynced purchase orders to QuickBooks bills
 * This is an alias for /api/qbd/sync/purchase-orders for clarity
 */
app.post('/api/qbd/sync/bills', async (req, res) => {
    // Redirect to purchase-orders endpoint
    req.url = '/api/qbd/sync/purchase-orders';
    app.handle(req, res);
});

/**
 * GET /api/qbd/purchase-orders/sync-status
 * Get Purchase Order sync statistics: total, synced, pending, errors
 */
app.get('/api/qbd/purchase-orders/sync-status', async (req, res) => {
    try {
        // Get total count
        const totalResult = await db.get('SELECT COUNT(*) as count FROM halopsa_purchase_orders');
        const total = totalResult?.count || 0;

        // Get synced count
        const syncedResult = await db.get('SELECT COUNT(*) as count FROM halopsa_purchase_orders WHERE synced_to_qb = 1');
        const synced = syncedResult?.count || 0;

        // Get pending count (not synced and no errors)
        const pendingResult = await db.get(`
            SELECT COUNT(*) as count FROM halopsa_purchase_orders
            WHERE (synced_to_qb = 0 OR synced_to_qb IS NULL)
            AND (sync_error IS NULL OR sync_error = '')
        `);
        const pending = pendingResult?.count || 0;

        // Get error count
        const errorResult = await db.get(`
            SELECT COUNT(*) as count FROM halopsa_purchase_orders
            WHERE sync_error IS NOT NULL AND sync_error != ''
        `);
        const errors = errorResult?.count || 0;

        // Get total amount of unsynced POs
        const amountResult = await db.get(`
            SELECT SUM(total_amount) as total FROM halopsa_purchase_orders
            WHERE synced_to_qb = 0 OR synced_to_qb IS NULL
        `);
        const unsyncedAmount = amountResult?.total || 0;

        // Get recent sync activity
        const recentSync = await db.get(`
            SELECT MAX(last_sync) as last_sync FROM halopsa_purchase_orders
            WHERE synced_to_qb = 1
        `);
        const lastSuccessfulSync = recentSync?.last_sync || null;

        res.json({
            success: true,
            stats: {
                total,
                synced,
                pending,
                errors,
                unsyncedAmount,
                lastSuccessfulSync
            }
        });
    } catch (error) {
        console.error('Error getting PO sync status:', error);
        res.status(500).json({
            success: false,
            message: `Error getting sync status: ${error.message}`
        });
    }
});

/**
 * POST /api/qbd/purchase-orders/:id/retry-sync
 * Retry sync for a specific Purchase Order
 * Clears sync_error and marks for re-sync
 */
app.post('/api/qbd/purchase-orders/:id/retry-sync', async (req, res) => {
    try {
        const poId = parseInt(req.params.id);

        if (isNaN(poId)) {
            return res.status(400).json({
                success: false,
                message: 'Invalid purchase order ID'
            });
        }

        // Check if PO exists
        const po = await db.get('SELECT * FROM halopsa_purchase_orders WHERE id = ?', [poId]);

        if (!po) {
            return res.status(404).json({
                success: false,
                message: 'Purchase order not found'
            });
        }

        // Clear error and mark for re-sync
        await db.run(`
            UPDATE halopsa_purchase_orders
            SET sync_error = NULL,
                synced_to_qb = 0,
                qb_txn_id = NULL,
                last_sync_attempt = NULL
            WHERE id = ?
        `, [poId]);

        console.log(`Cleared sync error for PO ${po.po_number}, marked for retry`);

        res.json({
            success: true,
            message: `Purchase order ${po.po_number} marked for retry. Use QuickBooks Web Connector to sync.`,
            po_number: po.po_number
        });
    } catch (error) {
        console.error('Error retrying PO sync:', error);
        res.status(500).json({
            success: false,
            message: `Error retrying sync: ${error.message}`
        });
    }
});

/**
 * POST /api/qbd/purchase-orders/retry-all-errors
 * Retry sync for all Purchase Orders with errors
 * Clears all sync_error values and marks for re-sync
 */
app.post('/api/qbd/purchase-orders/retry-all-errors', async (req, res) => {
    try {
        // Get count of POs with errors
        const errorResult = await db.get(`
            SELECT COUNT(*) as count FROM halopsa_purchase_orders
            WHERE sync_error IS NOT NULL AND sync_error != ''
        `);
        const errorCount = errorResult?.count || 0;

        if (errorCount === 0) {
            return res.json({
                success: true,
                message: 'No purchase orders with errors to retry',
                retryCount: 0
            });
        }

        // Clear errors and mark for re-sync
        await db.run(`
            UPDATE halopsa_purchase_orders
            SET sync_error = NULL,
                synced_to_qb = 0,
                qb_txn_id = NULL,
                last_sync_attempt = NULL
            WHERE sync_error IS NOT NULL AND sync_error != ''
        `);

        console.log(`Cleared sync errors for ${errorCount} purchase orders, marked for retry`);

        res.json({
            success: true,
            message: `Cleared errors for ${errorCount} purchase orders. Use QuickBooks Web Connector to sync.`,
            retryCount: errorCount
        });
    } catch (error) {
        console.error('Error retrying all PO errors:', error);
        res.status(500).json({
            success: false,
            message: `Error retrying all errors: ${error.message}`
        });
    }
});

/**
 * GET /api/qbd/purchase-orders/errors
 * Get all Purchase Orders with sync errors
 */
app.get('/api/qbd/purchase-orders/errors', async (req, res) => {
    try {
        const errorPOs = await db.all(`
            SELECT id, po_number, vendor_name, total_amount, sync_error, last_sync_attempt
            FROM halopsa_purchase_orders
            WHERE sync_error IS NOT NULL AND sync_error != ''
            ORDER BY last_sync_attempt DESC
        `);

        res.json({
            success: true,
            errors: errorPOs,
            count: errorPOs.length
        });
    } catch (error) {
        console.error('Error getting PO errors:', error);
        res.status(500).json({
            success: false,
            message: `Error getting errors: ${error.message}`
        });
    }
});

/**
 * GET /api/items/all
 * Get all items from the database with pagination support
 */
app.get('/api/items/all', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 1000; // Default to large number for backward compatibility
        const offset = parseInt(req.query.offset) || 0;

        const items = await db.all(`
            SELECT * FROM qb_items
            ORDER BY name
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        res.json(items);
    } catch (error) {
        console.error('Error fetching all items:', error);
        res.status(500).json({ error: 'Failed to fetch items' });
    }
});

/**
 * GET /api/customers/all
 * Get all HaloPSA clients (source of truth) with mapping information
 */
app.get('/api/customers/all', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 10000; // Default to large number
        const offset = parseInt(req.query.offset) || 0;
        const filter = req.query.filter || 'all';

        // Build WHERE clause based on filter
        let whereClause = '';
        switch (filter) {
            case 'unmapped-stripe':
                whereClause = 'WHERE cm.stripe_customer_id IS NULL';
                break;
            case 'mapped-stripe':
                whereClause = 'WHERE cm.stripe_customer_id IS NOT NULL';
                break;
            case 'unmapped-qb':
                whereClause = 'WHERE cm.qb_customer_id IS NULL';
                break;
            case 'mapped-qb':
                whereClause = 'WHERE cm.qb_customer_id IS NOT NULL';
                break;
            case 'fully-unmapped':
                whereClause = 'WHERE cm.stripe_customer_id IS NULL AND cm.qb_customer_id IS NULL';
                break;
            case 'fully-mapped':
                whereClause = 'WHERE cm.stripe_customer_id IS NOT NULL AND cm.qb_customer_id IS NOT NULL';
                break;
            case 'all':
            default:
                whereClause = '';
                break;
        }

        const clients = await db.all(`
            SELECT
                hc.*,
                cm.stripe_customer_id,
                cm.stripe_customer_name,
                cm.stripe_customer_email,
                cm.qb_customer_id,
                COALESCE(qb.qb_full_name, qb.company_name, cm.qb_customer_name) as qb_customer_name,
                cm.auto_mapped,
                cm.mapping_confirmed
            FROM halopsa_clients hc
            LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id
            LEFT JOIN qb_customers qb ON cm.qb_customer_id = qb.qb_list_id
            ${whereClause}
            ORDER BY hc.name
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        res.json(clients);
    } catch (error) {
        console.error('Error fetching customers:', error);
        res.status(500).json({ error: 'Failed to fetch customers' });
    }
});

/**
 * GET /api/halopsa/purchase-orders
 * Get purchase orders with pagination support
 */
app.get('/api/halopsa/purchase-orders', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        const offset = parseInt(req.query.offset) || 0;

        console.log(`⚠️  Purchase orders endpoint hit with limit: ${limit}, offset: ${offset}`);

        const purchaseOrders = await db.all(`
            SELECT * FROM halopsa_purchase_orders
            ORDER BY po_date DESC
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        const total = await db.get('SELECT COUNT(*) as total FROM halopsa_purchase_orders');

        console.log(`⚠️  Found ${purchaseOrders.length} purchase orders, total: ${total.total}`);

        res.json({
            purchaseOrders,
            pagination: {
                limit,
                offset,
                total: total.total,
                hasMore: offset + purchaseOrders.length < total.total
            }
        });
    } catch (error) {
        console.error('Error fetching purchase orders:', error);
        res.status(500).json({ error: 'Failed to fetch purchase orders' });
    }
});

/**
 * POST /api/mappings/stripe-invoices
 * Auto-match Stripe transactions to HaloPSA invoices by parsing invoice numbers from descriptions
 */
app.post('/api/mappings/stripe-invoices', async (req, res) => {
    try {
        console.log('⚠️  Starting Stripe transaction to invoice mapping...');

        // Fetch all Stripe transactions that haven't been mapped yet
        const unmappedTransactions = await db.all(`
            SELECT * FROM stripe_transactions
            WHERE halopsa_invoice_id IS NULL AND description IS NOT NULL
        `);

        // Fetch all HaloPSA invoices for matching
        const invoices = await db.all('SELECT id, halopsa_id, invoice_number FROM halopsa_invoices');

        // Create a map of invoice numbers to invoice IDs for fast lookup
        const invoiceMap = new Map();
        invoices.forEach(inv => {
            invoiceMap.set(inv.invoice_number.toLowerCase(), inv.id);
            // Also store without special characters for fuzzy matching
            const cleaned = inv.invoice_number.replace(/[^a-z0-9]/gi, '').toLowerCase();
            if (cleaned) invoiceMap.set(cleaned, inv.id);
        });

        console.log(`⚠️  Found ${unmappedTransactions.length} unmapped transactions and ${invoices.length} invoices`);

        const matches = [];
        const noMatches = [];

        // Common invoice number patterns in Stripe descriptions
        const patterns = [
            /invoice\s*#?\s*([a-z0-9\-]+)/i,     // "Invoice #12345", "Invoice 12345", "invoice INV-001"
            /inv[-:]?\s*([a-z0-9\-]+)/i,         // "INV-12345", "INV:12345", "inv 12345"
            /bill\s*#?\s*([a-z0-9\-]+)/i,        // "Bill #12345"
            /ref(?:erence)?\s*#?\s*([a-z0-9\-]+)/i,  // "Ref #12345", "Reference 12345"
            /#([a-z0-9\-]{4,})/i,                // "#INV12345"
            /\b([a-z]{2,4}-\d{3,})\b/i           // "INV-001", "BILL-123"
        ];

        for (const tx of unmappedTransactions) {
            let matched = false;
            let matchedInvoiceId = null;
            let matchPattern = null;

            // Try each pattern
            for (const pattern of patterns) {
                const match = tx.description.match(pattern);
                if (match && match[1]) {
                    const extractedNumber = match[1].trim();

                    // Try exact match first
                    if (invoiceMap.has(extractedNumber.toLowerCase())) {
                        matchedInvoiceId = invoiceMap.get(extractedNumber.toLowerCase());
                        matchPattern = pattern.toString();
                        matched = true;
                        break;
                    }

                    // Try cleaned match (no special characters)
                    const cleanedNumber = extractedNumber.replace(/[^a-z0-9]/gi, '').toLowerCase();
                    if (cleanedNumber && invoiceMap.has(cleanedNumber)) {
                        matchedInvoiceId = invoiceMap.get(cleanedNumber);
                        matchPattern = pattern.toString();
                        matched = true;
                        break;
                    }
                }
            }

            if (matched) {
                // Update the transaction with the mapped invoice ID
                await db.run(`
                    UPDATE stripe_transactions
                    SET halopsa_invoice_id = ?, mapped_to_halo = 1
                    WHERE id = ?
                `, [matchedInvoiceId, tx.id]);

                matches.push({
                    stripe_transaction_id: tx.stripe_id,
                    description: tx.description,
                    invoice_id: matchedInvoiceId,
                    pattern: matchPattern
                });
            } else {
                noMatches.push({
                    stripe_transaction_id: tx.stripe_id,
                    description: tx.description
                });
            }
        }

        console.log(`⚠️  Mapping complete: ${matches.length} matched, ${noMatches.length} not matched`);

        res.json({
            success: true,
            matched: matches.length,
            unmatched: noMatches.length,
            total: unmappedTransactions.length,
            matches,
            noMatches: noMatches.slice(0, 10) // Return first 10 unmatched for debugging
        });
    } catch (error) {
        console.error('Error mapping Stripe transactions to invoices:', error);
        res.status(500).json({ error: 'Failed to map transactions', message: error.message });
    }
});

/**
 * GET /api/mappings/stripe-invoices
 * Get all Stripe transactions that have been mapped to HaloPSA invoices
 */
app.get('/api/mappings/stripe-invoices', async (req, res) => {
    try {
        const mappedTransactions = await db.all(`
            SELECT
                st.id,
                st.stripe_id,
                st.description,
                st.amount,
                st.currency,
                st.created,
                st.halopsa_invoice_id,
                hi.invoice_number,
                hi.client_name,
                hi.total_amount as invoice_amount,
                hi.invoice_date,
                hi.status as invoice_status
            FROM stripe_transactions st
            INNER JOIN halopsa_invoices hi ON st.halopsa_invoice_id = hi.id
            WHERE st.mapped_to_halo = 1
            ORDER BY st.created DESC
        `);

        res.json({
            success: true,
            count: mappedTransactions.length,
            mappings: mappedTransactions
        });
    } catch (error) {
        console.error('Error fetching mapped transactions:', error);
        res.status(500).json({ error: 'Failed to fetch mapped transactions' });
    }
});

// Create a singleton HaloPSA API instance
let halopsaAPIInstance = null;
let initializationPromise = null;

// Initialize or update the HaloPSA API instance
async function getHaloPSAAPI() {
    // Ensure database is ready first
    await db.ready;
    
    // If instance doesn't exist or initialization is in progress
    if (!halopsaAPIInstance || initializationPromise) {
        if (!initializationPromise) {
            console.log('Creating new HaloPSA API instance...');
            initializationPromise = (async () => {
                const instance = new HaloPSAAPI(db, configAPI);
                await instance.initialize();
                halopsaAPIInstance = instance;
                initializationPromise = null;
                console.log('HaloPSA API instance created and initialized');
                return instance;
            })();
        }
        return await initializationPromise;
    } else {
        // Instance exists, re-initialize with latest config
        console.log('Re-initializing existing HaloPSA API instance...');
        
        // Always create a fresh instance to ensure latest code is used
        console.log('Creating fresh HaloPSA API instance to ensure latest code...');
        const freshInstance = new HaloPSAAPI(db, configAPI);
        await freshInstance.initialize();
        halopsaAPIInstance = freshInstance;
        
        return halopsaAPIInstance;
    }
}

// Update configuration endpoint to refresh the API instance
app.post('/api/config', async (req, res) => {
    try {
        const updates = req.body;
        console.log('Updating configuration:', Object.keys(updates));
        
        for (const [key, value] of Object.entries(updates)) {
            await db.setConfig(key, value);
        }
        
        // Reset the cached HaloPSA API instance when configuration changes
        halopsaAPIInstance = null;
        console.log('HaloPSA API cache reset due to configuration changes');
        
        // Refresh the HaloPSA API instance if HaloPSA config was updated
        if (Object.keys(updates).some(key => key.startsWith('halopsa_'))) {
            console.log('HaloPSA configuration updated, refreshing API instance...');
            if (halopsaAPIInstance) {
                await halopsaAPIInstance.initialize();
            }
        }
        
        res.json({ success: true, message: 'Configuration updated successfully', updates });
    } catch (error) {
        console.error('Error updating configuration:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/halopsa/test', async (req, res) => {
    const requestId = Math.random().toString(36).substr(2, 6);
    console.log(`=== HaloPSA Test Request ${requestId} ===`);
    
    try {
        const halopsaAPI = await getHaloPSAAPI();
        const result = await halopsaAPI.testConnection();
        console.log(`✅ HaloPSA Test Request ${requestId} Raw Result:`, result);
        
        // Log the exact response being sent
        console.log(`📤 HaloPSA Test Request ${requestId} Sending Response:`, JSON.stringify(result));
        res.json(result);
    } catch (error) {
        console.error(`❌ HaloPSA Test Request ${requestId} Error:`, error);
        res.status(500).json({ success: false, message: 'Error testing HaloPSA connection' });
    }
});

app.post('/api/halopsa/obtain-token', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        console.log('HaloPSA API instance created for token request');
        
        // Diagnostic: check current state
        const config = await halopsaAPI.getConfig();
        console.log('Current HaloPSA config:', {
            apiUrl: config.halopsa_api_url,
            clientId: config.halopsa_client_id ? '***' : 'missing',
            clientSecret: config.halopsa_client_secret ? '***' : 'missing'
        });
        
        const result = await halopsaAPI.getAccessTokenViaClientCredentials();
        res.json(result);
    } catch (error) {
        console.error('Error obtaining HaloPSA token:', error);
        res.status(500).json({ success: false, message: 'Error obtaining HaloPSA token' });
    }
});

app.get('/api/halopsa/debug', async (req, res) => {
    try {
        const halopsaAPI = new HaloPSAAPI(db);
        const config = await halopsaAPI.getConfig();
        res.json({
            config: {
                apiUrl: config.halopsa_api_url,
                clientId: config.halopsa_client_id,
                clientSecret: config.halopsa_client_secret,
                accessToken: config.halopsa_access_token
            },
            instanceState: {
                clientId: halopsaAPI.clientId,
                clientSecret: halopsaAPI.clientSecret ? '***' : 'missing',
                apiUrl: halopsaAPI.apiUrl,
                accessToken: halopsaAPI.accessToken
            },
            troubleshooting: {
                message: "If you're getting 'invalid_client' errors:",
                steps: [
                    "1. Log into HaloPSA at https://psa.dtctoday.com",
                    "2. Go to Configuration > Integrations > Halo API",
                    "3. Verify your application is registered with Client ID: 75a64418-eb08-4b0b-81c3-bff949bdba5a",
                    "4. Ensure 'Client Credentials' grant type is enabled for your application",
                    "5. Verify the client secret is correct",
                    "6. Make sure your application has the necessary scopes/permissions"
                ],
                authEndpoint: "https://psa.dtctoday.com/auth/token",
                apiEndpoint: "https://psa.dtctoday.com/api"
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/stripe/transactions', async (req, res) => {
    try {
        const transactions = await db.all('SELECT * FROM stripe_transactions ORDER BY created DESC LIMIT 50');
        // Enhance transactions with customer names
        const enhancedTransactions = await Promise.all(transactions.map(async (tx) => {
            if (tx.customer_id) {
                const mapping = await db.get(
                    'SELECT stripe_customer_name FROM customer_mappings WHERE stripe_customer_id = ?',
                    [tx.customer_id]
                );
                if (mapping) {
                    tx.customer_name = mapping.stripe_customer_name;
                }
            }
            return tx;
        }));
        res.json(enhancedTransactions);
    } catch (error) {
        console.error('Error fetching Stripe transactions:', error);
        res.status(500).json({ error: 'Failed to fetch transactions' });
    }
});

app.get('/api/stripe/customers', async (req, res) => {
    try {
        // Fetch Stripe customers with mapping information
        const customers = await db.all(`
            SELECT
                sc.*,
                cm.id as mapping_id,
                cm.halopsa_client_id,
                cm.halopsa_client_name,
                cm.mapping_confirmed
            FROM stripe_customers sc
            LEFT JOIN customer_mappings cm ON sc.stripe_id = cm.stripe_customer_id
            ORDER BY sc.name, sc.email
        `);

        console.log(`[DEBUG] Fetched ${customers.length} customers from database`);
        if (customers.length > 0) {
            console.log(`[DEBUG] First customer: ${customers[0].name} (${customers[0].email})`);
        }

        // Transform the data structure to match what the frontend expects
        const transformedCustomers = customers.map(customer => ({
            id: customer.stripe_id,  // Use stripe_id as the identifier for the frontend
            stripe_id: customer.stripe_id,
            name: customer.name,
            email: customer.email,
            phone: customer.phone,
            description: customer.description,
            mapping_id: customer.mapping_id,
            halopsa_client_id: customer.halopsa_client_id,
            halopsa_client_name: customer.halopsa_client_name,
            mapping_confirmed: customer.mapping_confirmed
        }));

        res.json(transformedCustomers);
    } catch (error) {
        console.error('Error fetching Stripe customers:', error);
        res.status(500).json({ error: 'Failed to fetch customers' });
    }
});

app.post('/api/customers/map', async (req, res) => {
    try {
        const { stripe_customer_id, halopsa_client_id } = req.body;
        
        if (!stripe_customer_id || !halopsa_client_id) {
            return res.status(400).json({ error: 'stripe_customer_id and halopsa_client_id are required' });
        }
        
        // Check if stripe customer is already mapped
        const existingMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE stripe_customer_id = ? AND mapping_confirmed = 1',
            [stripe_customer_id]
        );
        
        if (existingMapping) {
            return res.status(400).json({ error: 'Stripe customer is already mapped to another HaloPSA client' });
        }
        
        // Check if halo client is already mapped
        const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));
        const existingHaloMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ? AND mapping_confirmed = 1',
            [halopsaClientIdNum]
        );
        
        if (existingHaloMapping) {
            return res.status(400).json({ error: 'HaloPSA client is already mapped to another Stripe customer' });
        }
        
        // Get customer details for the mapping record
        const stripeCustomer = await db.get(
            'SELECT name, email FROM stripe_customers WHERE stripe_id = ?',
            [stripe_customer_id]
        );
        
        const haloClient = await db.get(
            'SELECT name FROM halopsa_clients WHERE halopsa_id = ?',
            [halopsa_client_id]
        );
        
        await db.run(
            `INSERT INTO customer_mappings 
             (stripe_customer_id, stripe_customer_email, stripe_customer_name, 
              halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
             VALUES (?, ?, ?, ?, ?, CAST(? AS BOOLEAN), CAST(? AS BOOLEAN))`,
            [
                stripe_customer_id,
                stripeCustomer?.email || '',
                stripeCustomer?.name || '',
                halopsaClientIdNum,
                haloClient?.name || '',
                0, // Not auto-mapped (user confirmed) - use 0 instead of false
                1   // mapping_confirmed - use 1 instead of true
            ]
        );
        
        res.json({ success: true, message: 'Customer mapping saved' });
    } catch (error) {
        console.error('Error saving customer mapping:', error);
        console.error('Error details:', error.message);
        console.error('Error stack:', error.stack);
        res.status(500).json({ error: 'Failed to save mapping: ' + error.message });
    }
});

// Customer mappings endpoint is at line 3315+ with full JOIN support and pagination

// Alias endpoint for drag-drop interface - creates customer mapping
app.post('/api/customers/mappings/create', async (req, res) => {
    try {
        const { stripe_customer_id, stripe_customer_name, stripe_customer_email, halopsa_client_id, halopsa_client_name } = req.body;

        if (!stripe_customer_id || !halopsa_client_id) {
            return res.status(400).json({ error: 'stripe_customer_id and halopsa_client_id are required' });
        }

        // Check if stripe customer is already mapped
        const existingMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE stripe_customer_id = ? AND mapping_confirmed = 1',
            [stripe_customer_id]
        );

        if (existingMapping) {
            // Update existing mapping instead of error
            await db.run(
                `UPDATE customer_mappings
                 SET halopsa_client_id = ?, halopsa_client_name = ?,
                     stripe_customer_name = ?, stripe_customer_email = ?,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE stripe_customer_id = ?`,
                [
                    Math.floor(Number(halopsa_client_id)),
                    halopsa_client_name,
                    stripe_customer_name,
                    stripe_customer_email,
                    stripe_customer_id
                ]
            );
            return res.json({ success: true, message: 'Customer mapping updated' });
        }

        // Create new mapping
        const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));

        await db.run(
            `INSERT INTO customer_mappings
             (stripe_customer_id, stripe_customer_email, stripe_customer_name,
              halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
             VALUES (?, ?, ?, ?, ?, 0, 1)`,
            [
                stripe_customer_id,
                stripe_customer_email || '',
                stripe_customer_name || '',
                halopsaClientIdNum,
                halopsa_client_name || '',
            ]
        );

        res.json({ success: true, message: 'Customer mapping created' });
    } catch (error) {
        console.error('Error creating customer mapping:', error);
        res.status(500).json({ error: 'Failed to create mapping: ' + error.message });
    }
});

app.post('/api/customers/automatch', async (req, res) => {
    try {
        const stripeAPI = new StripeAPI(db);
        
        // Get threshold from configuration
        const thresholdRow = await db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        const threshold = thresholdRow && thresholdRow.value ? parseFloat(thresholdRow.value) : 0.55;
        
        console.log('Automatching with threshold:', threshold);
        
        // Get page parameters and ensure they're integers
        const { page = 1, pageSize = 50, forceFullScan = false } = req.body;
        const pageInt = Number.parseInt(page) || 1;
        const pageSizeInt = Number.parseInt(pageSize) || 50;
        const offset = Math.max(0, (pageInt - 1) * pageSizeInt);
        
        // Ensure integers are passed as integers, not floats
        const sqlPageSize = Math.floor(pageSizeInt);
        const sqlOffset = Math.floor(offset);
        
        console.log('Page params:', { page, pageInt, pageSize, pageSizeInt, offset });
        
        // Handle backward compatibility - if no pagination params, do a limited scan
        if (!req.body.page && !forceFullScan) {
            console.log('Legacy automatch call detected - performing limited scan');
            return await performLegacyAutomatch(res, stripeAPI, db, threshold);
        }
        
        // Get existing mappings to avoid duplicates
        const existingMappings = await db.all(`
            SELECT stripe_customer_id, halopsa_client_id, mapping_confirmed 
            FROM customer_mappings 
            WHERE mapping_confirmed = 1
        `);
        
        const stripeMapped = new Set(existingMappings.map(m => m.stripe_customer_id));
        const haloMapped = new Set(existingMappings.map(m => m.halopsa_client_id.toString()));
        
        // Handle empty mapped sets (SQL doesn't like IN () with empty list)
        // Convert all IDs to strings to avoid datatype mismatch
        const stripeMappedStrings = Array.from(stripeMapped).map(id => id.toString());
        const haloMappedStrings = Array.from(haloMapped).map(id => id.toString());
        
        const stripeWhereClause = stripeMappedStrings.length > 0 
            ? `AND stripe_id NOT IN (${stripeMappedStrings.map(() => '?').join(',')})`
            : '';
        const haloWhereClause = haloMappedStrings.length > 0 
            ? `WHERE halopsa_id NOT IN (${haloMappedStrings.map(() => '?').join(',')})`
            : '';
        
        // Get paginated stripe customers (only unmapped ones with valid names)
        const stripeQuery = `
            SELECT * FROM stripe_customers 
            WHERE name IS NOT NULL AND name != 'null' 
            ${stripeWhereClause}
            ORDER BY name
            LIMIT ? OFFSET ?
        `;
        const stripeParams = stripeMappedStrings.length > 0 
            ? [...stripeMappedStrings, sqlPageSize, sqlOffset]
            : [sqlPageSize, sqlOffset];
            
        console.log('SQL Query:', stripeQuery);
        console.log('SQL Params:', stripeParams);
        
        const stripeCustomers = await db.all(stripeQuery, stripeParams);
        
        // Get all halo clients (they're fewer, so we can load them all)
        const haloQuery = `
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
            ${haloWhereClause}
        `;
        const haloParams = haloMappedStrings.length > 0 ? [...haloMappedStrings] : [];
        const haloClients = await db.all(haloQuery, haloParams);
        
        console.log(`Automatch page ${page}: ${stripeCustomers.length} stripe customers, ${haloClients.length} halo clients`);
        
        const startTime = Date.now();
        const matches = [];
        const conflicts = [];
        
        // Pre-calculate name lowercases and word sets for performance
        const haloClientCache = haloClients.map(client => ({
            ...client,
            nameLower: client.name ? client.name.toLowerCase() : '',
            words: client.name ? new Set(client.name.toLowerCase().split(/\s+/).filter(w => w.length > 2)) : new Set()
        }));
        
        // Batch processing with performance monitoring
        let comparisons = 0;
        for (const stripeCustomer of stripeCustomers) {
            const stripeNameLower = stripeCustomer.name.toLowerCase();
            const stripeWords = new Set(stripeNameLower.split(/\s+/).filter(w => w.length > 2));
            
            const customerMatches = [];
            
            for (const haloClient of haloClientCache) {
                comparisons++;
                const score = calculateMatchScoreOptimized(stripeCustomer, haloClient, stripeNameLower, stripeWords);
                if (score >= threshold) {
                    customerMatches.push({
                        halo_client: haloClient,
                        score: score
                    });
                }
            }
            
            customerMatches.sort((a, b) => b.score - a.score);
            
            if (customerMatches.length === 1) {
                matches.push({
                    stripe_customer: stripeCustomer,
                    halo_client: customerMatches[0].halo_client,
                    match_score: customerMatches[0].score,
                    type: 'suggestion'
                });
            } else if (customerMatches.length > 1) {
                conflicts.push({
                    stripe_customer: stripeCustomer,
                    potential_matches: customerMatches,
                    type: 'conflict'
                });
            }
        }
        
        const processingTime = Date.now() - startTime;
        
        // Get total counts for pagination info
        const totalStripeQuery = `
            SELECT COUNT(*) as total FROM stripe_customers 
            WHERE name IS NOT NULL AND name != 'null' 
            ${stripeWhereClause}
        `;
        const totalStripeParams = stripeMappedStrings.length > 0 ? [...stripeMappedStrings] : [];
        const totalStripeCount = await db.get(totalStripeQuery, totalStripeParams);
        
        const totalPages = Math.ceil(totalStripeCount.total / pageSize);
        
        console.log(`Processed ${comparisons} comparisons in ${processingTime}ms`);
        
        // Second pass: find best matches for each halo client (to catch reverse conflicts)
        for (const haloClient of haloClients) {
            const clientMatches = [];
            
            for (const stripeCustomer of stripeCustomers) {
                const score = calculateMatchScoreOptimized(stripeCustomer, haloClient, 
                    stripeCustomer.name ? stripeCustomer.name.toLowerCase() : '', 
                    stripeCustomer.name ? new Set(stripeCustomer.name.toLowerCase().split(/\s+/).filter(w => w.length > 2)) : new Set()
                );
                if (score >= threshold) {
                    clientMatches.push({
                        stripe_customer: stripeCustomer,
                        score: score
                    });
                }
            }
            
            // Sort matches by score (highest first)
            clientMatches.sort((a, b) => b.score - a.score);
            
            if (clientMatches.length > 1) {
                // Check if this creates new conflicts not already captured
                const newConflict = {
                    halo_client: haloClient,
                    potential_matches: clientMatches,
                    type: 'conflict_reverse'
                };
                
                // Only add if not already in conflicts from stripe perspective
                const conflictExists = conflicts.some(conflict => 
                    conflict.stripe_customer && 
                    clientMatches.some(match => match.stripe_customer.stripe_id === conflict.stripe_customer.stripe_id)
                );
                
                if (!conflictExists) {
                    conflicts.push(newConflict);
                }
            }
        }
        
        console.log(`Found ${matches.length} suggestions and ${conflicts.length} conflicts`);
        
        // Return results with pagination info
        res.json({ 
            success: true, 
            matches: matches, // Old format for frontend compatibility
            suggestions: matches, // New format
            conflicts: conflicts, 
            threshold: threshold,
            pagination: {
                page: pageInt,
                pageSize: pageSizeInt,
                totalPages: totalPages,
                totalStripeCustomers: totalStripeCount.total,
                hasMore: pageInt < totalPages
            },
            performance: {
                processingTime: processingTime,
                comparisons: comparisons,
                matchesPerSecond: comparisons > 0 ? Math.round((comparisons / processingTime) * 1000) : 0
            },
            summary: {
                total_suggestions: matches.length,
                total_conflicts: conflicts.length,
                unmatched_stripe_customers: totalStripeCount.total - matches.length - conflicts.filter(c => c.stripe_customer).length,
                unmatched_halo_clients: haloClients.length - matches.length - conflicts.filter(c => c.halo_client).length
            }
        });
    } catch (error) {
        console.error('Error auto-matching customers:', error);
        console.error('Error details:', error.message);
        console.error('Error stack:', error.stack);
        res.status(500).json({ error: 'Failed to auto-match customers: ' + error.message });
    }
});

// QuickBooks to HaloPSA Auto-match endpoint
app.post('/api/quickbooks/automatch', async (req, res) => {
    try {
        // Get threshold from configuration
        const thresholdRow = await db.get('SELECT value FROM config WHERE key = ?', ['customer_match_threshold']);
        const threshold = thresholdRow && thresholdRow.value ? parseFloat(thresholdRow.value) : 0.55;

        console.log('QuickBooks automatch with threshold:', threshold);

        // Get existing QB-to-Halo mappings
        const existingMappings = await db.all(`
            SELECT qb_customer_id, halopsa_client_id, mapping_confirmed
            FROM customer_mappings
            WHERE qb_customer_id IS NOT NULL AND halopsa_client_id IS NOT NULL AND mapping_confirmed = 1
        `);

        const qbMapped = new Set(existingMappings.map(m => m.qb_customer_id));
        const haloMapped = new Set(existingMappings.map(m => m.halopsa_client_id?.toString()));

        // Convert all IDs to strings
        const qbMappedStrings = Array.from(qbMapped).filter(id => id).map(id => id.toString());
        const haloMappedStrings = Array.from(haloMapped).filter(id => id).map(id => id.toString());

        const qbWhereClause = qbMappedStrings.length > 0
            ? `AND qb_list_id NOT IN (${qbMappedStrings.map(() => '?').join(',')})`
            : '';
        const haloWhereClause = haloMappedStrings.length > 0
            ? `WHERE halopsa_id NOT IN (${haloMappedStrings.map(() => '?').join(',')})`
            : '';

        // Get unmapped QB customers
        const qbQuery = `
            SELECT * FROM qb_customers
            WHERE (qb_full_name IS NOT NULL AND qb_full_name != '' AND qb_full_name != 'null')
            OR (company_name IS NOT NULL AND company_name != '' AND company_name != 'null')
            ${qbWhereClause}
            ORDER BY qb_full_name
        `;
        const qbParams = qbMappedStrings.length > 0 ? [...qbMappedStrings] : [];
        const qbCustomers = await db.all(qbQuery, qbParams);

        // Get all unmapped halo clients
        const haloQuery = `
            SELECT id, halopsa_id, name, email, phone, address_line1,
                   created_at, last_sync, raw_data
            FROM halopsa_clients
            ${haloWhereClause}
        `;
        const haloParams = haloMappedStrings.length > 0 ? [...haloMappedStrings] : [];
        const haloClients = await db.all(haloQuery, haloParams);

        console.log(`QB Automatch: ${qbCustomers.length} QB customers, ${haloClients.length} halo clients`);

        const startTime = Date.now();
        const matches = [];
        const conflicts = [];

        // Pre-calculate name lowercases and word sets for performance
        const haloClientCache = haloClients.map(client => ({
            ...client,
            nameLower: client.name ? client.name.toLowerCase() : '',
            words: client.name ? new Set(client.name.toLowerCase().split(/\s+/).filter(w => w.length > 2)) : new Set()
        }));

        let comparisons = 0;
        for (const qbCustomer of qbCustomers) {
            // Use qb_full_name or company_name
            const qbName = qbCustomer.company_name || qbCustomer.qb_full_name || '';
            if (!qbName) continue;

            const qbNameLower = qbName.toLowerCase();
            const qbWords = new Set(qbNameLower.split(/\s+/).filter(w => w.length > 2));

            const customerMatches = [];

            for (const haloClient of haloClientCache) {
                comparisons++;
                const score = calculateQBMatchScore(qbCustomer, haloClient, qbNameLower, qbWords);
                if (score >= threshold) {
                    customerMatches.push({
                        halo_client: haloClient,
                        score: score
                    });
                }
            }

            customerMatches.sort((a, b) => b.score - a.score);

            if (customerMatches.length === 1) {
                matches.push({
                    qb_customer: qbCustomer,
                    halo_client: customerMatches[0].halo_client,
                    match_score: customerMatches[0].score,
                    type: 'suggestion'
                });
            } else if (customerMatches.length > 1) {
                conflicts.push({
                    qb_customer: qbCustomer,
                    potential_matches: customerMatches,
                    type: 'conflict'
                });
            }
        }

        const processingTime = Date.now() - startTime;

        console.log(`Processed ${comparisons} comparisons in ${processingTime}ms`);

        res.json({
            success: true,
            matches: matches,
            conflicts: conflicts,
            threshold: threshold,
            performance: {
                processingTime: processingTime,
                comparisons: comparisons,
                matchesPerSecond: comparisons > 0 ? Math.round((comparisons / processingTime) * 1000) : 0
            },
            summary: {
                total_suggestions: matches.length,
                total_conflicts: conflicts.length,
                unmatched_qb_customers: qbCustomers.length - matches.length - conflicts.length,
                unmatched_halo_clients: haloClients.length - matches.length
            }
        });
    } catch (error) {
        console.error('Error QB auto-matching:', error);
        res.status(500).json({ error: 'Failed to QB auto-match: ' + error.message });
    }
});

// Save QB to HaloPSA mapping
app.post('/api/quickbooks/save-mapping', async (req, res) => {
    try {
        const { qb_customer_id, halopsa_client_id } = req.body;

        if (!qb_customer_id || !halopsa_client_id) {
            return res.status(400).json({ error: 'QB customer ID and HaloPSA client ID are required' });
        }

        // Check if QB customer is already mapped
        const existingMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE qb_customer_id = ? AND mapping_confirmed = 1',
            [qb_customer_id]
        );

        if (existingMapping) {
            return res.status(400).json({ error: 'QB customer is already mapped' });
        }

        // Check if halo client is already mapped to QB
        const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));
        const existingHaloMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ? AND qb_customer_id IS NOT NULL AND mapping_confirmed = 1',
            [halopsaClientIdNum]
        );

        if (existingHaloMapping) {
            return res.status(400).json({ error: 'HaloPSA client is already mapped to another QB customer' });
        }

        // Get customer details
        const qbCustomer = await db.get(
            'SELECT qb_full_name, company_name, email FROM qb_customers WHERE qb_list_id = ?',
            [qb_customer_id]
        );

        const haloClient = await db.get(
            'SELECT name FROM halopsa_clients WHERE halopsa_id = ?',
            [halopsaClientIdNum]
        );

        await db.run(
            `INSERT INTO customer_mappings
             (qb_customer_id, qb_customer_name, halopsa_client_id, halopsa_client_name,
              auto_mapped, mapping_confirmed, mapping_source)
             VALUES (?, ?, ?, ?, 0, 1, 'quickbooks')`,
            [
                qb_customer_id,
                qbCustomer?.company_name || qbCustomer?.qb_full_name || null,
                halopsaClientIdNum,
                haloClient?.name || null
            ]
        );

        res.json({ success: true, message: 'QB customer mapping saved' });
    } catch (error) {
        console.error('Error saving QB mapping:', error);
        res.status(500).json({ error: 'Failed to save QB mapping: ' + error.message });
    }
});

// Calculate match score for QB customers
function calculateQBMatchScore(qbCustomer, haloClient, qbNameLower, qbWords) {
    let score = 0;

    // Email match
    if (qbCustomer.email && haloClient.email) {
        try {
            const email1 = qbCustomer.email.toLowerCase().trim();
            const email2 = haloClient.email.toLowerCase().trim();
            if (email1 === email2) {
                score += 0.8;
            }
        } catch (e) {
            // Email comparison failed, continue with name matching
        }
    }

    // Name matching using pre-calculated values
    if (qbNameLower && haloClient.nameLower) {
        try {
            // Exact match
            if (qbNameLower === haloClient.nameLower) {
                score += 0.8;
            }
            // One name is a clear subset of the other
            else if (isClearSubset(qbNameLower, haloClient.nameLower)) {
                score += 0.7;
            }
            // Contains match
            else if (qbNameLower.includes(haloClient.nameLower) || haloClient.nameLower.includes(qbNameLower)) {
                score += 0.6;
            }
            // Word overlap using pre-calculated sets
            else if (qbWords && haloClient.words) {
                const intersection = new Set([...qbWords].filter(x => haloClient.words.has(x)));
                const union = new Set([...qbWords, ...haloClient.words]);

                if (union.size > 0) {
                    const overlap = intersection.size / union.size;
                    if (overlap > 0.8) score += 0.6;
                    else if (overlap > 0.6) score += 0.4;
                    else if (overlap > 0.4) score += 0.3;
                }
            }
        } catch (e) {
            // Name matching failed, continue without adding score
            console.warn('QB name matching error:', e.message);
        }
    }

    return Math.min(score, 1.0);
}

// Legacy automatch for backward compatibility
async function performLegacyAutomatch(res, stripeAPI, db, threshold) {
    try {
        console.log('Performing legacy automatch (limited to 100 customers)');
        
        // Get existing mappings
        const existingMappings = await db.all(`
            SELECT stripe_customer_id, halopsa_client_id, mapping_confirmed 
            FROM customer_mappings 
            WHERE mapping_confirmed = 1
        `);
        
        const stripeMapped = new Set(existingMappings.map(m => m.stripe_customer_id));
        const haloMapped = new Set(existingMappings.map(m => m.halopsa_client_id.toString()));
        
        // Handle empty mapped sets - convert all IDs to strings
        const stripeMappedStrings = Array.from(stripeMapped).map(id => id.toString());
        const haloMappedStrings = Array.from(haloMapped).map(id => id.toString());
        
        const stripeWhereClause = stripeMappedStrings.length > 0 
            ? `AND stripe_id NOT IN (${stripeMappedStrings.map(() => '?').join(',')})`
            : '';
        const haloWhereClause = haloMappedStrings.length > 0 
            ? `WHERE halopsa_id NOT IN (${haloMappedStrings.map(() => '?').join(',')})`
            : '';
        
        // Get limited stripe customers for performance
        const stripeQuery = `
            SELECT * FROM stripe_customers 
            WHERE name IS NOT NULL AND name != 'null' 
            ${stripeWhereClause}
            ORDER BY name
            LIMIT 100
        `;
        const stripeParams = stripeMappedStrings.length > 0 ? [...stripeMappedStrings] : [];
        const stripeCustomers = await db.all(stripeQuery, stripeParams);
        
        // Get all unmapped halo clients
        const haloQuery = `
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
            ${haloWhereClause}
        `;
        const haloParams = haloMappedStrings.length > 0 ? [...haloMappedStrings] : [];
        const haloClients = await db.all(haloQuery, haloParams);
        
        console.log(`Legacy automatch: ${stripeCustomers.length} stripe customers, ${haloClients.length} halo clients`);
        
        const matches = [];
        const conflicts = [];
        
        for (const stripeCustomer of stripeCustomers) {
            const customerMatches = [];
            
            for (const haloClient of haloClients) {
                const score = calculateMatchScore(stripeCustomer, haloClient);
                if (score >= threshold) {
                    customerMatches.push({
                        halo_client: haloClient,
                        score: score
                    });
                }
            }
            
            customerMatches.sort((a, b) => b.score - a.score);
            
            if (customerMatches.length === 1) {
                matches.push({
                    stripe_customer: stripeCustomer,
                    halo_client: customerMatches[0].halo_client,
                    match_score: customerMatches[0].score,
                    type: 'suggestion'
                });
            } else if (customerMatches.length > 1) {
                conflicts.push({
                    stripe_customer: stripeCustomer,
                    potential_matches: customerMatches,
                    type: 'conflict'
                });
            }
        }
        
        console.log(`Legacy automatch complete: ${matches.length} matches, ${conflicts.length} conflicts`);
        
        res.json({ 
            success: true, 
            matches: matches,
            suggestions: matches,
            conflicts: conflicts,
            threshold: threshold,
            summary: {
                total_suggestions: matches.length,
                total_conflicts: conflicts.length
            },
            note: 'This is a limited scan. Use paginated API for full dataset.'
        });
        
    } catch (error) {
        console.error('Error in legacy automatch:', error);
        console.error('Legacy error details:', error.message);
        console.error('Legacy error stack:', error.stack);
        res.status(500).json({ error: 'Failed to perform automatch: ' + error.message });
    }
}

// Save/approve customer mappings
app.post('/api/customers/mappings/save', async (req, res) => {
    try {
        const { mappings, batch = false } = req.body;
        
        if (!mappings || !Array.isArray(mappings)) {
            return res.status(400).json({ error: 'Mappings array is required' });
        }
        
        const results = [];
        
        for (const mapping of mappings) {
            try {
                const { stripe_customer_id, halopsa_client_id, auto_mapped = false, mapping_confirmed = true } = mapping;
                
                if (!stripe_customer_id || !halopsa_client_id) {
                    results.push({
                        success: false,
                        error: 'stripe_customer_id and halopsa_client_id are required',
                        mapping: mapping
                    });
                    continue;
                }
                
                // Check if stripe customer is already mapped
                const existingMapping = await db.get(
                    'SELECT * FROM customer_mappings WHERE stripe_customer_id = ?',
                    [stripe_customer_id]
                );
                
                if (existingMapping && existingMapping.mapping_confirmed) {
                    results.push({
                        success: false,
                        error: 'Stripe customer is already mapped to another HaloPSA client',
                        mapping: mapping
                    });
                    continue;
                }
                
                // Check if halo client is already mapped
                // Ensure halopsa_client_id is an integer (not a float)
                const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));
                const existingHaloMapping = await db.get(
                    'SELECT * FROM customer_mappings WHERE halopsa_client_id = ? AND mapping_confirmed = 1',
                    [halopsaClientIdNum]
                );
                
                if (existingHaloMapping) {
                    results.push({
                        success: false,
                        error: 'HaloPSA client is already mapped to another Stripe customer',
                        mapping: mapping
                    });
                    continue;
                }
                
                // Get customer details for the mapping record
                const stripeCustomer = await db.get(
                    'SELECT name, email FROM stripe_customers WHERE stripe_id = ?',
                    [stripe_customer_id]
                );
                
                const haloClient = await db.get(
                    'SELECT name FROM halopsa_clients WHERE halopsa_id = ?',
                    [halopsa_client_id]
                );
                
                if (existingMapping) {
                    // Update existing mapping - use integers for boolean values
                    await db.run(
                        `UPDATE customer_mappings 
                         SET halopsa_client_id = ?, halopsa_client_name = ?, 
                             auto_mapped = ?, mapping_confirmed = ?,
                             updated_at = CURRENT_TIMESTAMP
                         WHERE stripe_customer_id = ?`,
                        [halopsaClientIdNum, haloClient?.name || '', auto_mapped ? 1 : 0, mapping_confirmed ? 1 : 0, stripe_customer_id]
                    );
                } else {
                    // Insert new mapping - use integers for boolean values
                    await db.run(
                        `INSERT INTO customer_mappings 
                         (stripe_customer_id, stripe_customer_email, stripe_customer_name, 
                          halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
                         VALUES (?, ?, ?, ?, ?, ?, ?)`,
                        [
                            stripe_customer_id,
                            stripeCustomer?.email || '',
                            stripeCustomer?.name || '',
                            halopsaClientIdNum,
                            haloClient?.name || '',
                            auto_mapped ? 1 : 0,
                            mapping_confirmed ? 1 : 0
                        ]
                    );
                }
                
                results.push({
                    success: true,
                    mapping: mapping
                });
                
            } catch (error) {
                results.push({
                    success: false,
                    error: error.message,
                    mapping: mapping
                });
            }
        }
        
        res.json({ 
            success: true, 
            results: results,
            summary: {
                total: mappings.length,
                successful: results.filter(r => r.success).length,
                failed: results.filter(r => !r.success).length
            }
        });
        
    } catch (error) {
        console.error('Error saving customer mappings:', error);
        res.status(500).json({ error: 'Failed to save customer mappings' });
    }
});

// Get existing customer mappings
app.get('/api/customers/mappings', async (req, res) => {
    try {
        const { include_unconfirmed = false, limit = null, offset = 0 } = req.query;

        let query = `
            SELECT cm.*,
                   sc.email as stripe_email, sc.name as stripe_name,
                   hc.name as halo_name, hc.email as halo_email
            FROM customer_mappings cm
            LEFT JOIN stripe_customers sc ON cm.stripe_customer_id = sc.stripe_id
            LEFT JOIN halopsa_clients hc ON cm.halopsa_client_id = hc.halopsa_id
        `;

        if (!include_unconfirmed) {
            query += ' WHERE cm.mapping_confirmed = 1';
        }

        query += ' ORDER BY cm.updated_at DESC, cm.created_at DESC';

        // Add pagination if limit is specified
        if (limit !== null && limit !== undefined) {
            query += ` LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
        }

        const mappings = await db.all(query);

        // If pagination is used, return array directly for InfiniteScroll compatibility
        if (limit !== null && limit !== undefined) {
            res.json(mappings);
        } else {
            // Legacy response format for backward compatibility
            res.json({ success: true, mappings: mappings });
        }

    } catch (error) {
        console.error('Error fetching customer mappings:', error);
        res.status(500).json({ error: 'Failed to fetch customer mappings' });
    }
});

// Delete customer mapping
app.delete('/api/customers/mappings/:stripe_customer_id', async (req, res) => {
    try {
        const { stripe_customer_id } = req.params;

        await db.run(
            'DELETE FROM customer_mappings WHERE stripe_customer_id = ?',
            [stripe_customer_id]
        );

        res.json({ success: true, message: 'Mapping deleted successfully' });

    } catch (error) {
        console.error('Error deleting customer mapping:', error);
        res.status(500).json({ error: 'Failed to delete customer mapping' });
    }
});

// ==================== UNMAP INDIVIDUAL PLATFORM MAPPINGS ====================

// DELETE /api/customers/mappings/stripe/:halopsa_client_id - Remove Stripe mapping
app.delete('/api/customers/mappings/stripe/:halopsa_client_id', async (req, res) => {
    try {
        const halopsa_client_id = Math.floor(Number(req.params.halopsa_client_id));

        if (!halopsa_client_id || isNaN(halopsa_client_id)) {
            return res.status(400).json({ error: 'Invalid HaloPSA client ID' });
        }

        // Find the mapping row
        const mapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ?',
            [halopsa_client_id]
        );

        if (!mapping) {
            return res.status(404).json({ error: 'No mapping found for this HaloPSA client' });
        }

        // Check if this mapping has QB data too
        const hasQBMapping = mapping.qb_customer_id !== null && mapping.qb_customer_id !== '';

        if (hasQBMapping) {
            // Keep the row but clear Stripe fields
            console.log(`[UNMAP] Removing Stripe mapping for HaloPSA client ${halopsa_client_id}, keeping QB mapping`);
            await db.run(
                `UPDATE customer_mappings
                 SET stripe_customer_id = NULL,
                     stripe_customer_name = NULL,
                     stripe_customer_email = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE halopsa_client_id = ?`,
                [halopsa_client_id]
            );
        } else {
            // No QB mapping, delete the entire row
            console.log(`[UNMAP] Deleting mapping row for HaloPSA client ${halopsa_client_id} (no QB mapping)`);
            await db.run(
                'DELETE FROM customer_mappings WHERE halopsa_client_id = ?',
                [halopsa_client_id]
            );
        }

        res.json({ success: true, message: 'Stripe mapping removed' });

    } catch (error) {
        console.error('Error removing Stripe mapping:', error);
        res.status(500).json({ error: 'Failed to remove Stripe mapping: ' + error.message });
    }
});

// DELETE /api/customers/mappings/qb/:halopsa_client_id - Remove QuickBooks mapping
app.delete('/api/customers/mappings/qb/:halopsa_client_id', async (req, res) => {
    try {
        const halopsa_client_id = Math.floor(Number(req.params.halopsa_client_id));

        if (!halopsa_client_id || isNaN(halopsa_client_id)) {
            return res.status(400).json({ error: 'Invalid HaloPSA client ID' });
        }

        // Find the mapping row
        const mapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ?',
            [halopsa_client_id]
        );

        if (!mapping) {
            return res.status(404).json({ error: 'No mapping found for this HaloPSA client' });
        }

        // Check if this mapping has Stripe data too
        const hasStripeMapping = mapping.stripe_customer_id !== null && mapping.stripe_customer_id !== '';

        if (hasStripeMapping) {
            // Keep the row but clear QB fields
            console.log(`[UNMAP] Removing QB mapping for HaloPSA client ${halopsa_client_id}, keeping Stripe mapping`);
            await db.run(
                `UPDATE customer_mappings
                 SET qb_customer_id = NULL,
                     qb_customer_name = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE halopsa_client_id = ?`,
                [halopsa_client_id]
            );
        } else {
            // No Stripe mapping, delete the entire row
            console.log(`[UNMAP] Deleting mapping row for HaloPSA client ${halopsa_client_id} (no Stripe mapping)`);
            await db.run(
                'DELETE FROM customer_mappings WHERE halopsa_client_id = ?',
                [halopsa_client_id]
            );
        }

        res.json({ success: true, message: 'QuickBooks mapping removed' });

    } catch (error) {
        console.error('Error removing QB mapping:', error);
        res.status(500).json({ error: 'Failed to remove QB mapping: ' + error.message });
    }
});

// POST /api/customers/mappings/stripe - Create or update Stripe → HaloPSA mapping
app.post('/api/customers/mappings/stripe', async (req, res) => {
    try {
        const { stripe_customer_id, halopsa_client_id } = req.body;

        if (!stripe_customer_id || !halopsa_client_id) {
            return res.status(400).json({
                error: 'stripe_customer_id and halopsa_client_id are required'
            });
        }

        const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));

        // Validate that HaloPSA client exists
        const haloClient = await db.get(
            'SELECT halopsa_id, name FROM halopsa_clients WHERE halopsa_id = ?',
            [halopsaClientIdNum]
        );

        if (!haloClient) {
            return res.status(404).json({ error: 'HaloPSA client not found' });
        }

        // Get Stripe customer details
        const stripeCustomer = await db.get(
            'SELECT stripe_id, name, email FROM stripe_customers WHERE stripe_id = ?',
            [stripe_customer_id]
        );

        if (!stripeCustomer) {
            return res.status(404).json({ error: 'Stripe customer not found' });
        }

        // Check if mapping already exists for this HaloPSA client
        const existingMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ?',
            [halopsaClientIdNum]
        );

        if (existingMapping) {
            // Update existing mapping to add Stripe fields
            console.log(`[MAP] Updating mapping for HaloPSA client ${halopsaClientIdNum} with Stripe customer ${stripe_customer_id}`);
            await db.run(
                `UPDATE customer_mappings
                 SET stripe_customer_id = ?,
                     stripe_customer_name = ?,
                     stripe_customer_email = ?,
                     mapping_confirmed = 0,
                     auto_mapped = 0,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE halopsa_client_id = ?`,
                [
                    stripe_customer_id,
                    stripeCustomer.name || '',
                    stripeCustomer.email || '',
                    halopsaClientIdNum
                ]
            );
        } else {
            // Create new mapping row
            console.log(`[MAP] Creating new mapping for HaloPSA client ${halopsaClientIdNum} with Stripe customer ${stripe_customer_id}`);
            await db.run(
                `INSERT INTO customer_mappings
                 (stripe_customer_id, stripe_customer_name, stripe_customer_email,
                  qb_customer_id, qb_customer_name,
                  halopsa_client_id, halopsa_client_name,
                  mapping_confirmed, auto_mapped)
                 VALUES (?, ?, ?, NULL, NULL, ?, ?, 0, 0)`,
                [
                    stripe_customer_id,
                    stripeCustomer.name || '',
                    stripeCustomer.email || '',
                    halopsaClientIdNum,
                    haloClient.name || ''
                ]
            );
        }

        res.json({ success: true, message: 'Stripe mapping created' });

    } catch (error) {
        console.error('Error creating Stripe mapping:', error);
        res.status(500).json({ error: 'Failed to create Stripe mapping: ' + error.message });
    }
});

// POST /api/customers/mappings/qb - Create or update QuickBooks → HaloPSA mapping
app.post('/api/customers/mappings/qb', async (req, res) => {
    try {
        const { qb_customer_id, halopsa_client_id } = req.body;

        if (!qb_customer_id || !halopsa_client_id) {
            return res.status(400).json({
                error: 'qb_customer_id and halopsa_client_id are required'
            });
        }

        const halopsaClientIdNum = Math.floor(Number(halopsa_client_id));

        // Validate that HaloPSA client exists
        const haloClient = await db.get(
            'SELECT halopsa_id, name FROM halopsa_clients WHERE halopsa_id = ?',
            [halopsaClientIdNum]
        );

        if (!haloClient) {
            return res.status(404).json({ error: 'HaloPSA client not found' });
        }

        // For now, we'll use qb_customer_id as the name since we don't have a QB customers table yet
        // TODO: When QB integration is built, fetch QB customer name from qb_customers table
        const qb_customer_name = qb_customer_id; // Placeholder until QB integration is complete

        // Check if mapping already exists for this HaloPSA client
        const existingMapping = await db.get(
            'SELECT * FROM customer_mappings WHERE halopsa_client_id = ?',
            [halopsaClientIdNum]
        );

        if (existingMapping) {
            // Update existing mapping to add QB fields
            console.log(`[MAP] Updating mapping for HaloPSA client ${halopsaClientIdNum} with QB customer ${qb_customer_id}`);
            await db.run(
                `UPDATE customer_mappings
                 SET qb_customer_id = ?,
                     qb_customer_name = ?,
                     mapping_confirmed = 0,
                     auto_mapped = 0,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE halopsa_client_id = ?`,
                [
                    qb_customer_id,
                    qb_customer_name,
                    halopsaClientIdNum
                ]
            );
        } else {
            // Create new mapping row
            console.log(`[MAP] Creating new mapping for HaloPSA client ${halopsaClientIdNum} with QB customer ${qb_customer_id}`);
            await db.run(
                `INSERT INTO customer_mappings
                 (stripe_customer_id, stripe_customer_name, stripe_customer_email,
                  qb_customer_id, qb_customer_name,
                  halopsa_client_id, halopsa_client_name,
                  mapping_confirmed, auto_mapped)
                 VALUES (NULL, NULL, NULL, ?, ?, ?, ?, 0, 0)`,
                [
                    qb_customer_id,
                    qb_customer_name,
                    halopsaClientIdNum,
                    haloClient.name || ''
                ]
            );
        }

        res.json({ success: true, message: 'QuickBooks mapping created' });

    } catch (error) {
        console.error('Error creating QB mapping:', error);
        res.status(500).json({ error: 'Failed to create QB mapping: ' + error.message });
    }
});

// ==================== STRIPE TRANSACTION TO HALOPSA INVOICE MAPPING ====================

// Helper function to parse invoice numbers from Stripe transaction descriptions
function parseInvoiceNumber(description) {
    if (!description) return null;

    const descStr = String(description).trim();
    const patterns = [
        // Pattern 1: "Invoice #12345" or "Invoice # 12345"
        /invoice\s*#\s*(\d+)/i,
        // Pattern 2: "INV-12345" or "INV 12345"
        /inv[-\s]*(\d+)/i,
        // Pattern 3: "Invoice 12345" or "Invoice: 12345"
        /invoice[:\s]+(\d+)/i,
        // Pattern 4: "Invoice Number: 12345" or "Invoice No: 12345"
        /invoice\s*(?:number|no|num)[:\s]*(\d+)/i,
        // Pattern 5: Standalone invoice number at start: "#12345"
        /^#(\d+)/,
        // Pattern 6: "Bill #12345" or "Bill 12345"
        /bill\s*#?\s*(\d+)/i,
        // Pattern 7: HaloPSA specific format (if known)
        /halo[-\s]*invoice[-\s]*(\d+)/i,
        // Pattern 8: Just digits after common prefixes
        /(?:^|\s)(?:inv|invoice|bill)[-\s#:]*(\d{3,})/i,
        // Pattern 9: Standalone number with 4+ digits (more conservative)
        /(?:^|\s)(\d{4,})(?:\s|$)/
    ];

    for (const pattern of patterns) {
        const match = descStr.match(pattern);
        if (match && match[1]) {
            return match[1];
        }
    }

    return null;
}

// POST /api/mappings/stripe-invoices - Auto-match Stripe transactions to HaloPSA invoices
app.post('/api/mappings/stripe-invoices', async (req, res) => {
    try {
        console.log('Starting Stripe-to-HaloPSA invoice auto-match...');

        // Get all Stripe transactions that haven't been mapped yet
        const unmappedTransactions = await db.all(`
            SELECT id, stripe_id, customer_id, amount, currency, description,
                   status, created, halopsa_invoice_id
            FROM stripe_transactions
            WHERE halopsa_invoice_id IS NULL
            ORDER BY created DESC
        `);

        console.log(`Found ${unmappedTransactions.length} unmapped Stripe transactions`);

        // Get all HaloPSA invoices for matching
        const haloInvoices = await db.all(`
            SELECT id, halopsa_id, invoice_number, halopsa_client_id, client_name,
                   invoice_date, total_amount, status, stripe_transaction_id
            FROM halopsa_invoices
            ORDER BY invoice_date DESC
        `);

        console.log(`Found ${haloInvoices.length} HaloPSA invoices`);

        // Create lookup map for invoices by invoice_number
        const invoiceMap = new Map();
        for (const invoice of haloInvoices) {
            invoiceMap.set(invoice.invoice_number, invoice);
        }

        const results = {
            success: true,
            total_processed: unmappedTransactions.length,
            matched: 0,
            failed: 0,
            already_mapped: 0,
            matches: [],
            failures: []
        };

        // Process each transaction
        for (const transaction of unmappedTransactions) {
            try {
                // Parse invoice number from description
                const invoiceNumber = parseInvoiceNumber(transaction.description);

                if (!invoiceNumber) {
                    results.failures.push({
                        stripe_id: transaction.stripe_id,
                        description: transaction.description,
                        reason: 'No invoice number found in description',
                        confidence: 0
                    });
                    results.failed++;
                    continue;
                }

                // Look up invoice in HaloPSA
                const matchedInvoice = invoiceMap.get(invoiceNumber);

                if (!matchedInvoice) {
                    results.failures.push({
                        stripe_id: transaction.stripe_id,
                        description: transaction.description,
                        parsed_invoice_number: invoiceNumber,
                        reason: 'Invoice number not found in HaloPSA',
                        confidence: 0
                    });
                    results.failed++;
                    continue;
                }

                // Check if invoice is already mapped to another transaction
                if (matchedInvoice.stripe_transaction_id &&
                    matchedInvoice.stripe_transaction_id !== transaction.stripe_id) {
                    results.failures.push({
                        stripe_id: transaction.stripe_id,
                        description: transaction.description,
                        parsed_invoice_number: invoiceNumber,
                        reason: `Invoice already mapped to Stripe transaction ${matchedInvoice.stripe_transaction_id}`,
                        confidence: 0
                    });
                    results.already_mapped++;
                    continue;
                }

                // Calculate confidence score based on amount matching
                let confidence = 0.7; // Base confidence for invoice number match

                // Convert Stripe amount (in cents) to dollars for comparison
                const stripeAmountDollars = transaction.amount / 100;
                const haloAmount = matchedInvoice.total_amount || 0;

                // Check if amounts match (within small tolerance for rounding)
                const amountDiff = Math.abs(stripeAmountDollars - haloAmount);
                if (amountDiff < 0.02) {
                    confidence = 1.0; // Perfect match
                } else if (amountDiff < 1.00) {
                    confidence = 0.9; // Very close match
                } else if (amountDiff < 10.00) {
                    confidence = 0.8; // Close match
                }
                // Otherwise keep base confidence of 0.7

                // Update the mapping in both tables
                await db.run(
                    'UPDATE stripe_transactions SET halopsa_invoice_id = ? WHERE id = ?',
                    [matchedInvoice.id, transaction.id]
                );

                await db.run(
                    'UPDATE halopsa_invoices SET stripe_transaction_id = ? WHERE id = ?',
                    [transaction.stripe_id, matchedInvoice.id]
                );

                results.matches.push({
                    stripe_transaction_id: transaction.stripe_id,
                    stripe_description: transaction.description,
                    stripe_amount: stripeAmountDollars,
                    halopsa_invoice_id: matchedInvoice.halopsa_id,
                    halopsa_invoice_number: matchedInvoice.invoice_number,
                    halopsa_client_name: matchedInvoice.client_name,
                    halopsa_amount: haloAmount,
                    amount_difference: amountDiff,
                    confidence: confidence,
                    match_reason: `Invoice number "${invoiceNumber}" parsed from description`
                });

                results.matched++;

            } catch (error) {
                console.error(`Error processing transaction ${transaction.stripe_id}:`, error);
                results.failures.push({
                    stripe_id: transaction.stripe_id,
                    description: transaction.description,
                    reason: error.message,
                    confidence: 0
                });
                results.failed++;
            }
        }

        console.log(`Invoice mapping complete: ${results.matched} matched, ${results.failed} failed, ${results.already_mapped} already mapped`);

        res.json(results);

    } catch (error) {
        console.error('Error auto-matching invoices:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to auto-match invoices: ' + error.message
        });
    }
});

// GET /api/mappings/stripe-invoices - Retrieve all mapped Stripe transactions with invoice details
app.get('/api/mappings/stripe-invoices', async (req, res) => {
    try {
        console.log('Retrieving Stripe-to-HaloPSA invoice mappings...');

        // Query for all mapped transactions with full invoice details
        const mappedTransactions = await db.all(`
            SELECT
                st.id as stripe_db_id,
                st.stripe_id,
                st.customer_id,
                st.amount as stripe_amount_cents,
                st.currency,
                st.description,
                st.status as stripe_status,
                st.created as stripe_created,
                st.halopsa_invoice_id,
                hi.halopsa_id,
                hi.invoice_number,
                hi.halopsa_client_id,
                hi.client_name,
                hi.invoice_date,
                hi.due_date,
                hi.total_amount as invoice_amount,
                hi.paid_amount,
                hi.balance_due,
                hi.status as invoice_status,
                hi.payment_status,
                hi.currency as invoice_currency,
                sc.name as stripe_customer_name,
                sc.email as stripe_customer_email
            FROM stripe_transactions st
            LEFT JOIN halopsa_invoices hi ON st.halopsa_invoice_id = hi.id
            LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
            WHERE st.halopsa_invoice_id IS NOT NULL
            ORDER BY st.created DESC
        `);

        // Transform the data for easier consumption
        const mappings = mappedTransactions.map(row => ({
            stripe_transaction: {
                id: row.stripe_id,
                db_id: row.stripe_db_id,
                customer_id: row.customer_id,
                customer_name: row.stripe_customer_name,
                customer_email: row.stripe_customer_email,
                amount: row.stripe_amount_cents / 100, // Convert cents to dollars
                amount_cents: row.stripe_amount_cents,
                currency: row.currency,
                description: row.description,
                status: row.stripe_status,
                created: row.stripe_created,
                created_date: new Date(row.stripe_created * 1000).toISOString()
            },
            halopsa_invoice: {
                id: row.halopsa_id,
                db_id: row.halopsa_invoice_id,
                invoice_number: row.invoice_number,
                client_id: row.halopsa_client_id,
                client_name: row.client_name,
                invoice_date: row.invoice_date,
                due_date: row.due_date,
                total_amount: row.invoice_amount,
                paid_amount: row.paid_amount,
                balance_due: row.balance_due,
                status: row.invoice_status,
                payment_status: row.payment_status,
                currency: row.invoice_currency
            },
            amount_match: {
                difference: Math.abs((row.stripe_amount_cents / 100) - (row.invoice_amount || 0)),
                is_exact: Math.abs((row.stripe_amount_cents / 100) - (row.invoice_amount || 0)) < 0.02
            }
        }));

        console.log(`Retrieved ${mappings.length} invoice mappings`);

        res.json({
            success: true,
            total: mappings.length,
            mappings: mappings
        });

    } catch (error) {
        console.error('Error retrieving invoice mappings:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to retrieve invoice mappings: ' + error.message
        });
    }
});

// ==================== END STRIPE-HALOPSA INVOICE MAPPING ====================

// Optimized version for batch processing
function calculateMatchScoreOptimized(stripeCustomer, haloClient, stripeNameLower, stripeWords) {
    let score = 0;
    
    // Email match
    if (stripeCustomer.email && haloClient.email) {
        try {
            const email1 = stripeCustomer.email.toLowerCase().trim();
            const email2 = haloClient.email.toLowerCase().trim();
            if (email1 === email2) {
                score += 0.8;
            }
        } catch (e) {
            // Email comparison failed, continue with name matching
        }
    }
    
    // Name matching using pre-calculated values
    if (stripeNameLower && haloClient.nameLower) {
        try {
            // Exact match
            if (stripeNameLower === haloClient.nameLower) {
                score += 0.8;
            }
            // One name is a clear subset of the other
            else if (isClearSubset(stripeNameLower, haloClient.nameLower)) {
                score += 0.7;
            }
            // Contains match
            else if (stripeNameLower.includes(haloClient.nameLower) || haloClient.nameLower.includes(stripeNameLower)) {
                score += 0.6;
            }
            // Word overlap using pre-calculated sets
            else if (stripeWords && haloClient.words) {
                const intersection = new Set([...stripeWords].filter(x => haloClient.words.has(x)));
                const union = new Set([...stripeWords, ...haloClient.words]);
                
                if (union.size > 0) {
                    const overlap = intersection.size / union.size;
                    if (overlap > 0.8) score += 0.6;
                    else if (overlap > 0.6) score += 0.4;
                    else if (overlap > 0.4) score += 0.3;
                }
            }
        } catch (e) {
            // Name matching failed, continue without adding score
            console.warn('Name matching error:', e.message);
        }
    }
    
    return Math.min(score, 1.0);
}

// Original function (keep for backward compatibility)
function calculateMatchScore(stripeCustomer, haloClient) {
    let score = 0;
    
    // Email match (most reliable) - requires both emails to be present
    if (stripeCustomer.email && haloClient.email) {
        const email1 = stripeCustomer.email.toLowerCase().trim();
        const email2 = haloClient.email.toLowerCase().trim();
        if (email1 === email2) {
            score += 0.8;
        }
    }
    
    // Improved name similarity matching with better scoring
    if (stripeCustomer.name && haloClient.name) {
        const name1 = stripeCustomer.name.toLowerCase().trim();
        const name2 = haloClient.name.toLowerCase().trim();
        
        // Human name matching - check if both appear to be personal names
        const isHumanName1 = isLikelyHumanName(name1);
        const isHumanName2 = isLikelyHumanName(name2);
        
        if (isHumanName1 && isHumanName2) {
            // Special handling for human names
            const humanScore = calculateHumanNameMatchScore(name1, name2);
            score += humanScore;
        } else {
            // Business/organization name matching
            // Exact match
            if (name1 === name2) {
                score += 0.8;
            }
            // One name is a clear subset of the other (e.g., "3 Bridges Pediatric" vs "3 Bridges Pediatric Dentistry")
            else if (isClearSubset(name1, name2)) {
                score += 0.7;
            }
            // Contains match with high confidence
            else if (name1.includes(name2) || name2.includes(name1)) {
                score += 0.6;
            }
            // Strong word overlap (most words match)
            else if (calculateWordOverlap(name1, name2) > 0.8) {
                score += 0.6;
            }
            // Partial match using common business name variations
            else if (hasStrongCommonWords(name1, name2)) {
                score += 0.5;
            }
            // Moderate word overlap
            else if (calculateWordOverlap(name1, name2) > 0.6) {
                score += 0.4;
            }
            // Weak word overlap but still meaningful
            else if (calculateWordOverlap(name1, name2) > 0.4) {
                score += 0.3;
            }
        }
    }
    
    return Math.min(score, 1.0);
}

// Check if a name appears to be a human name (personal name)
function isLikelyHumanName(name) {
    // Names with 2-4 words that don't contain obvious business terms
    const businessTerms = ['dental', 'dentistry', 'clinic', 'center', 'group', 'associates', 
                          'pediatric', 'orthodontics', 'care', 'practice', 'family', 'surgery', 
                          'smile', 'hospital', 'medical', 'doctor', 'dr', 'md', 'dds', 'llc',
                          'inc', 'corp', 'company', 'enterprises', 'department', 'center'];
    
    const words = name.split(/\s+/);
    
    // Too many words for a personal name
    if (words.length > 4) return false;
    
    // Contains business-related terms
    if (businessTerms.some(term => name.includes(term))) return false;
    
    // Most personal names have 2-3 words (First Last or First Middle Last)
    return words.length >= 2 && words.length <= 4;
}

// Calculate score for human name matching
function calculateHumanNameMatchScore(name1, name2) {
    let score = 0;
    
    const names1 = extractNameParts(name1);
    const names2 = extractNameParts(name2);
    
    // Exact match
    if (name1 === name2) {
        score += 0.8;
    }
    // Same name components in different order (e.g., "John Smith" vs "Smith, John")
    else if (hasSameNameComponents(names1, names2)) {
        score += 0.7;
    }
    // Last name match with first name similarity
    else if (names1.lastName && names2.lastName && names1.lastName === names2.lastName) {
        score += 0.6;
        if (names1.firstName && names2.firstName && 
            (names1.firstName.includes(names2.firstName) || names2.firstName.includes(names1.firstName))) {
            score += 0.2;
        }
    }
    // First name match
    else if (names1.firstName && names2.firstName && names1.firstName === names2.firstName) {
        score += 0.5;
    }
    // Partial name component matching
    else if (hasPartialNameMatch(names1, names2)) {
        score += 0.4;
    }
    
    return Math.min(score, 1.0);
}

// Extract name parts from a string
function extractNameParts(name) {
    const parts = {
        firstName: null,
        lastName: null,
        middleName: null,
        suffix: null
    };
    
    // Remove common honorifics and suffixes
    const cleanedName = name.replace(/\b(dr\.?|doctor|mr\.?|mrs\.?|ms\.?|prof\.?|professor)\b/gi, '').trim();
    const words = cleanedName.split(/\s+|,/).filter(word => word.length > 0);
    
    // Handle "Last, First" format
    if (name.includes(',')) {
        if (words.length >= 2) {
            parts.lastName = words[0];
            parts.firstName = words[1];
            // Check for middle name or suffix in remaining words
            if (words.length >= 3) {
                const remaining = words.slice(2).join(' ');
                if (isLikelySuffix(remaining)) {
                    parts.suffix = remaining;
                } else {
                    parts.middleName = remaining;
                }
            }
        }
    } else {
        // Handle "First Last" format
        if (words.length >= 1) parts.firstName = words[0];
        if (words.length >= 2) {
            // Check if last word is a suffix
            const lastWord = words[words.length - 1];
            if (isLikelySuffix(lastWord)) {
                parts.suffix = lastWord;
                parts.lastName = words.length >= 3 ? words[words.length - 2] : null;
            } else {
                parts.lastName = lastWord;
            }
        }
        if (words.length >= 3) {
            // Middle name is everything between first and last
            parts.middleName = words.slice(1, words.length - (parts.suffix ? 2 : 1)).join(' ');
        }
    }
    
    return parts;
}

// Check if a word is likely a name suffix
function isLikelySuffix(word) {
    const suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'esq', 'phd', 'md', 'dds'];
    return suffixes.includes(word.toLowerCase().replace('.', ''));
}

// Check if two names have the same components regardless of order
function hasSameNameComponents(names1, names2) {
    const components1 = [names1.firstName, names1.lastName].filter(Boolean);
    const components2 = [names2.firstName, names2.lastName].filter(Boolean);
    
    if (components1.length === 0 || components2.length === 0) return false;
    
    // Check if both names contain the same set of name components
    const set1 = new Set(components1);
    const set2 = new Set(components2);
    
    if (set1.size !== set2.size) return false;
    
    for (const component of set1) {
        if (!set2.has(component)) return false;
    }
    
    return true;
}

// Check for partial name matching (one name contains components of the other)
function hasPartialNameMatch(names1, names2) {
    const allComponents1 = [names1.firstName, names1.lastName, names1.middleName].filter(Boolean);
    const allComponents2 = [names2.firstName, names2.lastName, names2.middleName].filter(Boolean);
    
    // Check if any component from name1 appears in name2 or vice versa
    for (const comp1 of allComponents1) {
        for (const comp2 of allComponents2) {
            if (comp1.includes(comp2) || comp2.includes(comp1)) {
                return true;
            }
        }
    }
    
    return false;
}

// Check if one name is a clear subset of the other (e.g., short form vs full form)
function isClearSubset(name1, name2) {
    const shorter = name1.length < name2.length ? name1 : name2;
    const longer = name1.length < name2.length ? name2 : name1;
    
    // The shorter name should be at least 70% of the longer name and be contained within it
    const lengthRatio = shorter.length / longer.length;
    return lengthRatio > 0.7 && longer.includes(shorter);
}

// Calculate word overlap between two names
function calculateWordOverlap(name1, name2) {
    const words1 = new Set(name1.split(/[\s,.&]+/).filter(word => word.length > 2));
    const words2 = new Set(name2.split(/[\s,.&]+/).filter(word => word.length > 2));
    
    if (words1.size === 0 || words2.size === 0) return 0;
    
    const intersection = new Set([...words1].filter(x => words2.has(x)));
    const union = new Set([...words1, ...words2]);
    
    return intersection.size / union.size;
}

// More sophisticated common word matching
function hasStrongCommonWords(name1, name2) {
    const businessWords = ['dental', 'dentistry', 'clinic', 'center', 'group', 'associates', 'pediatric', 'orthodontics', 'care', 'practice', 'family', 'surgery', 'smile'];
    
    const words1 = name1.split(/[\s,.&]+/).filter(word => word.length > 2);
    const words2 = name2.split(/[\s,.&]+/).filter(word => word.length > 2);
    
    // Count matching unique words (excluding very common business words)
    const uniqueWords1 = words1.filter(word => !businessWords.includes(word));
    const uniqueWords2 = words2.filter(word => !businessWords.includes(word));
    
    // Check for strong matching (at least 2 unique words match)
    let matchCount = 0;
    for (const word1 of uniqueWords1) {
        for (const word2 of uniqueWords2) {
            if (word1.includes(word2) || word2.includes(word1)) {
                matchCount++;
                if (matchCount >= 2) return true;
            }
        }
    }
    
    return false;
}

// Get comprehensive customer view with HaloPSA as source of truth
app.get('/api/customers/view/:halopsa_client_id', async (req, res) => {
    try {
        const { halopsa_client_id } = req.params;
        
        if (!halopsa_client_id) {
            return res.status(400).json({ error: 'halopsa_client_id is required' });
        }
        
        // Get HaloPSA client details
        const haloClient = await db.get(`
            SELECT id, halopsa_id, name, email, phone, address, 
                   created_at, last_sync, raw_data
            FROM halopsa_clients 
            WHERE halopsa_id = ?
        `, [halopsa_client_id]);
        
        if (!haloClient) {
            return res.status(404).json({ error: 'HaloPSA client not found' });
        }
        
        // Get mapped Stripe customer
        const stripeMapping = await db.get(`
            SELECT cm.*, sc.email as stripe_email, sc.name as stripe_name
            FROM customer_mappings cm
            LEFT JOIN stripe_customers sc ON cm.stripe_customer_id = sc.stripe_id
            WHERE cm.halopsa_client_id = ? AND cm.mapping_confirmed = 1
        `, [halopsa_client_id]);
        
        // Get related Stripe transactions/invoices
        const stripeTransactions = await db.all(`
            SELECT * FROM stripe_invoices 
            WHERE customer_id = ? 
            ORDER BY created DESC 
            LIMIT 50
        `, [stripeMapping?.stripe_customer_id || '']);
        
        // Get related HaloPSA transactions (simulated - you'd extend this with actual HaloPSA data)
        const haloTransactions = await db.all(`
            SELECT * FROM halopsa_transactions 
            WHERE client_id = ? 
            ORDER BY date DESC 
            LIMIT 50
        `, [halopsa_client_id]);
        
        // Get purchase orders (simulated)
        const purchaseOrders = await db.all(`
            SELECT * FROM purchase_orders 
            WHERE client_id = ? 
            ORDER BY created_at DESC 
            LIMIT 20
        `, [halopsa_client_id]);
        
        // Get items/services (simulated)
        const clientItems = await db.all(`
            SELECT * FROM client_items 
            WHERE client_id = ? 
            ORDER BY last_used DESC 
            LIMIT 30
        `, [halopsa_client_id]);
        
        const customerView = {
            halo_client: haloClient,
            stripe_mapping: stripeMapping || null,
            summary: {
                total_invoices: stripeTransactions.length,
                total_transactions: haloTransactions.length,
                total_purchase_orders: purchaseOrders.length,
                total_items: clientItems.length
            },
            transactions: {
                stripe: stripeTransactions,
                halopsa: haloTransactions
            },
            purchase_orders: purchaseOrders,
            items: clientItems
        };
        
        res.json({ success: true, customer: customerView });
        
    } catch (error) {
        console.error('Error fetching customer view:', error);
        res.status(500).json({ error: 'Failed to fetch customer data' });
    }
});

// Get all customers with summary data for the customer list view
app.get('/api/customers/overview', async (req, res) => {
    try {
        const { page = 1, pageSize = 50, search = '' } = req.query;
        const offset = (parseInt(page) - 1) * parseInt(pageSize);
        
        // Base query for HaloPSA clients
        let baseQuery = `
            SELECT hc.*, 
                   cm.stripe_customer_id,
                   cm.stripe_customer_email,
                   cm.stripe_customer_name,
                   cm.mapping_confirmed,
                   cm.created_at as mapping_created
            FROM halopsa_clients hc
            LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id AND cm.mapping_confirmed = 1
        `;
        
        let whereClause = '';
        let params = [];
        
        if (search) {
            whereClause = ` WHERE hc.name LIKE ? OR hc.email LIKE ?`;
            params = [`%${search}%`, `%${search}%`];
        }
        
        const customers = await db.all(
            `${baseQuery}${whereClause} ORDER BY hc.name LIMIT ? OFFSET ?`,
            [...params, parseInt(pageSize), offset]
        );
        
        // Get total count for pagination
        const totalCount = await db.get(
            `SELECT COUNT(*) as total FROM halopsa_clients hc${whereClause}`,
            params
        );
        
        // Add summary data for each customer
        for (const customer of customers) {
            if (customer.stripe_customer_id) {
                const invoiceCount = await db.get(
                    'SELECT COUNT(*) as count FROM stripe_invoices WHERE customer_id = ?',
                    [customer.stripe_customer_id]
                );
                customer.stripe_invoice_count = invoiceCount.count;
                
                const lastInvoice = await db.get(
                    'SELECT created, amount FROM stripe_invoices WHERE customer_id = ? ORDER BY created DESC LIMIT 1',
                    [customer.stripe_customer_id]
                );
                customer.last_stripe_invoice = lastInvoice;
            }
            
            // Add HaloPSA transaction count (simulated)
            const transactionCount = await db.get(
                'SELECT COUNT(*) as count FROM halopsa_transactions WHERE client_id = ?',
                [customer.halopsa_id]
            );
            customer.halopsa_transaction_count = transactionCount.count;
        }
        
        res.json({
            success: true,
            customers: customers,
            pagination: {
                page: parseInt(page),
                pageSize: parseInt(pageSize),
                total: totalCount.total,
                totalPages: Math.ceil(totalCount.total / parseInt(pageSize))
            }
        });
        
    } catch (error) {
        console.error('Error fetching customer overview:', error);
        res.status(500).json({ error: 'Failed to fetch customer overview' });
    }
});

// Customer detail tab endpoints
app.get('/api/customers/overview/:client_id', async (req, res) => {
    try {
        const { client_id } = req.params;
        
        // Sample data for overview tab
        const overviewData = {
            recent_activity: [
                { date: new Date(), type: 'invoice', description: 'Invoice #INV-001 created' },
                { date: new Date(Date.now() - 86400000), type: 'payment', description: 'Payment received for Invoice #INV-001' },
                { date: new Date(Date.now() - 172800000), type: 'order', description: 'Purchase order #PO-123 submitted' }
            ],
            financial_summary: {
                total_invoiced: 1500.00,
                total_paid: 1200.00,
                outstanding: 300.00,
                average_invoice: 500.00
            },
            service_history: [
                { date: new Date(), type: 'support', description: 'Technical support ticket resolved' },
                { date: new Date(Date.now() - 259200000), type: 'maintenance', description: 'Scheduled maintenance completed' }
            ]
        };
        
        res.json(overviewData);
    } catch (error) {
        console.error('Error fetching customer overview:', error);
        res.status(500).json({ error: 'Failed to fetch customer overview data' });
    }
});

app.get('/api/halopsa/clients/:client_id/transactions', async (req, res) => {
    try {
        const { client_id } = req.params;
        
        // Get HaloPSA transactions for client
        const transactions = await db.all(`
            SELECT * FROM halopsa_transactions 
            WHERE client_id = ? 
            ORDER BY date DESC 
            LIMIT 50
        `, [client_id]);
        
        res.json({ transactions: transactions });
    } catch (error) {
        console.error('Error fetching HaloPSA transactions:', error);
        res.status(500).json({ error: 'Failed to fetch HaloPSA transactions' });
    }
});

app.get('/api/halopsa/clients/:client_id/purchase-orders', async (req, res) => {
    try {
        const { client_id } = req.params;
        
        // Get purchase orders for client
        const purchaseOrders = await db.all(`
            SELECT * FROM purchase_orders 
            WHERE client_id = ? 
            ORDER BY created_at DESC 
            LIMIT 20
        `, [client_id]);
        
        res.json({ purchase_orders: purchaseOrders });
    } catch (error) {
        console.error('Error fetching purchase orders:', error);
        res.status(500).json({ error: 'Failed to fetch purchase orders' });
    }
});

app.get('/api/halopsa/clients/:client_id/service-items', async (req, res) => {
    try {
        const { client_id } = req.params;
        
        // Get service items for client
        const serviceItems = await db.all(`
            SELECT * FROM client_items 
            WHERE client_id = ? 
            ORDER BY last_used DESC 
            LIMIT 30
        `, [client_id]);
        
        res.json({ service_items: serviceItems });
    } catch (error) {
        console.error('Error fetching service items:', error);
        res.status(500).json({ error: 'Failed to fetch service items' });
    }
});

// System status endpoint
app.get('/api/status', async (req, res) => {
    try {
        const status = {
            qbwc: { status: 'unknown', message: '' },
            stripe: { status: 'unknown', message: '' },
            halopsa: { status: 'unknown', message: '' },
            database: { status: 'unknown', message: '' }
        };
        
        // Check database
        try {
            await db.all('SELECT 1');
            status.database = { status: 'healthy', message: 'Database connection OK' };
        } catch (error) {
            status.database = { status: 'error', message: 'Database connection failed' };
        }
        
        // Check Stripe
        try {
            const stripeAPI = new StripeAPI(db);
            // Force re-initialization with latest config
            await stripeAPI.initialize();
            const stripeTest = await stripeAPI.testConnection();
            status.stripe = { 
                status: stripeTest.success ? 'healthy' : 'error', 
                message: stripeTest.message 
            };
        } catch (error) {
            status.stripe = { status: 'error', message: error.message };
        }
        
        // Check HaloPSA
        try {
            const halopsaAPI = await getHaloPSAAPI();
            const halopsaTest = await halopsaAPI.testConnection();
            console.log('HaloPSA status check result:', halopsaTest);
            status.halopsa = { 
                status: halopsaTest.success ? 'healthy' : 'error', 
                message: halopsaTest.message 
            };
        } catch (error) {
            console.error('HaloPSA status check error:', error);
            status.halopsa = { status: 'error', message: error.message };
        }
        
        res.json(status);
    } catch (error) {
        console.error('Error checking system status:', error);
        res.status(500).json({ error: 'Failed to check system status' });
    }
});

// Test QBXML generation endpoint
app.post('/api/qbwc/generate-test-xml', (req, res) => {
    try {
        const sampleData = [
            {
                vendor: 'Test Vendor Inc.',
                date: '2025-01-15',
                ref_num: 'PO-001',
                due_date: '2025-02-15',
                total_amount: 382.50,
                lines: [
                    {
                        item: 'TEST-ITEM-001',
                        description: 'Test Inventory Item',
                        quantity: 10,
                        unit_cost: 25.50,
                        line_amount: 255.00
                    },
                    {
                        item: 'TEST-ITEM-002',
                        description: 'Another Test Item',
                        quantity: 5,
                        unit_cost: 15.75,
                        line_amount: 78.75
                    }
                ]
            }
        ];
        
        const poQbxml = qbwcService.generatePurchaseOrderQBXML(sampleData);
        const vendorQbxml = qbwcService.generateVendorQBXML(sampleData);
        const inventoryQbxml = qbwcService.generateInventoryQBXML(sampleData);
        
        res.json({
            purchase_orders: poQbxml,
            vendors: vendorQbxml,
            inventory: inventoryQbxml,
            sample_data: sampleData
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// SSL Certificate endpoint that QuickBooks Web Connector expects
app.get('/.well-known/pki-validation/*', (req, res) => {
    res.status(200).send('Certificate validation endpoint - QBWC can verify SSL');
});

// Root certificate endpoint for QBWC - returns 200 OK with simple content
app.get('/ssl-certificate.crt', (req, res) => {
    res.set('Content-Type', 'text/plain');
    res.status(200).send(`QuickBooks Web Connector SSL Certificate Validation
Server: ${req.get('host')}
Status: SSL Certificate is valid and accessible
Timestamp: ${new Date().toISOString()}
Certificate validation successful for QuickBooks Web Connector.
`);
});

// Also handle the direct QBWC endpoint that QuickBooks tries to access
app.get('/qbwc', (req, res) => {
    res.set('Content-Type', 'text/xml');
    res.status(200).send(`<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <authenticateResponse xmlns="http://developer.intuit.com/">
      <authenticateResult>
        <string>Certificate validation successful</string>
      </authenticateResult>
    </authenticateResponse>
  </soap:Body>
</soap:Envelope>`);
});

// SSL/TLS certificate info endpoint for QBWC troubleshooting
app.get('/api/ssl/info', (req, res) => {
    const isHttps = req.protocol === 'https' || req.get('X-Forwarded-Proto') === 'https';
    const certificateInfo = {
        protocol: req.protocol,
        secure: req.secure,
        is_https: isHttps,
        host: req.get('host'),
        forwarded_host: req.get('X-Forwarded-Host'),
        forwarded_proto: req.get('X-Forwarded-Proto'),
        certificate_status: isHttps ? 'HTTPS active' : 'HTTP only - SSL required',
        quickbooks_requirements: {
            certificate_trust: 'QuickBooks requires trusted SSL certificates',
            localhost_issue: 'Localhost/self-signed certificates often cause verification errors',
            solution: 'Use a domain with valid SSL certificate or configure QBWC to accept self-signed'
        }
    };
    res.json(certificateInfo);
});

// Get QBWC configuration info including dynamic URLs
app.get('/api/qbwc/info', async (req, res) => {
    try {
        const baseUrl = `${req.protocol}://${req.get('host')}`;
        
        // Get configurable settings from database
        const qbwcConfig = await db.getConfig('qbwc');
        const username = (qbwcConfig && qbwcConfig.qbwc_username) ? qbwcConfig.qbwc_username.value : 'qbwc_user';
        const password = (qbwcConfig && qbwcConfig.qbwc_password) ? qbwcConfig.qbwc_password.value : 'password123';
        const appName = (qbwcConfig && qbwcConfig.qbwc_app_name) ? qbwcConfig.qbwc_app_name.value : 'CSV to QuickBooks IIF Sync';
        
        const status = {
            status: 'active',
            server: 'CSV to QuickBooks IIF Sync',
            version: '1.0.0',
            qbwc_endpoint: `${baseUrl}/qbwc`,
            qwc_config: `${baseUrl}/qbwc/config`,
            base_url: baseUrl,
            app_name: appName,
            test_credentials: {
                username: username,
                password: password
            },
            configurable: true,
            instructions: 'Make sure QuickBooks 2024 is OPEN before adding this app to QBWC'
        };
        res.json(status);
    } catch (error) {
        console.error('Error getting QBWC info:', error);
        // Fallback to default values
        const baseUrl = `${req.protocol}://${req.get('host')}`;
        res.json({
            status: 'active',
            server: 'CSV to QuickBooks IIF Sync',
            version: '1.0.0',
            qbwc_endpoint: `${baseUrl}/qbwc`,
            qwc_config: `${baseUrl}/qbwc/config`,
            base_url: baseUrl,
            app_name: 'CSV to QuickBooks IIF Sync',
            test_credentials: {
                username: 'qbwc_user',
                password: 'password123'
            },
            configurable: false,
            instructions: 'Make sure QuickBooks 2024 is OPEN before adding this app to QBWC'
        });
    }
});

app.get('/qbwc/status', async (req, res) => {
    try {
        // Get credentials from config
        const qbwcConfig = await db.getConfig('qbwc');
        const username = (qbwcConfig && qbwcConfig.qbwc_username) ? qbwcConfig.qbwc_username.value : 'qbwc_user';
        const password = (qbwcConfig && qbwcConfig.qbwc_password) ? qbwcConfig.qbwc_password.value : 'password123';

        res.json({
            status: 'active',
            server: 'CSV to QuickBooks IIF Sync',
            version: '1.0.0',
            qbwc_endpoint: `${req.protocol}://${req.get('host')}/qbwc`,
            qwc_config: `${req.protocol}://${req.get('host')}/qbwc/config`,
            test_credentials: {
                username: username,
                password: password
            },
            instructions: 'Make sure QuickBooks 2024 is OPEN before adding this app to QBWC'
        });
    } catch (error) {
        console.error('Error getting QBWC status:', error);
        res.json({
            status: 'active',
            server: 'CSV to QuickBooks IIF Sync',
            version: '1.0.0',
            qbwc_endpoint: `${req.protocol}://${req.get('host')}/qbwc`,
            qwc_config: `${req.protocol}://${req.get('host')}/qbwc/config`,
            test_credentials: {
                username: 'qbwc_user',
                password: 'password123'
            },
            instructions: 'Make sure QuickBooks 2024 is OPEN before adding this app to QBWC'
        });
    }
});

// ====================================================================
// TRANSACTION-INVOICE MAPPING ENDPOINTS
// ====================================================================

// Auto-map all unmapped Stripe transactions to HaloPSA invoices
app.post('/api/mappings/transactions/auto-map-all', async (req, res) => {
    try {
        const transactionMapper = new TransactionMapper(db);
        const result = await transactionMapper.autoMapAllTransactions();
        res.json(result);
    } catch (error) {
        console.error('Error auto-mapping transactions:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Auto-map a single Stripe transaction to HaloPSA invoices
app.post('/api/mappings/transactions/auto-map/:id', async (req, res) => {
    try {
        const transactionMapper = new TransactionMapper(db);
        const result = await transactionMapper.autoMapTransaction(parseInt(req.params.id));
        res.json(result);
    } catch (error) {
        console.error('Error auto-mapping transaction:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Manually map a Stripe transaction to HaloPSA invoice(s)
app.post('/api/mappings/transactions/manual', async (req, res) => {
    try {
        const { stripeTransactionId, invoiceIds } = req.body;

        if (!stripeTransactionId || !invoiceIds) {
            return res.status(400).json({
                success: false,
                message: 'stripeTransactionId and invoiceIds are required'
            });
        }

        const transactionMapper = new TransactionMapper(db);
        const result = await transactionMapper.manualMapTransaction(stripeTransactionId, invoiceIds);
        res.json(result);
    } catch (error) {
        console.error('Error manually mapping transaction:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get all unmapped Stripe transactions
app.get('/api/mappings/transactions/unmapped', async (req, res) => {
    try {
        const transactionMapper = new TransactionMapper(db);
        const unmapped = await transactionMapper.getUnmappedTransactions();
        res.json({ success: true, transactions: unmapped });
    } catch (error) {
        console.error('Error getting unmapped transactions:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// ====================================================================
// CLIENT MAPPING ENDPOINTS (Three-way: Stripe-HaloPSA-QuickBooks)
// ====================================================================

// Auto-map Stripe customers to HaloPSA clients
app.post('/api/mappings/clients/stripe-halopsa', async (req, res) => {
    try {
        const { threshold } = req.body;
        const clientMapper = new ClientMapper(db);
        const result = await clientMapper.autoMapStripeToHaloPSA(threshold);
        res.json(result);
    } catch (error) {
        console.error('Error auto-mapping Stripe to HaloPSA:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Auto-map HaloPSA clients to QuickBooks customers
app.post('/api/mappings/clients/halopsa-quickbooks', async (req, res) => {
    try {
        const { threshold } = req.body;
        const clientMapper = new ClientMapper(db);
        const result = await clientMapper.autoMapHaloPSAToQuickBooks(threshold);
        res.json(result);
    } catch (error) {
        console.error('Error auto-mapping HaloPSA to QuickBooks:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Manually map a customer across all three systems
app.post('/api/mappings/clients/manual', async (req, res) => {
    try {
        const { stripe_id, halopsa_id, qb_id } = req.body;

        if (!stripe_id && !halopsa_id && !qb_id) {
            return res.status(400).json({
                success: false,
                message: 'At least one ID (stripe_id, halopsa_id, or qb_id) is required'
            });
        }

        const clientMapper = new ClientMapper(db);
        const result = await clientMapper.manualMapCustomer({ stripe_id, halopsa_id, qb_id });
        res.json(result);
    } catch (error) {
        console.error('Error manually mapping customer:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get all customer mappings
app.get('/api/mappings/clients/all', async (req, res) => {
    try {
        const clientMapper = new ClientMapper(db);
        const mappings = await clientMapper.getAllMappings();
        res.json({ success: true, mappings: mappings });
    } catch (error) {
        console.error('Error getting all mappings:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get unmapped customers by source
app.get('/api/mappings/clients/unmapped/:source', async (req, res) => {
    try {
        const { source } = req.params;

        if (!['stripe', 'halopsa', 'quickbooks'].includes(source)) {
            return res.status(400).json({
                success: false,
                message: 'Source must be one of: stripe, halopsa, quickbooks'
            });
        }

        const clientMapper = new ClientMapper(db);
        const unmapped = await clientMapper.getUnmappedCustomers(source);
        res.json({ success: true, source: source, unmapped: unmapped });
    } catch (error) {
        console.error('Error getting unmapped customers:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// ====================================================================
// DEPOSIT & TRANSACTION TYPE ENDPOINTS
// ====================================================================

// Get deposit summary
app.get('/api/deposits/summary', async (req, res) => {
    try {
        const transactionMapper = new TransactionMapper(db);
        const summary = await transactionMapper.getDepositSummary();
        res.json(summary);
    } catch (error) {
        console.error('Error getting deposit summary:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get transactions by type (deposit, payment, final_payment)
app.get('/api/transactions/by-type/:type', async (req, res) => {
    try {
        const { type } = req.params;

        if (!['deposit', 'payment', 'final_payment'].includes(type)) {
            return res.status(400).json({
                success: false,
                message: 'Type must be one of: deposit, payment, final_payment'
            });
        }

        const transactionMapper = new TransactionMapper(db);
        const transactions = await transactionMapper.getTransactionsByType(type);
        res.json({ success: true, type: type, count: transactions.length, transactions: transactions });
    } catch (error) {
        console.error(`Error getting ${req.params.type} transactions:`, error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Reclassify all transactions (useful after updating detection logic)
app.post('/api/transactions/reclassify-all', async (req, res) => {
    try {
        const transactionMapper = new TransactionMapper(db);

        // Get all transactions
        const allTransactions = await db.query('SELECT id, description FROM stripe_transactions');

        let reclassifiedCount = 0;
        const reclassificationResults = {
            deposit: 0,
            payment: 0,
            final_payment: 0
        };

        for (const transaction of allTransactions) {
            const type = transactionMapper.detectTransactionType(transaction.description);

            // Update the transaction type
            await db.run(
                'UPDATE stripe_transactions SET transaction_type = ? WHERE id = ?',
                [type, transaction.id]
            );

            reclassifiedCount++;
            reclassificationResults[type]++;
        }

        res.json({
            success: true,
            message: `Reclassified ${reclassifiedCount} transactions`,
            total: reclassifiedCount,
            breakdown: reclassificationResults
        });
    } catch (error) {
        console.error('Error reclassifying transactions:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// ====================================================================
// TRANSACTION-INVOICE MAPPING ENDPOINTS
// ====================================================================

// GET /api/transaction-mappings/stripe - Get unmapped Stripe transactions
app.get('/api/transaction-mappings/stripe', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        const offset = parseInt(req.query.offset) || 0;

        const transactions = db.query(`
            SELECT
                st.*,
                sc.name as customer_name,
                sc.email as customer_email,
                tim.id as mapping_id,
                hi.invoice_number,
                CASE WHEN tim.id IS NOT NULL THEN 1 ELSE 0 END as mapped
            FROM stripe_transactions st
            LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
            LEFT JOIN transaction_invoice_mappings tim ON st.id = tim.stripe_transaction_id
            LEFT JOIN halopsa_invoices hi ON tim.halopsa_invoice_id = hi.id
            WHERE tim.id IS NULL
            ORDER BY st.created DESC
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        res.json({
            success: true,
            transactions: transactions || [],
            count: transactions?.length || 0
        });
    } catch (error) {
        console.error('Error fetching Stripe transactions:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// GET /api/transaction-mappings/invoices - Get HaloPSA invoices with mapping info
app.get('/api/transaction-mappings/invoices', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        const offset = parseInt(req.query.offset) || 0;

        const invoices = db.query(`
            SELECT
                hi.*,
                COUNT(tim.id) as mapped_transaction_count
            FROM halopsa_invoices hi
            LEFT JOIN transaction_invoice_mappings tim ON hi.id = tim.halopsa_invoice_id
            GROUP BY hi.id
            ORDER BY hi.invoice_date DESC
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        res.json({
            success: true,
            invoices: invoices || [],
            count: invoices?.length || 0
        });
    } catch (error) {
        console.error('Error fetching HaloPSA invoices:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// GET /api/transaction-mappings - Get mapped transaction-invoice pairs
app.get('/api/transaction-mappings', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 50;
        const offset = parseInt(req.query.offset) || 0;

        const mappings = db.query(`
            SELECT
                tim.*,
                st.stripe_id,
                st.amount as transaction_amount,
                st.currency as transaction_currency,
                st.description as transaction_description,
                st.created as transaction_date,
                hi.invoice_number,
                hi.client_name,
                hi.invoice_date
            FROM transaction_invoice_mappings tim
            JOIN stripe_transactions st ON tim.stripe_transaction_id = st.id
            JOIN halopsa_invoices hi ON tim.halopsa_invoice_id = hi.id
            ORDER BY tim.created_at DESC
            LIMIT ? OFFSET ?
        `, [limit, offset]);

        res.json({
            success: true,
            mappings: mappings || [],
            count: mappings?.length || 0
        });
    } catch (error) {
        console.error('Error fetching transaction mappings:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// POST /api/transaction-mappings - Create a transaction-invoice mapping
app.post('/api/transaction-mappings', async (req, res) => {
    try {
        const { stripe_transaction_id, halopsa_invoice_id, invoice_amount } = req.body;

        if (!stripe_transaction_id || !halopsa_invoice_id) {
            return res.status(400).json({
                success: false,
                message: 'stripe_transaction_id and halopsa_invoice_id are required'
            });
        }

        // Check if mapping already exists
        const existing = db.get(`
            SELECT * FROM transaction_invoice_mappings
            WHERE stripe_transaction_id = ? AND halopsa_invoice_id = ?
        `, [stripe_transaction_id, halopsa_invoice_id]);

        if (existing) {
            return res.status(409).json({
                success: false,
                message: 'Mapping already exists'
            });
        }

        // Create the mapping
        const result = db.run(`
            INSERT INTO transaction_invoice_mappings
            (stripe_transaction_id, halopsa_invoice_id, invoice_amount, auto_mapped, mapping_confidence, created_at, updated_at)
            VALUES (?, ?, ?, 0, 1.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `, [stripe_transaction_id, halopsa_invoice_id, invoice_amount]);

        res.json({
            success: true,
            message: 'Mapping created successfully',
            mapping_id: result.lastInsertRowid
        });
    } catch (error) {
        console.error('Error creating transaction mapping:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// DELETE /api/transaction-mappings/:id - Delete a transaction-invoice mapping
app.delete('/api/transaction-mappings/:id', async (req, res) => {
    try {
        const { id } = req.params;

        const result = db.run(`
            DELETE FROM transaction_invoice_mappings
            WHERE id = ?
        `, [id]);

        if (result.changes === 0) {
            return res.status(404).json({
                success: false,
                message: 'Mapping not found'
            });
        }

        res.json({
            success: true,
            message: 'Mapping deleted successfully'
        });
    } catch (error) {
        console.error('Error deleting transaction mapping:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// POST /api/transaction-mappings/auto-match - Auto-match transactions to invoices
app.post('/api/transaction-mappings/auto-match', async (req, res) => {
    try {
        // Get all unmapped Stripe transactions
        const transactions = db.query(`
            SELECT st.*, sc.name as customer_name, sc.email as customer_email
            FROM stripe_transactions st
            LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
            LEFT JOIN transaction_invoice_mappings tim ON st.id = tim.stripe_transaction_id
            WHERE tim.id IS NULL
        `);

        // Get all HaloPSA invoices
        const invoices = db.query(`
            SELECT * FROM halopsa_invoices
        `);

        const matches = [];

        // Auto-matching algorithm
        for (const tx of transactions) {
            const description = (tx.description || '').toLowerCase();

            // Extract invoice number patterns from description
            const patterns = [
                /invoice\s*#?\s*([A-Z0-9-]+)/i,
                /inv\s*#?\s*([A-Z0-9-]+)/i,
                /#\s*([A-Z0-9-]+)/i,
                /\b([A-Z]{2,5}-\d{3,})\b/i
            ];

            let extractedInvoiceNum = null;
            for (const pattern of patterns) {
                const match = description.match(pattern);
                if (match && match[1]) {
                    extractedInvoiceNum = match[1].toUpperCase();
                    break;
                }
            }

            if (!extractedInvoiceNum) continue;

            // Find matching invoice
            const matchingInvoice = invoices.find(inv => {
                const invNumber = (inv.invoice_number || '').toUpperCase();
                return invNumber === extractedInvoiceNum || invNumber.includes(extractedInvoiceNum);
            });

            if (matchingInvoice) {
                // Calculate confidence based on amount match
                const txAmount = tx.amount / 100; // Convert from cents
                const invAmount = matchingInvoice.total_amount;
                const amountDiff = Math.abs(txAmount - invAmount);
                const amountConfidence = amountDiff < 0.01 ? 1.0 : (amountDiff < 10 ? 0.9 : 0.7);

                // Create the mapping
                try {
                    db.run(`
                        INSERT INTO transaction_invoice_mappings
                        (stripe_transaction_id, halopsa_invoice_id, invoice_amount, auto_mapped, mapping_confidence, created_at, updated_at)
                        VALUES (?, ?, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    `, [tx.id, matchingInvoice.id, matchingInvoice.total_amount, amountConfidence]);

                    matches.push({
                        transaction_id: tx.stripe_id,
                        invoice_number: matchingInvoice.invoice_number,
                        confidence: amountConfidence
                    });
                } catch (err) {
                    console.error('Error creating auto-match mapping:', err);
                }
            }
        }

        res.json({
            success: true,
            message: `Auto-matched ${matches.length} transactions`,
            matches: matches,
            count: matches.length
        });
    } catch (error) {
        console.error('Error auto-matching transactions:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// ====================================================================
// AI-POWERED TRANSACTION MAPPING ENDPOINTS
// ====================================================================

// POST /api/ai/suggest-transaction-mappings - Generate AI-powered transaction-invoice mapping suggestions
app.post('/api/ai/suggest-transaction-mappings', async (req, res) => {
    try {
        console.log('Generating AI-powered transaction mapping suggestions...');
        const result = await aiService.suggestTransactionMappings();

        res.json({
            success: result.success,
            count: result.count,
            suggestions: result.suggestions,
            message: result.message || `Generated ${result.count} AI mapping suggestions`
        });
    } catch (error) {
        console.error('Error generating AI transaction mapping suggestions:', error);
        res.status(500).json({
            success: false,
            message: error.message,
            count: 0,
            suggestions: []
        });
    }
});

// GET /api/ai/transaction-suggestions - Get pending AI transaction mapping suggestions
app.get('/api/ai/transaction-suggestions', async (req, res) => {
    try {
        const result = await aiService.getPendingTransactionSuggestions();

        res.json({
            success: result.success,
            suggestions: result.suggestions
        });
    } catch (error) {
        console.error('Error fetching pending transaction suggestions:', error);
        res.status(500).json({
            success: false,
            message: error.message,
            suggestions: []
        });
    }
});

// POST /api/ai/approve-transaction-mapping/:id - Approve and create mapping from AI suggestion
app.post('/api/ai/approve-transaction-mapping/:id', async (req, res) => {
    try {
        const suggestionId = parseInt(req.params.id);

        if (isNaN(suggestionId)) {
            return res.status(400).json({
                success: false,
                message: 'Invalid suggestion ID'
            });
        }

        const result = await aiService.approveTransactionMapping(suggestionId);

        res.json({
            success: result.success,
            message: result.message
        });
    } catch (error) {
        console.error('Error approving transaction mapping:', error);
        res.status(500).json({
            success: false,
            message: error.message
        });
    }
});

// POST /api/ai/reject-transaction-mapping/:id - Reject AI suggestion
app.post('/api/ai/reject-transaction-mapping/:id', async (req, res) => {
    try {
        const suggestionId = parseInt(req.params.id);

        if (isNaN(suggestionId)) {
            return res.status(400).json({
                success: false,
                message: 'Invalid suggestion ID'
            });
        }

        const result = await aiService.rejectTransactionMapping(suggestionId);

        res.json({
            success: result.success,
            message: result.message
        });
    } catch (error) {
        console.error('Error rejecting transaction mapping:', error);
        res.status(500).json({
            success: false,
            message: error.message
        });
    }
});

// ====================================================================
// ACCOUNT MAPPING ENDPOINTS (QuickBooks Desktop Integration)
// ====================================================================

// Get all account mappings with full account details
app.get('/api/qbd/mappings', async (req, res) => {
    try {
        const mappings = await db.query(`
            SELECT
                am.id,
                am.mapping_type,
                am.qb_account_id,
                am.qb_account_name,
                am.description,
                am.is_active,
                am.created_at,
                am.updated_at,
                qa.account_name as qb_account_display_name,
                qa.account_type,
                qa.account_number,
                qa.fully_qualified_name
            FROM account_mappings am
            LEFT JOIN qb_accounts qa ON am.qb_account_id = qa.id
            WHERE am.is_active = 1
            ORDER BY am.mapping_type
        `);

        res.json({
            success: true,
            mappings: mappings,
            count: mappings.length
        });
    } catch (error) {
        console.error('Error getting account mappings:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get specific account mapping by type
app.get('/api/qbd/mappings/:type', async (req, res) => {
    try {
        const { type } = req.params;

        const mapping = await db.get(`
            SELECT
                am.id,
                am.mapping_type,
                am.qb_account_id,
                am.qb_account_name,
                am.description,
                am.is_active,
                am.created_at,
                am.updated_at,
                qa.account_name as qb_account_display_name,
                qa.account_type,
                qa.account_number,
                qa.fully_qualified_name,
                qa.is_active as qb_account_active
            FROM account_mappings am
            LEFT JOIN qb_accounts qa ON am.qb_account_id = qa.id
            WHERE am.mapping_type = ? AND am.is_active = 1
        `, [type]);

        if (!mapping) {
            return res.status(404).json({
                success: false,
                message: `Account mapping not found for type: ${type}`
            });
        }

        res.json({
            success: true,
            mapping: mapping
        });
    } catch (error) {
        console.error('Error getting account mapping:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Save or update account mapping
app.post('/api/qbd/mappings/:type', async (req, res) => {
    try {
        const { type } = req.params;
        const { qb_account_id } = req.body;

        // Validate that qb_account_id is provided
        if (!qb_account_id) {
            return res.status(400).json({
                success: false,
                message: 'qb_account_id is required'
            });
        }

        // Get the account details to store account name
        const account = await db.get(
            'SELECT id, account_name, account_type FROM qb_accounts WHERE id = ?',
            [qb_account_id]
        );

        if (!account) {
            return res.status(404).json({
                success: false,
                message: `QuickBooks account not found with id: ${qb_account_id}`
            });
        }

        // Check if mapping exists
        const existingMapping = await db.get(
            'SELECT id FROM account_mappings WHERE mapping_type = ?',
            [type]
        );

        if (existingMapping) {
            // Update existing mapping
            await db.run(
                'UPDATE account_mappings SET qb_account_id = ?, qb_account_name = ?, updated_at = CURRENT_TIMESTAMP WHERE mapping_type = ?',
                [qb_account_id, account.account_name, type]
            );
        } else {
            // Insert new mapping
            await db.run(
                'INSERT INTO account_mappings (mapping_type, qb_account_id, qb_account_name, description, is_active) VALUES (?, ?, ?, ?, ?)',
                [type, qb_account_id, account.account_name, `Account mapping for ${type}`, 1]
            );
        }

        // Fetch and return the updated mapping with full details
        const updatedMapping = await db.get(`
            SELECT
                am.id,
                am.mapping_type,
                am.qb_account_id,
                am.qb_account_name,
                am.description,
                am.is_active,
                am.created_at,
                am.updated_at,
                qa.account_name as qb_account_display_name,
                qa.account_type,
                qa.account_number,
                qa.fully_qualified_name
            FROM account_mappings am
            LEFT JOIN qb_accounts qa ON am.qb_account_id = qa.id
            WHERE am.mapping_type = ?
        `, [type]);

        res.json({
            success: true,
            message: 'Account mapping saved successfully',
            mapping: updatedMapping
        });
    } catch (error) {
        console.error('Error saving account mapping:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Remove account mapping (set qb_account_id to NULL)
app.delete('/api/qbd/mappings/:type', async (req, res) => {
    try {
        const { type } = req.params;

        // Check if mapping exists
        const existingMapping = await db.get(
            'SELECT id FROM account_mappings WHERE mapping_type = ?',
            [type]
        );

        if (!existingMapping) {
            return res.status(404).json({
                success: false,
                message: `Account mapping not found for type: ${type}`
            });
        }

        // Set qb_account_id and qb_account_name to NULL instead of deleting
        await db.run(
            'UPDATE account_mappings SET qb_account_id = NULL, qb_account_name = NULL, updated_at = CURRENT_TIMESTAMP WHERE mapping_type = ?',
            [type]
        );

        res.json({
            success: true,
            message: `Account mapping cleared for type: ${type}`
        });
    } catch (error) {
        console.error('Error removing account mapping:', error);
        res.status(500).json({ success: false, message: error.message });
    }
});

// AI-Powered Customer Mapping Endpoints
// ==============================================

// Note: AIService already imported at top of file (line 18)
// Initialize AI service with database

// Initialize AI service with API key from environment or config
(async () => {
    try {
        // Try to get API key from environment variable first
        let apiKey = process.env.ANTHROPIC_API_KEY;

        // If not in env, try to get from database config
        if (!apiKey) {
            const config = await db.get('SELECT value FROM ai_settings WHERE key = ?', ['anthropic_api_key']);
            apiKey = config?.value;
        }

        if (apiKey) {
            aiService.initialize(apiKey);
            console.log('✨ AI Service initialized successfully');
        } else {
            console.log('⚠️  AI Service not initialized - configure ANTHROPIC_API_KEY to enable AI features');
        }
    } catch (error) {
        console.error('❌ Failed to initialize AI service:', error.message);
    }
})();

/**
 * POST /api/ai/suggest-customer-mappings
 * Generate AI-powered customer mapping suggestions
 */
app.post('/api/ai/suggest-customer-mappings', async (req, res) => {
    try {
        if (!aiService.isReady()) {
            return res.status(503).json({
                success: false,
                message: 'AI service not configured. Set ANTHROPIC_API_KEY environment variable or configure in settings.'
            });
        }

        console.log('🤖 Starting AI customer mapping suggestion process...');

        // Fetch unmapped customers from all three systems
        const unmappedStripe = await db.all(`
            SELECT sc.*
            FROM stripe_customers sc
            LEFT JOIN customer_mappings cm ON sc.stripe_id = cm.stripe_customer_id
            WHERE cm.id IS NULL
            ORDER BY sc.created DESC
            LIMIT 50
        `);

        const unmappedHaloPSA = await db.all(`
            SELECT hc.*
            FROM halopsa_clients hc
            LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id
            WHERE cm.id IS NULL
            ORDER BY hc.name
            LIMIT 50
        `);

        const unmappedQB = await db.all(`
            SELECT qc.*
            FROM qb_customers qc
            LEFT JOIN customer_mappings cm ON qc.qb_list_id = cm.qb_customer_id
            WHERE cm.id IS NULL
            ORDER BY qc.qb_full_name
            LIMIT 50
        `);

        console.log(`   Found ${unmappedStripe.length} unmapped Stripe customers`);
        console.log(`   Found ${unmappedHaloPSA.length} unmapped HaloPSA clients`);
        console.log(`   Found ${unmappedQB.length} unmapped QB customers`);

        if (unmappedStripe.length === 0 && unmappedHaloPSA.length === 0 && unmappedQB.length === 0) {
            return res.json({
                success: true,
                message: 'No unmapped customers found - all customers are already mapped!',
                count: 0,
                suggestions: []
            });
        }

        // Call AI service to generate suggestions
        const aiSuggestions = await aiService.suggestCustomerMappings(
            unmappedStripe,
            unmappedHaloPSA,
            unmappedQB
        );

        // Store suggestions in database
        const insertStmt = db.db.prepare(`
            INSERT INTO ai_mapping_suggestions
            (suggestion_type, source_type, source_id, target_type, target_id, confidence, reasoning, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        `);

        let storedCount = 0;
        for (const suggestion of aiSuggestions) {
            try {
                // Resolve actual database IDs from system IDs
                let sourceDbId, targetDbId;

                // Get source DB ID
                if (suggestion.source_system === 'stripe') {
                    const source = await db.get('SELECT id FROM stripe_customers WHERE stripe_id = ?', [suggestion.source_id]);
                    sourceDbId = source?.id;
                } else if (suggestion.source_system === 'halopsa') {
                    const source = await db.get('SELECT id FROM halopsa_clients WHERE halopsa_id = ?', [suggestion.source_id]);
                    sourceDbId = source?.id;
                } else if (suggestion.source_system === 'quickbooks') {
                    const source = await db.get('SELECT id FROM qb_customers WHERE qb_list_id = ?', [suggestion.source_id]);
                    sourceDbId = source?.id;
                }

                // Get target DB ID
                if (suggestion.target_system === 'stripe') {
                    const target = await db.get('SELECT id FROM stripe_customers WHERE stripe_id = ?', [suggestion.target_id]);
                    targetDbId = target?.id;
                } else if (suggestion.target_system === 'halopsa') {
                    const target = await db.get('SELECT id FROM halopsa_clients WHERE halopsa_id = ?', [suggestion.target_id]);
                    targetDbId = target?.id;
                } else if (suggestion.target_system === 'quickbooks') {
                    const target = await db.get('SELECT id FROM qb_customers WHERE qb_list_id = ?', [suggestion.target_id]);
                    targetDbId = target?.id;
                }

                if (sourceDbId && targetDbId) {
                    insertStmt.run([
                        'customer_mapping',
                        suggestion.source_system,
                        sourceDbId,
                        suggestion.target_system,
                        targetDbId,
                        suggestion.confidence,
                        suggestion.reasoning
                    ]);
                    storedCount++;
                } else {
                    console.warn(`   ⚠️  Skipping suggestion: Could not resolve IDs for ${suggestion.source_system}:${suggestion.source_id} -> ${suggestion.target_system}:${suggestion.target_id}`);
                }
            } catch (error) {
                console.error('   ❌ Error storing suggestion:', error.message);
            }
        }

        console.log(`   ✅ Stored ${storedCount} AI mapping suggestions`);

        res.json({
            success: true,
            message: `Generated ${storedCount} AI-powered mapping suggestions`,
            count: storedCount,
            suggestions: aiSuggestions
        });
    } catch (error) {
        console.error('❌ Error generating AI suggestions:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'Failed to generate AI suggestions'
        });
    }
});

/**
 * GET /api/ai/customer-suggestions
 * Get pending AI customer mapping suggestions with full customer details
 */
app.get('/api/ai/customer-suggestions', async (req, res) => {
    try {
        const suggestions = await db.all(`
            SELECT
                ais.*,
                CASE
                    WHEN ais.source_type = 'stripe' THEN (SELECT json_object(
                        'stripe_id', stripe_id,
                        'name', name,
                        'email', email,
                        'phone', phone
                    ) FROM stripe_customers WHERE id = ais.source_id)
                    WHEN ais.source_type = 'halopsa' THEN (SELECT json_object(
                        'halopsa_id', halopsa_id,
                        'name', name,
                        'email', email,
                        'phone', phone
                    ) FROM halopsa_clients WHERE id = ais.source_id)
                    WHEN ais.source_type = 'quickbooks' THEN (SELECT json_object(
                        'qb_list_id', qb_list_id,
                        'qb_full_name', qb_full_name,
                        'email', email,
                        'phone', phone
                    ) FROM qb_customers WHERE id = ais.source_id)
                END as source_data,
                CASE
                    WHEN ais.target_type = 'stripe' THEN (SELECT json_object(
                        'stripe_id', stripe_id,
                        'name', name,
                        'email', email,
                        'phone', phone
                    ) FROM stripe_customers WHERE id = ais.target_id)
                    WHEN ais.target_type = 'halopsa' THEN (SELECT json_object(
                        'halopsa_id', halopsa_id,
                        'name', name,
                        'email', email,
                        'phone', phone
                    ) FROM halopsa_clients WHERE id = ais.target_id)
                    WHEN ais.target_type = 'quickbooks' THEN (SELECT json_object(
                        'qb_list_id', qb_list_id,
                        'qb_full_name', qb_full_name,
                        'email', email,
                        'phone', phone
                    ) FROM qb_customers WHERE id = ais.target_id)
                END as target_data
            FROM ai_mapping_suggestions ais
            WHERE ais.status = 'pending'
            AND ais.suggestion_type = 'customer_mapping'
            ORDER BY ais.confidence DESC, ais.created_at DESC
        `);

        // Parse JSON strings
        const enrichedSuggestions = suggestions.map(s => ({
            ...s,
            source_data: s.source_data ? JSON.parse(s.source_data) : null,
            target_data: s.target_data ? JSON.parse(s.target_data) : null
        }));

        res.json({
            success: true,
            count: enrichedSuggestions.length,
            suggestions: enrichedSuggestions
        });
    } catch (error) {
        console.error('❌ Error fetching AI suggestions:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to fetch AI suggestions'
        });
    }
});

/**
 * POST /api/ai/approve-customer-mapping/:id
 * Approve an AI suggestion and create actual customer mapping
 */
app.post('/api/ai/approve-customer-mapping/:id', async (req, res) => {
    try {
        const suggestionId = parseInt(req.params.id);

        // Get the suggestion
        const suggestion = await db.get(
            'SELECT * FROM ai_mapping_suggestions WHERE id = ? AND status = "pending"',
            [suggestionId]
        );

        if (!suggestion) {
            return res.status(404).json({
                success: false,
                message: 'Suggestion not found or already processed'
            });
        }

        // Get customer details to populate mapping
        let stripeCustomerId = null, stripeCustomerEmail = null, stripeCustomerName = null;
        let haloPSAClientId = null, haloPSAClientName = null;
        let qbCustomerId = null, qbCustomerName = null;

        // Determine which system is source and target
        if (suggestion.source_type === 'stripe') {
            const stripe = await db.get('SELECT stripe_id, email, name FROM stripe_customers WHERE id = ?', [suggestion.source_id]);
            if (stripe) {
                stripeCustomerId = stripe.stripe_id;
                stripeCustomerEmail = stripe.email;
                stripeCustomerName = stripe.name;
            }
        } else if (suggestion.target_type === 'stripe') {
            const stripe = await db.get('SELECT stripe_id, email, name FROM stripe_customers WHERE id = ?', [suggestion.target_id]);
            if (stripe) {
                stripeCustomerId = stripe.stripe_id;
                stripeCustomerEmail = stripe.email;
                stripeCustomerName = stripe.name;
            }
        }

        if (suggestion.source_type === 'halopsa') {
            const halo = await db.get('SELECT halopsa_id, name FROM halopsa_clients WHERE id = ?', [suggestion.source_id]);
            if (halo) {
                haloPSAClientId = halo.halopsa_id;
                haloPSAClientName = halo.name;
            }
        } else if (suggestion.target_type === 'halopsa') {
            const halo = await db.get('SELECT halopsa_id, name FROM halopsa_clients WHERE id = ?', [suggestion.target_id]);
            if (halo) {
                haloPSAClientId = halo.halopsa_id;
                haloPSAClientName = halo.name;
            }
        }

        if (suggestion.source_type === 'quickbooks') {
            const qb = await db.get('SELECT qb_list_id, qb_full_name FROM qb_customers WHERE id = ?', [suggestion.source_id]);
            if (qb) {
                qbCustomerId = qb.qb_list_id;
                qbCustomerName = qb.qb_full_name;
            }
        } else if (suggestion.target_type === 'quickbooks') {
            const qb = await db.get('SELECT qb_list_id, qb_full_name FROM qb_customers WHERE id = ?', [suggestion.target_id]);
            if (qb) {
                qbCustomerId = qb.qb_list_id;
                qbCustomerName = qb.qb_full_name;
            }
        }

        // Create customer mapping
        const result = db.run(`
            INSERT INTO customer_mappings
            (stripe_customer_id, stripe_customer_email, stripe_customer_name,
             halopsa_client_id, halopsa_client_name,
             qb_customer_id, qb_customer_name,
             auto_mapped, mapping_confirmed, mapping_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 'ai_suggestion')
        `, [
            stripeCustomerId, stripeCustomerEmail, stripeCustomerName,
            haloPSAClientId, haloPSAClientName,
            qbCustomerId, qbCustomerName
        ]);

        // Update suggestion status
        db.run(
            'UPDATE ai_mapping_suggestions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            ['approved', suggestionId]
        );

        console.log(`✅ Approved AI suggestion ${suggestionId} and created mapping ${result.lastInsertRowid}`);

        res.json({
            success: true,
            message: 'Mapping created successfully',
            mappingId: result.lastInsertRowid
        });
    } catch (error) {
        console.error('❌ Error approving AI suggestion:', error);
        res.status(500).json({
            success: false,
            message: error.message || 'Failed to approve suggestion'
        });
    }
});

/**
 * POST /api/ai/reject-customer-mapping/:id
 * Reject an AI suggestion
 */
app.post('/api/ai/reject-customer-mapping/:id', async (req, res) => {
    try {
        const suggestionId = parseInt(req.params.id);

        const result = db.run(
            'UPDATE ai_mapping_suggestions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = "pending"',
            ['rejected', suggestionId]
        );

        if (result.changes === 0) {
            return res.status(404).json({
                success: false,
                message: 'Suggestion not found or already processed'
            });
        }

        console.log(`🚫 Rejected AI suggestion ${suggestionId}`);

        res.json({
            success: true,
            message: 'Suggestion rejected'
        });
    } catch (error) {
        console.error('❌ Error rejecting AI suggestion:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to reject suggestion'
        });
    }
});

// =============================================================================
// REGISTER AI ENDPOINTS
// =============================================================================
const aiTriggers = registerAIEndpoints(app, aiService, db);

app.listen(port, () => {
    console.log(`\n🚀 CSV to QuickBooks IIF Server running on http://localhost:${port}`);
    console.log(`📊 API Documentation: http://localhost:${port}/`);
    console.log(`🔌 QBWC endpoint: http://localhost:${port}/qbwc`);
    console.log(`📁 QWC config: http://localhost:${port}/qbwc/config`);
    console.log(`📋 Status check: http://localhost:${port}/qbwc/status`);
    console.log(`\n👤 QuickBooks Web Connector Test Credentials:`);
    console.log(`   Username: qbwc_user`);
    console.log(`   Password: password123`);
    console.log(`\n🚨 IMPORTANT: QuickBooks 2024 must be OPEN before adding application to QBWC!`);
    console.log(`\n💡 To test with QuickBooks Web Connector:`);
    console.log(`   1. FIRST: Open QuickBooks Desktop 2024 with a company file loaded`);
    console.log(`   2. Download the QWC file: http://localhost:${port}/qbwc/config`);
    console.log(`   3. Open QuickBooks Web Connector`);
    console.log(`   4. Add the QWC file and use the test credentials`);
    console.log(`   5. Click "Update" to sync data`);
});

// Mapping API Endpoints
app.get('/api/halopsa/invoice/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const invoice = await db.get('SELECT * FROM halopsa_invoices WHERE id = ?', [id]);
        
        if (!invoice) {
            return res.status(404).json({ success: false, message: 'Invoice not found' });
        }
        
        res.json(invoice);
    } catch (error) {
        console.error('Error fetching invoice details:', error);
        res.status(500).json({ success: false, message: 'Error fetching invoice details' });
    }
});

app.get('/api/stripe/transactions', async (req, res) => {
    try {
        const { invoiceAmount, clientName } = req.query;
        
        let query = 'SELECT * FROM stripe_transactions WHERE 1=1';
        let params = [];
        
        // Add amount filter if provided (within 10% tolerance)
        if (invoiceAmount) {
            const amount = parseFloat(invoiceAmount);
            const minAmount = amount * 0.9; // 10% below
            const maxAmount = amount * 1.1; // 10% above
            query += ' AND (amount >= ? AND amount <= ?)';
            params.push(minAmount * 100, maxAmount * 100); // Convert to cents
        }
        
        // Add limit and order by date (most recent first)
        query += ' ORDER BY created DESC LIMIT 50';
        
        const transactions = await db.query(query, params);
        
        // Enhance transactions with customer names if available
        const enhancedTransactions = await Promise.all(
            transactions.map(async (transaction) => {
                // Try to get customer name from mappings
                const customerMapping = await db.get(
                    'SELECT customer_name FROM customer_mappings WHERE stripe_customer_id = ?',
                    [transaction.customer_id]
                );
                
                return {
                    ...transaction,
                    customer_name: customerMapping?.customer_name || null
                };
            })
        );
        
        res.json(enhancedTransactions);
    } catch (error) {
        console.error('Error fetching Stripe transactions:', error);
        res.status(500).json({ success: false, message: 'Error fetching Stripe transactions' });
    }
});

app.post('/api/map-invoice-to-stripe', async (req, res) => {
    try {
        const { invoiceId, stripeTransactionId } = req.body;
        
        if (!invoiceId || !stripeTransactionId) {
            return res.status(400).json({ success: false, message: 'Invoice ID and Stripe Transaction ID are required' });
        }
        
        // Verify invoice exists
        const invoice = await db.get('SELECT id FROM halopsa_invoices WHERE id = ?', [invoiceId]);
        if (!invoice) {
            return res.status(404).json({ success: false, message: 'Invoice not found' });
        }
        
        // Verify stripe transaction exists
        const stripeTx = await db.get('SELECT stripe_id FROM stripe_transactions WHERE stripe_id = ?', [stripeTransactionId]);
        if (!stripeTx) {
            return res.status(404).json({ success: false, message: 'Stripe transaction not found' });
        }
        
        // Update invoice with stripe transaction ID
        await db.run(
            'UPDATE halopsa_invoices SET stripe_transaction_id = ?, mapping_date = CURRENT_TIMESTAMP WHERE id = ?',
            [stripeTransactionId, invoiceId]
        );
        
        res.json({ 
            success: true, 
            message: 'Invoice successfully mapped to Stripe transaction',
            invoiceId,
            stripeTransactionId
        });
        
    } catch (error) {
        console.error('Error mapping invoice to Stripe:', error);
        res.status(500).json({ success: false, message: 'Error mapping invoice to Stripe' });
    }
});

app.post('/api/unmap-invoice', async (req, res) => {
    try {
        const { invoiceId } = req.body;

        if (!invoiceId) {
            return res.status(400).json({ success: false, message: 'Invoice ID is required' });
        }

        // Verify invoice exists
        const invoice = await db.get('SELECT id FROM halopsa_invoices WHERE id = ?', [invoiceId]);
        if (!invoice) {
            return res.status(404).json({ success: false, message: 'Invoice not found' });
        }

        // Clear stripe transaction ID
        await db.run(
            'UPDATE halopsa_invoices SET stripe_transaction_id = NULL, mapping_date = NULL WHERE id = ?',
            [invoiceId]
        );

        res.json({
            success: true,
            message: 'Invoice successfully unmapped',
            invoiceId
        });

    } catch (error) {
        console.error('Error unmapping invoice:', error);
        res.status(500).json({ success: false, message: 'Error unmapping invoice' });
    }
});

// =============================================================================
// AI SETTINGS ENDPOINTS - Multi-Provider Support (Anthropic + OpenRouter)
// =============================================================================

// Get all AI settings
app.get('/api/ai/settings', async (req, res) => {
    try {
        const settings = await db.all('SELECT key, value FROM ai_settings');
        const settingsObj = {};
        settings.forEach(s => {
            // Mask API keys for security
            if (s.key.includes('api_key') && s.value) {
                settingsObj[s.key] = '***' + s.value.slice(-4);
            } else {
                settingsObj[s.key] = s.value;
            }
        });

        // Add readiness status
        settingsObj.is_configured = aiService.isReady();
        settingsObj.current_provider = aiService.provider;

        res.json({ success: true, settings: settingsObj });
    } catch (error) {
        console.error('Error fetching AI settings:', error);
        res.status(500).json({ success: false, message: 'Error fetching AI settings' });
    }
});

// Save AI settings (supports both Anthropic and OpenRouter)
app.post('/api/ai/settings', async (req, res) => {
    try {
        const {
            ai_provider,
            anthropic_api_key,
            anthropic_model,
            openrouter_api_key,
            openrouter_model
        } = req.body;

        // Update provider selection
        if (ai_provider) {
            if (!['anthropic', 'openrouter'].includes(ai_provider)) {
                return res.status(400).json({
                    success: false,
                    message: 'Invalid provider. Must be "anthropic" or "openrouter"'
                });
            }
            await db.run(
                'INSERT OR REPLACE INTO ai_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                ['ai_provider', ai_provider]
            );
        }

        // Update Anthropic settings
        if (anthropic_api_key) {
            await db.run(
                'INSERT OR REPLACE INTO ai_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                ['anthropic_api_key', anthropic_api_key]
            );
        }

        if (anthropic_model) {
            await db.run(
                'INSERT OR REPLACE INTO ai_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                ['anthropic_model', anthropic_model]
            );
        }

        // Update OpenRouter settings
        if (openrouter_api_key) {
            await db.run(
                'INSERT OR REPLACE INTO ai_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                ['openrouter_api_key', openrouter_api_key]
            );
        }

        if (openrouter_model) {
            await db.run(
                'INSERT OR REPLACE INTO ai_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                ['openrouter_model', openrouter_model]
            );
        }

        // Re-initialize AI service with new settings
        const allSettings = await db.all('SELECT key, value FROM ai_settings');
        const config = {};
        allSettings.forEach(s => {
            config[s.key] = s.value;
        });
        aiService.initialize(config);

        res.json({
            success: true,
            message: 'AI settings saved successfully',
            provider: aiService.provider,
            is_ready: aiService.isReady()
        });

    } catch (error) {
        console.error('Error saving AI settings:', error);
        res.status(500).json({ success: false, message: 'Error saving AI settings' });
    }
});

// Get available Anthropic models
app.get('/api/ai/models', async (req, res) => {
    try {
        const models = [
            { id: 'claude-sonnet-4-20250514', name: 'Claude Sonnet 4', description: 'Most capable model for complex tasks' },
            { id: 'claude-3-5-sonnet-20241022', name: 'Claude 3.5 Sonnet', description: 'Excellent balance of intelligence and speed' },
            { id: 'claude-3-7-sonnet-20250219', name: 'Claude 3.7 Sonnet', description: 'Optimized for analysis and reasoning' }
        ];
        res.json({ success: true, models });
    } catch (error) {
        console.error('Error fetching AI models:', error);
        res.status(500).json({ success: false, message: 'Error fetching AI models' });
    }
});

// Get available OpenRouter models
app.get('/api/ai/openrouter-models', async (req, res) => {
    try {
        const models = [
            {
                id: 'anthropic/claude-3.5-sonnet',
                name: 'Claude 3.5 Sonnet',
                description: 'Best balance of performance and cost',
                provider: 'Anthropic'
            },
            {
                id: 'anthropic/claude-sonnet-4-20250514',
                name: 'Claude Sonnet 4',
                description: 'Most capable Claude model',
                provider: 'Anthropic'
            },
            {
                id: 'openai/gpt-4-turbo',
                name: 'GPT-4 Turbo',
                description: 'OpenAI\'s fastest GPT-4 model',
                provider: 'OpenAI'
            },
            {
                id: 'openai/gpt-4o',
                name: 'GPT-4o',
                description: 'OpenAI\'s flagship multimodal model',
                provider: 'OpenAI'
            },
            {
                id: 'google/gemini-pro-1.5',
                name: 'Gemini Pro 1.5',
                description: 'Google\'s advanced AI model',
                provider: 'Google'
            },
            {
                id: 'meta-llama/llama-3.1-70b-instruct',
                name: 'Llama 3.1 70B',
                description: 'Meta\'s open-source powerhouse',
                provider: 'Meta'
            }
        ];
        res.json({ success: true, models });
    } catch (error) {
        console.error('Error fetching OpenRouter models:', error);
        res.status(500).json({ success: false, message: 'Error fetching OpenRouter models' });
    }
});

// Test AI API connection (supports both providers)
app.post('/api/ai/test-connection', async (req, res) => {
    try {
        const { provider, api_key } = req.body;

        if (!provider) {
            return res.status(400).json({
                success: false,
                message: 'Provider is required for testing'
            });
        }

        // If no API key provided, try to get it from database
        let apiKeyToUse = api_key;
        if (!apiKeyToUse) {
            const keyField = provider === 'anthropic' ? 'anthropic_api_key' : 'openrouter_api_key';
            const savedKey = await db.get(`SELECT value FROM ai_settings WHERE key = ?`, [keyField]);

            if (!savedKey || !savedKey.value) {
                return res.status(400).json({
                    success: false,
                    message: `No ${provider} API key found. Please save your API key first.`
                });
            }
            apiKeyToUse = savedKey.value;
        }

        const https = require('https');

        if (provider === 'anthropic') {
            // Test Anthropic Direct API
            const testPayload = JSON.stringify({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 50,
                messages: [
                    { role: 'user', content: 'Reply with only "Connection successful"' }
                ]
            });

            const options = {
                hostname: 'api.anthropic.com',
                path: '/v1/messages',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': apiKeyToUse,
                    'anthropic-version': '2023-06-01'
                }
            };

            const apiRequest = https.request(options, (apiRes) => {
                let data = '';
                apiRes.on('data', (chunk) => { data += chunk; });
                apiRes.on('end', () => {
                    try {
                        const response = JSON.parse(data);
                        if (apiRes.statusCode === 200 && response.content) {
                            res.json({
                                success: true,
                                message: 'Anthropic API connection successful',
                                response: response.content[0]?.text || 'Connected'
                            });
                        } else {
                            res.status(apiRes.statusCode).json({
                                success: false,
                                message: response.error?.message || 'API test failed',
                                error: response.error
                            });
                        }
                    } catch (parseError) {
                        res.status(500).json({
                            success: false,
                            message: 'Invalid API response'
                        });
                    }
                });
            });

            apiRequest.on('error', (error) => {
                res.status(500).json({
                    success: false,
                    message: 'Connection failed: ' + error.message
                });
            });

            apiRequest.write(testPayload);
            apiRequest.end();

        } else if (provider === 'openrouter') {
            // Test OpenRouter API
            const testPayload = JSON.stringify({
                model: 'anthropic/claude-3.5-sonnet',
                messages: [
                    { role: 'user', content: 'Reply with only "Connection successful"' }
                ],
                max_tokens: 50
            });

            const options = {
                hostname: 'openrouter.ai',
                path: '/api/v1/chat/completions',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${apiKeyToUse}`,
                    'HTTP-Referer': 'http://localhost:3000',
                    'X-Title': 'csv-to-qb-iif'
                }
            };

            const apiRequest = https.request(options, (apiRes) => {
                let data = '';
                apiRes.on('data', (chunk) => { data += chunk; });
                apiRes.on('end', () => {
                    try {
                        const response = JSON.parse(data);
                        if (apiRes.statusCode === 200 && response.choices) {
                            res.json({
                                success: true,
                                message: 'OpenRouter API connection successful',
                                response: response.choices[0]?.message?.content || 'Connected'
                            });
                        } else {
                            res.status(apiRes.statusCode).json({
                                success: false,
                                message: response.error?.message || 'API test failed',
                                error: response.error
                            });
                        }
                    } catch (parseError) {
                        res.status(500).json({
                            success: false,
                            message: 'Invalid API response'
                        });
                    }
                });
            });

            apiRequest.on('error', (error) => {
                res.status(500).json({
                    success: false,
                    message: 'Connection failed: ' + error.message
                });
            });

            apiRequest.write(testPayload);
            apiRequest.end();

        } else {
            return res.status(400).json({
                success: false,
                message: 'Invalid provider. Must be "anthropic" or "openrouter"'
            });
        }

    } catch (error) {
        console.error('Error testing AI connection:', error);
        res.status(500).json({ success: false, message: 'Error testing connection' });
    }
});
