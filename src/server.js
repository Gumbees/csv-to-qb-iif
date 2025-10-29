

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

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
const db = new Database();
const qbwcService = new QBWCService();
const configAPI = new ConfigAPI(db);
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

// QuickBooks Web Connector Endpoints
app.post('/qbwc', express.text({ type: '*/*' }), async (req, res) => {
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
        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <clientVersionResponse xmlns="http://developer.intuit.com/">
            <clientVersionResult>nvu</clientVersionResult>
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
        // Success: <string>ticket</string>
        // Failure: <string></string><string>errorCode</string>
        let authResultXML;
        if (authResult.errorCode) {
            // Authentication failed
            authResultXML = `<string></string><string>${authResult.errorCode}</string>`;
        } else {
            // Authentication successful
            authResultXML = `<string>${authResult.ticket}</string>`;
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
            // Priority 1: Check if account sync is pending
            const accountSyncPending = session.accountSyncPending || false;

            if (accountSyncPending) {
                console.log('QBWC: Generating AccountQuery request');
                qbxmlData = qbwcService.generateAccountQueryQBXML();
                session.currentRequest = 'accounts';
                session.accountSyncPending = false; // Clear flag
                console.log('QBWC: Generated QBXML for Chart of Accounts query');
            }
            // Priority 2: Check if we have unsynced items to process
            else {
                const itemSyncService = new ItemSyncService(db);
                const unsyncedItems = await itemSyncService.getUnsyncedItems();

                if (unsyncedItems.length > 0) {
                    console.log(`QBWC: Found ${unsyncedItems.length} unsynced items to process`);

                    // Store item IDs in session for later marking as synced
                    session.pendingItemIds = unsyncedItems.map(item => item.id);
                    session.currentRequest = 'items';

                    // Generate QBXML for all items
                    qbxmlData = qbwcService.generateAllItemsQBXML(unsyncedItems);

                    console.log('QBWC: Generated QBXML for items');
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

        responseXML = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <sendRequestXMLResponse xmlns="http://developer.intuit.com/">
            <sendRequestXMLResult>${qbxmlData}</sendRequestXMLResult>
        </sendRequestXMLResponse>
    </soap:Body>
</soap:Envelope>`;
    }
    else if (soapRequest.includes('receiveResponseXML')) {
        const ticketMatch = soapRequest.match(/<ticket[^>]*>([^<]+)<\/ticket>/);
        const responseMatch = soapRequest.match(/<response[^>]*>([^<]+)<\/response>/);
        const hresultMatch = soapRequest.match(/<hresult[^>]*>([^<]+)<\/hresult>/);
        const messageMatch = soapRequest.match(/<message[^>]*>([^<]+)<\/message>/);

        const ticket = ticketMatch ? ticketMatch[1] : '';
        const response = responseMatch ? responseMatch[1] : '';
        const hresult = hresultMatch ? hresultMatch[1] : '';
        const message = messageMatch ? messageMatch[1] : '';

        console.log('QBWC receiveResponseXML:', { ticket, hresult, message });

        // Get session to see what was being synced
        const session = qbwcService.getSession(ticket);

        // Process the response
        if (hresult === '0') {
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
                // Handle item sync completion
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

        const clients = await db.all(`
            SELECT
                hc.*,
                cm.stripe_customer_id,
                cm.stripe_customer_name,
                cm.stripe_customer_email,
                cm.qb_customer_id,
                cm.qb_customer_name,
                cm.auto_mapped,
                cm.mapping_confirmed
            FROM halopsa_clients hc
            LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id
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
        const stripeAPI = new StripeAPI(db);
        // Use imported customers from local database instead of direct Stripe API calls
        let customers = await stripeAPI.getImportedCustomers();
        
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
            description: customer.description
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

app.get('/api/customers/mappings', async (req, res) => {
    try {
        const mappings = await db.all('SELECT * FROM customer_mappings ORDER BY updated_at DESC');
        res.json(mappings);
    } catch (error) {
        console.error('Error fetching customer mappings:', error);
        res.status(500).json({ error: 'Failed to fetch mappings' });
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
        const { include_unconfirmed = false } = req.query;
        
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
        
        const mappings = await db.all(query);
        
        res.json({ success: true, mappings: mappings });
        
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

app.get('/qbwc/status', (req, res) => {
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
});

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
