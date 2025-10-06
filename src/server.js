

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const dayjs = require('dayjs');
const Database = require('./database');
const crypto = require('crypto');
let StripeLib = null; try { StripeLib = require('stripe'); } catch (_) { /* optional dependency */ }

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
const db = new Database();
const stripeSecret = process.env.STRIPE_SECRET_KEY || '';
const stripeWebhookSecret = process.env.STRIPE_WEBHOOK_SECRET || '';
const stripe = (StripeLib && stripeSecret) ? new StripeLib(stripeSecret) : null;

// Ensure DB initialization completes before serving traffic
(async () => {
  try { await db.ready; console.log('Database initialized'); }
  catch (e) { console.error('Database init failed:', e); process.exit(1); }
})();

app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '25mb' }));

// Raw body for Stripe webhook signature verification (enable only if configured)
if (stripe && stripeWebhookSecret) {
  app.use('/api/webhooks/stripe', express.raw({ type: '*/*' }));
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
        // Simple DB check
        await db.getDashboardStats();
        res.status(200).json({ status: 'ok' });
    } catch (e) {
        res.status(500).json({ status: 'error', message: e.message });
    }
});

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/renderer.html');
});




app.post('/api/pick-file', upload.single('file'), async (req, res) => {
    try {


        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const content = req.file.buffer.toString('utf-8');
        const filename = req.file.originalname || 'uploaded.csv';
        const rows = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });

        const detectedType = detectCsvType(rows, filename);

        if (detectedType === 'po_bills') {
            const metaId = await db.createImportMetadata('LocalCSV', 'po_bills', {

                original_filename: filename, content_type: req.file.mimetype || 'text/csv',
                row_count: rows.length, raw_headers: rows.length ? Object.keys(rows[0]) : [], sample: rows.slice(0, 5)
            });
            await db.addImportRecords(metaId, rows.map(r => ({ external_id: r['RefNumber'] || r['Ref Number'] || null, checksum: hashRow(r), raw: r })));

            const importResult = await db.storeCsvImport(filename, content);
            if (importResult.isDuplicate) return res.json({ error: importResult.message, isDuplicate: true, importId: importResult.id, detectedType });
            const bills = parseBillsFromCsv(content);

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

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});
