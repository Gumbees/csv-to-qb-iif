const crypto = require('crypto');
const dayjs = require('dayjs');
const { parseCsv, detectCsvType, parseBillsFromCsv } = require('./csv');

function hashRow(obj) {
    return crypto.createHash('md5').update(JSON.stringify(obj)).digest('hex');
}

async function importFile(db, content, filename, mimetype) {
    const rows = parseCsv(content);
    const detectedType = detectCsvType(rows, filename);

    switch (detectedType) {
        case 'po_bills':
            return importPoBills(db, content, filename, mimetype, rows);
        case 'halo_invoices':
            return importHaloInvoices(db, content, filename, mimetype, rows);
        case 'bank_batch':
        case 'bank_generic':
            return importBankFile(db, content, filename, mimetype, rows, detectedType);
        case 'stripe_csv':
            throw new Error('Stripe CSV not supported yet. Use Stripe backfill API.');
        default:
            throw new Error('Unknown CSV structure');
    }
}

async function importPoBills(db, content, filename, mimetype, rows) {
    const metaId = await db.createImportMetadata('LocalCSV', 'po_bills', {
        original_filename: filename,
        content_type: mimetype || 'text/csv',
        row_count: rows.length,
        raw_headers: rows.length ? Object.keys(rows[0]) : [],
        sample: rows.slice(0, 5)
    });
    await db.addImportRecords(metaId, rows.map(r => ({ external_id: r['RefNumber'] || r['Ref Number'] || null, checksum: hashRow(r), raw: r })));
    const importResult = await db.storeCsvImport(filename, content);
    if (importResult.isDuplicate) {
        return { error: importResult.message, isDuplicate: true, importId: importResult.id, detectedType: 'po_bills' };
    }
    const bills = parseBillsFromCsv(content);
    return { bills, filePath: filename, importId: importResult.id, isDuplicate: false, import_meta_id: metaId, detectedType: 'po_bills' };
}

async function importHaloInvoices(db, content, filename, mimetype, rows) {
    const importMetaId = await db.createImportMetadata('HaloPSA', 'halo_invoices', {
        original_filename: filename,
        content_type: mimetype,
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
    return { detectedType: 'halo_invoices', imported, import_meta_id: importMetaId };
}

async function importBankFile(db, content, filename, mimetype, rows, detectedType) {
    const sourceName = detectedType === 'bank_batch' ? 'FNBPA' : 'BankCSV';
    const importMetaId = await db.createImportMetadata(sourceName, detectedType, {
        original_filename: filename,
        content_type: mimetype,
        row_count: rows.length,
        raw_headers: rows.length ? Object.keys(rows[0]) : [],
        sample: rows.slice(0, 5)
    });
    await db.addImportRecords(importMetaId, rows.map(r => ({ external_id: (r['Reference Number'] || r['reference number'] || r['Transaction ID'] || r['ID'] || null), checksum: hashRow(r), raw: r })));

    const parseNumber = (v) => {
        if (v == null) return 0;
        const n = Number(String(v).replace(/[^0-9.-]/g, ''));
        return isNaN(n) ? 0 : n;
    };

    let txns = [];
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
    return { detectedType, imported: result.count, import_meta_id: importMetaId };
}

module.exports = { importFile };