const { parse } = require('csv-parse/sync');
const dayjs = require('dayjs');

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

function getHeadersLower(rows) {
    return rows.length ? Object.keys(rows[0]).map(h => String(h).trim().toLowerCase()) : [];
}

function detectCsvType(rows, filename = '') {
    const h = getHeadersLower(rows);
    const has = (name) => h.includes(name);
    const any = (...names) => names.some(n => has(n));
    const all = (...names) => names.every(n => has(n));
    const fname = (filename || '').toLowerCase();

    if (has('po_id')) {
        return 'po_bills';
    }
    if (all('batch number', 'transfer description', 'effective date') && any('dr amount', 'cr amount')) {
        return 'bank_batch';
    }

    const invoiceNumberSyn = ['invoice number','invoice no','invoice #','inv','invoice','document number'];
    const invoiceIdSyn = ['invoiceid','invoice id','id'];
    const invDateSyn = ['invoice date','date','invoicedate'];
    const customerSyn = ['customer','customer name','account name','client','client name'];
    const amountsSyn = ['total','amount due','balance','subtotal'];
    if (any(...invoiceNumberSyn) || any(...invoiceIdSyn) || (any(...customerSyn) && any(...invDateSyn) && any(...amountsSyn)) || fname.includes('invoice')) {
        return 'halo_invoices';
    }

    if (any('balance transaction id', 'type') && any('amount', 'net')) {
        return 'stripe_csv';
    }

    if (any('vendor','supplier','vendor name') && any('refnumber','ref number','reference','reference number','docnum','document number') && any('item','item code','sku','product','product code','description') && any('qty','quantity','qnty') && any('cost','unit cost','price','unit price','rate')) {
        return 'po_bills';
    }

    if (any('date','posting date','transaction date') && any('amount', 'credit', 'debit') && any('description', 'memo', 'details')) {
        return 'bank_generic';
    }
    return 'unknown';
}

function parseBillsFromCsv(content) {
    const records = parse(content, { columns: true, skip_empty_lines: true, relax_column_count: true });
    if (!records.length) {
        throw new Error('CSV is empty or could not be parsed.');
    }
    const headerKeys = Object.keys(records[0]).map((h) => String(h).trim().toLowerCase());

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
        throw new Error(`Missing required columns for PO: ${missing.join(', ')}`);
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

module.exports = {
    formatDate,
    sanitize,
    computeDueDate,
    detectCsvType,
    parseBillsFromCsv,
    parseCsv: (c) => parse(c, { columns: true, skip_empty_lines: true, relax_column_count: true })
};