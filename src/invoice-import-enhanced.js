/**
 * Enhanced Invoice Data Extraction
 * Comprehensive helper function to extract all invoice fields from HaloPSA API response
 */

/**
 * Extract comprehensive invoice data from HaloPSA API invoice object
 * @param {Object} invoice - Raw invoice object from HaloPSA API
 * @param {Function} getInvoiceStatus - Status extraction function
 * @returns {Object} - Structured invoice data with all fields
 */
function extractComprehensiveInvoiceData(invoice, getInvoiceStatus) {
    // Calculate financial fields
    const totalAmount = parseFloat(invoice.total || invoice.total_amount || invoice.TotalAmount || invoice.amount || invoice.revenue || 0);
    const paidAmount = parseFloat(invoice.amountpaid || invoice.paid_amount || invoice.PaidAmount || invoice.paid || invoice.paidTotal || 0);
    const subtotal = parseFloat(invoice.subtotal || invoice.sub_total || invoice.netAmount || invoice.net_amount || totalAmount);
    const taxAmount = parseFloat(invoice.tax || invoice.tax_amount || invoice.TaxAmount || invoice.VAT || invoice.vat || 0);
    const discountAmount = parseFloat(invoice.discount || invoice.discount_amount || invoice.discountAmount || 0);
    const balanceDue = totalAmount - paidAmount;

    return {
        // Core identification fields
        halopsa_id: invoice.id || invoice.InvoiceID || invoice.InvoiceId,
        invoice_number: invoice.invoicenumber || invoice.invoice_number || invoice.InvoiceNumber || invoice.number || invoice.Number || null,

        // Client information
        halopsa_client_id: invoice.client_id || invoice.ClientID || invoice.clientId || null,
        client_name: invoice.client_name || invoice.ClientName || invoice.account_name || invoice.AccountName || invoice.name || null,

        // Date fields
        invoice_date: invoice.invoice_date || invoice.InvoiceDate || invoice.date || invoice.Date || invoice.datecreated || null,
        due_date: invoice.due_date || invoice.DueDate || invoice.duedate || invoice.paymentDueDate || null,
        sent_date: invoice.sent_date || invoice.sentDate || invoice.datesent || null,
        approved_date: invoice.approved_date || invoice.approvedDate || invoice.dateapproved || null,
        last_payment_date: invoice.last_payment_date || invoice.lastPaymentDate || invoice.datepaid || null,
        modified_date: invoice.modified_date || invoice.modifiedDate || invoice.datemodified || null,

        // Financial fields
        total_amount: totalAmount,
        paid_amount: paidAmount,
        balance_due: balanceDue,
        subtotal: subtotal,
        tax_amount: taxAmount,
        tax_rate: parseFloat(invoice.tax_rate || invoice.taxRate || invoice.taxpercentage || 0),
        discount_amount: discountAmount,

        // Status and payment information
        status: getInvoiceStatus(invoice),
        payment_status: invoice.paymentstatus || invoice.payment_status || getInvoiceStatus(invoice),

        // Transaction details
        currency: invoice.currency || invoice.Currency || 'USD',
        payment_terms: invoice.payment_terms || invoice.paymentTerms || invoice.terms || null,
        payment_method: invoice.payment_method || invoice.paymentMethod || invoice.paymenttype || null,

        // Reference fields
        reference_number: invoice.reference || invoice.reference_number || invoice.referenceNumber || null,
        po_number: invoice.po_number || invoice.PONumber || invoice.purchaseorder || null,
        notes: invoice.notes || invoice.description || invoice.memo || null,

        // Complex data as JSON
        line_items: invoice.lines ? JSON.stringify(invoice.lines) :
                   (invoice.items ? JSON.stringify(invoice.items) : null),
        tax_details: invoice.tax_details ? JSON.stringify(invoice.tax_details) :
                    (invoice.taxes ? JSON.stringify(invoice.taxes) : null),
        payment_history: invoice.payments ? JSON.stringify(invoice.payments) :
                        (invoice.payment_history ? JSON.stringify(invoice.payment_history) : null),
        custom_fields: invoice.customfields || invoice.custom_fields ?
                      JSON.stringify(invoice.customfields || invoice.custom_fields) : null,

        // Audit fields
        approved_by: invoice.approved_by || invoice.approvedBy || invoice.approver || null,
        created_by: invoice.created_by || invoice.createdBy || invoice.agent_id || invoice.agentId || null,
        modified_by: invoice.modified_by || invoice.modifiedBy || invoice.lastmodifiedby || null,

        // Integration fields
        stripe_transaction_id: null, // Will be linked later in mapping
        qb_txn_id: null, // Will be set when synced to QuickBooks
        synced_to_qb: false,
        sync_error: null,

        // Raw data for debugging
        raw_data: JSON.stringify(invoice)
    };
}

/**
 * SQL for inserting comprehensive invoice data
 */
const INSERT_INVOICE_SQL = `
    INSERT INTO halopsa_invoices (
        halopsa_id, invoice_number, halopsa_client_id, client_name,
        invoice_date, due_date, sent_date, approved_date, last_payment_date, modified_date,
        total_amount, paid_amount, balance_due, subtotal, tax_amount, tax_rate, discount_amount,
        status, payment_status, currency, payment_terms, payment_method,
        reference_number, po_number, notes,
        line_items, tax_details, payment_history, custom_fields,
        approved_by, created_by, modified_by,
        stripe_transaction_id, qb_txn_id, synced_to_qb, sync_error,
        raw_data, last_sync
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
    )
`;

/**
 * SQL for updating comprehensive invoice data
 */
const UPDATE_INVOICE_SQL = `
    UPDATE halopsa_invoices SET
        invoice_number = ?, halopsa_client_id = ?, client_name = ?,
        invoice_date = ?, due_date = ?, sent_date = ?, approved_date = ?, last_payment_date = ?, modified_date = ?,
        total_amount = ?, paid_amount = ?, balance_due = ?, subtotal = ?, tax_amount = ?, tax_rate = ?, discount_amount = ?,
        status = ?, payment_status = ?, currency = ?, payment_terms = ?, payment_method = ?,
        reference_number = ?, po_number = ?, notes = ?,
        line_items = ?, tax_details = ?, payment_history = ?, custom_fields = ?,
        approved_by = ?, created_by = ?, modified_by = ?,
        stripe_transaction_id = ?, qb_txn_id = ?, synced_to_qb = ?, sync_error = ?,
        raw_data = ?, last_sync = CURRENT_TIMESTAMP
    WHERE halopsa_id = ?
`;

/**
 * Get array of values for INSERT statement (excluding halopsa_id which goes at the start)
 */
function getInsertValues(invoiceData) {
    return [
        invoiceData.halopsa_id,
        invoiceData.invoice_number,
        invoiceData.halopsa_client_id,
        invoiceData.client_name,
        invoiceData.invoice_date,
        invoiceData.due_date,
        invoiceData.sent_date,
        invoiceData.approved_date,
        invoiceData.last_payment_date,
        invoiceData.modified_date,
        invoiceData.total_amount,
        invoiceData.paid_amount,
        invoiceData.balance_due,
        invoiceData.subtotal,
        invoiceData.tax_amount,
        invoiceData.tax_rate,
        invoiceData.discount_amount,
        invoiceData.status,
        invoiceData.payment_status,
        invoiceData.currency,
        invoiceData.payment_terms,
        invoiceData.payment_method,
        invoiceData.reference_number,
        invoiceData.po_number,
        invoiceData.notes,
        invoiceData.line_items,
        invoiceData.tax_details,
        invoiceData.payment_history,
        invoiceData.custom_fields,
        invoiceData.approved_by,
        invoiceData.created_by,
        invoiceData.modified_by,
        invoiceData.stripe_transaction_id,
        invoiceData.qb_txn_id,
        invoiceData.synced_to_qb,
        invoiceData.sync_error,
        invoiceData.raw_data
    ];
}

/**
 * Get array of values for UPDATE statement (excluding halopsa_id which goes at the end)
 */
function getUpdateValues(invoiceData) {
    return [
        invoiceData.invoice_number,
        invoiceData.halopsa_client_id,
        invoiceData.client_name,
        invoiceData.invoice_date,
        invoiceData.due_date,
        invoiceData.sent_date,
        invoiceData.approved_date,
        invoiceData.last_payment_date,
        invoiceData.modified_date,
        invoiceData.total_amount,
        invoiceData.paid_amount,
        invoiceData.balance_due,
        invoiceData.subtotal,
        invoiceData.tax_amount,
        invoiceData.tax_rate,
        invoiceData.discount_amount,
        invoiceData.status,
        invoiceData.payment_status,
        invoiceData.currency,
        invoiceData.payment_terms,
        invoiceData.payment_method,
        invoiceData.reference_number,
        invoiceData.po_number,
        invoiceData.notes,
        invoiceData.line_items,
        invoiceData.tax_details,
        invoiceData.payment_history,
        invoiceData.custom_fields,
        invoiceData.approved_by,
        invoiceData.created_by,
        invoiceData.modified_by,
        invoiceData.stripe_transaction_id,
        invoiceData.qb_txn_id,
        invoiceData.synced_to_qb,
        invoiceData.sync_error,
        invoiceData.raw_data,
        invoiceData.halopsa_id // WHERE clause
    ];
}

module.exports = {
    extractComprehensiveInvoiceData,
    INSERT_INVOICE_SQL,
    UPDATE_INVOICE_SQL,
    getInsertValues,
    getUpdateValues
};
