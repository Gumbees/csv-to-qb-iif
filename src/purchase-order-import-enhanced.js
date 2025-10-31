/**
 * Enhanced Purchase Order Data Extraction
 * Comprehensive helper function to extract all purchase order fields from HaloPSA API response
 */

/**
 * Extract comprehensive purchase order data from HaloPSA API purchase order object
 * @param {Object} po - Raw purchase order object from HaloPSA API
 * @param {Function} getPOStatus - Status extraction function
 * @returns {Object} - Structured purchase order data with all fields
 */
function extractComprehensivePOData(po, getPOStatus) {
    // Calculate financial fields
    const totalAmount = parseFloat(po.total || po.total_amount || po.TotalAmount || po.amount || po.value || 0);
    const paidAmount = parseFloat(po.paid || po.paid_amount || po.PaidAmount || po.amountpaid || 0);
    const subtotal = parseFloat(po.subtotal || po.sub_total || po.netAmount || po.net_amount || totalAmount);
    const taxAmount = parseFloat(po.tax || po.tax_amount || po.TaxAmount || po.VAT || po.vat || 0);
    const shippingCost = parseFloat(po.shipping || po.shipping_cost || po.freight || po.freight_cost || 0);
    const discountAmount = parseFloat(po.discount || po.discount_amount || po.discountAmount || 0);
    const balanceDue = totalAmount - paidAmount;

    return {
        // Core identification fields
        halopsa_id: po.id || po.PurchaseOrderID || po.POId || po.po_id,
        po_number: po.po_number || po.ponumber || po.PONumber || po.number || po.Number || null,

        // Client information
        halopsa_client_id: po.client_id || po.ClientID || po.clientId || null,
        client_name: po.client_name || po.ClientName || po.account_name || po.AccountName || po.customer_name || null,

        // Vendor information
        vendor_id: po.vendor_id || po.supplier_id || po.VendorID || po.SupplierID || null,
        vendor_name: po.vendor_name || po.vendor || po.supplier || po.VendorName || po.SupplierName || null,
        vendor_email: po.vendor_email || po.supplier_email || po.VendorEmail || null,
        vendor_phone: po.vendor_phone || po.supplier_phone || po.VendorPhone || null,

        // Date fields
        po_date: po.po_date || po.date || po.PODate || po.Date || po.order_date || po.datecreated || null,
        expected_delivery: po.expected_delivery || po.expected_date || po.delivery_date || po.expectedDelivery || null,
        actual_delivery: po.actual_delivery || po.delivered_date || po.actualDelivery || po.datedelivered || null,
        ordered_date: po.ordered_date || po.order_date || po.dateordered || po.dateOrdered || null,
        received_date: po.received_date || po.receipt_date || po.datereceived || po.dateReceived || null,
        approved_date: po.approved_date || po.approval_date || po.dateapproved || po.dateApproved || null,
        modified_date: po.modified_date || po.last_modified || po.datemodified || po.dateModified || null,

        // Financial fields
        total_amount: totalAmount,
        paid_amount: paidAmount,
        balance_due: balanceDue,
        subtotal: subtotal,
        tax_amount: taxAmount,
        tax_rate: parseFloat(po.tax_rate || po.taxRate || po.taxpercentage || 0),
        shipping_cost: shippingCost,
        discount_amount: discountAmount,

        // Status information
        status: getPOStatus(po),
        approval_status: po.approval_status || po.approvalStatus || po.approved ? 'approved' : 'pending',

        // Transaction details
        currency: po.currency || po.Currency || 'USD',
        payment_terms: po.payment_terms || po.paymentTerms || po.terms || null,
        payment_method: po.payment_method || po.paymentMethod || po.paymenttype || null,

        // Reference fields
        reference_number: po.reference || po.reference_number || po.referenceNumber || po.ref || null,
        requisition_number: po.requisition || po.requisition_number || po.req_number || po.RequisitionNumber || null,
        contract_number: po.contract || po.contract_number || po.contractNumber || null,
        invoice_id: po.invoice_id || po.invoiceId || po.InvoiceID || po.billed_invoice_id || null,

        // Shipping information
        shipping_address: po.shipping_address ? JSON.stringify(po.shipping_address) :
                         (po.delivery_address ? JSON.stringify(po.delivery_address) : null),
        shipping_method: po.shipping_method || po.delivery_method || po.shippingMethod || po.carrier || null,
        tracking_number: po.tracking || po.tracking_number || po.trackingNumber || po.tracking_id || null,
        freight_terms: po.freight_terms || po.freightTerms || po.delivery_terms || null,

        // Billing information
        billing_address: po.billing_address ? JSON.stringify(po.billing_address) : null,

        // Notes
        notes: po.notes || po.note || po.comments || po.description || null,
        internal_notes: po.internal_notes || po.internalNotes || po.private_notes || null,
        shipping_notes: po.shipping_notes || po.delivery_notes || po.shippingNotes || null,

        // Complex data as JSON
        line_items: po.lines ? JSON.stringify(po.lines) :
                   (po.items ? JSON.stringify(po.items) :
                   (po.lineitems ? JSON.stringify(po.lineitems) : null)),
        custom_fields: po.customfields || po.custom_fields ?
                      JSON.stringify(po.customfields || po.custom_fields) : null,

        // Audit fields
        approved_by: po.approved_by || po.approvedBy || po.approver || po.approved_by_id || null,
        created_by: po.created_by || po.createdBy || po.agent_id || po.agentId || po.creator || null,
        modified_by: po.modified_by || po.modifiedBy || po.lastmodifiedby || null,
        ordered_by: po.ordered_by || po.orderedBy || po.purchaser || po.buyer || null,
        received_by: po.received_by || po.receivedBy || po.receiver || null,

        // Integration fields (SQLite booleans must be 0/1, not true/false)
        qb_txn_id: null, // Will be set when synced to QuickBooks
        synced_to_qb: 0,  // SQLite boolean: 0 = false, 1 = true
        sync_error: null,

        // Raw data for debugging
        raw_data: JSON.stringify(po)
    };
}

/**
 * SQL for inserting comprehensive purchase order data
 */
const INSERT_PO_SQL = `
    INSERT INTO halopsa_purchase_orders (
        halopsa_id, po_number, halopsa_client_id, client_name,
        vendor_id, vendor_name, vendor_email, vendor_phone,
        po_date, expected_delivery, actual_delivery, ordered_date, received_date, approved_date, modified_date,
        total_amount, paid_amount, balance_due, subtotal, tax_amount, tax_rate, shipping_cost, discount_amount,
        status, approval_status, currency, payment_terms, payment_method,
        reference_number, requisition_number, contract_number, invoice_id,
        shipping_address, shipping_method, tracking_number, freight_terms, billing_address,
        notes, internal_notes, shipping_notes,
        line_items, custom_fields,
        approved_by, created_by, modified_by, ordered_by, received_by,
        qb_txn_id, synced_to_qb, sync_error,
        raw_data, last_sync
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
    )
`;

/**
 * SQL for updating comprehensive purchase order data
 */
const UPDATE_PO_SQL = `
    UPDATE halopsa_purchase_orders SET
        po_number = ?, halopsa_client_id = ?, client_name = ?,
        vendor_id = ?, vendor_name = ?, vendor_email = ?, vendor_phone = ?,
        po_date = ?, expected_delivery = ?, actual_delivery = ?, ordered_date = ?, received_date = ?, approved_date = ?, modified_date = ?,
        total_amount = ?, paid_amount = ?, balance_due = ?, subtotal = ?, tax_amount = ?, tax_rate = ?, shipping_cost = ?, discount_amount = ?,
        status = ?, approval_status = ?, currency = ?, payment_terms = ?, payment_method = ?,
        reference_number = ?, requisition_number = ?, contract_number = ?, invoice_id = ?,
        shipping_address = ?, shipping_method = ?, tracking_number = ?, freight_terms = ?, billing_address = ?,
        notes = ?, internal_notes = ?, shipping_notes = ?,
        line_items = ?, custom_fields = ?,
        approved_by = ?, created_by = ?, modified_by = ?, ordered_by = ?, received_by = ?,
        qb_txn_id = ?, synced_to_qb = ?, sync_error = ?,
        raw_data = ?, last_sync = CURRENT_TIMESTAMP
    WHERE halopsa_id = ?
`;

/**
 * Get array of values for INSERT statement
 */
function getInsertValues(poData) {
    return [
        poData.halopsa_id,
        poData.po_number,
        poData.halopsa_client_id,
        poData.client_name,
        poData.vendor_id,
        poData.vendor_name,
        poData.vendor_email,
        poData.vendor_phone,
        poData.po_date,
        poData.expected_delivery,
        poData.actual_delivery,
        poData.ordered_date,
        poData.received_date,
        poData.approved_date,
        poData.modified_date,
        poData.total_amount,
        poData.paid_amount,
        poData.balance_due,
        poData.subtotal,
        poData.tax_amount,
        poData.tax_rate,
        poData.shipping_cost,
        poData.discount_amount,
        poData.status,
        poData.approval_status,
        poData.currency,
        poData.payment_terms,
        poData.payment_method,
        poData.reference_number,
        poData.requisition_number,
        poData.contract_number,
        poData.invoice_id,
        poData.shipping_address,
        poData.shipping_method,
        poData.tracking_number,
        poData.freight_terms,
        poData.billing_address,
        poData.notes,
        poData.internal_notes,
        poData.shipping_notes,
        poData.line_items,
        poData.custom_fields,
        poData.approved_by,
        poData.created_by,
        poData.modified_by,
        poData.ordered_by,
        poData.received_by,
        poData.qb_txn_id,
        poData.synced_to_qb,
        poData.sync_error,
        poData.raw_data
    ];
}

/**
 * Get array of values for UPDATE statement (excluding halopsa_id which goes at the end)
 */
function getUpdateValues(poData) {
    return [
        poData.po_number,
        poData.halopsa_client_id,
        poData.client_name,
        poData.vendor_id,
        poData.vendor_name,
        poData.vendor_email,
        poData.vendor_phone,
        poData.po_date,
        poData.expected_delivery,
        poData.actual_delivery,
        poData.ordered_date,
        poData.received_date,
        poData.approved_date,
        poData.modified_date,
        poData.total_amount,
        poData.paid_amount,
        poData.balance_due,
        poData.subtotal,
        poData.tax_amount,
        poData.tax_rate,
        poData.shipping_cost,
        poData.discount_amount,
        poData.status,
        poData.approval_status,
        poData.currency,
        poData.payment_terms,
        poData.payment_method,
        poData.reference_number,
        poData.requisition_number,
        poData.contract_number,
        poData.invoice_id,
        poData.shipping_address,
        poData.shipping_method,
        poData.tracking_number,
        poData.freight_terms,
        poData.billing_address,
        poData.notes,
        poData.internal_notes,
        poData.shipping_notes,
        poData.line_items,
        poData.custom_fields,
        poData.approved_by,
        poData.created_by,
        poData.modified_by,
        poData.ordered_by,
        poData.received_by,
        poData.qb_txn_id,
        poData.synced_to_qb,
        poData.sync_error,
        poData.raw_data,
        poData.halopsa_id // WHERE clause
    ];
}

module.exports = {
    extractComprehensivePOData,
    INSERT_PO_SQL,
    UPDATE_PO_SQL,
    getInsertValues,
    getUpdateValues
};
