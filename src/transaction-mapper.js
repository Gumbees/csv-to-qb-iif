/**
 * Transaction Mapper - Auto-maps Stripe transactions to HaloPSA invoices
 * Parses invoice IDs from transaction descriptions with error tolerance
 */

class TransactionMapper {
    constructor(db) {
        this.db = db;

        // Down payment detection patterns
        this.depositPatterns = [
            /\b(\d+%?\s*)?d\.?p\.?\s+(for|on)/i,     // "65% DP for", "DP for", "D.P. for"
            /\bdown\s*payment/i,                      // "down payment"
            /\bdeposit\s+(for|on)/i,                  // "deposit for"
            /\b(\d+%)\s+(down|deposit)/i,             // "65% down", "50% deposit"
            /\bpaid\s+in\s+full/i,                    // "paid in full" (actually full payment)
            /\bp\.?i\.?f\.?\b/i                       // "PIF" or "P.I.F."
        ];

        // Final/remaining balance patterns
        this.finalPaymentPatterns = [
            /\bremaining\s+balance/i,
            /\bfinal\s+payment/i,
            /\bbalance\s+due/i,
            /\brest\s+of\s+payment/i,
            /\bcompleting\s+payment/i
        ];
    }

    /**
     * Detect transaction type based on description
     * @param {string} description - Stripe transaction description
     * @returns {string} - 'deposit', 'final_payment', or 'payment'
     */
    detectTransactionType(description) {
        if (!description) return 'payment';

        // Check for "Paid in Full" or "PIF" (full payment, not deposit)
        if (/\bpaid\s+in\s+full/i.test(description) || /\bp\.?i\.?f\.?\b/i.test(description)) {
            return 'payment'; // Full payment, not a deposit
        }

        // Check for final/remaining balance first
        for (const pattern of this.finalPaymentPatterns) {
            if (pattern.test(description)) {
                return 'final_payment';
            }
        }

        // Check for down payment/deposit patterns
        for (const pattern of this.depositPatterns) {
            if (pattern.test(description)) {
                // Make sure it's not "DP" as part of something else
                if (/\b(\d+%?\s*)?d\.?p\.?\s+(for|on)/i.test(description)) {
                    return 'deposit';
                }
            }
        }

        // Default to regular payment
        return 'payment';
    }

    /**
     * Parse HaloPSA invoice IDs from Stripe transaction description
     * Handles various formats:
     * - "123, 456, 789" (with spaces)
     * - "123,456,789" (no spaces)
     * - "Invoice 123, 456" (with text)
     * - "INV-123, INV-456" (with prefix)
     *
     * @param {string} description - Stripe transaction description
     * @returns {Array<number>} - Array of invoice IDs (as numbers)
     */
    parseInvoiceIDs(description) {
        if (!description) return [];

        // Extract all numbers that could be invoice IDs
        // Look for patterns: standalone numbers, numbers after "INV", "Invoice", "#", etc.
        const patterns = [
            /\bINV[- ]?(\d+)/gi,           // INV-123 or INV 123 or INV123
            /\bInvoice[- ]?(\d+)/gi,       // Invoice-123 or Invoice 123
            /#(\d+)/g,                      // #123
            /\b(\d{3,})\b/g                 // Standalone 3+ digit numbers
        ];

        const foundIds = new Set();

        for (const pattern of patterns) {
            const matches = description.matchAll(pattern);
            for (const match of matches) {
                const id = parseInt(match[1] || match[0].replace(/\D/g, ''));
                if (id && id > 0) {
                    foundIds.add(id);
                }
            }
        }

        // Also try comma-separated values (the primary expected format)
        const commaSeparated = description.split(',')
            .map(part => part.trim())
            .map(part => parseInt(part.replace(/\D/g, '')))
            .filter(id => id && id > 0);

        commaSeparated.forEach(id => foundIds.add(id));

        return Array.from(foundIds).sort((a, b) => a - b);
    }

    /**
     * Auto-map a Stripe transaction to HaloPSA invoices
     * @param {number} stripeTransactionId - ID from stripe_transactions table
     * @returns {Object} - Mapping result with matched invoices
     */
    async autoMapTransaction(stripeTransactionId) {
        try {
            // Get the Stripe transaction
            const transaction = await this.db.get(
                'SELECT * FROM stripe_transactions WHERE id = ?',
                [stripeTransactionId]
            );

            if (!transaction) {
                return { success: false, message: 'Transaction not found' };
            }

            // Detect transaction type
            const transactionType = this.detectTransactionType(transaction.description);

            // Update transaction type in database
            await this.db.run(
                'UPDATE stripe_transactions SET transaction_type = ? WHERE id = ?',
                [transactionType, stripeTransactionId]
            );

            // If it's a down payment, don't try to map to invoices
            if (transactionType === 'deposit') {
                return {
                    success: true,
                    message: 'Classified as down payment (no invoice mapping expected)',
                    transaction_type: transactionType,
                    description: transaction.description,
                    auto_mapped: false,
                    is_deposit: true
                };
            }

            // Parse invoice IDs from description
            const invoiceIds = this.parseInvoiceIDs(transaction.description);

            if (invoiceIds.length === 0) {
                return {
                    success: false,
                    message: 'No invoice IDs found in description',
                    description: transaction.description,
                    transaction_type: transactionType,
                    auto_mapped: false
                };
            }

            console.log(`Found ${invoiceIds.length} invoice IDs in description: ${invoiceIds.join(', ')}`);

            // Look up HaloPSA invoices by ID
            const matchedInvoices = [];
            const unmatchedIds = [];

            for (const invoiceId of invoiceIds) {
                const invoice = await this.db.get(
                    'SELECT id, halopsa_id, invoice_number, total_amount FROM halopsa_invoices WHERE halopsa_id = ?',
                    [invoiceId]
                );

                if (invoice) {
                    matchedInvoices.push(invoice);
                } else {
                    unmatchedIds.push(invoiceId);
                }
            }

            // Calculate total from matched invoices
            const totalMatched = matchedInvoices.reduce((sum, inv) => sum + (inv.total_amount || 0), 0);
            const stripeAmount = transaction.amount / 100; // Stripe stores in cents

            // Check if amounts match (within $1 tolerance for rounding)
            const amountMatches = Math.abs(totalMatched - stripeAmount) < 1.0;

            // Update the transaction with mapping
            if (matchedInvoices.length > 0) {
                const primaryInvoiceId = matchedInvoices[0].id;

                await this.db.run(
                    `UPDATE stripe_transactions SET
                     halopsa_invoice_id = ?,
                     mapped_to_halo = 1
                     WHERE id = ?`,
                    [primaryInvoiceId, stripeTransactionId]
                );

                // Create detailed mapping records if we have a mapping table
                try {
                    for (const invoice of matchedInvoices) {
                        await this.db.run(
                            `INSERT OR IGNORE INTO transaction_invoice_mappings
                             (stripe_transaction_id, halopsa_invoice_id, invoice_amount, auto_mapped, mapping_confidence)
                             VALUES (?, ?, ?, 1, ?)`,
                            [stripeTransactionId, invoice.id, invoice.total_amount, amountMatches ? 1.0 : 0.8]
                        );
                    }
                } catch (e) {
                    // Table might not exist yet - we'll create it later
                    console.log('transaction_invoice_mappings table not yet created');
                }

                return {
                    success: true,
                    message: `Mapped to ${matchedInvoices.length} invoice(s)`,
                    matched_invoices: matchedInvoices.length,
                    unmatched_ids: unmatchedIds,
                    total_matched: totalMatched,
                    stripe_amount: stripeAmount,
                    amount_matches: amountMatches,
                    auto_mapped: true,
                    transaction_type: transactionType,
                    invoices: matchedInvoices.map(inv => ({
                        halopsa_id: inv.halopsa_id,
                        invoice_number: inv.invoice_number,
                        amount: inv.total_amount
                    }))
                };
            }

            return {
                success: false,
                message: `Found ${invoiceIds.length} invoice ID(s) but none exist in database`,
                invoice_ids: invoiceIds,
                transaction_type: transactionType,
                auto_mapped: false
            };

        } catch (error) {
            console.error('Error auto-mapping transaction:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Auto-map all unmapped Stripe transactions
     * @returns {Object} - Summary of mapping results
     */
    async autoMapAllTransactions() {
        try {
            // Get all unmapped transactions that have descriptions
            const unmappedTransactions = await this.db.query(
                `SELECT id, description, amount
                 FROM stripe_transactions
                 WHERE (mapped_to_halo = 0 OR mapped_to_halo IS NULL)
                 AND description IS NOT NULL
                 AND description != ''`
            );

            console.log(`Found ${unmappedTransactions.length} unmapped transactions to process`);

            let successCount = 0;
            let failCount = 0;
            let totalInvoicesMatched = 0;
            const results = [];

            for (const transaction of unmappedTransactions) {
                const result = await this.autoMapTransaction(transaction.id);

                if (result.success) {
                    successCount++;
                    totalInvoicesMatched += result.matched_invoices || 0;
                } else {
                    failCount++;
                }

                results.push({
                    transaction_id: transaction.id,
                    description: transaction.description,
                    ...result
                });
            }

            return {
                success: true,
                total: unmappedTransactions.length,
                mapped: successCount,
                failed: failCount,
                invoices_matched: totalInvoicesMatched,
                results: results
            };

        } catch (error) {
            console.error('Error auto-mapping all transactions:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Manually map a Stripe transaction to HaloPSA invoice(s)
     * @param {number} stripeTransactionId - Stripe transaction ID
     * @param {Array<number>} invoiceIds - Array of HaloPSA invoice IDs to map to
     * @returns {Object} - Mapping result
     */
    async manualMapTransaction(stripeTransactionId, invoiceIds) {
        try {
            if (!Array.isArray(invoiceIds) || invoiceIds.length === 0) {
                return { success: false, message: 'No invoice IDs provided' };
            }

            // Verify all invoices exist
            const matchedInvoices = [];
            for (const invoiceId of invoiceIds) {
                const invoice = await this.db.get(
                    'SELECT id, halopsa_id, invoice_number, total_amount FROM halopsa_invoices WHERE id = ?',
                    [invoiceId]
                );

                if (invoice) {
                    matchedInvoices.push(invoice);
                } else {
                    return { success: false, message: `Invoice ID ${invoiceId} not found` };
                }
            }

            // Update primary mapping
            await this.db.run(
                `UPDATE stripe_transactions SET
                 halopsa_invoice_id = ?,
                 mapped_to_halo = 1
                 WHERE id = ?`,
                [matchedInvoices[0].id, stripeTransactionId]
            );

            // Create detailed mapping records
            try {
                for (const invoice of matchedInvoices) {
                    await this.db.run(
                        `INSERT OR REPLACE INTO transaction_invoice_mappings
                         (stripe_transaction_id, halopsa_invoice_id, invoice_amount, auto_mapped, mapping_confidence)
                         VALUES (?, ?, ?, 0, 1.0)`,
                        [stripeTransactionId, invoice.id, invoice.total_amount]
                    );
                }
            } catch (e) {
                console.log('transaction_invoice_mappings table not yet created');
            }

            return {
                success: true,
                message: `Manually mapped to ${matchedInvoices.length} invoice(s)`,
                invoices: matchedInvoices
            };

        } catch (error) {
            console.error('Error manually mapping transaction:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Get unmapped transactions for manual review
     * @returns {Array} - List of unmapped transactions
     */
    async getUnmappedTransactions() {
        try {
            return await this.db.query(
                `SELECT st.*,
                        sc.name as customer_name,
                        sc.email as customer_email
                 FROM stripe_transactions st
                 LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
                 WHERE (st.mapped_to_halo = 0 OR st.mapped_to_halo IS NULL)
                 ORDER BY st.created DESC
                 LIMIT 100`
            );
        } catch (error) {
            console.error('Error getting unmapped transactions:', error);
            return [];
        }
    }

    /**
     * Get deposit transactions summary
     * @returns {Object} - Summary of all deposits
     */
    async getDepositSummary() {
        try {
            const deposits = await this.db.query(
                `SELECT st.*,
                        sc.name as customer_name,
                        sc.email as customer_email,
                        cm.halopsa_client_id,
                        cm.halopsa_client_name
                 FROM stripe_transactions st
                 LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
                 LEFT JOIN customer_mappings cm ON st.customer_id = cm.stripe_customer_id
                 WHERE st.transaction_type = 'deposit'
                 ORDER BY st.created DESC`
            );

            const summary = {
                total_deposits: deposits.length,
                total_amount: deposits.reduce((sum, d) => sum + (d.amount / 100), 0),
                deposits_by_customer: {},
                deposits: deposits.map(d => ({
                    id: d.id,
                    stripe_id: d.stripe_id,
                    customer_name: d.customer_name,
                    halopsa_client_name: d.halopsa_client_name,
                    amount: d.amount / 100,
                    description: d.description,
                    created: new Date(d.created * 1000).toISOString(),
                    has_final_payment: !!d.related_transaction_id
                }))
            };

            // Group by customer
            deposits.forEach(d => {
                const customerName = d.customer_name || 'Unknown';
                if (!summary.deposits_by_customer[customerName]) {
                    summary.deposits_by_customer[customerName] = {
                        count: 0,
                        total: 0,
                        deposits: []
                    };
                }
                summary.deposits_by_customer[customerName].count++;
                summary.deposits_by_customer[customerName].total += d.amount / 100;
                summary.deposits_by_customer[customerName].deposits.push({
                    id: d.id,
                    amount: d.amount / 100,
                    description: d.description,
                    date: new Date(d.created * 1000).toISOString()
                });
            });

            return {
                success: true,
                ...summary
            };
        } catch (error) {
            console.error('Error getting deposit summary:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Get transactions by type
     * @param {string} transactionType - 'deposit', 'payment', or 'final_payment'
     * @returns {Array} - List of transactions
     */
    async getTransactionsByType(transactionType) {
        try {
            return await this.db.query(
                `SELECT st.*,
                        sc.name as customer_name,
                        sc.email as customer_email
                 FROM stripe_transactions st
                 LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
                 WHERE st.transaction_type = ?
                 ORDER BY st.created DESC`,
                [transactionType]
            );
        } catch (error) {
            console.error(`Error getting ${transactionType} transactions:`, error);
            return [];
        }
    }
}

module.exports = TransactionMapper;
