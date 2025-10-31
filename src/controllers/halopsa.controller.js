/**
 * HaloPSA Controller
 * Handles HaloPSA API integration
 */

class HaloPSAController {
    constructor(db, haloPsaAPI) {
        this.db = db;
        this.haloPsaAPI = haloPsaAPI;
    }

    /**
     * Test HaloPSA connection
     */
    async testConnection(req, res) {
        try {
            const result = await this.haloPsaAPI.testConnection();
            res.json(result);
        } catch (error) {
            console.error('HaloPSA connection test error:', error);
            res.status(500).json({ error: 'Failed to test HaloPSA connection' });
        }
    }

    /**
     * Get HaloPSA clients
     */
    async getClients(req, res) {
        try {
            const clients = await this.haloPsaAPI.getClients();
            res.json(clients);
        } catch (error) {
            console.error('Error fetching HaloPSA clients:', error);
            res.status(500).json({ error: 'Failed to fetch clients' });
        }
    }

    /**
     * Import HaloPSA clients
     */
    async importClients(req, res) {
        try {
            const result = await this.haloPsaAPI.importClientsFromHaloPSA();
            res.json(result);
        } catch (error) {
            console.error('Error importing HaloPSA clients:', error);
            res.status(500).json({ error: 'Failed to import clients' });
        }
    }

    /**
     * Import HaloPSA invoices
     */
    async importInvoices(req, res) {
        try {
            const result = await this.haloPsaAPI.importInvoices();
            res.json(result);
        } catch (error) {
            console.error('Error importing HaloPSA invoices:', error);
            res.status(500).json({ error: 'Failed to import invoices' });
        }
    }

    /**
     * Get HaloPSA invoices from database
     */
    async getInvoices(req, res) {
        try {
            const invoices = await this.db.all(
                'SELECT * FROM halopsa_invoices ORDER BY invoice_date DESC LIMIT 100'
            );
            res.json(invoices);
        } catch (error) {
            console.error('Error fetching invoices:', error);
            res.status(500).json({ error: 'Failed to fetch invoices' });
        }
    }

    /**
     * Get single invoice by ID
     */
    async getInvoiceById(req, res) {
        try {
            const { id } = req.params;
            const invoice = await this.db.get(
                'SELECT * FROM halopsa_invoices WHERE halopsa_id = ?',
                [id]
            );

            if (!invoice) {
                return res.status(404).json({ error: 'Invoice not found' });
            }

            res.json(invoice);
        } catch (error) {
            console.error('Error fetching invoice:', error);
            res.status(500).json({ error: 'Failed to fetch invoice' });
        }
    }

    /**
     * Get client transactions
     */
    async getClientTransactions(req, res) {
        try {
            const { client_id } = req.params;
            const transactions = await this.db.all(
                'SELECT * FROM halopsa_transactions WHERE client_id = ? ORDER BY date DESC',
                [client_id]
            );
            res.json(transactions);
        } catch (error) {
            console.error('Error fetching client transactions:', error);
            res.status(500).json({ error: 'Failed to fetch transactions' });
        }
    }

    /**
     * Get client purchase orders
     */
    async getClientPurchaseOrders(req, res) {
        try {
            const { client_id } = req.params;
            const pos = await this.db.all(
                'SELECT * FROM halopsa_purchase_orders WHERE halopsa_client_id = ? ORDER BY po_date DESC',
                [client_id]
            );
            res.json(pos);
        } catch (error) {
            console.error('Error fetching purchase orders:', error);
            res.status(500).json({ error: 'Failed to fetch purchase orders' });
        }
    }

    /**
     * Import HaloPSA purchase orders
     */
    async importPurchaseOrders(req, res) {
        try {
            const result = await this.haloPsaAPI.importPurchaseOrders();
            res.json(result);
        } catch (error) {
            console.error('Error importing HaloPSA purchase orders:', error);
            res.status(500).json({ error: 'Failed to import purchase orders' });
        }
    }

    /**
     * Get HaloPSA purchase orders from database
     */
    async getPurchaseOrders(req, res) {
        try {
            const pos = await this.db.all(
                'SELECT * FROM halopsa_purchase_orders ORDER BY po_date DESC LIMIT 100'
            );
            res.json(pos);
        } catch (error) {
            console.error('Error fetching purchase orders:', error);
            res.status(500).json({ error: 'Failed to fetch purchase orders' });
        }
    }

    /**
     * Obtain OAuth token
     */
    async obtainToken(req, res) {
        try {
            const tokenResult = await this.haloPsaAPI.getAccessTokenViaClientCredentials();
            res.json(tokenResult);
        } catch (error) {
            console.error('Error obtaining token:', error);
            res.status(500).json({ error: 'Failed to obtain token' });
        }
    }
}

module.exports = HaloPSAController;
