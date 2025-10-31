/**
 * Stripe Controller
 * Handles Stripe API integration
 */

class StripeController {
    constructor(db, stripeAPI) {
        this.db = db;
        this.stripeAPI = stripeAPI;
    }

    /**
     * Test Stripe connection
     */
    async testConnection(req, res) {
        try {
            const result = await this.stripeAPI.testConnection();
            res.json(result);
        } catch (error) {
            console.error('Stripe connection test error:', error);
            res.status(500).json({ error: 'Failed to test Stripe connection' });
        }
    }

    /**
     * Import Stripe customers
     */
    async importCustomers(req, res) {
        try {
            const result = await this.stripeAPI.importCustomers();
            res.json(result);
        } catch (error) {
            console.error('Error importing Stripe customers:', error);
            res.status(500).json({ error: 'Failed to import customers' });
        }
    }

    /**
     * Import Stripe transactions
     */
    async importTransactions(req, res) {
        try {
            const { importAll } = req.body;
            const result = await this.stripeAPI.importTransactions(null, importAll);
            res.json(result);
        } catch (error) {
            console.error('Error importing Stripe transactions:', error);
            res.status(500).json({ error: 'Failed to import transactions' });
        }
    }

    /**
     * Get imported Stripe customers with optional filtering
     */
    async getImportedCustomers(req, res) {
        try {
            const { filter = 'all' } = req.query;

            // Build query based on filter
            let query = `
                SELECT
                    sc.*,
                    cm.id as mapping_id,
                    cm.halopsa_client_id,
                    cm.halopsa_client_name,
                    cm.mapping_confirmed
                FROM stripe_customers sc
                LEFT JOIN customer_mappings cm ON sc.stripe_id = cm.stripe_customer_id
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

            query += ' ORDER BY sc.name ASC';

            const customers = this.db.all(query);
            res.json(customers);
        } catch (error) {
            console.error('Error fetching imported customers:', error);
            res.status(500).json({ error: 'Failed to fetch customers' });
        }
    }

    /**
     * Get imported Stripe transactions
     */
    async getImportedTransactions(req, res) {
        try {
            const transactions = await this.stripeAPI.getImportedTransactions();
            res.json(transactions);
        } catch (error) {
            console.error('Error fetching imported transactions:', error);
            res.status(500).json({ error: 'Failed to fetch transactions' });
        }
    }

    /**
     * Get Stripe customer by ID
     */
    async getCustomerById(req, res) {
        try {
            const { id } = req.params;
            const customer = await this.db.get(
                'SELECT * FROM stripe_customers WHERE stripe_id = ?',
                [id]
            );

            if (!customer) {
                return res.status(404).json({ error: 'Customer not found' });
            }

            res.json(customer);
        } catch (error) {
            console.error('Error fetching customer:', error);
            res.status(500).json({ error: 'Failed to fetch customer' });
        }
    }

    /**
     * Get transactions for a Stripe customer
     */
    async getCustomerTransactions(req, res) {
        try {
            const { id } = req.params;
            const transactions = await this.db.all(
                'SELECT * FROM stripe_transactions WHERE customer_id = ? ORDER BY created DESC',
                [id]
            );
            res.json(transactions);
        } catch (error) {
            console.error('Error fetching customer transactions:', error);
            res.status(500).json({ error: 'Failed to fetch transactions' });
        }
    }

    /**
     * Handle Stripe webhook
     */
    async handleWebhook(req, res) {
        try {
            // TODO: Implement webhook signature verification
            const event = req.body;

            console.log('Stripe webhook received:', event.type);

            // Handle different event types
            switch (event.type) {
                case 'customer.created':
                case 'customer.updated':
                    // Handle customer events
                    break;
                case 'charge.succeeded':
                case 'charge.failed':
                    // Handle transaction events
                    break;
                default:
                    console.log(`Unhandled event type: ${event.type}`);
            }

            res.json({ received: true });
        } catch (error) {
            console.error('Webhook error:', error);
            res.status(500).json({ error: 'Webhook processing failed' });
        }
    }
}

module.exports = StripeController;
