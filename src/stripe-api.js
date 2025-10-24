const Stripe = require('stripe');

class StripeAPI {
    constructor(db) {
        this.db = db;
        this.stripe = null;
        this.initialize();
    }

    async initialize() {
        try {
            const config = await this.getConfig();
            if (config.stripe_secret_key) {
                this.stripe = new Stripe(config.stripe_secret_key);
                console.log('Stripe API initialized successfully');
            }
        } catch (error) {
            console.error('Error initializing Stripe API:', error);
        }
    }

    async getConfig() {
        const configRows = await this.db.all('SELECT key, value FROM config');
        const config = {};
        configRows.forEach(row => {
            config[row.key] = row.value;
        });
        return config;
    }

    async testConnection() {
        try {
            if (!this.stripe) {
                return { success: false, message: 'Stripe not configured - check secret key' };
            }

            const balance = await this.stripe.balance.retrieve();
            return { 
                success: true, 
                message: `Stripe connection successful. Balance: ${balance.available[0].amount / 100} ${balance.available[0].currency}`
            };
        } catch (error) {
            return { success: false, message: `Stripe connection failed: ${error.message}` };
        }
    }

    async syncTransactions() {
        try {
            if (!this.stripe) {
                return { success: false, message: 'Stripe not configured' };
            }

            let syncedCount = 0;
            const latestTransaction = await this.db.get(
                'SELECT MAX(created) as latest FROM stripe_transactions'
            );
            
            const startingAfter = latestTransaction ? latestTransaction.latest : undefined;
            const charges = await this.stripe.charges.list({
                limit: 100,
                starting_after: startingAfter
            });

            for (const charge of charges.data) {
                // Check if transaction already exists
                const existing = await this.db.get(
                    'SELECT id FROM stripe_transactions WHERE stripe_id = ?',
                    [charge.id]
                );

                if (!existing) {
                    await this.db.run(
                        `INSERT INTO stripe_transactions (
                            stripe_id, customer_id, amount, currency, description, 
                            status, created, invoice_id, payment_intent_id, refunded, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [
                            charge.id,
                            charge.customer,
                            charge.amount,
                            charge.currency,
                            charge.description,
                            charge.status,
                            charge.created,
                            charge.invoice,
                            charge.payment_intent,
                            charge.refunded,
                            JSON.stringify(charge)
                        ]
                    );
                    syncedCount++;
                }
            }

            return { 
                success: true, 
                message: `Synced ${syncedCount} new transactions`,
                count: syncedCount 
            };
        } catch (error) {
            console.error('Error syncing Stripe transactions:', error);
            return { success: false, message: `Error syncing transactions: ${error.message}` };
        }
    }

    async getCustomers() {
        try {
            if (!this.stripe) {
                return [];
            }

            const customers = await this.stripe.customers.list({ limit: 100 });
            return customers.data.map(customer => ({
                id: customer.id,
                email: customer.email,
                name: customer.name,
                description: customer.description,
                created: customer.created,
                metadata: customer.metadata
            }));
        } catch (error) {
            console.error('Error fetching Stripe customers:', error);
            return [];
        }
    }

    async getTransactionDetails(transactionId) {
        try {
            if (!this.stripe) {
                return null;
            }

            const charge = await this.stripe.charges.retrieve(transactionId);
            return charge;
        } catch (error) {
            console.error('Error fetching transaction details:', error);
            return null;
        }
    }

    async mapTransactionToCustomer(transactionId, haloClientId) {
        try {
            const transaction = await this.db.get(
                'SELECT * FROM stripe_transactions WHERE stripe_id = ?',
                [transactionId]
            );

            if (!transaction) {
                return { success: false, message: 'Transaction not found' };
            }

            // Update transaction mapping
            await this.db.run(
                'UPDATE stripe_transactions SET mapped_to_halo = ? WHERE stripe_id = ?',
                [true, transactionId]
            );

            // Update customer mapping if customer exists
            if (transaction.customer_id) {
                await this.db.run(
                    `INSERT OR REPLACE INTO customer_mappings 
                    (stripe_customer_id, halopsa_client_id, mapping_confirmed, updated_at) 
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
                    [transaction.customer_id, haloClientId, true]
                );
            }

            return { success: true, message: 'Transaction mapped successfully' };
        } catch (error) {
            console.error('Error mapping transaction:', error);
            return { success: false, message: 'Error mapping transaction' };
        }
    }

    async getDashboardStats() {
        try {
            const totalTransactions = await this.db.get(
                'SELECT COUNT(*) as count FROM stripe_transactions'
            );
            
            const mappedTransactions = await this.db.get(
                'SELECT COUNT(*) as count FROM stripe_transactions WHERE mapped_to_halo = 1'
            );
            
            const totalCustomers = await this.db.get(
                'SELECT COUNT(DISTINCT customer_id) as count FROM stripe_transactions WHERE customer_id IS NOT NULL'
            );
            
            const mappedCustomers = await this.db.get(
                'SELECT COUNT(*) as count FROM customer_mappings WHERE mapping_confirmed = 1'
            );

            return {
                totalTransactions: totalTransactions.count,
                mappedTransactions: mappedTransactions.count,
                totalCustomers: totalCustomers.count,
                mappedCustomers: mappedCustomers.count,
                syncRate: totalTransactions.count > 0 ? 
                    (mappedTransactions.count / totalTransactions.count * 100).toFixed(1) : 0
            };
        } catch (error) {
            console.error('Error fetching dashboard stats:', error);
            return {};
        }
    }
}

module.exports = StripeAPI;
