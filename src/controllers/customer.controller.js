/**
 * Customer Controller
 * Handles customer mapping between Stripe and HaloPSA
 */

class CustomerController {
    constructor(db) {
        this.db = db;
    }

    /**
     * Get all customer mappings
     */
    async getMappings(req, res) {
        try {
            const mappings = await this.db.all(`
                SELECT
                    cm.*,
                    sc.name as stripe_name,
                    sc.email as stripe_email,
                    hc.name as halopsa_name
                FROM customer_mappings cm
                LEFT JOIN stripe_customers sc ON cm.stripe_customer_id = sc.stripe_id
                LEFT JOIN halopsa_clients hc ON cm.halopsa_client_id = hc.halopsa_id
                ORDER BY cm.created_at DESC
            `);
            res.json(mappings);
        } catch (error) {
            console.error('Error fetching mappings:', error);
            res.status(500).json({ error: 'Failed to fetch customer mappings' });
        }
    }

    /**
     * Create manual customer mapping
     */
    async createMapping(req, res) {
        try {
            const { stripe_customer_id, halopsa_client_id } = req.body;

            if (!stripe_customer_id || !halopsa_client_id) {
                return res.status(400).json({ error: 'Missing required fields' });
            }

            // Get customer details
            const stripeCustomer = await this.db.get(
                'SELECT * FROM stripe_customers WHERE stripe_id = ?',
                [stripe_customer_id]
            );

            const haloPsaClient = await this.db.get(
                'SELECT * FROM halopsa_clients WHERE halopsa_id = ?',
                [halopsa_client_id]
            );

            if (!stripeCustomer || !haloPsaClient) {
                return res.status(404).json({ error: 'Customer or client not found' });
            }

            // Create mapping
            await this.db.run(`
                INSERT OR REPLACE INTO customer_mappings
                (stripe_customer_id, stripe_customer_email, stripe_customer_name,
                 halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            `, [
                stripe_customer_id,
                stripeCustomer.email,
                stripeCustomer.name,
                halopsa_client_id,
                haloPsaClient.name,
                false,
                true
            ]);

            res.json({ success: true, message: 'Mapping created successfully' });
        } catch (error) {
            console.error('Error creating mapping:', error);
            res.status(500).json({ error: 'Failed to create mapping' });
        }
    }

    /**
     * Delete customer mapping
     */
    async deleteMapping(req, res) {
        try {
            const { stripe_customer_id } = req.params;

            await this.db.run(
                'DELETE FROM customer_mappings WHERE stripe_customer_id = ?',
                [stripe_customer_id]
            );

            res.json({ success: true, message: 'Mapping deleted successfully' });
        } catch (error) {
            console.error('Error deleting mapping:', error);
            res.status(500).json({ error: 'Failed to delete mapping' });
        }
    }

    /**
     * Auto-match customers using fuzzy matching
     */
    async autoMatch(req, res) {
        try {
            // Get all Stripe customers without mappings
            const stripeCustomers = await this.db.all(`
                SELECT sc.* FROM stripe_customers sc
                LEFT JOIN customer_mappings cm ON sc.stripe_id = cm.stripe_customer_id
                WHERE cm.id IS NULL
            `);

            // Get all HaloPSA clients
            const haloPsaClients = await this.db.all('SELECT * FROM halopsa_clients');

            const matches = [];
            const threshold = 0.85;

            for (const stripeCustomer of stripeCustomers) {
                let bestMatch = null;
                let bestScore = 0;

                for (const haloPsaClient of haloPsaClients) {
                    const score = this.calculateMatchScore(stripeCustomer, haloPsaClient);

                    if (score > bestScore) {
                        bestScore = score;
                        bestMatch = haloPsaClient;
                    }
                }

                if (bestScore >= threshold && bestMatch) {
                    matches.push({
                        stripe_customer: stripeCustomer,
                        halopsa_client: bestMatch,
                        confidence: bestScore,
                        auto_mapped: true
                    });

                    // Create the mapping
                    await this.db.run(`
                        INSERT OR IGNORE INTO customer_mappings
                        (stripe_customer_id, stripe_customer_email, stripe_customer_name,
                         halopsa_client_id, halopsa_client_name, auto_mapped, mapping_confirmed)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    `, [
                        stripeCustomer.stripe_id,
                        stripeCustomer.email,
                        stripeCustomer.name,
                        bestMatch.halopsa_id,
                        bestMatch.name,
                        true,
                        false
                    ]);
                }
            }

            res.json({
                success: true,
                message: `Auto-matched ${matches.length} customers`,
                matches: matches.length,
                details: matches
            });
        } catch (error) {
            console.error('Error auto-matching customers:', error);
            res.status(500).json({ error: 'Failed to auto-match customers' });
        }
    }

    /**
     * Calculate match score between Stripe customer and HaloPSA client
     */
    calculateMatchScore(stripeCustomer, haloPsaClient) {
        let score = 0;
        let factors = 0;

        // Email exact match (highest weight)
        if (stripeCustomer.email && haloPsaClient.email) {
            if (stripeCustomer.email.toLowerCase() === haloPsaClient.email.toLowerCase()) {
                score += 1.0;
            }
            factors++;
        }

        // Name similarity using Levenshtein distance
        if (stripeCustomer.name && haloPsaClient.name) {
            const similarity = this.stringSimilarity(
                stripeCustomer.name.toLowerCase(),
                haloPsaClient.name.toLowerCase()
            );
            score += similarity;
            factors++;
        }

        return factors > 0 ? score / factors : 0;
    }

    /**
     * Calculate string similarity using Levenshtein distance
     */
    stringSimilarity(str1, str2) {
        const longer = str1.length > str2.length ? str1 : str2;
        const shorter = str1.length > str2.length ? str2 : str1;

        if (longer.length === 0) {
            return 1.0;
        }

        const editDistance = this.levenshteinDistance(longer, shorter);
        return (longer.length - editDistance) / longer.length;
    }

    /**
     * Calculate Levenshtein distance
     */
    levenshteinDistance(str1, str2) {
        const matrix = [];

        for (let i = 0; i <= str2.length; i++) {
            matrix[i] = [i];
        }

        for (let j = 0; j <= str1.length; j++) {
            matrix[0][j] = j;
        }

        for (let i = 1; i <= str2.length; i++) {
            for (let j = 1; j <= str1.length; j++) {
                if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
                    matrix[i][j] = matrix[i - 1][j - 1];
                } else {
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j - 1] + 1,
                        matrix[i][j - 1] + 1,
                        matrix[i - 1][j] + 1
                    );
                }
            }
        }

        return matrix[str2.length][str1.length];
    }

    /**
     * Get customer view (combined data from all sources)
     */
    async getCustomerView(req, res) {
        try {
            const { halopsa_client_id } = req.params;

            // Get client details
            const client = await this.db.get(
                'SELECT * FROM halopsa_clients WHERE halopsa_id = ?',
                [halopsa_client_id]
            );

            if (!client) {
                return res.status(404).json({ error: 'Client not found' });
            }

            // Get mapped Stripe customer
            const mapping = await this.db.get(
                'SELECT * FROM customer_mappings WHERE halopsa_client_id = ?',
                [halopsa_client_id]
            );

            let stripeData = null;
            if (mapping) {
                stripeData = await this.db.get(
                    'SELECT * FROM stripe_customers WHERE stripe_id = ?',
                    [mapping.stripe_customer_id]
                );
            }

            // Get transactions
            const transactions = await this.db.all(
                'SELECT * FROM halopsa_transactions WHERE client_id = ? ORDER BY date DESC LIMIT 10',
                [halopsa_client_id]
            );

            // Get invoices
            const invoices = await this.db.all(
                'SELECT * FROM halopsa_invoices WHERE halopsa_client_id = ? ORDER BY invoice_date DESC LIMIT 10',
                [halopsa_client_id]
            );

            res.json({
                client,
                stripe_customer: stripeData,
                transactions,
                invoices,
                mapping
            });
        } catch (error) {
            console.error('Error fetching customer view:', error);
            res.status(500).json({ error: 'Failed to fetch customer view' });
        }
    }

    /**
     * Get customer overview
     */
    async getCustomerOverview(req, res) {
        try {
            const clients = await this.db.all(`
                SELECT
                    hc.*,
                    cm.stripe_customer_id,
                    cm.mapping_confirmed,
                    sc.name as stripe_name,
                    sc.email as stripe_email,
                    COUNT(DISTINCT hi.id) as invoice_count,
                    SUM(hi.total_amount) as total_revenue
                FROM halopsa_clients hc
                LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id
                LEFT JOIN stripe_customers sc ON cm.stripe_customer_id = sc.stripe_id
                LEFT JOIN halopsa_invoices hi ON hc.halopsa_id = hi.halopsa_client_id
                GROUP BY hc.halopsa_id
                ORDER BY hc.name
            `);

            res.json(clients);
        } catch (error) {
            console.error('Error fetching customer overview:', error);
            res.status(500).json({ error: 'Failed to fetch customer overview' });
        }
    }
}

module.exports = CustomerController;
