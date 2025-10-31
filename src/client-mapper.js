/**
 * Client Mapper - Three-way mapping between Stripe, HaloPSA, and QuickBooks clients
 * Auto-maps based on email, name similarity, and provides manual mapping interface
 */

class ClientMapper {
    constructor(db) {
        this.db = db;
    }

    /**
     * Calculate Levenshtein distance for fuzzy name matching
     */
    levenshteinDistance(str1, str2) {
        const s1 = (str1 || '').toLowerCase();
        const s2 = (str2 || '').toLowerCase();

        const matrix = [];

        for (let i = 0; i <= s2.length; i++) {
            matrix[i] = [i];
        }

        for (let j = 0; j <= s1.length; j++) {
            matrix[0][j] = j;
        }

        for (let i = 1; i <= s2.length; i++) {
            for (let j = 1; j <= s1.length; j++) {
                if (s2.charAt(i - 1) === s1.charAt(j - 1)) {
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

        return matrix[s2.length][s1.length];
    }

    /**
     * Calculate similarity score (0-1) between two names
     */
    nameSimilarity(name1, name2) {
        if (!name1 || !name2) return 0;

        const distance = this.levenshteinDistance(name1, name2);
        const maxLength = Math.max(name1.length, name2.length);

        return maxLength > 0 ? (1 - distance / maxLength) : 0;
    }

    /**
     * Auto-map Stripe customers to HaloPSA clients
     * @param {number} threshold - Confidence threshold (0-1, default 0.85)
     * @returns {Object} - Mapping results
     */
    async autoMapStripeToHaloPSA(threshold = 0.85) {
        try {
            // Get all Stripe customers
            const stripeCustomers = await this.db.query(
                'SELECT id, stripe_id, email, name FROM stripe_customers'
            );

            // Get all HaloPSA clients
            const halopsaClients = await this.db.query(
                'SELECT id, halopsa_id, name, email FROM halopsa_clients WHERE inactive = 0'
            );

            console.log(`Auto-mapping ${stripeCustomers.length} Stripe customers to ${halopsaClients.length} HaloPSA clients...`);

            let mapped = 0;
            let skipped = 0;
            const mappings = [];

            for (const stripeCustomer of stripeCustomers) {
                // Check if already mapped
                const existingMapping = await this.db.get(
                    'SELECT id FROM customer_mappings WHERE stripe_customer_id = ?',
                    [stripeCustomer.stripe_id]
                );

                if (existingMapping) {
                    skipped++;
                    continue;
                }

                let bestMatch = null;
                let bestScore = 0;

                for (const halopsaClient of halopsaClients) {
                    let score = 0;

                    // Email exact match = 1.0
                    if (stripeCustomer.email && halopsaClient.email &&
                        stripeCustomer.email.toLowerCase() === halopsaClient.email.toLowerCase()) {
                        score = 1.0;
                    } else {
                        // Name similarity
                        const nameScore = this.nameSimilarity(stripeCustomer.name, halopsaClient.name);
                        score = nameScore * 0.9; // Slightly lower weight for name-only matches
                    }

                    if (score > bestScore) {
                        bestScore = score;
                        bestMatch = halopsaClient;
                    }
                }

                if (bestMatch && bestScore >= threshold) {
                    // Create mapping
                    await this.db.run(
                        `INSERT INTO customer_mappings
                         (stripe_customer_id, stripe_customer_email, stripe_customer_name,
                          halopsa_client_id, halopsa_client_name,
                          auto_mapped, mapping_confirmed, mapping_source)
                         VALUES (?, ?, ?, ?, ?, 1, 0, ?)`,
                        [
                            stripeCustomer.stripe_id,
                            stripeCustomer.email,
                            stripeCustomer.name,
                            bestMatch.halopsa_id,
                            bestMatch.name,
                            `auto-${bestScore.toFixed(2)}`
                        ]
                    );

                    mapped++;
                    mappings.push({
                        stripe: stripeCustomer.name,
                        halopsa: bestMatch.name,
                        confidence: bestScore
                    });
                }
            }

            return {
                success: true,
                total: stripeCustomers.length,
                mapped: mapped,
                skipped: skipped,
                threshold: threshold,
                mappings: mappings
            };

        } catch (error) {
            console.error('Error auto-mapping Stripe to HaloPSA:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Auto-map HaloPSA clients to QuickBooks customers
     * @param {number} threshold - Confidence threshold (0-1, default 0.85)
     * @returns {Object} - Mapping results
     */
    async autoMapHaloPSAToQuickBooks(threshold = 0.85) {
        try {
            // Get all HaloPSA clients
            const halopsaClients = await this.db.query(
                'SELECT id, halopsa_id, name, email FROM halopsa_clients WHERE inactive = 0'
            );

            // Get all QuickBooks customers
            const qbCustomers = await this.db.query(
                'SELECT id, qb_list_id, qb_full_name, company_name, email FROM qb_customers WHERE is_active = 1'
            );

            console.log(`Auto-mapping ${halopsaClients.length} HaloPSA clients to ${qbCustomers.length} QB customers...`);

            let mapped = 0;
            let updated = 0;

            for (const halopsaClient of halopsaClients) {
                let bestMatch = null;
                let bestScore = 0;

                for (const qbCustomer of qbCustomers) {
                    let score = 0;

                    // Email exact match
                    if (halopsaClient.email && qbCustomer.email &&
                        halopsaClient.email.toLowerCase() === qbCustomer.email.toLowerCase()) {
                        score = 1.0;
                    } else {
                        // Name similarity - check both full_name and company_name
                        const fullNameScore = this.nameSimilarity(halopsaClient.name, qbCustomer.qb_full_name);
                        const companyScore = this.nameSimilarity(halopsaClient.name, qbCustomer.company_name);
                        score = Math.max(fullNameScore, companyScore) * 0.9;
                    }

                    if (score > bestScore) {
                        bestScore = score;
                        bestMatch = qbCustomer;
                    }
                }

                if (bestMatch && bestScore >= threshold) {
                    // Check if mapping exists
                    const existingMapping = await this.db.get(
                        'SELECT id FROM customer_mappings WHERE halopsa_client_id = ?',
                        [halopsaClient.halopsa_id]
                    );

                    if (existingMapping) {
                        // Update existing mapping
                        await this.db.run(
                            `UPDATE customer_mappings SET
                             qb_customer_id = ?,
                             qb_customer_name = ?,
                             updated_at = CURRENT_TIMESTAMP
                             WHERE id = ?`,
                            [bestMatch.qb_list_id, bestMatch.qb_full_name, existingMapping.id]
                        );
                        updated++;
                    } else {
                        // Create new mapping
                        await this.db.run(
                            `INSERT INTO customer_mappings
                             (halopsa_client_id, halopsa_client_name,
                              qb_customer_id, qb_customer_name,
                              auto_mapped, mapping_confirmed, mapping_source)
                             VALUES (?, ?, ?, ?, 1, 0, ?)`,
                            [
                                halopsaClient.halopsa_id,
                                halopsaClient.name,
                                bestMatch.qb_list_id,
                                bestMatch.qb_full_name,
                                `auto-qb-${bestScore.toFixed(2)}`
                            ]
                        );
                        mapped++;
                    }
                }
            }

            return {
                success: true,
                total: halopsaClients.length,
                mapped: mapped,
                updated: updated,
                threshold: threshold
            };

        } catch (error) {
            console.error('Error auto-mapping HaloPSA to QuickBooks:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Get unmapped customers/clients for manual mapping
     * @param {string} source - 'stripe', 'halopsa', or 'quickbooks'
     * @returns {Array} - List of unmapped customers
     */
    async getUnmappedCustomers(source = 'stripe') {
        try {
            if (source === 'stripe') {
                return await this.db.query(
                    `SELECT sc.*
                     FROM stripe_customers sc
                     LEFT JOIN customer_mappings cm ON sc.stripe_id = cm.stripe_customer_id
                     WHERE cm.id IS NULL
                     ORDER BY sc.name
                     LIMIT 100`
                );
            } else if (source === 'halopsa') {
                return await this.db.query(
                    `SELECT hc.*
                     FROM halopsa_clients hc
                     LEFT JOIN customer_mappings cm ON hc.halopsa_id = cm.halopsa_client_id
                     WHERE cm.id IS NULL AND hc.inactive = 0
                     ORDER BY hc.name
                     LIMIT 100`
                );
            } else if (source === 'quickbooks') {
                return await this.db.query(
                    `SELECT qc.*
                     FROM qb_customers qc
                     LEFT JOIN customer_mappings cm ON qc.qb_list_id = cm.qb_customer_id
                     WHERE cm.id IS NULL AND qc.is_active = 1
                     ORDER BY qc.qb_full_name
                     LIMIT 100`
                );
            }

            return [];
        } catch (error) {
            console.error('Error getting unmapped customers:', error);
            return [];
        }
    }

    /**
     * Manually create a customer mapping
     * @param {Object} mapping - { stripe_id, halopsa_id, qb_id }
     * @returns {Object} - Result
     */
    async manualMapCustomer(mapping) {
        try {
            const { stripe_id, halopsa_id, qb_id } = mapping;

            // Get customer details
            let stripeData = null, halopsaData = null, qbData = null;

            if (stripe_id) {
                stripeData = await this.db.get(
                    'SELECT stripe_id, email, name FROM stripe_customers WHERE stripe_id = ?',
                    [stripe_id]
                );
            }

            if (halopsa_id) {
                halopsaData = await this.db.get(
                    'SELECT halopsa_id, name FROM halopsa_clients WHERE halopsa_id = ?',
                    [halopsa_id]
                );
            }

            if (qb_id) {
                qbData = await this.db.get(
                    'SELECT qb_list_id, qb_full_name FROM qb_customers WHERE qb_list_id = ?',
                    [qb_id]
                );
            }

            // Insert or update mapping
            await this.db.run(
                `INSERT INTO customer_mappings
                 (stripe_customer_id, stripe_customer_email, stripe_customer_name,
                  halopsa_client_id, halopsa_client_name,
                  qb_customer_id, qb_customer_name,
                  auto_mapped, mapping_confirmed, mapping_source)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 'manual')
                 ON CONFLICT(stripe_customer_id, halopsa_client_id, qb_customer_id) DO UPDATE SET
                 updated_at = CURRENT_TIMESTAMP,
                 mapping_confirmed = 1`,
                [
                    stripeData?.stripe_id || null,
                    stripeData?.email || null,
                    stripeData?.name || null,
                    halopsaData?.halopsa_id || null,
                    halopsaData?.name || null,
                    qbData?.qb_list_id || null,
                    qbData?.qb_full_name || null
                ]
            );

            return {
                success: true,
                message: 'Customer mapping created',
                mapping: {
                    stripe: stripeData?.name,
                    halopsa: halopsaData?.name,
                    quickbooks: qbData?.qb_full_name
                }
            };

        } catch (error) {
            console.error('Error creating manual mapping:', error);
            return { success: false, message: error.message };
        }
    }

    /**
     * Get all customer mappings with details
     * @returns {Array} - Complete mapping details
     */
    async getAllMappings() {
        try {
            return await this.db.query(
                `SELECT
                    cm.*,
                    sc.name as stripe_name_full,
                    sc.email as stripe_email_full,
                    hc.name as halopsa_name_full,
                    hc.email as halopsa_email_full,
                    qc.qb_full_name as qb_name_full,
                    qc.email as qb_email_full
                 FROM customer_mappings cm
                 LEFT JOIN stripe_customers sc ON cm.stripe_customer_id = sc.stripe_id
                 LEFT JOIN halopsa_clients hc ON cm.halopsa_client_id = hc.halopsa_id
                 LEFT JOIN qb_customers qc ON cm.qb_customer_id = qc.qb_list_id
                 ORDER BY cm.updated_at DESC`
            );
        } catch (error) {
            console.error('Error getting all mappings:', error);
            return [];
        }
    }
}

module.exports = ClientMapper;
