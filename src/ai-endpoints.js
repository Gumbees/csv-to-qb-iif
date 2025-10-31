/**
 * AI Suggestions API Endpoints
 * Centralized endpoints for AI-powered mapping suggestions
 */

/**
 * Register all AI-related API endpoints
 * @param {Express.Application} app - Express app instance
 * @param {AIService} aiService - AI service instance
 * @param {DatabaseManager} db - Database instance
 */
function registerAIEndpoints(app, aiService, db) {

    // =============================================================================
    // AI SUGGESTION STATISTICS
    // =============================================================================

    /**
     * GET /api/ai/stats
     * Get statistics about AI suggestions (pending counts, confidence breakdown)
     */
    app.get('/api/ai/stats', async (req, res) => {
        try {
            const stats = aiService.getSuggestionStats();
            res.json({ success: true, stats });
        } catch (error) {
            console.error('Error getting AI stats:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    /**
     * GET /api/ai/suggestions
     * Get all pending suggestions (both customer and transaction)
     */
    app.get('/api/ai/suggestions', async (req, res) => {
        try {
            const type = req.query.type || 'all'; // 'customer', 'transaction', or 'all'
            const suggestions = aiService.getPendingSuggestions(type);
            res.json({ success: true, suggestions });
        } catch (error) {
            console.error('Error getting AI suggestions:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =============================================================================
    // BULK OPERATIONS
    // =============================================================================

    /**
     * POST /api/ai/bulk/approve-high-confidence
     * Bulk approve all high confidence suggestions
     */
    app.post('/api/ai/bulk/approve-high-confidence', async (req, res) => {
        try {
            const { type, threshold } = req.body; // type: 'customer' or 'transaction', threshold: number

            if (!type || !['customer', 'transaction'].includes(type)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid type. Must be "customer" or "transaction"'
                });
            }

            const result = aiService.bulkApproveHighConfidence(type, threshold || 0.9);
            res.json(result);
        } catch (error) {
            console.error('Error in bulk approve:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    /**
     * POST /api/ai/bulk/reject-low-confidence
     * Bulk reject all low confidence suggestions
     */
    app.post('/api/ai/bulk/reject-low-confidence', async (req, res) => {
        try {
            const { type, threshold } = req.body; // type: 'customer' or 'transaction', threshold: number

            if (!type || !['customer', 'transaction'].includes(type)) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid type. Must be "customer" or "transaction"'
                });
            }

            const result = aiService.bulkRejectLowConfidence(type, threshold || 0.5);
            res.json(result);
        } catch (error) {
            console.error('Error in bulk reject:', error);
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =============================================================================
    // AUTO-TRIGGER HELPERS
    // =============================================================================

    /**
     * Automatically trigger AI customer mapping suggestions after data import
     * Only runs if auto_suggest_enabled is true
     */
    async function autoTriggerCustomerSuggestions() {
        try {
            if (!aiService.isReady()) {
                console.log('AI service not ready, skipping auto-suggestions');
                return;
            }

            if (!aiService.isAutoSuggestEnabled()) {
                console.log('Auto-suggest disabled, skipping');
                return;
            }

            console.log('Auto-triggering AI customer mapping suggestions...');

            // Get unmapped customers from all systems
            const unmappedStripe = await db.all(`
                SELECT sc.* FROM stripe_customers sc
                WHERE NOT EXISTS (
                    SELECT 1 FROM customer_mappings cm
                    WHERE cm.stripe_customer_id = sc.stripe_id
                )
                LIMIT 100
            `);

            const unmappedHalo = await db.all(`
                SELECT hc.* FROM halopsa_clients hc
                WHERE NOT EXISTS (
                    SELECT 1 FROM customer_mappings cm
                    WHERE cm.halopsa_client_id = hc.halopsa_id
                )
                LIMIT 100
            `);

            const unmappedQB = await db.all(`
                SELECT qc.* FROM qb_customers qc
                WHERE NOT EXISTS (
                    SELECT 1 FROM customer_mappings cm
                    WHERE cm.qb_customer_id = qc.qb_list_id
                )
                LIMIT 100
            `);

            if (unmappedStripe.length === 0 && unmappedHalo.length === 0 && unmappedQB.length === 0) {
                console.log('No unmapped customers found, skipping AI suggestions');
                return;
            }

            const suggestions = await aiService.suggestCustomerMappings(
                unmappedStripe,
                unmappedHalo,
                unmappedQB
            );

            const stored = aiService.storeSuggestions(suggestions);
            console.log(`✓ Auto-generated ${stored} customer mapping suggestions`);

        } catch (error) {
            console.error('Error in auto-trigger customer suggestions:', error);
            // Don't throw - this is a background task
        }
    }

    /**
     * Automatically trigger AI transaction mapping suggestions after transaction import
     * Only runs if auto_suggest_enabled is true
     */
    async function autoTriggerTransactionSuggestions() {
        try {
            if (!aiService.isReady()) {
                console.log('AI service not ready, skipping auto-suggestions');
                return;
            }

            if (!aiService.isAutoSuggestEnabled()) {
                console.log('Auto-suggest disabled, skipping');
                return;
            }

            console.log('Auto-triggering AI transaction mapping suggestions...');
            const result = await aiService.suggestTransactionMappings();

            if (result.success && result.count > 0) {
                console.log(`✓ Auto-generated ${result.count} transaction mapping suggestions`);
            }

        } catch (error) {
            console.error('Error in auto-trigger transaction suggestions:', error);
            // Don't throw - this is a background task
        }
    }

    // Export helper functions for use in import endpoints
    return {
        autoTriggerCustomerSuggestions,
        autoTriggerTransactionSuggestions
    };
}

module.exports = registerAIEndpoints;
