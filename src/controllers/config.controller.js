/**
 * Configuration Controller
 * Handles all configuration-related business logic
 */

class ConfigController {
    constructor(db, configAPI) {
        this.db = db;
        this.configAPI = configAPI;
    }

    /**
     * Get all configuration or by category
     */
    async getConfig(req, res) {
        try {
            const { category } = req.params;

            if (category) {
                const configs = await this.configAPI.getFeatureConfig(category);
                return res.json(configs);
            }

            const allConfigs = await this.db.query('SELECT * FROM config ORDER BY category, key');
            res.json(allConfigs);
        } catch (error) {
            console.error('Error getting config:', error);
            res.status(500).json({ error: 'Failed to get configuration' });
        }
    }

    /**
     * Update configuration
     */
    async updateConfig(req, res) {
        try {
            const updates = req.body;

            if (!updates || typeof updates !== 'object') {
                return res.status(400).json({ error: 'Invalid configuration data' });
            }

            // Update each config key
            for (const [key, value] of Object.entries(updates)) {
                await this.db.setConfig(key, value);
            }

            // Return updated configuration
            const updatedConfigs = await this.db.query('SELECT * FROM config ORDER BY category, key');
            res.json({
                success: true,
                message: 'Configuration updated successfully',
                configs: updatedConfigs
            });
        } catch (error) {
            console.error('Error updating config:', error);
            res.status(500).json({ error: 'Failed to update configuration' });
        }
    }

    /**
     * Get system status based on configuration
     */
    async getSystemStatus(req, res) {
        try {
            const status = await this.configAPI.getSystemStatus();
            res.json(status);
        } catch (error) {
            console.error('Error getting system status:', error);
            res.status(500).json({ error: 'Failed to get system status' });
        }
    }

    /**
     * Export configuration
     */
    async exportConfig(req, res) {
        try {
            const exportData = await this.configAPI.exportConfig();
            res.json(exportData);
        } catch (error) {
            console.error('Error exporting config:', error);
            res.status(500).json({ error: 'Failed to export configuration' });
        }
    }

    /**
     * Import configuration
     */
    async importConfig(req, res) {
        try {
            const importData = req.body;

            if (!importData || !importData.configs) {
                return res.status(400).json({ error: 'Invalid import data' });
            }

            const result = await this.configAPI.importConfig(importData);
            res.json(result);
        } catch (error) {
            console.error('Error importing config:', error);
            res.status(500).json({ error: 'Failed to import configuration' });
        }
    }
}

module.exports = ConfigController;
