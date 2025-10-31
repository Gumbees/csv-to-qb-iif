class ConfigAPI {
    constructor(database) {
        this.db = database;
    }

    // Get all configuration by category
    async getConfigByCategory(category = null) {
        try {
            const configs = await this.db.getConfig(category);
            
            // Format for UI display
            return configs.map(config => ({
                key: config.key,
                value: this.parseConfigValue(config.value, config.type),
                type: config.type,
                category: config.category,
                label: config.label,
                description: config.description,
                options: config.options ? JSON.parse(config.options) : null,
                isRequired: config.is_required === 1
            }));
        } catch (error) {
            throw error;
        }
    }

    // Parse configuration value based on type
    parseConfigValue(value, type) {
        switch (type) {
            case 'number':
                return parseFloat(value) || 0;
            case 'boolean':
                return value === 'true' || value === '1';
            case 'json':
                try {
                    return JSON.parse(value);
                } catch {
                    return value;
                }
            default:
                return value;
        }
    }

    // Format value for storage
    formatConfigValue(value, type) {
        switch (type) {
            case 'boolean':
                return value ? 'true' : 'false';
            case 'number':
                return value.toString();
            case 'json':
                return JSON.stringify(value);
            default:
                return value;
        }
    }

    // Update configuration
    async updateConfig(key, value, type) {
        try {
            const formattedValue = this.formatConfigValue(value, type);
            await this.db.setConfig(key, formattedValue);
            
            // Return updated config
            const configs = await this.db.getConfig();
            const updated = configs.find(c => c.key === key);
            
            return {
                key: updated.key,
                value: this.parseConfigValue(updated.value, updated.type),
                type: updated.type,
                category: updated.category,
                label: updated.label
            };
        } catch (error) {
            throw error;
        }
    }

    // Bulk update configuration
    async updateMultipleConfig(configUpdates) {
        try {
            const updates = {};
            
            for (const update of configUpdates) {
                updates[update.key] = this.formatConfigValue(update.value, update.type);
            }
            
            await this.db.setMultipleConfig(updates);
            
            // Return all updated configs
            const allConfigs = await this.db.getConfig();
            return allConfigs.map(config => ({
                key: config.key,
                value: this.parseConfigValue(config.value, config.type),
                type: config.type,
                category: config.category,
                label: config.label
            }));
        } catch (error) {
            throw error;
        }
    }

    // Validate configuration
    validateConfig(key, value, type, options = null) {
        const errors = [];
        
        switch (type) {
            case 'number':
                if (isNaN(value)) errors.push('Must be a valid number');
                break;
            case 'boolean':
                if (typeof value !== 'boolean') errors.push('Must be true or false');
                break;
            case 'select':
                if (options && !options.includes(value)) {
                    errors.push(`Must be one of: ${options.join(', ')}`);
                }
                break;
            case 'password':
                if (value && value.length < 8) {
                    errors.push('Password must be at least 8 characters');
                }
                break;
        }
        
        // Required field validation
        if ((value === null || value === undefined || value === '') && key !== 'stripe_secret_key') {
            errors.push('This field is required');
        }
        
        return errors;
    }

    // Get configuration for specific features
    async getFeatureConfig(feature) {
        const featureConfigs = {
            'stripe': [
                'stripe_secret_key', 'stripe_webhook_secret', 'stripe_sync_enabled', 'stripe_sync_interval'
            ],
            'quickbooks': [
                'quickbooks_company_file', 'quickbooks_sync_method', 'quickbooks_default_account'
            ],
            'accounting': [
                'accounting_currency', 'accounting_timezone', 'cash_basis_enabled'
            ],
            'msp': [
                'msp_tax_rate', 'msp_payment_terms', 'msp_invoice_prefix'
            ],
            'general': [
                'auto_match_threshold', 'duplicate_check_enabled', 'webhook_enabled'
            ],
            'halopsa': [
                'halopsa_api_url', 'halopsa_client_id', 'halopsa_client_secret', 
                'halopsa_access_token', 'halopsa_purchase_order_report_id', 
                'halopsa_invoice_report_id'
            ]
        };
        
        const keys = featureConfigs[feature] || [];
        const allConfigs = await this.db.getConfig();
        
        return allConfigs
            .filter(config => keys.includes(config.key))
            .map(config => ({
                key: config.key,
                value: this.parseConfigValue(config.value, config.type),
                type: config.type,
                category: config.category,
                label: config.label,
                description: config.description,
                options: config.options ? JSON.parse(config.options) : null
            }));
    }

    // Export configuration for backup
    async exportConfig() {
        try {
            const configs = await this.db.getConfig();
            
            const exportData = {
                timestamp: new Date().toISOString(),
                version: '1.0',
                configs: configs.map(config => ({
                    key: config.key,
                    value: config.value,
                    type: config.type,
                    category: config.category,
                    label: config.label,
                    description: config.description,
                    options: config.options
                }))
            };
            
            return exportData;
        } catch (error) {
            throw error;
        }
    }

    // Import configuration from backup
    async importConfig(importData) {
        try {
            if (!importData.configs || !Array.isArray(importData.configs)) {
                throw new Error('Invalid configuration format');
            }
            
            const updates = {};
            
            for (const config of importData.configs) {
                if (config.key && config.value !== undefined) {
                    updates[config.key] = config.value;
                }
            }
            
            await this.db.setMultipleConfig(updates);
            
            return {
                success: true,
                imported: Object.keys(updates).length,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            throw error;
        }
    }

    // Get system status based on configuration
    async getSystemStatus() {
        try {
            const configs = await this.db.getConfig();
            const status = {
                stripe: { configured: false, testable: false },
                quickbooks: { configured: false },
                halopsa: { configured: false, testable: false },
                general: { ready: false }
            };
            
            // Check Stripe configuration
            const stripeKey = configs.find(c => c.key === 'stripe_secret_key');
            status.stripe.configured = !!stripeKey && stripeKey.value.length > 0;
            status.stripe.testable = status.stripe.configured;
            
            // Check QuickBooks configuration
            const qbFile = configs.find(c => c.key === 'quickbooks_company_file');
            status.quickbooks.configured = !!qbFile && qbFile.value.length > 0;
            
            // Check HaloPSA configuration
            const halopsaKey = configs.find(c => c.key === 'halopsa_api_key');
            const halopsaUrl = configs.find(c => c.key === 'halopsa_api_url');
            status.halopsa.configured = !!halopsaKey && halopsaKey.value.length > 0 && !!halopsaUrl && halopsaUrl.value.length > 0;
            status.halopsa.testable = status.halopsa.configured;
            
            // Overall readiness
            status.general.ready = status.stripe.configured || status.halopsa.configured;
            
            return status;
        } catch (error) {
            throw error;
        }
    }

    // Reset configuration to defaults
    async resetToDefaults() {
        try {
            // This would re-run the default configuration initialization
            // For now, we'll just return a message
            return {
                success: true,
                message: 'Configuration reset feature coming soon'
            };
        } catch (error) {
            throw error;
        }
    }
}

module.exports = ConfigAPI;