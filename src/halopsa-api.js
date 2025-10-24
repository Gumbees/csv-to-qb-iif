class HaloPSAAPI {
    constructor(db) {
        this.db = db;
        this.apiKey = null;
        this.apiUrl = null;
        this.initialize();
    }

    async initialize() {
        try {
            const config = await this.getConfig();
            this.apiKey = config.halopsa_api_key;
            this.apiUrl = config.halopsa_api_url;
            
            if (this.apiKey && this.apiUrl) {
                console.log('HaloPSA API initialized successfully');
            }
        } catch (error) {
            console.error('Error initializing HaloPSA API:', error);
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

    async getClients() {
        try {
            if (!this.apiKey || !this.apiUrl) {
                console.warn('HaloPSA API not configured');
                return [];
            }

            // Mock implementation - replace with actual HaloPSA API call
            // In a real implementation, you would make an HTTP request to HaloPSA API
            const mockClients = [
                { id: 1, name: 'ABC Dental Clinic', email: 'info@abcdental.com', phone: '555-0101' },
                { id: 2, name: 'XYZ Medical Center', email: 'billing@xyzmedical.com', phone: '555-0102' },
                { id: 3, name: 'Smith & Associates', email: 'accounting@smithlaw.com', phone: '555-0103' },
                { id: 4, name: 'Johnson Manufacturing', email: 'payments@johnsonmfg.com', phone: '555-0104' },
                { id: 5, name: 'Thompson Consulting', email: 'finance@thompsonconsult.com', phone: '555-0105' }
            ];

            // Example of actual API call (commented out for now):
            /*
            const response = await fetch(`${this.apiUrl}/api/client`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HaloPSA API error: ${response.statusText}`);
            }
            
            const data = await response.json();
            return data.clients || [];
            */

            return mockClients;
        } catch (error) {
            console.error('Error fetching HaloPSA clients:', error);
            // Return mock data for development
            return [
                { id: 1, name: 'ABC Dental Clinic', email: 'info@abcdental.com', phone: '555-0101' },
                { id: 2, name: 'XYZ Medical Center', email: 'billing@xyzmedical.com', phone: '555-0102' }
            ];
        }
    }

    async getClientDetails(clientId) {
        try {
            if (!this.apiKey || !this.apiUrl) {
                return null;
            }

            // Mock implementation
            const mockClients = {
                1: { id: 1, name: 'ABC Dental Clinic', email: 'info@abcdental.com', phone: '555-0101', address: '123 Main St' },
                2: { id: 2, name: 'XYZ Medical Center', email: 'billing@xyzmedical.com', phone: '555-0102', address: '456 Oak Ave' }
            };

            return mockClients[clientId] || null;

            // Actual implementation would be:
            /*
            const response = await fetch(`${this.apiUrl}/api/client/${clientId}`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'Content-Type': 'application/json'
                }
            });
            
            if (response.ok) {
                return await response.json();
            }
            return null;
            */
        } catch (error) {
            console.error('Error fetching client details:', error);
            return null;
        }
    }

    async testConnection() {
        try {
            if (!this.apiKey || !this.apiUrl) {
                return { success: false, message: 'HaloPSA API not configured' };
            }

            // Mock successful connection test
            return { 
                success: true, 
                message: 'HaloPSA connection test successful (using mock data for development)'
            };

            // Actual implementation would be:
            /*
            const response = await fetch(`${this.apiUrl}/api/test`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'Content-Type': 'application/json'
                }
            });
            
            if (response.ok) {
                return { success: true, message: 'HaloPSA connection successful' };
            } else {
                return { success: false, message: `HaloPSA connection failed: ${response.statusText}` };
            }
            */
        } catch (error) {
            return { success: false, message: `HaloPSA connection test failed: ${error.message}` };
        }
    }

    async createInvoiceFromTransaction(transactionId, clientId) {
        try {
            // This would create an invoice in HaloPSA from a Stripe transaction
            // For now, return a mock implementation
            console.log(`Creating invoice for transaction ${transactionId} for client ${clientId}`);
            
            return { 
                success: true, 
                message: 'Invoice created successfully (mock implementation)',
                invoiceId: Math.floor(Math.random() * 10000)
            };
        } catch (error) {
            console.error('Error creating invoice:', error);
            return { success: false, message: 'Error creating invoice' };
        }
    }
}

module.exports = HaloPSAAPI;
