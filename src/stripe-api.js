const https = require('https');

class StripeAPI {
    constructor(db) {
        this.db = db;
        this.apiKey = null;
        this.baseUrl = 'https://api.stripe.com/v1';
        this.initialize();
    }

    async initialize() {
        try {
            const config = await this.getConfig();
            this.apiKey = config.stripe_secret_key ? config.stripe_secret_key.trim() : null;
            
            if (this.apiKey) {
                console.log('Stripe API initialized (HTTP client mode)');
            } else {
                console.log('Stripe API not configured - no secret key set');
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
            // Re-initialize with latest configuration
            await this.initialize();
            
            if (!this.apiKey) {
                return { success: false, message: 'Stripe not configured - please save a valid secret key first' };
            }

            console.log('Testing Stripe connection with key:', this.apiKey.substring(0, 20) + '...');
            
            // Test connection by making a simple API call
            const result = await this.makeRequest('/balance');
            
            if (result.error) {
                return { success: false, message: `Stripe connection failed: ${result.error.message}` };
            }
            
            const balance = result.available[0];
            return { 
                success: true, 
                message: `Stripe connection successful. Available balance: ${(balance.amount / 100).toFixed(2)} ${balance.currency}`
            };
        } catch (error) {
            console.error('Stripe connection test error:', error);
            return { success: false, message: `Stripe connection failed: ${error.message}` };
        }
    }

    async makeRequest(path) {
        return new Promise((resolve, reject) => {
            const options = {
                hostname: 'api.stripe.com',
                port: 443,
                path: `/v1${path}`,
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'Content-Type': 'application/json'
                }
            };

            const req = https.request(options, (res) => {
                let data = '';

                res.on('data', (chunk) => {
                    data += chunk;
                });

                res.on('end', () => {
                    try {
                        const jsonData = JSON.parse(data);
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve(jsonData);
                        } else {
                            reject(new Error(jsonData.error ? jsonData.error.message : `HTTP ${res.statusCode}`));
                        }
                    } catch (error) {
                        reject(new Error('Failed to parse response'));
                    }
                });
            });

            req.on('error', (error) => {
                reject(error);
            });

            req.setTimeout(10000, () => {
                req.destroy();
                reject(new Error('Request timeout'));
            });

            req.end();
        });
    }

    async importCustomers(progressCallback = null) {
        try {
            // Re-initialize with latest configuration
            await this.initialize();
            
            if (!this.apiKey) {
                return { success: false, message: 'Stripe not configured - please save a valid secret key first' };
            }

            console.log('Starting Stripe customer import with key:', this.apiKey.substring(0, 20) + '...');
            
            let importedCount = 0;
            let updatedCount = 0;
            let hasMore = true;
            let startingAfter = '';
            let totalProcessed = 0;
            let totalCustomers = 0;
            
            // First, get total count for progress tracking
            if (progressCallback) {
                progressCallback({ stage: 'counting', progress: 0, message: 'Counting customers...' });
            }
            
            // Handle pagination to get ALL customers
            while (hasMore) {
                const url = startingAfter ? `/customers?limit=100&starting_after=${startingAfter}` : '/customers?limit=100';
                const customers = await this.makeRequest(url);
                
                if (progressCallback && !startingAfter) {
                    totalCustomers = customers.data.length; // Estimate total
                }
                
                totalProcessed += customers.data.length;
                
                for (let i = 0; i < customers.data.length; i++) {
                    const customer = customers.data[i];
                    
                    if (progressCallback) {
                        const progress = Math.min(Math.round((totalProcessed / Math.max(totalCustomers, 1)) * 100), 100);
                        progressCallback({ 
                            stage: 'importing', 
                            progress: progress, 
                            message: `Processing customer ${i + 1} of ${customers.data.length}...`,
                            current: i + 1,
                            total: customers.data.length
                        });
                    }
                    
                    const existing = await this.db.get(
                        'SELECT id FROM stripe_customers WHERE stripe_id = ?',
                        [customer.id]
                    );

                    if (existing) {
                        // Update existing customer
                        await this.db.run(
                            `UPDATE stripe_customers SET 
                             email = ?, name = ?, description = ?, phone = ?,
                             address_line1 = ?, address_city = ?, address_state = ?, 
                             address_postal_code = ?, address_country = ?, metadata = ?,
                             raw_data = ?, last_sync = CURRENT_TIMESTAMP
                             WHERE stripe_id = ?`,
                            [
                                customer.email,
                                customer.name,
                                customer.description,
                                customer.phone,
                                customer.address?.line1,
                                customer.address?.city,
                                customer.address?.state,
                                customer.address?.postal_code,
                                customer.address?.country,
                                JSON.stringify(customer.metadata || {}),
                                JSON.stringify(customer),
                                customer.id
                            ]
                        );
                        updatedCount++;
                    } else {
                        // Insert new customer
                        await this.db.run(
                            `INSERT INTO stripe_customers (
                                stripe_id, email, name, description, phone,
                                address_line1, address_city, address_state, 
                                address_postal_code, address_country, created,
                                metadata, raw_data
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                            [
                                customer.id,
                                customer.email,
                                customer.name,
                                customer.description,
                                customer.phone,
                                customer.address?.line1,
                                customer.address?.city,
                                customer.address?.state,
                                customer.address?.postal_code,
                                customer.address?.country,
                                customer.created,
                                JSON.stringify(customer.metadata || {}),
                                JSON.stringify(customer)
                            ]
                        );
                        importedCount++;
                    }
                }

                // Check if there are more pages
                hasMore = customers.has_more;
                if (hasMore && customers.data.length > 0) {
                    startingAfter = customers.data[customers.data.length - 1].id;
                } else {
                    hasMore = false;
                }
            }

            const total = importedCount + updatedCount;
            return { 
                success: true, 
                message: `Imported ${importedCount} new customers, updated ${updatedCount} existing customers (total: ${total})`,
                imported: importedCount,
                updated: updatedCount,
                total: total
            };
        } catch (error) {
            console.error('Error importing Stripe customers:', error);
            return { success: false, message: `Error importing customers: ${error.message}` };
        }
    }

    async importTransactions(progressCallback = null, importAll = false) {
        try {
            // Re-initialize with latest configuration
            await this.initialize();
            
            if (!this.apiKey) {
                return { success: false, message: 'Stripe not configured - please save a valid secret key first' };
            }

            console.log('Starting Stripe transaction import with key:', this.apiKey.substring(0, 20) + '...', importAll ? '(Importing ALL transactions)' : '(Importing new transactions only)');
            
            let importedCount = 0;
            let hasMore = true;
            let startingAfter = '';
            let latestDate = 0;
            let totalProcessed = 0;
            let totalTransactions = 0;
            
            if (progressCallback) {
                progressCallback({ stage: 'counting', progress: 0, message: 'Counting transactions...' });
            }
            
            // If importAll is false, only get transactions newer than the latest in database
            // If importAll is true, get ALL transactions from Stripe
            let baseParams = '';
            if (!importAll) {
                const latestTransaction = await this.db.get(
                    'SELECT MAX(created) as latest_created FROM stripe_transactions'
                );
                if (latestTransaction?.latest_created) {
                    baseParams = `created[gte]=${latestTransaction.latest_created + 1}&`;
                }
            }
            
            // Handle pagination to get ALL transactions
            while (hasMore) {
                const url = startingAfter 
                    ? `/charges?${baseParams}limit=100&starting_after=${startingAfter}`
                    : `/charges?${baseParams}limit=100`;
                
                const charges = await this.makeRequest(url);
                
                if (progressCallback && !startingAfter) {
                    // Try to get total count for better progress tracking
                    totalTransactions = charges.data.length;
                    if (charges.has_more) {
                        totalTransactions = Math.min(totalTransactions * 10, 10000); // Estimate up to 10K transactions
                    }
                }
                
                totalProcessed += charges.data.length;
                
                for (let i = 0; i < charges.data.length; i++) {
                    const charge = charges.data[i];
                    
                    if (progressCallback) {
                        const progress = Math.min(Math.round((totalProcessed / Math.max(totalTransactions, 1)) * 100), 100);
                        progressCallback({ 
                            stage: 'importing', 
                            progress: progress, 
                            message: `Processing transaction ${i + 1} of ${charges.data.length}...`,
                            current: i + 1,
                            total: charges.data.length
                        });
                    }
                    
                    const existing = await this.db.get(
                        'SELECT id FROM stripe_transactions WHERE stripe_id = ?',
                        [charge.id]
                    );

                    if (!existing) {
                        try {
                            // Store only the essential fields + raw JSON to avoid binding issues
                            const safeData = {
                                stripe_id: charge.id || '',
                                customer_id: charge.customer || '',
                                amount: Number(charge.amount) || 0,
                                currency: charge.currency || 'usd',
                                description: charge.description || '',
                                status: charge.status || 'unknown',
                                created: Number(charge.created) || 0,
                                invoice_id: charge.invoice || null,
                                payment_intent_id: charge.payment_intent || null,
                                refunded: Boolean(charge.refunded),
                                raw_data: JSON.stringify(charge)
                            };
                            
                            await this.db.run(
                                `INSERT INTO stripe_transactions (
                                    stripe_id, customer_id, amount, currency, description, 
                                    status, created, invoice_id, payment_intent_id, 
                                    refunded, raw_data
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                                [
                                    safeData.stripe_id,
                                    safeData.customer_id,
                                    safeData.amount,
                                    safeData.currency,
                                    safeData.description,
                                    safeData.status,
                                    safeData.created,
                                    safeData.invoice_id,
                                    safeData.payment_intent_id,
                                    safeData.refunded ? 1 : 0, // Convert boolean to integer for SQLite
                                    safeData.raw_data
                                ]
                            );
                            importedCount++;
                            latestDate = Math.max(latestDate, charge.created);
                        } catch (insertError) {
                            console.error('Error inserting transaction', charge.id, ':', insertError.message);
                            throw insertError;
                        }
                    }
                }

                // Check if there are more pages
                hasMore = charges.has_more;
                if (hasMore && charges.data.length > 0) {
                    startingAfter = charges.data[charges.data.length - 1].id;
                } else {
                    hasMore = false;
                }
            }

            return { 
                success: true, 
                message: `Imported ${importedCount} new transactions`,
                count: importedCount,
                latest_date: importedCount > 0 ? latestDate : null
            };
        } catch (error) {
            console.error('Error importing Stripe transactions:', error);
            return { success: false, message: `Error importing transactions: ${error.message}` };
        }
    }

    async getImportedCustomers() {
        try {
            const customers = await this.db.all(
                'SELECT * FROM stripe_customers ORDER BY name, email'
            );
            return customers;
        } catch (error) {
            console.error('Error fetching imported customers:', error);
            return [];
        }
    }

    async getImportedTransactions(limit = null, offset = 0) {
        try {
            let query = `SELECT t.*, c.name as customer_name, c.email as customer_email
                 FROM stripe_transactions t
                 LEFT JOIN stripe_customers c ON t.customer_id = c.stripe_id
                 ORDER BY t.created DESC`;

            const params = [];
            if (limit !== null) {
                query += ' LIMIT ? OFFSET ?';
                params.push(limit, offset);
            }

            const transactions = await this.db.all(query, params);
            return transactions;
        } catch (error) {
            console.error('Error fetching imported transactions:', error);
            return [];
        }
    }

    async getTransactionsCount() {
        try {
            const result = await this.db.get('SELECT COUNT(*) as count FROM stripe_transactions');
            return result ? result.count : 0;
        } catch (error) {
            console.error('Error getting transactions count:', error);
            return 0;
        }
    }

    async getCustomers() {
        try {
            if (!this.apiKey) {
                return [];
            }

            const customers = await this.makeRequest('/customers?limit=100');
            return customers.data.map(customer => ({
                id: customer.id,
                email: customer.email,
                name: customer.name,
                description: customer.description,
                created: customer.created
            }));
        } catch (error) {
            console.error('Error fetching Stripe customers:', error);
            return [];
        }
    }
}

module.exports = StripeAPI;
