const https = require('https');
const {
    extractComprehensiveInvoiceData,
    INSERT_INVOICE_SQL,
    UPDATE_INVOICE_SQL,
    getInsertValues,
    getUpdateValues
} = require('./invoice-import-enhanced');
const {
    extractComprehensivePOData,
    INSERT_PO_SQL,
    UPDATE_PO_SQL,
    getInsertValues: getPOInsertValues,
    getUpdateValues: getPOUpdateValues
} = require('./purchase-order-import-enhanced');

class HaloPSAAPI {
    constructor(db, instanceId = 'default') {
        this.db = db;
        this._instanceId = instanceId;
        this.apiUrl = null;
        this.clientId = null;
        this.clientSecret = null;
        this._initialized = false;
    }

    async initialize() {
        const config = await this.getConfig();
        this.apiUrl = config.halopsa_api_url;
        this.clientId = config.halopsa_client_id;
        this.clientSecret = config.halopsa_client_secret;
        this.accessToken = null; // We'll get a fresh token for each request
        this._initialized = true;
        
        console.log('HaloPSA API initialized with configuration:', {
            apiUrl: this.apiUrl,
            clientId: this.clientId,
            hasClientSecret: !!this.clientSecret,
            accessToken: 'will be retrieved per request'
        });
    }

    async getConfig() {
        try {
            const rows = await this.db.query('SELECT key, value FROM config');
            const config = {};
            rows.forEach(row => {
                config[row.key] = row.value;
            });
            return config;
        } catch (error) {
            console.error('Error getting config:', error);
            throw error;
        }
    }

    // updateConfig method removed - we don't need to store access tokens

    async testConnection() {
        try {
            const instanceId = this._instanceId || 'no-instance-id';
            console.log(`=== Starting HaloPSA testConnection for instance ${instanceId} ===`);
            
            // First ensure we're initialized with the current configuration
            if (!this._initialized) {
                await this.initialize();
            }
            
            console.log('Instance configuration check:', {
                hasApiUrl: !!this.apiUrl,
                hasClientId: !!this.clientId, 
                hasClientSecret: !!this.clientSecret,
                apiUrl: this.apiUrl,
                clientId: this.clientId ? `${this.clientId.substring(0, 8)}...` : 'null'
            });
            
            // Detailed configuration validation
            const missingConfigs = [];
            if (!this.apiUrl) missingConfigs.push('API URL');
            if (!this.clientId) missingConfigs.push('Client ID');
            if (!this.clientSecret) missingConfigs.push('Client Secret');
            
            if (missingConfigs.length > 0) {
                console.log('Instance configuration missing - checking database directly...');
                
                // Check database for missing configuration
                const configCheck = await this.getConfig();
                const dbMissingConfigs = [];
                if (!configCheck.halopsa_api_url) dbMissingConfigs.push('API URL');
                if (!configCheck.halopsa_client_id) dbMissingConfigs.push('Client ID');
                if (!configCheck.halopsa_client_secret) dbMissingConfigs.push('Client Secret');
                
                if (dbMissingConfigs.length > 0) {
                    const detailedMessage = `HALOPSA_CONFIG_ERROR: Missing configuration in database: ${dbMissingConfigs.join(', ')}. Please configure HaloPSA settings with valid API URL, Client ID, and Client Secret.`;
                    console.log(`Configuration error: ${detailedMessage}`);
                    return { 
                        success: false, 
                        message: detailedMessage
                    };
                }
                
                console.log('✅ Configuration found in database, attempting to initialize...');
                
                // Initialize from database config
                this.apiUrl = configCheck.halopsa_api_url;
                this.clientId = configCheck.halopsa_client_id;
                this.clientSecret = configCheck.halopsa_client_secret;
                this._initialized = true;
            }
            
            console.log('✅ Configuration ready, testing token generation...');
            
            // Test token generation as the connection test
            const tokenResult = await this.getAccessTokenViaClientCredentials();
            if (tokenResult.success) {
                console.log('✅ HaloPSA connection test PASSED - Token generation successful');
                
                // Test actual API access with various endpoints from the documentation
                console.log('✅ Testing API endpoints from documentation...');
                
                const endpointsToTest = [
                    '/api/status',
                    '/api/health', 
                    '/api/Organisation',
                    '/api/tickets',  // Primary endpoint from Postman documentation
                    '/api/clients',   // Standard clients endpoint
                    '/api/Client'     // Previous endpoint
                ];
                
                let workingEndpoints = [];
                let failedEndpoints = [];
                let dataFound = [];
                
                for (const endpoint of endpointsToTest) {
                    try {
                        const result = await this.makeRequest(endpoint + '?count=1');
                        workingEndpoints.push(endpoint);
                        
                        if (Array.isArray(result) && result.length > 0) {
                            dataFound.push(`${endpoint} (${result.length} items)`);
                            console.log(`✅ ${endpoint} - Found ${result.length} items`);
                        } else if (typeof result === 'object') {
                            dataFound.push(`${endpoint} (object data)`);
                            console.log(`✅ ${endpoint} - Found object data`);
                        } else {
                            console.log(`✅ ${endpoint} - Accessible but no data returned`);
                        }
                    } catch (endpointError) {
                        failedEndpoints.push(`${endpoint}: ${endpointError.message}`);
                        console.log(`❌ ${endpoint} - ${endpointError.message}`);
                    }
                    // Small delay between requests
                    await new Promise(resolve => setTimeout(resolve, 200));
                }
                
                if (dataFound.length > 0) {
                    const successMessage = `HaloPSA connection successful! Authentication works. Accessible endpoints with data: ${dataFound.join(', ')}.`;
                    console.log('✅ API endpoint access test PASSED');
                    
                    return { 
                        success: true, 
                        message: successMessage
                    };
                } else if (workingEndpoints.length > 0) {
                    const diagnosticMessage = `HaloPSA authentication successful. Working endpoints: ${workingEndpoints.join(', ')} but no data returned. Failed endpoints: ${failedEndpoints.join('; ')}.`;
                    
                    return {
                        success: true,
                        message: diagnosticMessage
                    };
                } else {
                    const errorMessage = `HaloPSA authentication successful but no API endpoints accessible. Failed endpoints: ${failedEndpoints.join('; ')}.`;
                    
                    return {
                        success: false,
                        message: errorMessage
                    };
                }
            } else {
                console.log('❌ HaloPSA connection test FAILED - Token generation failed');
                
                // Provide detailed error analysis
                let errorAnalysis = 'Authentication failed. ';
                
                if (tokenResult.error.includes('404')) {
                    errorAnalysis += 'The authentication endpoint was not found. Check that the API URL is correct and the /auth/token endpoint exists.';
                } else if (tokenResult.error.includes('400')) {
                    errorAnalysis += 'Invalid request to authentication endpoint. Check that Client ID and Client Secret are correct.';
                } else if (tokenResult.error.includes('401')) {
                    errorAnalysis += 'Invalid client credentials. Please verify the Client ID and Client Secret.';
                } else if (tokenResult.error.includes('403')) {
                    errorAnalysis += 'Access forbidden. Check that the client credentials have proper permissions.';
                } else if (tokenResult.error.includes('ECONNREFUSED') || tokenResult.error.includes('ENOTFOUND')) {
                    errorAnalysis += 'Cannot connect to the HaloPSA server. Check network connectivity and that the API URL is correct.';
                } else {
                    errorAnalysis += `Specific error: ${tokenResult.error}`;
                }
                
                const errorResponse = { 
                    success: false, 
                    message: `HaloPSA connection failed: ${errorAnalysis}` 
                };
                console.log('Returning detailed error response:', errorResponse);
                return errorResponse;
            }
            
        } catch (error) {
            console.error('HaloPSA connection test failed - Full error:', error);
            
            // Provide detailed error message based on error type
            let detailedMessage = 'HaloPSA connection test failed: ';
            
            if (error.message.includes('Cannot read properties')) {
                detailedMessage += 'Configuration loading error. The database may be corrupted or schema is incorrect.';
            } else if (error.message.includes('SQLITE_ERROR')) {
                detailedMessage += 'Database error. Check that the database file exists and is accessible.';
            } else if (error.message.includes('network') || error.message.includes('connect')) {
                detailedMessage += 'Network connectivity issue. Check internet connection and firewall settings.';
            } else {
                detailedMessage += error.message;
            }
            
            return { success: false, message: detailedMessage };
        }
    }

    async getAccessTokenViaClientCredentials() {
        try {
            console.log('Starting client credentials authentication...');
            console.log('Authentication details:', {
                apiUrl: this.apiUrl,
                clientId: this.clientId,
                hasClientSecret: !!this.clientSecret
            });
            
            if (!this.clientId || !this.clientSecret) {
                console.error('Client credentials not configured:', {
                    hasClientId: !!this.clientId,
                    hasClientSecret: !!this.clientSecret
                });
                return { success: false, error: 'Client credentials not configured' };
            }

            // The authentication endpoint should be based on the API URL, not a fixed URL
            // Try different authentication endpoint patterns
            const authHostname = this.apiUrl.replace('https://', '').replace('/', '');
            const tokenEndpoints = [
                '/oauth/token',  // Standard OAuth endpoint
                '/auth/token',   // Alternative auth endpoint  
                '/api/oauth/token', // API-specific OAuth endpoint
                '/api/auth/token'   // API-specific auth endpoint
            ];

            const postData = new URLSearchParams({
                grant_type: 'client_credentials',
                client_id: this.clientId,
                client_secret: this.clientSecret,
                scope: 'all'  // The correct scope according to the API documentation
            }).toString();

            console.log('Token request data (partial):', {
                grant_type: 'client_credentials',
                client_id: this.clientId,
                client_secret: '***',
                scope: 'all'
            });

            // Try each endpoint until one works
            for (const tokenPath of tokenEndpoints) {
                try {
                    console.log(`Trying authentication endpoint: https://${authHostname}${tokenPath}`);
                    const tokenResponse = await this.tryTokenEndpoint(authHostname, tokenPath, postData);
                    if (tokenResponse.success) {
                        console.log('✅ Token authentication successful using endpoint:', tokenPath);
                        return tokenResponse;
                    }
                } catch (error) {
                    console.log(`❌ Endpoint ${tokenPath} failed:`, error.message);
                    continue;
                }
            }
            
            // If all endpoints failed, return the last error
            console.error('❌ All authentication endpoints failed');
            return { success: false, error: 'All authentication endpoints failed. Check API documentation for correct token endpoint.' };
        } catch (error) {
            console.error('❌ Token generation error:', error);
            return { success: false, error: 'Error obtaining access token: ' + error.message };
        }
    }

    /**
     * Try a specific token endpoint
     */
    tryTokenEndpoint(hostname, path, postData) {
        return new Promise((resolve, reject) => {
            const options = {
                hostname: hostname,
                port: 443,
                path: path,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(postData)
                }
            };

            const req = https.request(options, (res) => {
                let data = '';
                res.on('data', (chunk) => data += chunk);
                res.on('end', () => {
                    console.log(`Token response status for ${path}:`, res.statusCode);
                    
                    if (res.statusCode === 200) {
                        try {
                            const tokenData = JSON.parse(data);
                            resolve({ 
                                success: true, 
                                access_token: tokenData.access_token,
                                token_type: tokenData.token_type,
                                expires_in: tokenData.expires_in
                            });
                        } catch (error) {
                            console.error(`❌ Error parsing token response for ${path}:`, error);
                            resolve({ success: false, error: 'Error parsing token response: ' + error.message });
                        }
                    } else {
                        console.log(`❌ Token authentication failed for ${path}: Status ${res.statusCode}`);
                        resolve({ success: false, error: `HTTP ${res.statusCode}: ${data.substring(0, 100)}` });
                    }
                });
            });

            req.on('error', (error) => {
                console.error(`❌ Token request error for ${path}:`, error);
                resolve({ success: false, error: 'Error obtaining access token: ' + error.message });
            });

            req.write(postData);
            req.end();
        });
    }

    async makeRequest(path, method = 'GET', data = null) {
        console.log(`makeRequest called with path: ${path}, method: ${method}`);
        
        // Ensure we're initialized before making requests
        if (!this._initialized) {
            await this.initialize();
        }
        
        if (!this.apiUrl) {
            console.error('HaloPSA API URL not configured in makeRequest');
            throw new Error('HaloPSA API URL not configured');
        }

        // Always get a fresh access token for each request
        console.log('Obtaining fresh access token...');
        const tokenResult = await this.getAccessTokenViaClientCredentials();
        if (!tokenResult.success) {
            console.error('HaloPSA authentication failed:', tokenResult.error);
            throw new Error('HaloPSA authentication failed: ' + tokenResult.error);
        }
        const accessToken = tokenResult.access_token;
        console.log('Fresh access token obtained successfully');

        return new Promise((resolve, reject) => {
            const options = {
                hostname: this.apiUrl.replace('https://', ''),
                port: 443,
                path: path,
                method: method,
                    headers: {
                        'Authorization': `Bearer ${accessToken}`,
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'User-Agent': 'CSV-to-QB-IIF-Sync/1.0'
                    }
            };

            if (data && method !== 'GET') {
                options.headers['Content-Length'] = Buffer.byteLength(data);
            }

            console.log(`Making HaloPSA API request: ${method} ${path}`);
            
            const req = https.request(options, (res) => {
                let responseData = '';
                
                console.log(`API Response Status: ${res.statusCode}`);
                console.log('API Response Headers:', res.headers);
                
                res.on('data', (chunk) => {
                    responseData += chunk;
                });
                
                res.on('end', async () => {
                    console.log('API Response Body (first 500 chars):', responseData.substring(0, 500));
                    
                    if (res.statusCode >= 200 && res.statusCode < 300) {
                        try {
                            const jsonData = JSON.parse(responseData);
                            resolve(jsonData);
                        } catch (error) {
                            console.error('Failed to parse response:', error);
                            reject(new Error('Failed to parse response: ' + error.message));
                        }
                    } else if (res.statusCode === 401) {
                        // 401 means our fresh token was invalid - this is a configuration issue
                        console.error('HaloPSA authentication invalid (401) - check client credentials');
                        reject(new Error(`HaloPSA authentication invalid (401): ${responseData}`));
                    } else if (res.statusCode === 403) {
                        console.error('HaloPSA API permission denied (403)');
                        reject(new Error(`HaloPSA API permission denied: ${responseData}`));
                    } else if (res.statusCode === 404) {
                        console.error('HaloPSA API endpoint not found (404)');
                        reject(new Error(`HaloPSA API endpoint not found: ${path}`));
                    } else {
                        console.error(`HaloPSA API error: ${res.statusCode}`);
                        reject(new Error(`HaloPSA API error ${res.statusCode}: ${responseData}`));
                    }
                });
            });

            req.on('error', (error) => {
                console.error('Request error:', error);
                reject(error);
            });
            
            if (data && method !== 'GET') {
                req.write(data);
            }
            
            req.end();
        });
    }

    async getClients() {
        try {
            console.log('Fetching clients from HaloPSA...');
            
            // Based on the API documentation, try these endpoints for client data
            const endpoints = [
                '/api/Organisation',      // This worked with limited permissions
                '/api/Organisation?count=200',
                '/api/clients',           // Try the standard endpoint
                '/api/Clients',           // Capitalized version
                '/api/client',            // Singular version
                '/api/Client',            // Original capital singular
                '/api/tickets',           // From Postman documentation (though this is for tickets)
                '/api/companies',         // Common alternative for client data
                '/api/Companies'
            ];
            
            for (const endpoint of endpoints) {
                console.log(`Trying endpoint: ${endpoint}`);
                try {
                    const clients = await this.makeRequest(endpoint);
                    console.log(`✅ Success with endpoint: ${endpoint}`);
                    
                    // Log what we received for debugging
                    console.log(`Received ${Array.isArray(clients) ? clients.length : 'data'} items`);
                    if (Array.isArray(clients) && clients.length > 0) {
                        console.log('First item structure:', Object.keys(clients[0]).slice(0, 10));
                        console.log('First item sample:', JSON.stringify(clients[0], null, 2).substring(0, 300));
                    }
                    
                    return clients;
                } catch (error) {
                    console.log(`❌ Failed with endpoint ${endpoint}: ${error.message}`);
                    // Continue to next endpoint
                }
                // Small delay to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 300));
            }
            
            throw new Error('All client endpoints failed. The API may not have the standard client endpoints exposed.');
            
        } catch (error) {
            console.error('Error fetching clients:', error);
            
            // Provide more detailed error message
            if (error.message.includes('403')) {
                throw new Error(`Permission denied: Client credentials may be limited to specific endpoints only. Error: ${error.message}`);
            } else if (error.message.includes('404')) {
                throw new Error(`Endpoint not found: Try using different endpoint names or check API documentation. Error: ${error.message}`);
            } else {
                throw new Error(`Failed to fetch clients: ${error.message}`);
            }
        }
    }

    async importClientsFromHaloPSA() {
        try {
            console.log('Starting import of clients from HaloPSA...');

            const clients = await this.getClients();

            if (!clients || !Array.isArray(clients)) {
                console.error('Invalid clients response:', typeof clients);
                return { success: false, message: 'Invalid clients data received from HaloPSA' };
            }

            console.log(`Received ${clients.length} clients from HaloPSA`);

            let importedCount = 0;
            let updatedCount = 0;
            let totalClientsProcessed = 0;
            let errorCount = 0;

            for (const client of clients) {
                if (!client.id || !client.name) {
                    console.log('Skipping client without id or name:', client);
                    continue;
                }

                try {
                    // Extract comprehensive client data from HaloPSA API response
                    const clientData = {
                        halopsa_id: client.id,
                        name: client.name,
                        toplevel_id: client.toplevel_id || client.topLevelId || null,
                        toplevel_name: client.toplevel_name || client.topLevelName || null,
                        inactive: client.inactive || false,
                        colour: client.colour || client.color || null,
                        email: client.override_org_email || client.contactemail || client.email || null,
                        phone: client.override_org_phone || client.phone || client.tel_number || null,
                        website: client.override_org_website || client.website || null,
                        notes: client.notes || client.announce || null,
                        // Parse address from override_org_address object or individual fields
                        address_line1: (client.override_org_address && client.override_org_address.address1) || client.address1 || null,
                        address_line2: (client.override_org_address && client.override_org_address.address2) || client.address2 || null,
                        address_city: (client.override_org_address && client.override_org_address.city) || client.city || null,
                        address_state: (client.override_org_address && client.override_org_address.state) || client.state || null,
                        address_postal_code: (client.override_org_address && client.override_org_address.postcode) || client.postcode || client.postal_code || null,
                        address_country: (client.override_org_address && client.override_org_address.country) || client.country || null,
                        primary_contact_name: client.contactname || client.contact_name || null,
                        primary_contact_email: client.contactemail || client.contact_email || null,
                        account_manager_id: client.accountmanagertech || client.accountManagerTech || null,
                        account_manager_name: client.accountmanagertech_name || client.accountManagerTechName || null,
                        hourly_rate: client.hourlyrate || client.hourlyRate || client.hourlyrate2 || 0,
                        monthly_charge: client.periodcharge || client.periodCharge || client.monthlycharge || 0,
                        prepay_balance: client.prepayrecurringcharge || client.prepayRecurringCharge || 0,
                        credit_balance: client.credit_balance || client.creditBalance || 0,
                        invoice_enabled: client.invoiceyes !== undefined ? client.invoiceyes : true,
                        date_created: client.datecreated || client.dateCreated || client.created_date || null,
                        start_date: client.startdate || client.startDate || null,
                        end_date: client.enddate || client.endDate || null,
                        customer_type: client.customertype || client.customerType || client.customertype_new || client.customer_type || null,
                        payment_terms: client.paymentterms || client.paymentTerms || null,
                        billing_email: client.accountsemailaddress || client.accountsEmailAddress || client.accounts_email || null,
                        accounts_ref: client.accountsid || client.accountsId || client.accounts_id || null,
                        vip_customer: client.is_vip || client.isVip || false,
                        custom_fields: client.customfields ? JSON.stringify(client.customfields) : null,
                        raw_data: JSON.stringify(client)
                    };

                    console.log(`Processing client ${clientData.name} (ID: ${clientData.halopsa_id})`);

                    // Check if client already exists
                    const existingClient = await this.db.get(
                        'SELECT id FROM halopsa_clients WHERE halopsa_id = ?',
                        [clientData.halopsa_id]
                    );

                    if (existingClient) {
                        // Update existing client
                        await this.db.run(
                            `UPDATE halopsa_clients SET
                             name = ?, toplevel_id = ?, toplevel_name = ?, inactive = ?, colour = ?,
                             email = ?, phone = ?, website = ?, notes = ?,
                             address_line1 = ?, address_line2 = ?, address_city = ?, address_state = ?,
                             address_postal_code = ?, address_country = ?,
                             primary_contact_name = ?, primary_contact_email = ?,
                             account_manager_id = ?, account_manager_name = ?,
                             hourly_rate = ?, monthly_charge = ?, prepay_balance = ?, credit_balance = ?,
                             invoice_enabled = ?, date_created = ?, start_date = ?, end_date = ?,
                             customer_type = ?, payment_terms = ?, billing_email = ?, accounts_ref = ?,
                             vip_customer = ?, custom_fields = ?, raw_data = ?,
                             last_sync = CURRENT_TIMESTAMP
                             WHERE halopsa_id = ?`,
                            [
                                clientData.name, clientData.toplevel_id, clientData.toplevel_name,
                                clientData.inactive, clientData.colour, clientData.email, clientData.phone,
                                clientData.website, clientData.notes, clientData.address_line1,
                                clientData.address_line2, clientData.address_city, clientData.address_state,
                                clientData.address_postal_code, clientData.address_country,
                                clientData.primary_contact_name, clientData.primary_contact_email,
                                clientData.account_manager_id, clientData.account_manager_name,
                                clientData.hourly_rate, clientData.monthly_charge,
                                clientData.prepay_balance, clientData.credit_balance,
                                clientData.invoice_enabled, clientData.date_created,
                                clientData.start_date, clientData.end_date,
                                clientData.customer_type, clientData.payment_terms,
                                clientData.billing_email, clientData.accounts_ref,
                                clientData.vip_customer, clientData.custom_fields,
                                clientData.raw_data, clientData.halopsa_id
                            ]
                        );
                        updatedCount++;
                        console.log(`✅ Updated client ${clientData.name}`);
                    } else {
                        // Insert new client
                        await this.db.run(
                            `INSERT INTO halopsa_clients
                             (halopsa_id, name, toplevel_id, toplevel_name, inactive, colour,
                              email, phone, website, notes,
                              address_line1, address_line2, address_city, address_state,
                              address_postal_code, address_country,
                              primary_contact_name, primary_contact_email,
                              account_manager_id, account_manager_name,
                              hourly_rate, monthly_charge, prepay_balance, credit_balance,
                              invoice_enabled, date_created, start_date, end_date,
                              customer_type, payment_terms, billing_email, accounts_ref,
                              vip_customer, custom_fields, raw_data, last_sync)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                            [
                                clientData.halopsa_id, clientData.name, clientData.toplevel_id,
                                clientData.toplevel_name, clientData.inactive, clientData.colour,
                                clientData.email, clientData.phone, clientData.website, clientData.notes,
                                clientData.address_line1, clientData.address_line2, clientData.address_city,
                                clientData.address_state, clientData.address_postal_code, clientData.address_country,
                                clientData.primary_contact_name, clientData.primary_contact_email,
                                clientData.account_manager_id, clientData.account_manager_name,
                                clientData.hourly_rate, clientData.monthly_charge,
                                clientData.prepay_balance, clientData.credit_balance,
                                clientData.invoice_enabled, clientData.date_created,
                                clientData.start_date, clientData.end_date,
                                clientData.customer_type, clientData.payment_terms,
                                clientData.billing_email, clientData.accounts_ref,
                                clientData.vip_customer, clientData.custom_fields, clientData.raw_data
                            ]
                        );
                        importedCount++;
                        console.log(`✅ Imported new client ${clientData.name}`);
                    }

                    totalClientsProcessed++;
                } catch (dbError) {
                    console.error(`Error processing client ${client.id}:`, dbError);
                    console.error('Client data:', JSON.stringify(client, null, 2).substring(0, 300));
                    errorCount++;
                }
            }

            console.log(`Import completed: ${importedCount} new, ${updatedCount} updated, ${errorCount} errors, ${totalClientsProcessed} total`);

            return {
                success: true,
                message: `Imported ${importedCount} new clients, updated ${updatedCount} existing clients, ${errorCount} errors`,
                imported: importedCount,
                updated: updatedCount,
                errors: errorCount,
                total: totalClientsProcessed
            };
        } catch (error) {
            console.error('Error importing clients:', error);
            return { success: false, message: `Error importing clients: ${error.message}` };
        }
    }

    async getInvoiceReport() {
        try {
            console.log('Fetching invoices directly from HaloPSA API with pagination...');
            
            // Try standard invoice endpoints including report endpoints
            const invoiceEndpoints = [
                '/api/Invoice',
                '/api/Invoices',
                '/api/invoice',
                '/api/invoices',
                '/api/Report/Invoices', // Report endpoint
                '/api/report/invoices', // Lowercase report endpoint
                '/api/Report/Invoice',  // Singular report
                '/api/report/invoice'   // Singular lowercase
            ];
            
            let allInvoices = [];
            
            for (const baseEndpoint of invoiceEndpoints) {
                try {
                    console.log(`Trying invoice endpoint: ${baseEndpoint}`);
                    
                    let page = 1;
                    let hasMore = true;
                    const pageSize = 100; // HaloPSA typically supports pageSize parameter
                    
                    // Quick test with small limit first
                    try {
                        const testResult = await this.makeRequest(`${baseEndpoint}?limit=5`);
                        if (testResult && (Array.isArray(testResult) || testResult.items || testResult.data)) {
                            console.log(`✅ Endpoint ${baseEndpoint} is accessible`);
                            console.log('Sample data structure:', typeof testResult);
                        }
                    } catch (error) {
                        console.error(`❌ Endpoint ${baseEndpoint} test failed:`, error.message);
                        // Continue anyway - the test might fail but pagination might work
                    }
                    
                    while (hasMore && page <= 10) { // Smaller safety limit to prevent timeouts
                        // Build endpoint with pagination parameters - try different formats
                        const endpointVariations = [
                            `${baseEndpoint}?page=${page}&pageSize=20`,  // Reduced page size
                            `${baseEndpoint}?page=${page}&count=20`,
                            `${baseEndpoint}?pageNumber=${page}&pageSize=20`,
                            `${baseEndpoint}?skip=${(page-1)*20}&take=20`,
                            `${baseEndpoint}?limit=20&offset=${(page-1)*20}`
                        ];
                        
                        let pageInvoices = [];
                        let endpointUsed = '';
                        
                            for (const endpoint of endpointVariations) {
                            try {
                                console.log(`Fetching page ${page} from ${endpoint}`);
                                // Add timeout protection for each request
                                const response = await Promise.race([
                                    this.makeRequest(endpoint),
                                    new Promise((_, reject) => setTimeout(() => reject(new Error('Request timeout after 10 seconds')), 10000))
                                ]);
                                endpointUsed = endpoint;
                                
                                if (response && (Array.isArray(response) || typeof response === 'object')) {
                                    let invoices = response;
                                    
                                    // Handle different pagination response formats
                                    if (response.invoices && Array.isArray(response.invoices)) {
                                        invoices = response.invoices;
                                        hasMore = (response.record_count > page * 20) || false;  // Use actual record count
                                        console.log(`📊 Pagination info: ${invoices.length} invoices, total records: ${response.record_count}, page: ${page}`);
                                    } else if (response.items && Array.isArray(response.items)) {
                                        invoices = response.items;
                                        hasMore = response.totalPages > page || response.hasNext || response.nextPage !== null || false;
                                    } else if (response.records && Array.isArray(response.records)) {
                                        invoices = response.records;
                                        hasMore = response.pageCount > page || response.totalPages > page || false;
                                    } else if (response.data && Array.isArray(response.data)) {
                                        invoices = response.data;
                                        hasMore = response.total > page * pageSize || response.hasNext || false;
                                    } else if (Array.isArray(response)) {
                                        invoices = response;
                                        hasMore = invoices.length === 20; // Match the reduced page size
                                    } else if (typeof response === 'object') {
                                        invoices = [response];
                                        hasMore = false;
                                    }
                                    
                                    if (invoices.length > 0) {
                                        pageInvoices = invoices;
                                        console.log(`✅ Found ${pageInvoices.length} invoices on page ${page}`);
                                        break; // Success with this endpoint variation
                                    }
                                }
                            } catch (error) {
                                console.log(`❌ Endpoint variation failed: ${error.message}`);
                                continue; // Try next variation
                            }
                        }
                        
                        if (pageInvoices.length > 0) {
                            allInvoices = allInvoices.concat(pageInvoices);
                            page++;
                            
                            // Small delay between pages to be respectful to the API
                            await new Promise(resolve => setTimeout(resolve, 200));
                            
                            // If we got less than a full page, assume we've reached the end
                            if (pageInvoices.length < 20) {  // Match the reduced page size
                                hasMore = false;
                            }
                        } else {
                            console.log(`❌ No invoices found on page ${page} with any endpoint variation`);
                            hasMore = false;
                        }
                    }
                    
                    if (allInvoices.length > 0) {
                        console.log(`✅ Successfully fetched ${allInvoices.length} invoices from ${baseEndpoint}`);
                        return { success: true, data: allInvoices, endpoint: baseEndpoint };
                    }
                    
                } catch (error) {
                    console.log(`❌ Invoice endpoint ${baseEndpoint} failed: ${error.message}`);
                    allInvoices = []; // Reset for next endpoint
                    // Continue to next endpoint
                }
                await new Promise(resolve => setTimeout(resolve, 300));
            }
            
            throw new Error('No invoice endpoints accessible. Please check HaloPSA API permissions and ensure invoices endpoint is available.');
            
        } catch (error) {
            console.error('Error fetching invoices:', error);
            return { success: false, message: `Error fetching invoices: ${error.message}` };
        }
    }

    async importInvoices() {
        try {
            console.log('Starting import of ALL invoices from HaloPSA API...');
            
            // Remove the count limit to get ALL invoices
            console.log('Fetching ALL invoices without limit...');
            
            const result = await this.makeRequest('/api/Invoice');
            
            if (!result) {
                return { success: false, message: 'Unable to fetch invoices from HaloPSA API' };
            }
            
            // Process the invoices we received
            let invoiceList = result;
            
            // The response structure is: { page_size: 100, record_count: X, invoices: [...] }
            if (result.invoices && Array.isArray(result.invoices)) {
                invoiceList = result.invoices;
                console.log(`API returned ${invoiceList.length} invoices (total records: ${result.record_count || 'N/A'})`);
            } else if (Array.isArray(result)) {
                invoiceList = result;
                console.log(`API returned ${invoiceList.length} invoices`);
            } else {
                console.log(`Unexpected response format:`, typeof result);
                invoiceList = [result];
            }
            
            console.log(`Processing ${invoiceList.length} invoices from HaloPSA API...`);
            
            // If we got fewer than 100 invoices but there are supposed to be thousands,
            // try a different approach to get all invoices
            if (invoiceList.length < 100 && (result.record_count > 100 || invoiceList.length < 50)) {
                console.log(`Warning: Only received ${invoiceList.length} invoices but expected more. Trying alternative endpoints...`);
                
                // Try a few different endpoints to get more data
                const alternativeEndpoints = [
                    '/api/Invoices',  // Plural endpoint
                    '/api/Invoice?pageSize=1000', // Large page size
                    '/api/Invoice?take=1000'      // Alternative parameter
                ];
                
                for (const endpoint of alternativeEndpoints) {
                    try {
                        console.log(`Trying alternative endpoint: ${endpoint}`);
                        const altResult = await this.makeRequest(endpoint);
                        if (altResult && altResult.invoices && Array.isArray(altResult.invoices) && altResult.invoices.length > invoiceList.length) {
                            invoiceList = altResult.invoices;
                            console.log(`✅ Alternative endpoint ${endpoint} returned ${invoiceList.length} invoices`);
                            break;
                        }
                    } catch (error) {
                        console.log(`❌ Alternative endpoint ${endpoint} failed: ${error.message}`);
                    }
                }
            }
            
            let newCount = 0;
            let updateCount = 0;
            let errorCount = 0;
            let skipCount = 0;
            
            for (const invoice of invoiceList) {
                try {
                    // Use the actual field names from the HaloPSA API response
                    const invoiceId = invoice.id || invoice.InvoiceID || invoice.InvoiceId || invoice.InvoiceNumber || null;
                    
                    if (!invoiceId) {
                        console.log('Skipping invoice without valid ID:', JSON.stringify(invoice, null, 2).substring(0, 200));
                        skipCount++;
                        continue;
                    }

                    // Extract comprehensive invoice data using enhanced extraction
                    const invoiceData = extractComprehensiveInvoiceData(invoice, this.getInvoiceStatus.bind(this));

                    console.log(`Processing invoice ${invoiceData.invoice_number || 'N/A'} (ID: ${invoiceData.halopsa_id}) for client ${invoiceData.client_name || 'N/A'}`);

                    // Validate required fields
                    if (!invoiceData.halopsa_id) {
                        console.log('❌ Skipping invoice - missing halopsa_id');
                        skipCount++;
                        continue;
                    }

                    // Handle edge case where invoice number is 0 or invalid
                    if (!invoiceData.invoice_number || invoiceData.invoice_number === '0' || invoiceData.invoice_number === 0) {
                        console.log(`⚠️ Invoice ${invoiceData.halopsa_id} has invalid invoice number (${invoiceData.invoice_number}), using ID as fallback`);
                        invoiceData.invoice_number = `INV-${invoiceData.halopsa_id}`;
                    }

                    // Check if invoice already exists
                    const existingInvoice = await this.db.get(
                        'SELECT id FROM halopsa_invoices WHERE halopsa_id = ? OR invoice_number = ?',
                        [invoiceData.halopsa_id, invoiceData.invoice_number]
                    );

                    if (existingInvoice) {
                        // Update existing invoice with comprehensive data
                        await this.db.run(UPDATE_INVOICE_SQL, getUpdateValues(invoiceData));
                        updateCount++;
                        console.log(`✅ Updated invoice ${invoiceData.invoice_number} with comprehensive data`);
                    } else {
                        // Insert new invoice with comprehensive data
                        await this.db.run(INSERT_INVOICE_SQL, getInsertValues(invoiceData));
                        newCount++;
                        console.log(`✅ Imported new invoice ${invoiceData.invoice_number} with comprehensive data`);
                    }
                } catch (dbError) {
                    console.error(`Error processing invoice:`, dbError);
                    console.log('Problematic invoice data:', JSON.stringify(invoice, null, 2).substring(0, 300));
                    errorCount++;
                }
            }
            
            console.log(`HaloPSA invoice import completed: ${newCount} new, ${updateCount} updated, ${errorCount} errors, ${skipCount} skipped`);
            console.log(`📊 Total invoices processed: ${invoiceList.length} from HaloPSA API`);
            
            return {
                success: true,
                message: `Imported ${newCount} new invoices, updated ${updateCount} existing invoices, ${errorCount} errors, ${skipCount} skipped (Total: ${invoiceList.length} from API)`,
                imported: newCount,
                updated: updateCount,
                errors: errorCount,
                total: newCount + updateCount,
                apiTotal: invoiceList.length
            };
            
            
            if (!invoiceResult.success) {
                return { success: false, message: invoiceResult.message };
            }
            
            let invoices = invoiceResult.data;
            
            // Handle different response formats
            if (Array.isArray(invoices)) {
                // Direct array of invoices
            } else if (invoices.root && Array.isArray(invoices.root)) {
                invoices = invoices.root;
            } else if (typeof invoices === 'object') {
                invoices = [invoices];
            } else {
                return { success: false, message: 'Invalid invoice data format received from HaloPSA API' };
            }
            
            console.log(`Received ${invoices.length} invoices from HaloPSA API`);
            
            // Debug: Log first few invoices to see actual structure
            if (invoices.length > 0) {
                console.log('First invoice sample structure:', JSON.stringify(invoices[0], null, 2).substring(0, 500));
                console.log('Invoice fields available:', Object.keys(invoices[0]).join(', '));
            }
            
            let importedCount = 0;
            let updatedCount = 0;
            let errors = 0;
            let skippedCount = 0;
            
            for (const invoice of invoices) {
                try {
                    // Use the actual field names from the logs: id, invoicenumber, client_id, client_name, etc.
                    const invoiceId = invoice.id || invoice.InvoiceID || invoice.InvoiceId || invoice.InvoiceNumber || null;
                    
                    if (!invoiceId) {
                        console.log('Skipping invoice without valid ID:', JSON.stringify(invoice, null, 2).substring(0, 200));
                        skippedCount++;
                        continue;
                    }
                    
                    // Extract invoice data using the actual field names from the HaloPSA API response
                    const invoiceData = {
                        halopsa_id: invoiceId,
                        invoice_number: invoice.invoicenumber || invoice.invoice_number || invoice.InvoiceNumber || invoice.number || invoice.Number || null,
                        halopsa_client_id: invoice.client_id || invoice.ClientID || invoice.clientId || null,
                        client_name: invoice.client_name || invoice.ClientName || invoice.account_name || invoice.AccountName || invoice.name || null,
                        invoice_date: invoice.invoice_date || invoice.InvoiceDate || invoice.date || invoice.Date || null,
                        due_date: invoice.due_date || invoice.DueDate || invoice.duedate || null,
                        total_amount: invoice.total || invoice.total_amount || invoice.TotalAmount || invoice.amount || invoice.revenue || 0,
                        paid_amount: invoice.amountpaid || invoice.paid_amount || invoice.PaidAmount || invoice.paid || invoice.paidTotal || 0,
                        status: this.getInvoiceStatus(invoice),
                        raw_data: JSON.stringify(invoice)
                    };
                    
                    // Log processing for debugging with more details
                    console.log(`Processing invoice ${invoiceData.invoice_number || 'N/A'} (ID: ${invoiceData.halopsa_id}) for client ${invoiceData.client_name || 'N/A'}`);
                    
                    // Validate required fields
                    if (!invoiceData.halopsa_id) {
                        console.log('❌ Skipping invoice - missing halopsa_id');
                        skippedCount++;
                        continue;
                    }
                    
                    if (!invoiceData.invoice_number) {
                        console.log(`⚠️ Invoice ${invoiceData.halopsa_id} missing invoice number, using ID as fallback`);
                        invoiceData.invoice_number = `INV-${invoiceData.halopsa_id}`;
                    }
                    
                    // Check if invoice already exists
                    const existingInvoice = await this.db.get(
                        'SELECT id FROM halopsa_invoices WHERE halopsa_id = ? OR invoice_number = ?',
                        [invoiceData.halopsa_id, invoiceData.invoice_number]
                    );
                    
                    if (existingInvoice) {
                        // Update existing invoice
                        await this.db.run(
                            `UPDATE halopsa_invoices SET 
                             invoice_number = ?, halopsa_client_id = ?, client_name = ?, invoice_date = ?,
                             due_date = ?, total_amount = ?, paid_amount = ?, status = ?, raw_data = ?,
                             last_sync = CURRENT_TIMESTAMP
                             WHERE id = ?`,
                            [...Object.values(invoiceData).slice(1), existingInvoice.id]
                        );
                        updatedCount++;
                        console.log(`✅ Updated invoice ${invoiceData.invoice_number}`);
                    } else {
                        // Insert new invoice
                        await this.db.run(
                            `INSERT INTO halopsa_invoices 
                             (halopsa_id, invoice_number, halopsa_client_id, client_name, invoice_date,
                             due_date, total_amount, paid_amount, status, raw_data, last_sync) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                            Object.values(invoiceData)
                        );
                        importedCount++;
                        console.log(`✅ Imported new invoice ${invoiceData.invoice_number}`);
                    }
                } catch (dbError) {
                    console.error(`Error processing invoice:`, dbError);
                    console.log('Problematic invoice data:', JSON.stringify(invoice, null, 2).substring(0, 300));
                    errors++;
                }
            }
            
            console.log(`HaloPSA invoice import completed: ${importedCount} new, ${updatedCount} updated, ${errors} errors, ${skippedCount} skipped`);
            
            return {
                success: true,
                message: `Imported ${importedCount} new invoices, updated ${updatedCount} existing invoices, ${errors} errors, ${skippedCount} skipped`,
                imported: importedCount,
                updated: updatedCount,
                errors: errors,
                total: importedCount + updatedCount
            };
            
        } catch (error) {
            console.error('Error importing HaloPSA invoices:', error);
            return { success: false, message: `Error importing HaloPSA invoices: ${error.message}` };
        }
    }

    /**
     * Quick test for invoice endpoints - returns first few invoices for testing
     */
    async getSampleInvoices() {
        try {
            console.log('Testing invoice endpoints for quick sample...');
            
            // Try different endpoints with small limits
            const testEndpoints = [
                '/api/Invoice?count=3',  // This one actually returned data
                '/api/Invoice?limit=3',
                '/api/Invoices?limit=3', 
                '/api/Invoice?take=3',
                '/api/Invoice?pageSize=3'
            ];
            
            for (const endpoint of testEndpoints) {
                try {
                    console.log(`Trying invoice endpoint: ${endpoint}`);
                    const result = await Promise.race([
                        this.makeRequest(endpoint),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout after 30 seconds')), 30000))
                    ]);
                    
                    if (result) {
                        let invoices = result;
                        
                        // Handle the actual response format from the working endpoint
                        if (result.invoices && Array.isArray(result.invoices)) {
                            invoices = result.invoices;
                            console.log(`✅ Found ${invoices.length} invoices via ${endpoint} (record_count: ${result.record_count || 'N/A'})`);
                        } else if (result.items && Array.isArray(result.items)) {
                            invoices = result.items;
                        } else if (result.data && Array.isArray(result.data)) {
                            invoices = result.data;
                        } else if (result.records && Array.isArray(result.records)) {
                            invoices = result.records;
                        } else if (Array.isArray(result)) {
                            invoices = result;
                        }
                        
                        if (invoices.length > 0) {
                            console.log(`✅ Quick test successful: found ${invoices.length} invoice(s) via ${endpoint}`);
                            return { success: true, data: invoices, endpoint: endpoint, fullResponse: result };
                        }
                    }
                } catch (error) {
                    console.log(`❌ Endpoint ${endpoint} failed: ${error.message}`);
                }
            }
            
            return { success: false, message: 'No working invoice endpoints found for quick test' };
            
        } catch (error) {
            console.error('Quick invoice test error:', error);
            return { success: false, message: `Quick test error: ${error.message}` };
        }
    }

    /**
     * Extract invoice status from HaloPSA invoice data
     * Based on paymentstatus field and other indicators
     */
    getInvoiceStatus(invoice) {
        // Handle different status field names
        const status = invoice.paymentstatus || invoice.status || invoice.Status || invoice.state || 'unknown';

        // Convert numeric status codes to meaningful strings if needed
        if (typeof status === 'number') {
            switch (status) {
                case 0: return 'Unpaid';
                case 1: return 'Partially Paid';
                case 2: return 'Paid';
                case 3: return 'Overdue';
                case 4: return 'Voided';
                default: return `Status ${status}`;
            }
        }

        // Handle string statuses
        const statusStr = String(status).toLowerCase();
        if (statusStr.includes('paid') || statusStr === '2') return 'Paid';
        if (statusStr.includes('partial') || statusStr === '1') return 'Partially Paid';
        if (statusStr.includes('unpaid') || statusStr === '0') return 'Unpaid';
        if (statusStr.includes('overdue') || statusStr === '3') return 'Overdue';
        if (statusStr.includes('void') || statusStr === '4') return 'Voided';

        return statusStr.charAt(0).toUpperCase() + statusStr.slice(1);
    }

    /**
     * Import purchase orders from HaloPSA Report ID 350 (Purchase Order Export)
     * Report 350 returns LINE ITEMS, not PO headers - we group them by PO_ID to reconstruct POs
     */
    async importPurchaseOrders() {
        try {
            console.log('Starting import of purchase orders from HaloPSA Report 350 (line items)...');

            // Report 350 returns individual line items with these fields:
            // Item, Description, Qty, Cost, Amount, Vendor, Date, RefNumber, PO_ID
            const reportId = 350;

            // Fetch all line items from Report 350 with pagination
            let allLineItems = [];
            let page = 1;
            let hasMore = true;
            const pageSize = 100;

            console.log(`Fetching line items from Report ${reportId} with pagination...`);

            while (hasMore) {
                try {
                    const endpoint = `/api/Report/${reportId}?loadreport=true&page_no=${page}&page_size=${pageSize}`;
                    console.log(`Fetching page ${page} from Report ${reportId}...`);

                    const response = await this.makeRequest(endpoint);

                    if (response && response.report && response.report.rows) {
                        const rows = response.report.rows;
                        allLineItems = allLineItems.concat(rows);

                        console.log(`✅ Page ${page}: Found ${rows.length} line items (Total so far: ${allLineItems.length})`);

                        // Continue if we got a full page
                        hasMore = rows.length === pageSize;
                        page++;

                        // Small delay between pages
                        await new Promise(resolve => setTimeout(resolve, 200));
                    } else {
                        console.log(`⚠️ No rows found in response for page ${page}`);
                        hasMore = false;
                    }
                } catch (error) {
                    console.error(`Error fetching Report ${reportId} page ${page}:`, error.message);
                    hasMore = false;
                }
            }

            if (allLineItems.length === 0) {
                return {
                    success: false,
                    message: `No line items found in Report ${reportId}. Check API permissions or report configuration.`
                };
            }

            console.log(`\n📊 Fetched ${allLineItems.length} total line items from Report ${reportId}`);
            console.log(`Now grouping by PO to create purchase order headers...\n`);

            // Group line items by PO_ID to reconstruct purchase orders
            const poGroups = {};
            for (const lineItem of allLineItems) {
                const poId = lineItem.PO_ID || lineItem.po_id;
                if (!poId) {
                    console.log('⚠️ Skipping line item without PO_ID:', lineItem);
                    continue;
                }

                if (!poGroups[poId]) {
                    poGroups[poId] = {
                        po_id: poId,
                        po_number: lineItem.RefNumber || lineItem.refnumber || `PO-${poId}`,
                        vendor_name: lineItem.Vendor || lineItem.vendor,
                        po_date: lineItem.Date || lineItem.date,
                        line_items: []
                    };
                }

                // Add line item to this PO
                poGroups[poId].line_items.push({
                    item: lineItem.Item || lineItem.item,
                    description: lineItem.Description || lineItem.description,
                    qty: parseFloat(lineItem.Qty || lineItem.qty || 0),
                    cost: parseFloat(lineItem.Cost || lineItem.cost || 0),
                    amount: parseFloat(lineItem.Amount || lineItem.amount || 0)
                });
            }

            console.log(`✅ Grouped line items into ${Object.keys(poGroups).length} purchase orders\n`);
            console.log(`Processing purchase orders and line items...`);

            let newCount = 0;
            let updateCount = 0;
            let errorCount = 0;
            let itemsAdded = 0;
            let itemsUpdated = 0;

            // Process each purchase order group
            for (const [poId, poGroup] of Object.entries(poGroups)) {
                try {
                    // Calculate total amount from line items
                    const totalAmount = poGroup.line_items.reduce((sum, item) => sum + item.amount, 0);

                    // Create PO data for database insertion
                    const poData = {
                        halopsa_id: poGroup.po_id,
                        po_number: poGroup.po_number,
                        vendor_name: poGroup.vendor_name,
                        po_date: poGroup.po_date,
                        total_amount: totalAmount,
                        subtotal: totalAmount,  // Simplified - no tax breakdown in Report 350
                        status: 'Imported from Report',
                        line_items: JSON.stringify(poGroup.line_items),  // Store line items as JSON
                        raw_data: JSON.stringify(poGroup),  // Store full PO group data
                        synced_to_qb: 0
                    };

                    console.log(`Processing PO ${poData.po_number} (ID: ${poData.halopsa_id}) - ${poGroup.line_items.length} line items, total: $${totalAmount.toFixed(2)}`);

                    // Check if PO already exists
                    const existingPO = await this.db.get(
                        'SELECT id FROM halopsa_purchase_orders WHERE halopsa_id = ? OR po_number = ?',
                        [poData.halopsa_id, poData.po_number]
                    );

                    if (existingPO) {
                        // Update existing PO
                        await this.db.run(
                            `UPDATE halopsa_purchase_orders SET
                             po_number = ?, vendor_name = ?, po_date = ?, total_amount = ?, subtotal = ?,
                             status = ?, line_items = ?, raw_data = ?, last_sync = CURRENT_TIMESTAMP
                             WHERE halopsa_id = ?`,
                            [poData.po_number, poData.vendor_name, poData.po_date, poData.total_amount,
                             poData.subtotal, poData.status, poData.line_items, poData.raw_data, poData.halopsa_id]
                        );
                        updateCount++;
                        console.log(`✅ Updated PO ${poData.po_number}`);
                    } else {
                        // Insert new PO
                        await this.db.run(
                            `INSERT INTO halopsa_purchase_orders
                             (halopsa_id, po_number, vendor_name, po_date, total_amount, subtotal,
                              status, line_items, raw_data, synced_to_qb, last_sync)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                            [poData.halopsa_id, poData.po_number, poData.vendor_name, poData.po_date,
                             poData.total_amount, poData.subtotal, poData.status, poData.line_items,
                             poData.raw_data, poData.synced_to_qb]
                        );
                        newCount++;
                        console.log(`✅ Imported new PO ${poData.po_number}`);
                    }

                    // Extract and import line items to qb_items table
                    for (const lineItem of poGroup.line_items) {
                        try {
                            const itemName = lineItem.item || `Item-${poData.po_number}-Line`;
                            const itemType = 'Inventory';

                            // Check if item exists by name
                            const existingItem = await this.db.get(
                                'SELECT id, quantity_on_hand FROM qb_items WHERE name = ? AND item_type = ?',
                                [itemName, itemType]
                            );

                            if (existingItem) {
                                // Update existing item - accumulate quantity
                                await this.db.run(
                                    `UPDATE qb_items SET
                                     description = ?, purchase_description = ?, purchase_cost = ?,
                                     quantity_on_hand = ?, source_system = ?, last_sync = CURRENT_TIMESTAMP
                                     WHERE id = ?`,
                                    [lineItem.description, lineItem.description, lineItem.cost,
                                     existingItem.quantity_on_hand + lineItem.qty, 'HaloPSA-Report350',
                                     existingItem.id]
                                );
                                itemsUpdated++;
                            } else {
                                // Insert new item
                                await this.db.run(
                                    `INSERT INTO qb_items
                                     (item_type, name, description, purchase_description, purchase_cost,
                                      quantity_on_hand, source_system, synced_to_qb, is_active, last_sync)
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
                                    [itemType, itemName, lineItem.description, lineItem.description,
                                     lineItem.cost, lineItem.qty, 'HaloPSA-Report350', 0, 1]
                                );
                                itemsAdded++;
                            }
                        } catch (itemError) {
                            console.error(`Error processing item "${lineItem.item}" from PO ${poData.po_number}:`, itemError.message);
                        }
                    }

                } catch (dbError) {
                    console.error(`Error processing PO ${poGroup.po_number}:`, dbError);
                    errorCount++;
                }
            }

            console.log(`\n📊 Purchase Order Import Summary:`);
            console.log(`   POs: ${newCount} new, ${updateCount} updated (${Object.keys(poGroups).length} total)`);
            console.log(`   Items: ${itemsAdded} new, ${itemsUpdated} updated`);
            console.log(`   Errors: ${errorCount}`);
            console.log(`   Line items processed: ${allLineItems.length}\n`);

            return {
                success: true,
                message: `Imported ${newCount} new purchase orders, updated ${updateCount} existing POs. Items: ${itemsAdded} new, ${itemsUpdated} updated. ${errorCount} errors (Total line items: ${allLineItems.length})`,
                imported: newCount,
                updated: updateCount,
                errors: errorCount,
                items_added: itemsAdded,
                items_updated: itemsUpdated,
                total: Object.keys(poGroups).length,
                line_items_total: allLineItems.length
            };

        } catch (error) {
            console.error('Error importing purchase orders:', error);
            return { success: false, message: `Error importing purchase orders: ${error.message}` };
        }
    }

    /**
     * Extract purchase order status from HaloPSA PO data
     */
    getPOStatus(po) {
        // Handle different status field names
        const status = po.status || po.Status || po.state || po.approval_status || po.approvalStatus || 'unknown';

        // Convert numeric status codes to meaningful strings if needed
        if (typeof status === 'number') {
            switch (status) {
                case 0: return 'Draft';
                case 1: return 'Pending Approval';
                case 2: return 'Approved';
                case 3: return 'Ordered';
                case 4: return 'Partially Received';
                case 5: return 'Received';
                case 6: return 'Cancelled';
                case 7: return 'Closed';
                default: return `Status ${status}`;
            }
        }

        // Handle string statuses
        const statusStr = String(status).toLowerCase();
        if (statusStr.includes('draft')) return 'Draft';
        if (statusStr.includes('pending')) return 'Pending Approval';
        if (statusStr.includes('approved')) return 'Approved';
        if (statusStr.includes('ordered')) return 'Ordered';
        if (statusStr.includes('partial')) return 'Partially Received';
        if (statusStr.includes('received')) return 'Received';
        if (statusStr.includes('cancel')) return 'Cancelled';
        if (statusStr.includes('closed') || statusStr.includes('complete')) return 'Closed';

        return statusStr.charAt(0).toUpperCase() + statusStr.slice(1);
    }
}

module.exports = HaloPSAAPI;