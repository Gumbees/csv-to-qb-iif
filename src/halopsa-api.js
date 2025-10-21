const { BrowserWindow, session } = require('electron');
const axios = require('axios');
const { parse } = require('url');
const Store = require('electron-store');

// Initialize secure storage for tokens
const tokenStore = new Store({
  name: 'halopsa-auth',
  encryptionKey: 'csv-to-qb-iif-halopsa-auth-key-2024'
});

class HaloPSAAPI {
  constructor(config) {
    this.clientId = config.clientId;
    this.clientSecret = config.clientSecret;
    this.redirectUri = config.redirectUri || 'http://localhost:8080/callback';
    this.baseUrl = config.baseUrl || 'https://dtcdev.halopsa.com';
    this.tenant = config.tenant || 'dtcteamdev';
    this.scope = config.scope || 'all';
    
    // Load stored tokens if available
    this.accessToken = tokenStore.get('accessToken');
    this.refreshToken = tokenStore.get('refreshToken');
    this.tokenExpiry = tokenStore.get('tokenExpiry');
  }

  /**
   * Get the authorization URL for OAuth flow
   */
  getAuthorizationUrl() {
    const params = new URLSearchParams({
      response_type: 'code',
      client_id: this.clientId,
      redirect_uri: this.redirectUri,
      scope: this.scope,
      tenant: this.tenant,
      state: Date.now().toString() // Add state for security
    });

    return `${this.baseUrl}/auth/authorize?${params.toString()}`;
  }

  /**
   * Authenticate user via OAuth 2.0 Authorization Code flow
   */
  async authenticate() {
    return new Promise((resolve, reject) => {
      const authWindow = new BrowserWindow({
        width: 800,
        height: 600,
        webPreferences: {
          nodeIntegration: false,
          contextIsolation: true,
          webSecurity: true
        },
        show: false,
        title: 'HaloPSA Authentication',
        modal: true
      });

      const authUrl = this.getAuthorizationUrl();
      console.log('HaloPSA Auth URL:', authUrl);

      authWindow.loadURL(authUrl);
      authWindow.show();

      // Set timeout for authentication
      const timeout = setTimeout(() => {
        authWindow.destroy();
        reject(new Error('Authentication timeout after 5 minutes'));
      }, 300000); // 5 minutes

      // Handle the callback
      const handleCallback = async (url) => {
        console.log('Callback URL received:', url);

        clearTimeout(timeout);

        const raw_code = /code=([^&]*)/.exec(url) || null;
        const code = (raw_code && raw_code.length > 1) ? raw_code[1] : null;
        const error = /[?&]error=([^&]*)/.exec(url);

        if (code || error) {
          // Close the browser if code found or error
          authWindow.destroy();
        }

        if (code) {
          try {
            console.log('Authorization code received, exchanging for token...');
            const tokens = await this.exchangeCodeForToken(code);
            resolve(tokens);
          } catch (err) {
            console.error('Error exchanging code for token:', err);
            reject(err);
          }
        } else if (error) {
          const errorMessage = decodeURIComponent(error[1]);
          console.error('OAuth error:', errorMessage);
          reject(new Error('OAuth authentication error: ' + errorMessage));
        }
      };

      // Handle window closed by user
      authWindow.on('closed', () => {
        clearTimeout(timeout);
        reject(new Error('Authentication window closed by user'));
      });

      // Listen for redirect
      authWindow.webContents.on('will-redirect', (event, url) => {
        if (url.startsWith(this.redirectUri)) {
          event.preventDefault();
          handleCallback(url);
        }
      });

      // Also check for navigation (some OAuth providers don't redirect)
      authWindow.webContents.on('will-navigate', (event, url) => {
        if (url.startsWith(this.redirectUri)) {
          event.preventDefault();
          handleCallback(url);
        }
      });

      // Handle page title change (some OAuth flows change title)
      authWindow.webContents.on('page-title-updated', (event, title) => {
        if (title.includes('authorization_code=') || title.includes('code=')) {
          const codeMatch = title.match(/(?:authorization_)?code=([^&\s]+)/);
          if (codeMatch) {
            handleCallback(`${this.redirectUri}?code=${codeMatch[1]}`);
          }
        }
      });
    });
  }

  /**
   * Exchange authorization code for access token
   */
  async exchangeCodeForToken(code) {
    try {
      console.log('Exchanging code for token...');

      const tokenData = new URLSearchParams({
        grant_type: 'authorization_code',
        client_id: this.clientId,
        client_secret: this.clientSecret,
        code: code,
        redirect_uri: this.redirectUri
      });

      console.log('Token request URL:', `${this.baseUrl}/auth/token`);
      console.log('Token request data:', tokenData.toString());

      const response = await axios.post(`${this.baseUrl}/auth/token`, tokenData, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json'
        },
        timeout: 30000 // 30 second timeout
      });

      console.log('Token response status:', response.status);
      console.log('Token response data:', response.data);

      const { access_token, refresh_token, expires_in } = response.data;

      if (!access_token) {
        throw new Error('No access token received from HaloPSA');
      }

      // Store tokens securely
      this.accessToken = access_token;
      this.refreshToken = refresh_token;
      this.tokenExpiry = Date.now() + ((expires_in || 3600) * 1000);

      tokenStore.set('accessToken', access_token);
      tokenStore.set('refreshToken', refresh_token);
      tokenStore.set('tokenExpiry', this.tokenExpiry);

      console.log('Tokens stored successfully');
      return response.data;
    } catch (error) {
      console.error('Token exchange error:', error.response?.data || error.message);

      if (error.response) {
        const errorMsg = error.response.data?.error_description ||
                        error.response.data?.error ||
                        `HTTP ${error.response.status}: ${error.response.statusText}`;
        throw new Error(`Failed to exchange code for token: ${errorMsg}`);
      } else {
        throw new Error(`Failed to exchange code for token: ${error.message}`);
      }
    }
  }

  /**
   * Refresh the access token using refresh token
   */
  async refreshAccessToken() {
    if (!this.refreshToken) {
      throw new Error('No refresh token available');
    }

    try {
      const response = await axios.post(`${this.baseUrl}/auth/token`,
        new URLSearchParams({
          grant_type: 'refresh_token',
          client_id: this.clientId,
          client_secret: this.clientSecret,
          refresh_token: this.refreshToken
        }),
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      const { access_token, refresh_token, expires_in } = response.data;
      
      // Update stored tokens
      this.accessToken = access_token;
      this.refreshToken = refresh_token;
      this.tokenExpiry = Date.now() + (expires_in * 1000);
      
      tokenStore.set('accessToken', access_token);
      tokenStore.set('refreshToken', refresh_token);
      tokenStore.set('tokenExpiry', this.tokenExpiry);

      return response.data;
    } catch (error) {
      throw new Error(`Failed to refresh token: ${error.message}`);
    }
  }

  /**
   * Check if token needs refresh and refresh if necessary
   */
  async ensureValidToken() {
    if (!this.accessToken) {
      throw new Error('Not authenticated. Please authenticate first.');
    }

    // Check if token is expired or will expire soon (5 minutes buffer)
    if (this.tokenExpiry && Date.now() > this.tokenExpiry - 300000) {
      await this.refreshAccessToken();
    }
  }

  /**
   * Make authenticated API request
   */
  async makeRequest(method, endpoint, data = null, params = null) {
    await this.ensureValidToken();

    try {
      const response = await axios({
        method,
        url: `${this.baseUrl}/api${endpoint}`,
        headers: {
          'Authorization': `Bearer ${this.accessToken}`,
          'Content-Type': 'application/json'
        },
        data,
        params
      });

      return response.data;
    } catch (error) {
      if (error.response?.status === 401) {
        // Token might be invalid, try refreshing
        await this.refreshAccessToken();
        
        // Retry the request
        return axios({
          method,
          url: `${this.baseUrl}/api${endpoint}`,
          headers: {
            'Authorization': `Bearer ${this.accessToken}`,
            'Content-Type': 'application/json'
          },
          data,
          params
        }).then(res => res.data);
      }
      
      throw error;
    }
  }

  /**
   * Get list of reports
   */
  async getReports(params = {}) {
    return this.makeRequest('GET', '/Report', null, params);
  }

  /**
   * Get specific report by ID
   */
  async getReport(reportId, includeData = true) {
    try {
      console.log(`Fetching report ${reportId} with data:`, includeData);

      const response = await this.makeRequest('GET', `/Report/${reportId}`, null, {
        includedetails: true,
        loadreport: includeData
      });

      console.log('Report response:', response);
      return response;
    } catch (error) {
      console.error(`Error fetching report ${reportId}:`, error);
      throw error;
    }
  }

  /**
   * Run a report and get CSV data
   */
  async runReportAsCSV(reportId, parameters = {}) {
    try {
      console.log(`Running report ${reportId} as CSV with parameters:`, parameters);

      // First get the report definition
      const reportDef = await this.getReport(reportId, false);
      console.log('Report definition:', reportDef);

      // Run the report with CSV format
      const csvResponse = await this.makeRequest('GET', `/Report/${reportId}/run`, null, {
        format: 'csv',
        ...parameters
      });

      console.log('CSV report response:', csvResponse);
      return csvResponse;
    } catch (error) {
      console.error(`Error running report ${reportId} as CSV:`, error);
      throw error;
    }
  }

  /**
   * Get purchase orders
   */
  async getPurchaseOrders(params = {}) {
    // Adjust endpoint based on actual HaloPSA API
    return this.makeRequest('GET', '/PurchaseOrder', null, params);
  }

  /**
   * Transform report data to CSV format matching application structure
   */
  transformReportDataToCSV(reportData, columnMapping = {}) {
    try {
      console.log('Transforming report data to CSV format');
      console.log('Report data structure:', reportData);

      const rows = reportData.rows || reportData.data || reportData;
      if (!Array.isArray(rows)) {
        throw new Error('Report data is not in array format');
      }

      // Default column mapping - can be customized per report
      const defaultMapping = {
        vendor: ['vendor', 'supplier', 'supplier_name', 'company', 'client'],
        refNumber: ['ref_number', 'po_number', 'reference', 'ref', 'invoice_number'],
        date: ['date', 'transaction_date', 'po_date', 'created_date', 'date_created'],
        item: ['item', 'product', 'item_code', 'description', 'service'],
        description: ['description', 'notes', 'details', 'memo'],
        qty: ['qty', 'quantity', 'amount', 'hours'],
        cost: ['cost', 'unit_cost', 'price', 'rate', 'amount'],
        terms: ['terms', 'payment_terms', 'net_terms']
      };

      const mapping = { ...defaultMapping, ...columnMapping };

      // Transform data to CSV format
      const csvData = rows.map(row => {
        const csvRow = {};

        // Find values using column mapping
        for (const [csvField, possibleColumns] of Object.entries(mapping)) {
          let value = '';

          // Try to find the value in the row using various column names
          for (const colName of possibleColumns) {
            if (row[colName] !== undefined && row[colName] !== null) {
              value = row[colName];
              break;
            }
          }

          // Apply formatting based on field type
          switch (csvField) {
            case 'vendor':
              csvRow.Vendor = String(value || '').trim();
              break;
            case 'refNumber':
              csvRow.RefNumber = String(value || '').trim();
              break;
            case 'date':
              csvRow.Date = this.formatDate(value);
              csvRow.TxnDate = this.formatDate(value);
              break;
            case 'item':
              csvRow.Item = String(value || '').trim();
              break;
            case 'description':
              csvRow.Description = String(value || '').trim();
              break;
            case 'qty':
              csvRow.Qty = Number(value) || 0;
              break;
            case 'cost':
              csvRow.Cost = Number(value) || 0;
              break;
            case 'terms':
              csvRow.Terms = String(value || 'Net 30').trim();
              break;
          }
        }

        // Ensure required fields have defaults
        csvRow.Vendor = csvRow.Vendor || 'Unknown Vendor';
        csvRow.RefNumber = csvRow.RefNumber || `REP-${Date.now()}`;
        csvRow.Date = csvRow.Date || new Date().toLocaleDateString('en-US');
        csvRow.TxnDate = csvRow.TxnDate || csvRow.Date;
        csvRow.Item = csvRow.Item || 'Unknown Item';
        csvRow.Qty = csvRow.Qty || 1;
        csvRow.Cost = csvRow.Cost || 0;
        csvRow.Terms = csvRow.Terms || 'Net 30';

        return csvRow;
      });

      console.log(`Transformed ${csvData.length} rows to CSV format`);
      return csvData;
    } catch (error) {
      console.error('Error transforming report data:', error);
      throw new Error(`Failed to transform report data: ${error.message}`);
    }
  }

  /**
   * Format date for CSV export
   */
  formatDate(dateValue) {
    if (!dateValue) return new Date().toLocaleDateString('en-US');

    try {
      const date = new Date(dateValue);
      if (isNaN(date.getTime())) {
        return new Date().toLocaleDateString('en-US');
      }
      return date.toLocaleDateString('en-US');
    } catch (error) {
      return new Date().toLocaleDateString('en-US');
    }
  }

  /**
   * Export purchase orders as CSV-compatible data
   */
  async exportPurchaseOrdersAsCSV(params = {}) {
    try {
      console.log('Exporting purchase orders as CSV with params:', params);

      const orders = await this.getPurchaseOrders(params);
      console.log(`Found ${orders.length} purchase orders`);

      // Transform HaloPSA data to match expected CSV format
      const csvData = [];

      for (const order of orders) {
        try {
          // Get purchase order lines
          const orderDetails = await this.makeRequest('GET', `/PurchaseOrder/${order.id}/lines`);
          const lines = orderDetails.lines || orderDetails || [];

          if (lines.length === 0) {
            // If no lines, create a single entry for the order
            csvData.push({
              'Vendor': order.supplier_name || order.vendor || 'Unknown Vendor',
              'RefNumber': order.po_number || order.reference || order.id,
              'Date': this.formatDate(order.date_created || order.created_date),
              'Item': 'Purchase Order',
              'Description': order.description || order.notes || '',
              'Qty': 1,
              'Cost': order.total_amount || order.amount || 0,
              'Terms': order.payment_terms || 'Net 30',
              'TxnDate': this.formatDate(order.date_created || order.created_date)
            });
          } else {
            // Create entries for each line item
            for (const line of lines) {
              csvData.push({
                'Vendor': order.supplier_name || order.vendor || 'Unknown Vendor',
                'RefNumber': order.po_number || order.reference || order.id,
                'Date': this.formatDate(order.date_created || order.created_date),
                'Item': line.item_code || line.product || line.description || 'Unknown Item',
                'Description': line.description || line.notes || '',
                'Qty': line.quantity || line.qty || 1,
                'Cost': line.unit_cost || line.cost || line.price || 0,
                'Terms': order.payment_terms || 'Net 30',
                'TxnDate': this.formatDate(order.date_created || order.created_date)
              });
            }
          }
        } catch (lineError) {
          console.warn(`Failed to get lines for order ${order.id}:`, lineError.message);

          // Add order without line details
          csvData.push({
            'Vendor': order.supplier_name || order.vendor || 'Unknown Vendor',
            'RefNumber': order.po_number || order.reference || order.id,
            'Date': this.formatDate(order.date_created || order.created_date),
            'Item': 'Purchase Order',
            'Description': order.description || order.notes || '',
            'Qty': 1,
            'Cost': order.total_amount || order.amount || 0,
            'Terms': order.payment_terms || 'Net 30',
            'TxnDate': this.formatDate(order.date_created || order.created_date)
          });
        }
      }

      console.log(`Generated ${csvData.length} CSV rows from purchase orders`);
      return csvData;
    } catch (error) {
      console.error('Error exporting purchase orders:', error);
      throw new Error(`Failed to export purchase orders: ${error.message}`);
    }
  }

  /**
   * Clear stored authentication
   */
  logout() {
    this.accessToken = null;
    this.refreshToken = null;
    this.tokenExpiry = null;
    
    tokenStore.delete('accessToken');
    tokenStore.delete('refreshToken');
    tokenStore.delete('tokenExpiry');
  }

  /**
   * Check if user is authenticated
   */
  isAuthenticated() {
    return !!this.accessToken;
  }
}

module.exports = HaloPSAAPI;
