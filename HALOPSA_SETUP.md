# HaloPSA Integration Setup Guide

This guide walks you through setting up HaloPSA integration with the CSV to QB IIF application.

## Prerequisites

- HaloPSA account with API access permissions
- Report permissions in HaloPSA (for purchase order and invoice exports)
- Application registration in HaloPSA for OAuth2

## Step 1: Configure HaloPSA API Access

### OAuth2 Authentication (Required)

HaloPSA uses **OAuth 2.0 Authorization Code flow** for authentication.

#### 1. Register Your Application

1. Go to: `https://login.halopsa.com/admin/applications`
2. Register a new application with:
   - **Application Name**: "CSV to QB IIF Integration"
   - **Application URL**: Your application's URL
   - **Redirect URI**: `https://your-app.com/halo-callback` (or appropriate callback URL)
3. Save the **Client ID** and **Client Secret**

#### 2. Manual Access Token Setup (Current Implementation)

Since this is a desktop application, we use a **manual OAuth2 workflow**:

1. **Get Authorization Code:**
   - Visit: `https://login.halopsa.com/oauth/authorize?response_type=code&client_id={YOUR_CLIENT_ID}&redirect_uri={YOUR_REDIRECT_URI}`
   - Log in and authorize the application
   - Copy the authorization code from the redirect URL

2. **Exchange Code for Token:**
   ```bash
   curl -X POST https://login.halopsa.com/oauth/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=authorization_code&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&code={AUTHORIZATION_CODE}&redirect_uri={REDIRECT_URI}"
   ```

3. **Save Access Token:**
   - Copy the `access_token` from the response
   - Paste it into the "OAuth Access Token" field in the application

#### Authentication Flow:
```
Authorization: Bearer {access_token}
```

**Note**: The access token expires and needs to be refreshed periodically. Future versions will implement automatic token refresh.

## Step 2: Application Configuration

### Using the Web Interface

1. Open the application and go to **Settings** → **HaloPSA**
2. Fill in the following fields:

| Field | Description | Example |
|-------|-------------|---------|
| HaloPSA Instance URL | Your HaloPSA instance URL | `https://yourcompany.halopsa.com` |
| OAuth Client ID | Client ID from HaloPSA app registration | `abc123def456` |
| OAuth Client Secret | Client Secret from HaloPSA app registration | `secret_key_here` |
| OAuth Access Token | Manually obtained access token | `eyJhbGci...` (JWT token) |
| Purchase Order Report ID | Report ID for PO export | `123` |
| Invoice Report ID | Report ID for invoice export | `456` |

### Manual Configuration via Database

You can also set these values directly in the database:

```sql
UPDATE config SET value = 'https://yourcompany.halopsa.com' WHERE key = 'halopsa_api_url';
UPDATE config SET value = 'your-client-id' WHERE key = 'halopsa_client_id';
UPDATE config SET value = 'your-client-secret' WHERE key = 'halopsa_client_secret';
UPDATE config SET value = 'your-access-token' WHERE key = 'halopsa_access_token';
UPDATE config SET value = '123' WHERE key = 'halopsa_purchase_order_report_id';
UPDATE config SET value = '456' WHERE key = 'halopsa_invoice_report_id';
```

## Step 3: Creating HaloPSA Reports

### Purchase Order Report Setup

1. In HaloPSA, go to **Reports** → **New Report**
2. Create a report that includes:
   - Purchase Order Number
   - Client Name/ID
   - PO Date
   - Total Amount
   - Status
   - Vendor Information

3. Save the report and note the Report ID from the URL or report settings

### Invoice Report Setup

1. In HaloPSA, go to **Reports** → **New Report**
2. Create a report that includes:
   - Invoice Number
   - Client Name/ID
   - Invoice Date
   - Due Date
   - Total Amount
   - Status
   - Payment Information

3. Save the report and note the Report ID

## Step 4: Testing the Integration

### Using the Test Script

Run the test script to verify your configuration:

```bash
node test-halopsa.js
```

### Using the API Endpoints

Test each endpoint individually:

1. **Test Connection**
   ```
   GET /api/halopsa/test
   ```

2. **Get Clients**
   ```
   GET /api/halopsa/clients
   ```

3. **Test Purchase Order Report**
   ```
   GET /api/halopsa/reports/purchase-orders
   ```

4. **Test Invoice Report**
   ```
   GET /api/halopsa/reports/invoices
   ```

5. **Import Data**
   ```
   POST /api/halopsa/import/clients
   POST /api/halopsa/import/purchase-orders
   POST /api/halopsa/import/invoices
   ```

## Step 5: Customer Mapping

### Automatic Matching

The application can automatically match Stripe customers with HaloPSA clients based on:
- Email address similarity
- Name similarity

Configure the match threshold in settings (default: 85% confidence).

### Manual Mapping

Use the customer mapping interface to manually link:
- Stripe Customer ID → HaloPSA Client ID

## Step 6: Data Flow

### Standard Integration Flow

1. **HaloPSA Data Import**
   - Clients are imported from HaloPSA
   - Purchase orders are imported via reports
   - Invoices are imported via reports

2. **Stripe Data Import**
   - Transactions are imported from Stripe
   - Customers are matched with HaloPSA clients

3. **QuickBooks Export**
   - Combined data is exported to QuickBooks IIF format

## Troubleshooting

### Common Issues

**Connection Failed**
- Verify API key and URL
- Check firewall/network access to HaloPSA
- Ensure API permissions are correct

**Report Data Empty**
- Verify Report IDs are correct
- Check report permissions in HaloPSA
- Ensure reports return data

**Client Matching Fails**
- Adjust match threshold in settings
- Verify email/name consistency between systems
- Use manual mapping for difficult cases

### Error Messages

| Error | Solution |
|-------|----------|
| `HaloPSA not configured` | Check API key and URL settings |
| `Report ID not configured` | Set Purchase Order and Invoice Report IDs |
| `Invalid report data` | Verify report structure and permissions |
| `Authentication failed` | Check API key or OAuth credentials |

## API Reference

### Configuration Endpoints

- `GET /api/config/halopsa` - Get HaloPSA configuration
- `POST /api/config/halopsa` - Update HaloPSA configuration

### Data Endpoints

- `GET /api/halopsa/clients` - Get HaloPSA clients
- `GET /api/halopsa/reports/purchase-orders` - Get PO report data
- `GET /api/halopsa/reports/invoices` - Get invoice report data
- `POST /api/halopsa/import/*` - Import HaloPSA data

### Testing Endpoints

- `GET /api/halopsa/test` - Test HaloPSA connection
- `GET /api/status` - Check system health including HaloPSA

## Security Considerations

- Store API keys securely in the database
- Use HTTPS for all API communications
- Regularly rotate API keys
- Limit API permissions to minimum required
- Monitor API usage logs in HaloPSA