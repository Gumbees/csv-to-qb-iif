# MSP Cash Accounting Platform Setup Guide

## Overview
This platform integrates HaloPSA invoices and Stripe payments for cash-based MSP accounting, providing direct integration with QuickBooks Desktop.

## New Features Added

### Time-Based UUIDs
- **Customer Internal IDs**: Now use UUID version 1 (time-based) instead of MD5 hashes
- **Benefits**: Chronological ordering, guaranteed uniqueness, better database performance
- **Format**: `xxxxxxxx-xxxx-1xxx-xxxx-xxxxxxxxxxxx` where timestamp is embedded

### UI-Configurable System
- **Centralized Configuration**: All settings stored in database `app_config` table
- **Categories**: Stripe, QuickBooks, Accounting, MSP, General settings
- **Types Supported**: String, Number, Boolean, Select, Password, JSON
- **Real-time Updates**: Configuration changes take effect immediately

### Configuration Management
- **Import/Export**: Backup and restore configuration as JSON
- **Validation**: Type-specific validation with error messages
- **System Status**: Real-time status monitoring of configured services

## Quick Start

### 1. Access the Application
The application is running at: **http://localhost:3000**

### 2. Configure System Settings
1. Go to **Configuration** tab
2. Set up Stripe credentials in **Stripe Settings** section
3. Configure QuickBooks settings in **QuickBooks Settings**
4. Set MSP-specific options in **MSP Settings**
5. Click **Save All Changes**

### 3. Set Up Stripe Integration

#### Get Your Stripe API Keys:
1. Go to [Stripe Dashboard](https://dashboard.stripe.com)
2. Navigate to Developers → API Keys
3. Copy your **Secret Key** (starts with `sk_`)

#### Configure in UI:
1. Go to Configuration → Stripe Settings
2. Enter your Stripe Secret Key
3. Set sync interval and enable features
4. Save configuration

### 4. Set Up Stripe Webhooks (Recommended)
For real-time payment processing:

1. In Stripe Dashboard: Developers → Webhooks
2. Add endpoint: `https://your-domain.com/webhook/stripe`
3. Copy the webhook secret to Configuration → Stripe Settings

## Core Features

### Cash-Based Accounting
- **Revenue Recognition**: Income recorded when payments are received (cash basis)
- **Fee Tracking**: Automatic Stripe fee calculation and tracking
- **Payment Dates**: Uses actual payment dates for accounting periods
- **Time-Based UUIDs**: Chronological customer identifiers for better tracking

### Direct Stripe Integration
- **Real-time Sync**: Pull customers and transactions directly from Stripe API
- **Webhook Support**: Instant updates for new payments
- **Subscription Tracking**: Monitor recurring revenue
- **Configurable Sync**: Adjust sync intervals through UI

### Configuration System
- **Database Storage**: All settings stored in SQLite database
- **UI Management**: No need to edit files or environment variables
- **Categories**: Organized by functionality (Stripe, QuickBooks, etc.)
- **Validation**: Type-safe configuration with error prevention

### HaloPSA Invoice Import
- **Duplicate Prevention**: Checksum-based duplicate detection
- **Status Tracking**: Paid/unpaid/partial status management
- **Payment Linking**: Automatically links Stripe payments to HaloPSA invoices
- **Configurable Thresholds**: Adjust auto-matching confidence scores

### Customer Mapping System
- **Auto-Matching**: Intelligent customer name matching with configurable thresholds
- **Manual Override**: Manual mapping confirmation
- **Unified IDs**: Time-based UUID customer identity across systems

## Configuration Categories

### Stripe Settings
- `stripe_secret_key` - API secret key (password type)
- `stripe_webhook_secret` - Webhook verification secret
- `stripe_sync_enabled` - Enable/disable automatic sync
- `stripe_sync_interval` - Sync frequency in minutes

### QuickBooks Settings
- `quickbooks_company_file` - Path to QB company file
- `quickbooks_sync_method` - Cash or accrual accounting
- `quickbooks_default_account` - Default deposit account

### Accounting Settings
- `accounting_currency` - Primary currency (USD, CAD, EUR, GBP)
- `accounting_timezone` - Timezone for date calculations
- `cash_basis_enabled` - Enable cash basis accounting

### MSP Settings
- `msp_tax_rate` - Default tax rate for invoices
- `msp_payment_terms` - Default payment terms
- `msp_invoice_prefix` - Invoice number prefix

### General Settings
- `auto_match_threshold` - Confidence score for customer matching (0-1)
- `duplicate_check_enabled` - Prevent duplicate imports
- `webhook_enabled` - Enable real-time webhooks

## Step-by-Step Setup

### Step 1: System Configuration
1. Access **Configuration** tab
2. Review all settings categories
3. Configure Stripe credentials first
4. Set MSP-specific options
5. Save all changes

### Step 2: Test Stripe Connection
1. Go to **Stripe Integration** tab
2. Click **Test Stripe Connection**
3. Verify successful connection in system status

### Step 3: Initial Data Sync
1. Click **Sync All Stripe Data**
2. Choose sync options (customers, transactions)
3. Set date range if needed
4. Review imported data

### Step 4: Import HaloPSA Invoices
1. Export invoices from HaloPSA as CSV
2. Go to **HaloPSA Invoices** tab
3. Upload CSV file
4. Review import results

### Step 5: Customer Mapping
1. Go to **Customer Mappings** tab
2. Run **Auto-Matching** (adjust threshold in config if needed)
3. Review suggestions and confirm matches
4. Manual mapping available for complex cases

### Step 6: QuickBooks Cash Basis Sync
1. Go to **QuickBooks Sync** tab
2. Set accounting period (start/end dates)
3. Click **Load QuickBooks Data**
4. Review transactions ready for sync
5. **Mark as Synced** after QuickBooks import

## Technical Implementation

### Time-Based UUIDs
```javascript
// Generated UUID format: time_low-time_mid-time_high_and_version-clock_seq-node
// Example: 1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed
// Benefits: Chronological, unique, database-friendly
```

### Configuration Database Schema
```sql
CREATE TABLE app_config (
    key TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL,
    type TEXT NOT NULL, -- string, number, boolean, json, select, password
    category TEXT NOT NULL, -- stripe, quickbooks, general, accounting, msp
    label TEXT NOT NULL,
    description TEXT,
    options TEXT -- JSON array for select options
);
```

### Configuration API Endpoints
- `GET /api/config` - Get all configuration
- `PUT /api/config/:key` - Update single setting
- `PUT /api/config` - Bulk update settings
- `GET /api/config/export` - Export configuration as JSON
- `POST /api/config/import` - Import configuration from JSON
- `GET /api/config/status` - Get system status

## CSV File Formats

### HaloPSA Invoices CSV
Required columns:
- `InvoiceID` or `invoice_id` - Unique identifier
- `InvoiceNumber` or `invoice_number` - Human-readable number
- `CustomerName` or `customer_name` - Customer name
- `TotalAmount` or `total_amount` - Invoice total
- `InvoiceDate` or `invoice_date` - Invoice date

### Stripe Transactions CSV (Alternative to API)
Required columns:
- `ChargeID` or `charge_id` - Stripe charge ID
- `Amount` - Transaction amount
- `Created` - Transaction date

## QuickBooks Integration

### Cash Basis Accounting
- **Revenue**: Recorded when Stripe payments are received
- **Fees**: Stripe fees recorded as expenses
- **Dates**: Uses payment dates, not invoice dates
- **UUIDs**: Time-based customer identifiers for consistency

### Configuration-Driven Sync
- Adjust accounting method through UI (cash/accrual)
- Configure default accounts and currencies
- Set sync intervals and thresholds

## Advanced Features

### Real-time Configuration
- Changes take effect without restart
- Stripe API reinitialization on key updates
- Validation prevents invalid configurations

### Configuration Backup
- Export all settings as JSON file
- Import configuration for migration or recovery
- Version tracking and validation

### System Status Monitoring
- Real-time status of configured services
- Visual indicators for configuration completeness
- Error reporting for misconfigured services

## Troubleshooting

### Common Issues

#### Configuration Not Saving
- Check database permissions
- Verify configuration table exists
- Review browser console for errors

#### UUID Generation Issues
- Ensure system clock is accurate
- Check for duplicate time values
- Verify UUID format compliance

#### Stripe Connection Failed
- Verify API key in Configuration tab
- Check internet connectivity
- Test with Stripe test mode first

### Configuration Validation
- Type checking prevents invalid values
- Required field validation
- Option validation for select fields

## Security Features

### Configuration Security
- Password fields masked in UI
- No configuration in version control
- Database encryption for sensitive data

### API Key Management
- Secure storage in database
- No exposure in client-side code
- Regular rotation recommended

## Production Deployment

### Configuration Best Practices
1. Set `NODE_ENV=production`
2. Configure production Stripe keys through UI
3. Set up proper backups for configuration
4. Regular configuration exports

### Database Considerations
- SQLite database location: `C:\Users\Public\Documents\csv-to-qb-iif\csv-to-qb-iif.db`
- Regular backups recommended
- Monitor database size and performance

## Support and Maintenance

### Regular Tasks
- Monitor configuration changes
- Backup configuration regularly
- Update Stripe API configuration as needed
- Review system status indicators

### Configuration Migration
- Use export/import for environment changes
- Validate configuration after migration
- Test all integrated services

This platform now provides a fully configurable, robust cash-based accounting solution for MSPs with enterprise-grade configuration management and time-based UUID tracking.