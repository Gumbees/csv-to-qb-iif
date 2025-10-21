# Plaid Integration Design for ACH Bank Transactions

## Overview
Integrate Plaid API to import bank transactions, specifically ACH transactions, for reconciliation with invoices and QuickBooks export.

## Architecture

### Database Schema Extensions
```sql
-- Plaid OAuth tokens
CREATE TABLE plaid_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  access_token TEXT NOT NULL,
  item_id TEXT UNIQUE NOT NULL,
  institution_id TEXT,
  institution_name TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Plaid connected accounts
CREATE TABLE plaid_accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plaid_token_id INTEGER NOT NULL,
  account_id TEXT UNIQUE NOT NULL,
  account_name TEXT,
  account_type TEXT,
  account_subtype TEXT,
  mask TEXT,
  current_balance REAL,
  available_balance REAL,
  iso_currency_code TEXT DEFAULT 'USD',
  active BOOLEAN DEFAULT TRUE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (plaid_token_id) REFERENCES plaid_tokens (id)
);

-- Bank transactions from Plaid
CREATE TABLE bank_transactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plaid_account_id INTEGER NOT NULL,
  transaction_id TEXT UNIQUE NOT NULL,
  account_id TEXT NOT NULL,
  amount REAL NOT NULL,
  iso_currency_code TEXT DEFAULT 'USD',
  date TEXT NOT NULL,
  datetime DATETIME,
  authorized_date TEXT,
  authorized_datetime DATETIME,
  name TEXT NOT NULL,
  merchant_name TEXT,
  payment_channel TEXT,
  transaction_type TEXT,
  account_owner TEXT,
  category TEXT,
  subcategory TEXT,
  pending BOOLEAN DEFAULT FALSE,
  invoice_id INTEGER,
  matched_to_transaction BOOLEAN DEFAULT FALSE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (plaid_account_id) REFERENCES plaid_accounts (id),
  FOREIGN KEY (invoice_id) REFERENCES invoices (id)
);
```

### Key Components

#### 1. PlaidOAuth Class (`src/plaid-oauth.js`)
- **Link Token Generation**: Create link tokens for Plaid Link
- **Public Token Exchange**: Convert public tokens to access tokens
- **Item Management**: Store and manage Plaid items
- **Token Refresh**: Handle token rotation

#### 2. PlaidAPI Class (`src/plaid-api.js`)
- **Account Sync**: Fetch and sync account information
- **Transaction Import**: Import bank transactions
- **Balance Updates**: Sync account balances
- **Transaction Categorization**: Categorize ACH vs other transactions

#### 3. Bank Transaction Matching
- **ACH Pattern Detection**: Identify ACH transactions by pattern
- **Invoice Matching**: Match transactions to invoices by amount/date
- **Customer Matching**: Match by customer name/account info
- **Confidence Scoring**: Score matches for manual review

### Plaid Link Integration

#### Frontend Flow:
1. **Initialize Link**: Create link token from backend
2. **Open Plaid Link**: Launch Plaid Link modal
3. **Handle Success**: Exchange public token for access token
4. **Store Credentials**: Save access token and item info

#### Backend Flow:
1. **Token Exchange**: Convert public token to access token
2. **Account Discovery**: Fetch connected accounts
3. **Initial Sync**: Import recent transactions
4. **Webhook Setup**: Handle Plaid webhooks for updates

### Transaction Import Strategy

#### Import Filters:
- **Date Range**: Import last 30-90 days initially
- **Transaction Types**: Focus on ACH, wire transfers, checks
- **Amount Thresholds**: Optional minimum amounts
- **Account Types**: Checking, savings, business accounts

#### ACH Transaction Detection:
```javascript
function isACHTransaction(transaction) {
  const achPatterns = [
    /ach/i,
    /wire/i,
    /transfer/i,
    /payment/i,
    /direct deposit/i,
    /electronic/i
  ];

  return achPatterns.some(pattern =>
    pattern.test(transaction.name) ||
    pattern.test(transaction.payment_channel)
  );
}
```

#### Matching Logic:
1. **Exact Amount Match**: Transaction amount = invoice amount
2. **Date Proximity**: Within 7 days of invoice due date
3. **Customer Name Match**: Fuzzy matching of names
4. **Reference Number**: Match transaction description to invoice numbers

### Security Considerations

#### Data Protection:
- **Encrypted Storage**: Encrypt access tokens at rest
- **Scope Limitation**: Request minimal required permissions
- **Token Rotation**: Handle Plaid token updates
- **Audit Logging**: Log all bank data access

#### Compliance:
- **PCI Compliance**: No card data handling
- **Bank Regulations**: Follow banking API guidelines
- **Data Retention**: Configurable data retention policies

### API Integration Points

#### Plaid Endpoints:
- `/link/token/create` - Create link tokens
- `/item/public_token/exchange` - Exchange tokens
- `/accounts/get` - Fetch account info
- `/transactions/get` - Import transactions
- `/accounts/balance/get` - Get current balances

#### Webhook Handlers:
- `TRANSACTIONS` - New transaction data available
- `ITEM_ERROR` - Item requires user intervention
- `DEFAULT_UPDATE` - Transaction/balance updates

### User Experience

#### Connection Flow:
1. **Bank Selection**: Choose from 11,000+ supported institutions
2. **Credential Entry**: Secure login through Plaid Link
3. **Account Selection**: Choose which accounts to sync
4. **Permission Grant**: Approve data access scopes

#### Dashboard Features:
- **Connected Accounts**: List all linked bank accounts
- **Balance Overview**: Current account balances
- **Recent Transactions**: Latest bank transactions
- **Match Suggestions**: Suggested invoice matches
- **ACH Summary**: ACH-specific transaction summary

### Implementation Phases

#### Phase 1: Basic Integration
- Plaid Link setup
- Token exchange
- Account listing
- Basic transaction import

#### Phase 2: Transaction Processing
- ACH transaction filtering
- Transaction categorization
- Duplicate detection
- Basic matching logic

#### Phase 3: Advanced Matching
- Fuzzy name matching
- Confidence scoring
- Manual match interface
- Bulk matching operations

#### Phase 4: Automation
- Webhook integration
- Automatic daily sync
- Smart matching rules
- Export integration

### Configuration

#### Environment Variables:
```env
PLAID_CLIENT_ID=your_plaid_client_id
PLAID_SECRET=your_plaid_secret
PLAID_ENV=sandbox  # sandbox, development, production
PLAID_PRODUCTS=transactions,auth,identity
PLAID_COUNTRY_CODES=US
```

#### Plaid Link Config:
```javascript
{
  products: ['transactions', 'auth'],
  country_codes: ['US'],
  language: 'en',
  user: {
    client_user_id: 'unique_user_id'
  },
  client_name: 'CSV to QuickBooks IIF',
  account_filters: {
    depository: {
      account_subtypes: ['checking', 'savings']
    }
  }
}
```

This design provides a comprehensive foundation for importing ACH bank transactions and integrating them with the existing invoice and QuickBooks workflow.