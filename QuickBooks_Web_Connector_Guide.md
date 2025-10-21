# QuickBooks Web Connector Implementation Guide

## Overview
This guide explains how to use QuickBooks Web Connector (QBWC) as an alternative to IIF file imports for syncing HaloPSA purchase order data with QuickBooks Desktop.

## Why Use Web Connector Instead of IIF?

### Advantages of Web Connector:
- **Real-time sync**: Data transfers happen live between systems
- **Two-way communication**: Can both import and export data
- **Security**: No file handling required, uses secure web services
- **Automation**: Can be scheduled to run automatically
- **Reliability**: Built-in error handling and retry mechanisms

### IIF Limitations:
- **Manual process**: Requires downloading/uploading files
- **One-way only**: Primarily for importing data
- **File corruption risk**: IIF files can be sensitive to format errors
- **No feedback**: Difficult to track successful imports

## QuickBooks Web Connector Architecture

```
[HaloPSA Data] → [Web Service] ↔ [QBWC] ↔ [QuickBooks Desktop]
```

## Prerequisites

### Software Requirements:
1. **QuickBooks Desktop** (2015 or later recommended)
2. **QuickBooks Web Connector** (version 2.2.0.34 or later)
3. **.NET Framework** 4.5 or later
4. **Web server** (IIS, Node.js, etc.) for hosting the web service

### QuickBooks Edition Support:
- ✅ QuickBooks Pro (2015+)
- ✅ QuickBooks Premier (2015+) 
- ✅ QuickBooks Enterprise (2015+)
- ✅ Canadian/UK Editions (2015+)

## Implementation Steps

### Step 1: Install QuickBooks Web Connector

#### Latest Version (Recommended):
- **Download**: [QBWC 34.0.10010.76 for QB 2024](https://developer.intuit.com/app/developer/qbdesktop/docs/get-started/install-the-quickbooks-web-connector)
- **Installation**:
  1. Create temporary directory (e.g., `C:\temp`)
  2. Unzip downloaded file
  3. Run `QBWebConnectorInstaller.exe` as Administrator
  4. Follow installation prompts

#### Compatibility Notes:
- For QuickBooks POS, use version 2.1.0.30
- TLS 1.2 support requires QBWC 2.2.0.34+

### Step 2: Create the Web Service (.QWC) File

The `.qwc` file tells QBWC how to connect to your web service:

```xml
<?xml version="1.0" encoding="utf-8"?>
<QBWCXML>
    <AppName>HaloPSA Purchase Order Sync</AppName>
    <AppID></AppID>
    <AppURL>https://yourserver.com/qbwc</AppURL>
    <AppDescription>Sync HaloPSA purchase orders with QuickBooks</AppDescription>
    <AppSupport>https://yourserver.com/support</AppSupport>
    <UserName>halopsa_user</UserName>
    <OwnerID>{GUID-GOES-HERE}</OwnerID>
    <FileID>{GUID-GOES-HERE}</FileID>
    <QBType>QBFS</QBType>
    <Style>Document</Style>
    <Scheduler>
        <RunEveryNMinutes>60</RunEveryNMinutes>
    </Scheduler>
</QBWCXML>
```

### Step 3: Implement Required Web Service Methods

Your web service must implement these SOAP methods:

#### 1. `authenticate` - User authentication
```xml
<soap:Envelope>
    <soap:Body>
        <authenticate>
            <strUserName>username</strUserName>
            <strPassword>password</strPassword>
        </authenticate>
    </soap:Body>
</soap:Envelope>
```

#### 2. `sendRequestXML` - Send data to QuickBooks
```xml
<soap:Envelope>
    <soap:Body>
        <sendRequestXML>
            <ticket>session_ticket</ticket>
            <strHCPResponse>HCPResponse</strHCPResponse>
            <strCompanyFileName>company_file.qbw</strCompanyFileName>
            <qbXMLCountry>US</qbXMLCountry>
            <qbXMLMajorVers>13</qbXMLMajorVers>
            <qbXMLMinorVers>0</qbXMLMinorVers>
        </sendRequestXML>
    </soap:Body>
</soap:Envelope>
```

#### 3. `receiveResponseXML` - Receive response from QuickBooks
```xml
<soap:Envelope>
    <soap:Body>
        <receiveResponseXML>
            <ticket>session_ticket</ticket>
            <response>qbXML_response</response>
            <hresult>error_code</hresult>
            <message>status_message</message>
        </receiveResponseXML>
    </soap:Body>
</soap:Envelope>
```

#### 4. `connectionError` - Handle connection issues
#### 5. `getLastError` - Retrieve last error
#### 6. `closeConnection` - End session

### Step 4: QBXML Schema for Purchase Orders

Here's the QBXML structure for creating bills from purchase orders:

```xml
<?xml version="1.0" encoding="utf-8"?>
<?qbxml version="13.0"?>
<QBXML>
    <QBXMLMsgsRq onError="stopOnError">
        <BillAddRq>
            <BillAdd>
                <VendorRef>
                    <FullName>Vendor Name</FullName>
                </VendorRef>
                <APAccountRef>
                    <FullName>Accounts Payable</FullName>
                </APAccountRef>
                <TxnDate>2025-01-15</TxnDate>
                <RefNumber>PO-12345</RefNumber>
                <DueDate>2025-02-15</DueDate>
                <Memo>HaloPSA Purchase Order</Memo>
                <BillLineAdd>
                    <ItemRef>
                        <FullName>Inventory Item</FullName>
                    </ItemRef>
                    <Desc>Item Description</Desc>
                    <Quantity>10</Quantity>
                    <Cost>25.00</Cost>
                    <Amount>250.00</Amount>
                </BillLineAdd>
            </BillAdd>
        </BillAddRq>
    </QBXMLMsgsRq>
</QBXML>
```

### Step 5: Node.js Web Service Implementation

Here's a simplified Node.js implementation:

```javascript
const express = require('express');
const soap = require('soap');
const xml2js = require('xml2js');

const app = express();
app.use(express.raw({ type: 'text/xml' }));

// QBWC SOAP service implementation
const service = {
    QBWebConnectorSvc: {
        QBWebConnectorSvcSoap: {
            authenticate: function(args) {
                const { strUserName, strPassword } = args;
                // Validate credentials
                if (strUserName === 'halopsa_user' && strPassword === 'valid_password') {
                    return {
                        authenticateResult: {
                            string: [Math.random().toString(36).substring(7), 'none']
                        }
                    };
                }
                return { authenticateResult: { string: ['n', 'Invalid credentials'] } };
            },

            sendRequestXML: function(args) {
                const { ticket, strCompanyFileName } = args;
                
                // Generate QBXML for purchase orders
                const qbxml = generatePurchaseOrderQBXML();
                
                return {
                    sendRequestXMLResult: qbxml
                };
            },

            receiveResponseXML: function(args) {
                const { ticket, response, hresult, message } = args;
                
                // Process QuickBooks response
                console.log('QB Response:', response);
                
                return {
                    receiveResponseXMLResult: 0 // Success
                };
            },

            connectionError: function(args) {
                console.error('Connection error:', args);
                return { connectionErrorResult: 'done' };
            },

            getLastError: function(args) {
                return { getLastErrorResult: 'No error' };
            },

            closeConnection: function(args) {
                return { closeConnectionResult: 'OK' };
            }
        }
    }
};

// Generate QBXML from HaloPSA data
function generatePurchaseOrderQBXML() {
    const builder = new xml2js.Builder();
    const qbxml = {
        QBXML: {
            QBXMLMsgsRq: {
                $: { onError: 'stopOnError' },
                BillAddRq: [
                    // Add bills from HaloPSA purchase orders
                ]
            }
        }
    };
    
    return builder.buildObject(qbxml);
}

// SOAP WSDL
const wsdl = `<?xml version="1.0" encoding="utf-8"?>
<wsdl:definitions xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                  xmlns:tm="http://microsoft.com/wsdl/mime/textMatching/"
                  xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/"
                  xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                  xmlns:tns="http://developer.intuit.com/"
                  xmlns:s="http://www.w3.org/2001/XMLSchema"
                  xmlns:soap12="http://schemas.xmlsoap.org/wsdl/soap12/"
                  xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                  targetNamespace="http://developer.intuit.com/"
                  xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
    <!-- WSDL definitions would go here -->
</wsdl:definitions>`;

app.post('/qbwc', (req, res) => {
    const xml = req.body.toString();
    soap.listen(req, res, '/qbwc', service, wsdl);
});

app.listen(3000, () => {
    console.log('QBWC Web Service running on port 3000');
});
```

### Step 6: Configuration and Testing

#### Adding the Web Service to QBWC:
1. Double-click the `.qwc` file
2. QBWC will open and prompt for credentials
3. Enter username/password configured in your web service
4. Test the connection

#### Scheduling Options:
- **Manual**: User initiates sync
- **Automatic**: Run every X minutes (1-525,946 minutes max)
- **On-demand**: Via API calls from your application

### Step 7: Error Handling and Logging

#### Common Issues:
1. **Authentication failures** - Check credentials in web service
2. **XML parsing errors** - Validate QBXML format
3. **Connection timeouts** - Adjust timeout settings
4. **Certificate issues** - Ensure SSL/TLS configuration

#### QBWC Log Location:
- `C:\Program Files (x86)\Common Files\Intuit\QuickBooks\QBWebConnector\logs`

### Step 8: Security Considerations

#### Best Practices:
1. **Use HTTPS** for all web service communications
2. **Validate input** to prevent XML injection
3. **Implement rate limiting** to prevent abuse
4. **Use strong authentication** with secure credentials
5. **Keep QBWC updated** to latest version

#### Data Protection:
- QuickBooks data never leaves the local network
- Web service only receives/sends QBXML messages
- No direct database access required

## Migration from IIF to QBWC

### Step-by-Step Migration:
1. **Phase 1**: Run both systems in parallel
2. **Phase 2**: Redirect new data to QBWC
3. **Phase 3**: Migrate historical data via one-time QBWC sync
4. **Phase 4**: Decommission IIF file generation

### Data Mapping:
Ensure your HaloPSA fields map correctly to QBWC QBXML:
- Vendor Name → `VendorRef/FullName`
- PO Number → `RefNumber`
- Item SKU → `ItemRef/FullName`
- Quantity → `BillLineAdd/Quantity`
- Cost → `BillLineAdd/Cost`

## Troubleshooting

### Common Problems and Solutions:

#### QBWC Won't Start:
- Check if QuickBooks is running
- Verify administrator privileges
- Check firewall settings

#### Authentication Failures:
- Verify web service credentials
- Check network connectivity
- Validate SSL certificates

#### Data Sync Issues:
- Examine QBWC logs
- Check web service response handling
- Validate QBXML format

### Debugging Tools:
1. **QBWC Log Viewer** - Built into QBWC interface
2. **Fiddler/Wireshark** - Network traffic analysis
3. **QuickBooks SDK Validator** - QBXML validation

## Benefits Over IIF Approach

| Feature | IIF Files | QBWC |
|---------|-----------|------|
| Automation | Manual | Automated |
| Real-time | No | Yes |
| Error Handling | Limited | Comprehensive |
| Two-way Sync | No | Yes |
| Security | File-based | Web service |
| Scalability | Limited | High |

## Conclusion

QuickBooks Web Connector provides a robust, automated alternative to IIF file imports. While it requires more initial setup, the long-term benefits of real-time synchronization, better error handling, and automation make it superior for production environments.

For the HaloPSA purchase order integration, QBWC allows you to:
- Automatically sync new purchase orders as they're created
- Handle vendor and inventory item creation dynamically
- Provide immediate feedback on sync status
- Scale to handle large volumes of data

Consider implementing QBWC for production use while keeping the IIF option available for emergency backups or one-time data migrations.