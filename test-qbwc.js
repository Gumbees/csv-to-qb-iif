const QBWCService = require('./src/qbwc-service');

// Create a test QBWC service instance
const qbwc = new QBWCService();

// Test 1: Generate a QWC configuration file
console.log('=== Testing QWC File Generation ===');
const qwcConfig = {
    appName: 'Test QBWC App',
    appUrl: 'http://localhost:3000/qbwc',
    description: 'Test QuickBooks Web Connector',
    supportUrl: 'http://localhost:3000/support',
    username: 'test_user',
    interval: 30
};

const qwcFile = qbwc.generateQWCFile(qwcConfig);
console.log('QWC File generated successfully (sample):');
console.log(qwcFile.substring(0, 200) + '...');
console.log('');

// Test 2: Generate sample purchase order QBXML
console.log('=== Testing Purchase Order QBXML Generation ===');
const samplePOs = [
    {
        vendor: 'Test Vendor',
        date: '2025-01-15',
        ref_num: 'PO-001',
        due_date: '2025-02-15',
        total_amount: 382.50,
        lines: [
            {
                item: 'Test Item 1',
                description: 'Test Purchase Item',
                quantity: 10,
                unit_cost: 25.50,
                line_amount: 255.00
            },
            {
                item: 'Test Item 2',
                description: 'Another Test Item',
                quantity: 5,
                unit_cost: 15.75,
                line_amount: 78.75
            }
        ]
    }
];

const poQbxml = qbwc.generatePurchaseOrderQBXML(samplePOs);
console.log('Purchase Order QBXML generated successfully (sample):');
console.log(poQbxml.substring(0, 300) + '...');
console.log('');

// Test 3: Test authentication
console.log('=== Testing Authentication ===');
const authResult = qbwc.authenticate('halopsa_user', 'password123');
console.log('Authentication result:', authResult);
console.log('');

// Test 4: Test GUID generation
console.log('=== Testing GUID Generation ===');
const guid = qbwc.generateGUID();
console.log('Generated GUID:', guid);
console.log('');

console.log('✅ All QBWC service tests completed successfully!');