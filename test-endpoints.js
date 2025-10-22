const http = require('http');
const fs = require('fs');

const BASE_URL = 'http://localhost:3000';

// Test endpoints that actually exist
const endpoints = [
    { path: '/healthz', method: 'GET' },
    { path: '/', method: 'GET' },
    { path: '/api/pick-file', method: 'POST', requiresFile: true },
    { path: '/api/process-import', method: 'POST' },
    { path: '/api/qbd/accounts', method: 'GET' },
    { path: '/api/qbd/accounts', method: 'POST' }
];

async function testEndpoint(endpoint) {
    return new Promise((resolve) => {
        const options = {
            hostname: 'localhost',
            port: 3000,
            path: endpoint.path,
            method: endpoint.method,
            headers: {}
        };

        // Add multipart headers for file uploads
        if (endpoint.requiresFile) {
            const boundary = '----TestBoundary12345';
            options.headers['Content-Type'] = `multipart/form-data; boundary=${boundary}`;
            
            const testContent = 'Vendor,Date,Amount\nTest,2025-01-15,100.00';
            const body = `--${boundary}\r\n` +
                        'Content-Disposition: form-data; name="file"; filename="test.csv"\r\n' +
                        'Content-Type: text/csv\r\n\r\n' +
                        testContent + '\r\n' +
                        `--${boundary}--\r\n`;
            
            options.headers['Content-Length'] = Buffer.byteLength(body);
            
            const req = http.request(options, (res) => {
                let data = '';
                res.on('data', (chunk) => {
                    data += chunk;
                });
                res.on('end', () => {
                    let parsedData = null;
                    try {
                        parsedData = data ? JSON.parse(data) : null;
                    } catch (error) {
                        parsedData = { content: 'Non-JSON response' };
                    }
                    
                    resolve({
                        endpoint: endpoint.path,
                        method: endpoint.method,
                        statusCode: res.statusCode,
                        statusMessage: res.statusMessage,
                        data: parsedData
                    });
                });
            });

            req.on('error', (error) => {
                resolve({
                    endpoint: endpoint.path,
                    method: endpoint.method,
                    error: error.message
                });
            });

            req.write(body);
            req.end();
        } else {
            const req = http.request(options, (res) => {
                let data = '';
                res.on('data', (chunk) => {
                    data += chunk;
                });
                res.on('end', () => {
                    let parsedData = null;
                    try {
                        parsedData = data ? JSON.parse(data) : null;
                    } catch (error) {
                        parsedData = { content: 'Non-JSON response' };
                    }
                    
                    resolve({
                        endpoint: endpoint.path,
                        method: endpoint.method,
                        statusCode: res.statusCode,
                        statusMessage: res.statusMessage,
                        data: parsedData
                    });
                });
            });

            req.on('error', (error) => {
                resolve({
                    endpoint: endpoint.path,
                    method: endpoint.method,
                    error: error.message
                });
            });

            req.end();
        }
    });
}

async function testAllEndpoints() {
    console.log('=== Testing Actual API Endpoints ===\n');
    
    for (const endpoint of endpoints) {
        console.log(`Testing: ${endpoint.method} ${endpoint.path}`);
        
        try {
            const result = await testEndpoint(endpoint);
            
            if (result.error) {
                console.log(`❌ ${endpoint.method} ${endpoint.path}: ${result.error}`);
            } else if (result.statusCode === 200 || result.statusCode === 201) {
                console.log(`✅ ${endpoint.method} ${endpoint.path}: ${result.statusCode} ${result.statusMessage}`);
                if (result.data) {
                    console.log(`   Response: ${JSON.stringify(result.data).substring(0, 100)}...`);
                }
            } else if (result.statusCode === 404) {
                console.log(`❌ ${endpoint.method} ${endpoint.path}: Not found (404)`);
            } else if (result.statusCode === 405) {
                console.log(`⚠️ ${endpoint.method} ${endpoint.path}: Method not allowed (405)`);
            } else if (result.statusCode === 400) {
                console.log(`⚠️ ${endpoint.method} ${endpoint.path}: Bad request (400)`);
                if (result.data && result.data.error) {
                    console.log(`   Error: ${result.data.error}`);
                }
            } else if (result.statusCode === 500) {
                console.log(`❌ ${endpoint.method} ${endpoint.path}: Internal server error (500)`);
                if (result.data && result.data.error) {
                    console.log(`   Error: ${result.data.error}`);
                }
            } else {
                console.log(`⚠️ ${endpoint.method} ${endpoint.path}: ${result.statusCode} ${result.statusMessage}`);
            }
        } catch (error) {
            console.log(`❌ ${endpoint.method} ${endpoint.path}: ${error.message}`);
        }
        
        console.log('');
    }
}

testAllEndpoints();