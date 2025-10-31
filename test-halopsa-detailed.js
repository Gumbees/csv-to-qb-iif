const https = require('https');

async function testHalopsaWithDetailedResponse() {
    const baseUrl = 'https://psa.dtctoday.com';
    const token = '4nlcJvuzxMu3FZJEe2dBmKMgoIyPU96Q';
    const endpoint = '/api/Client';
    
    console.log('Testing Halopsa API with detailed response analysis...\n');
    
    const options = {
        hostname: 'psa.dtctoday.com',
        port: 443,
        path: endpoint,
        method: 'GET',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'HaloPSA-API-Integration/1.0'
        }
    };
    
    return new Promise((resolve) => {
        const req = https.request(options, (res) => {
            console.log('=== RESPONSE DETAILS ===');
            console.log(`Status: ${res.statusCode} ${res.statusMessage}`);
            console.log('Headers:');
            Object.keys(res.headers).forEach(key => {
                console.log(`  ${key}: ${res.headers[key]}`);
            });
            
            let responseData = '';
            res.on('data', (chunk) => {
                responseData += chunk;
            });
            
            res.on('end', () => {
                console.log('\n=== RESPONSE BODY ANALYSIS ===');
                console.log(`Body Length: ${responseData.length} characters`);
                
                if (responseData.length === 0) {
                    console.log('❌ Response body is EMPTY');
                } else {
                    console.log('First 1000 characters:');
                    console.log(responseData.substring(0, 1000));
                    
                    // Check if it's HTML
                    if (responseData.includes('<!DOCTYPE') || responseData.includes('<html')) {
                        console.log('\n✅ Response is HTML (likely an error page)');
                        
                        // Extract error message from HTML if possible
                        const errorMatch = responseData.match(/<title[^>]*>([^<]*)<\/title>/i);
                        if (errorMatch) {
                            console.log('HTML Title:', errorMatch[1]);
                        }
                        
                        const bodyMatch = responseData.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
                        if (bodyMatch) {
                            const bodyText = bodyMatch[1].replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
                            console.log('Body Text (first 200 chars):', bodyText.substring(0, 200));
                        }
                    } else {
                        console.log('\n✅ Response is NOT HTML');
                        try {
                            JSON.parse(responseData);
                            console.log('✅ Response is valid JSON');
                        } catch (e) {
                            console.log('❌ Response is not valid JSON');
                        }
                    }
                }
                
                // Analyze the 403 Forbidden response
                if (res.statusCode === 403) {
                    console.log('\n=== 403 FORBIDDEN ANALYSIS ===');
                    console.log('Possible causes:');
                    console.log('1. Token has expired');
                    console.log('2. Token scope does not include client read permissions');
                    console.log('3. API endpoint path is incorrect');
                    console.log('4. Rate limiting or IP restriction');
                    console.log('5. API instance configuration issue');
                }
                
                resolve({ status: res.statusCode, data: responseData });
            });
        });
        
        req.on('error', (error) => {
            console.log('❌ Request Error:', error.message);
            resolve({ error: error.message });
        });
        
        req.setTimeout(10000, () => {
            console.log('❌ Request timeout');
            req.destroy();
            resolve({ error: 'Timeout' });
        });
        
        console.log('=== REQUEST DETAILS ===');
        console.log('URL:', baseUrl + endpoint);
        console.log('Method: GET');
        console.log('Headers:', options.headers);
        console.log('Sending request...\n');
        
        req.end();
    });
}

async function testAlternativeEndpoints() {
    console.log('\n\n=== TESTING ALTERNATIVE ENDPOINTS ===');
    
    const endpoints = [
        '/api/Client?count=10',
        '/api/Clients',
        '/api/customers',
        '/api/Customer',
        '/api/Organisation',
        '/api/Organisations'
    ];
    
    const token = '4nlcJvuzxMu3FZJEe2dBmKMgoIyPU96Q';
    
    for (const endpoint of endpoints) {
        console.log(`\n--- Testing: ${endpoint} ---`);
        
        const options = {
            hostname: 'psa.dtctoday.com',
            port: 443,
            path: endpoint,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        };
        
        try {
            const result = await new Promise((resolve) => {
                const req = https.request(options, (res) => {
                    let data = '';
                    res.on('data', (chunk) => data += chunk);
                    res.on('end', () => {
                        resolve({ status: res.statusCode, data: data.substring(0, 200) });
                    });
                });
                
                req.on('error', (error) => resolve({ error: error.message }));
                req.setTimeout(5000, () => resolve({ error: 'Timeout' }));
                req.end();
            });
            
            if (result.error) {
                console.log('Error:', result.error);
            } else {
                console.log(`Status: ${result.status}`);
                if (result.status === 200) {
                    console.log('✅ 200 OK - Endpoint might work!');
                    console.log('Response:', result.data);
                }
            }
        } catch (error) {
            console.log('Request failed:', error.message);
        }
        
        await new Promise(resolve => setTimeout(resolve, 500));
    }
}

testHalopsaWithDetailedResponse().then(() => {
    return testAlternativeEndpoints();
}).then(() => {
    console.log('\n=== TESTING COMPLETE ===');
    process.exit(0);
});