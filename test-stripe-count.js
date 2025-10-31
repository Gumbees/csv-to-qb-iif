const http = require('http');

const options = {
    hostname: 'localhost',
    port: 3000,
    path: '/api/stripe/transactions/imported?limit=10',
    method: 'GET'
};

const req = http.request(options, (res) => {
    let data = '';
    res.on('data', (chunk) => data += chunk);
    res.on('end', () => {
        try {
            const json = JSON.parse(data);
            console.log('Response structure:', Object.keys(json));
            console.log('Has pagination:', !!json.pagination);
            console.log('Has transactions:', !!json.transactions);
            if (json.transactions) {
                console.log('Transactions array length:', json.transactions.length);
            }
            if (json.pagination) {
                console.log('Pagination:', JSON.stringify(json.pagination, null, 2));
            }
        } catch (error) {
            console.error('Parse error:', error.message);
            console.log('Raw data:', data.substring(0, 200));
        }
    });
});

req.on('error', (error) => console.error('Request error:', error.message));
req.end();
