const HaloPSAAPI = require('./src/halopsa-api.js');
const Database = require('./src/database.js');

async function testReportAPI() {
    const db = new Database();
    await db.ready;

    const halopsaAPI = new HaloPSAAPI(db);

    console.log('🔍 Testing HaloPSA Report API...\n');

    try {
        // First, let's check what report 350 contains
        console.log('📋 Fetching Report 350 definition...');
        const reportDef = await halopsaAPI.makeRequest('/api/Report/350?includedetails=true&loadreport=false');

        console.log(`\n✅ Report Name: ${reportDef.name || 'Unknown'}`);
        console.log(`   Description: ${reportDef.description || 'N/A'}`);

        if (reportDef.columns && Array.isArray(reportDef.columns)) {
            console.log(`\n📊 Available Columns (${reportDef.columns.length}):`);
            reportDef.columns.slice(0, 20).forEach(col => {
                console.log(`   - ${col.name} (${col.data_type || 'unknown type'})`);
            });
            if (reportDef.columns.length > 20) {
                console.log(`   ... and ${reportDef.columns.length - 20} more columns`);
            }
        }

        // Now fetch the actual data
        console.log('\n\n📥 Fetching Report 350 data (first page)...');
        const reportData = await halopsaAPI.makeRequest('/api/Report/350?loadreport=true&page_no=1&page_size=5');

        console.log('\n🔍 Report Data Structure:');
        console.log(`   Keys: ${Object.keys(reportData).join(', ')}`);

        if (reportData.report) {
            console.log(`   report keys: ${Object.keys(reportData.report).join(', ')}`);

            if (reportData.report.rows && Array.isArray(reportData.report.rows)) {
                console.log(`   \n✅ Found ${reportData.report.rows.length} rows`);

                if (reportData.report.rows.length > 0) {
                    const firstRow = reportData.report.rows[0];
                    console.log(`\n📝 First Purchase Order Structure:`);
                    console.log(`   Field count: ${Object.keys(firstRow).length}`);
                    console.log(`   Fields: ${Object.keys(firstRow).join(', ')}`);

                    // Look for line items specifically
                    console.log(`\n🔍 Checking for line items...`);
                    ['lines', 'lineitems', 'line_items', 'items', 'orderlines', 'po_lines'].forEach(field => {
                        if (firstRow[field]) {
                            console.log(`   ✅ Found '${field}':`,
                                Array.isArray(firstRow[field]) ?
                                    `${firstRow[field].length} items` :
                                    typeof firstRow[field]
                            );

                            if (Array.isArray(firstRow[field]) && firstRow[field].length > 0) {
                                console.log(`      First item keys:`, Object.keys(firstRow[field][0]).join(', '));
                            }
                        }
                    });

                    // Show sample data
                    console.log(`\n📄 Sample Data (first row):`);
                    console.log(JSON.stringify(firstRow, null, 2).substring(0, 1000) + '...');
                }
            }
        }

        console.log('\n\n✅ Report API test complete!');

    } catch (error) {
        console.error('\n❌ Error testing Report API:', error.message);
        if (error.response) {
            console.error('   Response status:', error.response.status);
            console.error('   Response data:', error.response.data);
        }
    }

    await db.close();
}

testReportAPI().catch(console.error);
