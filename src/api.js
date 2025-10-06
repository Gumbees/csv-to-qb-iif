const express = require('express');
const multer = require('multer');
const { importFile } = require('./importer');
const { generateBillsIif, generateItemsIif } = require('./exporter');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });

function createApiRouter(db, stripe, stripeWebhookSecret) {
    const router = express.Router();

    // Stripe webhook (optional if STRIPE_SECRET_KEY provided)
    router.post('/webhooks/stripe', async (req, res) => {
        if (!stripe || !stripeWebhookSecret) return res.status(501).send('Stripe not configured');
        const sig = req.headers['stripe-signature'];
        let event;
        try {
            event = stripe.webhooks.constructEvent(req.body, sig, stripeWebhookSecret);
        } catch (err) {
            return res.status(400).send(`Webhook Error: ${err.message}`);
        }
        try {
            // Placeholder: we can expand handling later
            res.json({ received: true });
        } catch (err) {
            console.error('Stripe webhook error:', err);
            res.status(500).json({ error: err.message });
        }
    });

    // REST backfill from Stripe (simple version) — requires STRIPE_SECRET_KEY
    router.post('/import/stripe/backfill', async (req, res) => {
        try {
            if (!stripe) return res.status(501).json({ error: 'Stripe not configured' });
            // ... (implementation from server.js)
        } catch (err) {
            console.error('Stripe backfill error:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.post('/pick-file', upload.single('file'), async (req, res) => {
        try {
            if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
            const { buffer, originalname, mimetype } = req.file;
            const result = await importFile(db, buffer.toString('utf-8'), originalname, mimetype);
            res.json(result);
        } catch (err) {
            res.status(400).json({ error: err.message || String(err) });
        }
    });

    router.post('/drop-csv', async (req, res) => {
        try {
            const { content, filename } = req.body || {};
            if (!content) return res.status(400).json({ error: 'Missing CSV content' });
            const result = await importFile(db, content, filename, 'text/csv');
            res.json(result);
        } catch (err) {
            res.status(400).json({ error: err.message || String(err) });
        }
    });

    router.post('/process-import', async (req, res) => {
        const { importId, bills } = req.body;
        try {
            await db.processCsvImport(importId, bills);
            res.json({ success: true });
        } catch (err) {
            console.error('Error processing import:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.post('/export-iif', async (req, res) => {
        const { bills, suggestedName = 'bills_output.iif', transactionIds = [] } = req.body;
        const iif = await generateBillsIif(db, bills);
        try {
            const totalAmount = bills.reduce((sum, bill) => sum + (Number(bill.total_amount) || 0), 0);
            if (Array.isArray(transactionIds) && transactionIds.length) {
                await db.recordExport(suggestedName, suggestedName, transactionIds, totalAmount);
            }
        } catch (err) {
            console.error('Error recording export:', err);
        }
        res.setHeader('Content-disposition', `attachment; filename=${suggestedName}`);
        res.setHeader('Content-type', 'text/plain');
        res.send(iif);
    });
    
    router.post('/export/qbd/items-iif', async (req, res) => {
        try {
            const iifContent = await generateItemsIif(db);
            res.setHeader('Content-disposition', `attachment; filename=qbd_items.iif`);
            res.setHeader('Content-type', 'text/plain');
            res.send(iifContent);
        } catch(err) {
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/dashboard-stats', async (req, res) => {
        try {
            const stats = await db.getDashboardStats();
            res.json(stats);
        } catch (err) {
            console.error('Error getting dashboard stats:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/recent-transactions', async (req, res) => {
        const hours = req.query.hours || 24;
        try {
            const transactions = await db.getRecentTransactions(hours);
            res.json(transactions);
        } catch (err) {
            console.error('Error getting recent transactions:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/all-transactions', async (req, res) => {
        try {
            const transactions = await db.getAllTransactions();
            res.json(transactions);
        } catch (err) {
            console.error('Error getting all transactions:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/inventory-summary', async (req, res) => {
        try {
            const summary = await db.getInventorySummary();
            res.json(summary);
        } catch (err) {
            console.error('Error getting inventory summary:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/inventory-for-export', async (req, res) => {
        try {
            const inventory = await db.getInventoryForExport();
            res.json(inventory);
        } catch (err) {
            console.error('Error getting inventory for export:', err);
            res.status(500).json({ error: err.message });
        }
    });
    router.get('/import-history', async (req, res) => {
        try {
            const history = await db.getImportHistory();
            res.json(history);
        } catch (err) {
            console.error('Error getting import history:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/export-history', async (req, res) => {
        try {
            const history = await db.getExportHistory();
            res.json(history);
        } catch (err) {
            console.error('Error getting export history:', err);
            res.status(500).json({ error:err.message });
        }
    });

    router.get('/imports-metadata', async (req, res) => {
        try {
            const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
            const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
            const data = await db.getImportMetadata(limit, offset);
            res.json(data);
        } catch (err) {
            console.error('Error getting imports metadata:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/imports-metadata/:id', async (req, res) => {
        try {
            const meta = await db.getImportMetadataById(parseInt(req.params.id, 10));
            if (!meta) return res.status(404).json({ error: 'Not found' });
            res.json(meta);
        } catch (err) {
            console.error('Error getting import metadata by id:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/imports-metadata/:id/records', async (req, res) => {
        try {
            const limit = Math.min(parseInt(req.query.limit || '50', 10), 1000);
            const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
            const records = await db.getImportRecords(parseInt(req.params.id, 10), limit, offset);
            res.json(records);
        } catch (err) {
            console.error('Error getting import records:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/ledger', async (req, res) => {
        try {
            const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
            const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
            const rows = await db.getLedgerTransactions(limit, offset);
            res.json(rows);
        } catch (err) {
            console.error('Error getting ledger:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/item-transactions/:itemId', async (req, res) => {
        const { itemId } = req.params;
        try {
            const transactions = await db.getItemTransactions(itemId);
            res.json(transactions);
        } catch (err) {
            console.error('Error getting item transactions:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.get('/qbd/accounts', async (req, res) => {
        try {
            const rows = await db.getQbdAccounts();
            res.json(rows);
        } catch (err) {
            console.error('Error getting QBD accounts:', err);
            res.status(500).json({ error: err.message });
        }
    });

    router.post('/qbd/accounts', async (req, res) => {
        try {
            const id = await db.upsertQbdAccount(req.body || {});
            res.json({ id });
        } catch (err) {
            console.error('Error upserting QBD account:', err);
            res.status(400).json({ error: err.message });
        }
    });

    router.post('/qbd/accounts/default', async (req, res) => {
        try {
            const { role, accountId } = req.body || {};
            if (!role || !accountId) return res.status(400).json({ error: 'Missing role or accountId' });
            const ok = await db.setDefaultQbdAccount(role, Number(accountId));
            res.json({ success: ok });
        } catch (err) {
            console.error('Error setting default QBD account:', err);
            res.status(400).json({ error: err.message });
        }
    });

    return router;
}

module.exports = { createApiRouter };
