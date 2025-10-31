const express = require('express');
const configRoutes = require('./config.routes');
const stripeRoutes = require('./stripe.routes');
const haloPsaRoutes = require('./halopsa.routes');
const customerRoutes = require('./customer.routes');
const importRoutes = require('./import.routes');
const exportRoutes = require('./export.routes');
const dashboardRoutes = require('./dashboard.routes');
const qbwcRoutes = require('./qbwc.routes');
const qbdRoutes = require('./qbd.routes');

const router = express.Router();

// Mount route modules
router.use('/config', configRoutes);
router.use('/stripe', stripeRoutes);
router.use('/halopsa', haloPsaRoutes);
router.use('/customers', customerRoutes);
router.use('/import', importRoutes);
router.use('/export', exportRoutes);
router.use('/dashboard', dashboardRoutes);
router.use('/qbwc', qbwcRoutes);
router.use('/qbd', qbdRoutes);

// Health check endpoint
router.get('/healthz', async (req, res) => {
    res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// System status endpoint
router.get('/status', async (req, res) => {
    try {
        const { db, stripeAPI, haloPsaAPI } = req.app.locals;

        const status = {
            server: 'running',
            database: db ? 'connected' : 'disconnected',
            stripe: stripeAPI ? 'initialized' : 'not_configured',
            halopsa: haloPsaAPI ? 'initialized' : 'not_configured',
            timestamp: new Date().toISOString()
        };

        res.json(status);
    } catch (error) {
        console.error('Status check error:', error);
        res.status(500).json({ error: 'Failed to get system status' });
    }
});

module.exports = router;
