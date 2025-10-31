const express = require('express');
const { asyncHandler } = require('../middleware/error-handler');

const router = express.Router();

/**
 * Initialize routes with dependencies
 */
function initHaloPSARoutes(db, haloPsaAPI) {
    const HaloPSAController = require('../controllers/halopsa.controller');
    const controller = new HaloPSAController(db, haloPsaAPI);

    // GET /api/halopsa/test - Test HaloPSA connection
    router.get('/test', asyncHandler(controller.testConnection.bind(controller)));

    // GET /api/halopsa/clients - Get clients
    router.get('/clients', asyncHandler(controller.getClients.bind(controller)));

    // POST /api/halopsa/import/clients - Import clients
    router.post('/import/clients', asyncHandler(controller.importClients.bind(controller)));

    // POST /api/halopsa/import/invoices - Import invoices
    router.post('/import/invoices', asyncHandler(controller.importInvoices.bind(controller)));

    // GET /api/halopsa/invoices - Get invoices
    router.get('/invoices', asyncHandler(controller.getInvoices.bind(controller)));

    // GET /api/halopsa/invoice/:id - Get invoice by ID
    router.get('/invoice/:id', asyncHandler(controller.getInvoiceById.bind(controller)));

    // GET /api/halopsa/clients/:client_id/transactions - Get client transactions
    router.get('/clients/:client_id/transactions', asyncHandler(controller.getClientTransactions.bind(controller)));

    // GET /api/halopsa/clients/:client_id/purchase-orders - Get client purchase orders
    router.get('/clients/:client_id/purchase-orders', asyncHandler(controller.getClientPurchaseOrders.bind(controller)));

    // POST /api/halopsa/obtain-token - Obtain OAuth token
    router.post('/obtain-token', asyncHandler(controller.obtainToken.bind(controller)));

    return router;
}

module.exports = initHaloPSARoutes;
