const express = require('express');
const { asyncHandler } = require('../middleware/error-handler');

const router = express.Router();

/**
 * Initialize routes with dependencies
 */
function initStripeRoutes(db, stripeAPI) {
    const StripeController = require('../controllers/stripe.controller');
    const controller = new StripeController(db, stripeAPI);

    // GET /api/stripe/test - Test Stripe connection
    router.get('/test', asyncHandler(controller.testConnection.bind(controller)));

    // POST /api/stripe/import/customers - Import Stripe customers
    router.post('/import/customers', asyncHandler(controller.importCustomers.bind(controller)));

    // POST /api/stripe/import/transactions - Import Stripe transactions
    router.post('/import/transactions', asyncHandler(controller.importTransactions.bind(controller)));

    // GET /api/stripe/customers/imported - Get imported customers
    router.get('/customers/imported', asyncHandler(controller.getImportedCustomers.bind(controller)));

    // GET /api/stripe/transactions/imported - Get imported transactions
    router.get('/transactions/imported', asyncHandler(controller.getImportedTransactions.bind(controller)));

    // GET /api/stripe/customers/:id - Get customer by ID
    router.get('/customers/:id', asyncHandler(controller.getCustomerById.bind(controller)));

    // GET /api/stripe/customers/:id/transactions - Get customer transactions
    router.get('/customers/:id/transactions', asyncHandler(controller.getCustomerTransactions.bind(controller)));

    // POST /api/stripe/webhook - Stripe webhook handler
    router.post('/webhook', express.json({ type: 'application/json' }), asyncHandler(controller.handleWebhook.bind(controller)));

    return router;
}

module.exports = initStripeRoutes;
