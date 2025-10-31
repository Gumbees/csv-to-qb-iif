const express = require('express');
const { asyncHandler } = require('../middleware/error-handler');
const { validateRequired } = require('../middleware/validation');

const router = express.Router();

/**
 * Initialize routes with dependencies
 */
function initCustomerRoutes(db) {
    const CustomerController = require('../controllers/customer.controller');
    const controller = new CustomerController(db);

    // GET /api/customers/mappings - Get all customer mappings
    router.get('/mappings', asyncHandler(controller.getMappings.bind(controller)));

    // POST /api/customers/map - Create manual mapping
    router.post('/map',
        validateRequired(['stripe_customer_id', 'halopsa_client_id']),
        asyncHandler(controller.createMapping.bind(controller))
    );

    // POST /api/customers/mappings/save - Save mappings (alias for map)
    router.post('/mappings/save', asyncHandler(controller.createMapping.bind(controller)));

    // DELETE /api/customers/mappings/:stripe_customer_id - Delete mapping
    router.delete('/mappings/:stripe_customer_id', asyncHandler(controller.deleteMapping.bind(controller)));

    // POST /api/customers/automatch - Auto-match customers
    router.post('/automatch', asyncHandler(controller.autoMatch.bind(controller)));

    // GET /api/customers/view/:halopsa_client_id - Get customer view
    router.get('/view/:halopsa_client_id', asyncHandler(controller.getCustomerView.bind(controller)));

    // GET /api/customers/overview - Get customer overview
    router.get('/overview', asyncHandler(controller.getCustomerOverview.bind(controller)));

    // GET /api/customers/overview/:client_id - Get specific customer overview
    router.get('/overview/:client_id', asyncHandler(controller.getCustomerView.bind(controller)));

    return router;
}

module.exports = initCustomerRoutes;
