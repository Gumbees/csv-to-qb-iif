const express = require('express');
const { asyncHandler } = require('../middleware/error-handler');
const { validateRequired } = require('../middleware/validation');

const router = express.Router();

/**
 * Initialize routes with dependencies
 */
function initConfigRoutes(db, configAPI) {
    const ConfigController = require('../controllers/config.controller');
    const controller = new ConfigController(db, configAPI);

    // GET /api/config - Get all configuration
    router.get('/', asyncHandler(controller.getConfig.bind(controller)));

    // GET /api/config/:category - Get configuration by category
    router.get('/:category', asyncHandler(controller.getConfig.bind(controller)));

    // POST /api/config - Update configuration
    router.post('/', asyncHandler(controller.updateConfig.bind(controller)));

    // PUT /api/config/:category - Update configuration by category
    router.put('/:category', asyncHandler(controller.updateConfig.bind(controller)));

    // GET /api/config/export - Export configuration
    router.get('/export', asyncHandler(controller.exportConfig.bind(controller)));

    // POST /api/config/import - Import configuration
    router.post('/import', asyncHandler(controller.importConfig.bind(controller)));

    // GET /api/config/status - Get system status
    router.get('/status', asyncHandler(controller.getSystemStatus.bind(controller)));

    return router;
}

module.exports = initConfigRoutes;
