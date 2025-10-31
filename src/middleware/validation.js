/**
 * Request validation middleware
 */

/**
 * Validate required fields in request body
 */
const validateRequired = (fields) => {
    return (req, res, next) => {
        const missing = [];

        for (const field of fields) {
            if (req.body[field] === undefined || req.body[field] === null || req.body[field] === '') {
                missing.push(field);
            }
        }

        if (missing.length > 0) {
            return res.status(400).json({
                error: `Missing required fields: ${missing.join(', ')}`
            });
        }

        next();
    };
};

/**
 * Validate file upload
 */
const validateFileUpload = (req, res, next) => {
    if (!req.file) {
        return res.status(400).json({
            error: 'No file uploaded'
        });
    }
    next();
};

/**
 * Validate CSV file upload
 */
const validateCSVUpload = (req, res, next) => {
    if (!req.file) {
        return res.status(400).json({
            error: 'No file uploaded'
        });
    }

    const filename = req.file.originalname.toLowerCase();
    if (!filename.endsWith('.csv')) {
        return res.status(400).json({
            error: 'File must be a CSV'
        });
    }

    next();
};

/**
 * Validate ID parameter
 */
const validateIdParam = (paramName = 'id') => {
    return (req, res, next) => {
        const id = req.params[paramName];

        if (!id) {
            return res.status(400).json({
                error: `Missing ${paramName} parameter`
            });
        }

        next();
    };
};

module.exports = {
    validateRequired,
    validateFileUpload,
    validateCSVUpload,
    validateIdParam
};
