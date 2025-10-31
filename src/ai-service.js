const Anthropic = require('@anthropic-ai/sdk');

/**
 * AIService - Handles AI-powered customer and transaction mapping suggestions
 * Supports multiple AI providers: Anthropic Direct and OpenRouter
 */
class AIService {
  constructor(database) {
    this.db = database;
    this.anthropicClient = null;
    this.provider = 'anthropic'; // 'anthropic' or 'openrouter'
    this.anthropicModel = 'claude-3-5-sonnet-20241022';
    this.openrouterModel = 'anthropic/claude-3.5-sonnet';
    this.openrouterApiKey = null;
    this.initialized = false;
  }

  /**
   * Initialize the AI service with provider-specific configuration
   * @param {Object} config - Configuration object
   * @param {string} config.provider - AI provider ('anthropic' or 'openrouter')
   * @param {string} config.anthropic_api_key - Anthropic API key (if using Anthropic)
   * @param {string} config.openrouter_api_key - OpenRouter API key (if using OpenRouter)
   * @param {string} config.anthropic_model - Anthropic model name
   * @param {string} config.openrouter_model - OpenRouter model name
   */
  initialize(config) {
    if (!config) {
      console.warn('AIService: No configuration provided, AI features disabled');
      return;
    }

    try {
      this.provider = config.provider || 'anthropic';

      if (this.provider === 'anthropic') {
        if (!config.anthropic_api_key) {
          console.warn('AIService: No Anthropic API key provided');
          this.initialized = false;
          return;
        }
        this.anthropicClient = new Anthropic({ apiKey: config.anthropic_api_key });
        this.anthropicModel = config.anthropic_model || 'claude-sonnet-4-20250514';
        this.initialized = true;
        console.log(`AIService: Initialized with Anthropic provider (${this.anthropicModel})`);
      } else if (this.provider === 'openrouter') {
        if (!config.openrouter_api_key) {
          console.warn('AIService: No OpenRouter API key provided');
          this.initialized = false;
          return;
        }
        this.openrouterApiKey = config.openrouter_api_key;
        this.openrouterModel = config.openrouter_model || 'anthropic/claude-3.5-sonnet';
        this.initialized = true;
        console.log(`AIService: Initialized with OpenRouter provider (${this.openrouterModel})`);
      } else {
        console.error(`AIService: Unknown provider '${this.provider}'`);
        this.initialized = false;
      }
    } catch (error) {
      console.error('AIService: Failed to initialize:', error.message);
      this.initialized = false;
    }
  }

  /**
   * Check if AI service is ready to use
   * @returns {boolean}
   */
  isReady() {
    if (!this.initialized) return false;

    if (this.provider === 'anthropic') {
      return this.anthropicClient !== null;
    } else if (this.provider === 'openrouter') {
      return this.openrouterApiKey !== null;
    }

    return false;
  }

  /**
   * Call OpenRouter API
   * @param {string} prompt - User prompt
   * @param {string} systemPrompt - System prompt
   * @returns {Promise<Object>} API response
   */
  async callOpenRouter(prompt, systemPrompt) {
    if (!this.openrouterApiKey) {
      throw new Error('OpenRouter API key not configured');
    }

    try {
      const https = require('https');
      const payload = JSON.stringify({
        model: this.openrouterModel,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: prompt }
        ],
        max_tokens: 8000,
        temperature: 0.2
      });

      return new Promise((resolve, reject) => {
        const options = {
          hostname: 'openrouter.ai',
          path: '/api/v1/chat/completions',
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${this.openrouterApiKey}`,
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:3000',
            'X-Title': 'csv-to-qb-iif'
          }
        };

        const req = https.request(options, (res) => {
          let data = '';

          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            try {
              const response = JSON.parse(data);

              if (res.statusCode === 200) {
                // Convert OpenRouter format to Anthropic-compatible format
                const content = response.choices?.[0]?.message?.content || '';
                resolve({
                  content: [{ text: content }]
                });
              } else {
                reject(new Error(response.error?.message || `OpenRouter API error: ${res.statusCode}`));
              }
            } catch (parseError) {
              reject(new Error(`Failed to parse OpenRouter response: ${parseError.message}`));
            }
          });
        });

        req.on('error', (error) => {
          reject(new Error(`OpenRouter request failed: ${error.message}`));
        });

        req.write(payload);
        req.end();
      });
    } catch (error) {
      throw new Error(`OpenRouter API call failed: ${error.message}`);
    }
  }

  /**
   * Call the configured AI provider
   * @param {string} prompt - User prompt
   * @param {string} systemPrompt - System prompt
   * @returns {Promise<Object>} API response
   */
  async callAI(prompt, systemPrompt) {
    if (!this.isReady()) {
      throw new Error('AI service not initialized. Configure API key first.');
    }

    if (this.provider === 'anthropic') {
      return await this.anthropicClient.messages.create({
        model: this.anthropicModel,
        max_tokens: 8000,
        temperature: 0.2,
        system: systemPrompt,
        messages: [{ role: 'user', content: prompt }]
      });
    } else if (this.provider === 'openrouter') {
      return await this.callOpenRouter(prompt, systemPrompt);
    } else {
      throw new Error(`Unknown AI provider: ${this.provider}`);
    }
  }

  /**
   * Generate AI-powered customer mapping suggestions
   * @param {Array} unmappedStripeCustomers - Stripe customers without mappings
   * @param {Array} unmappedHaloPSAClients - HaloPSA clients without mappings
   * @param {Array} unmappedQBCustomers - QuickBooks customers without mappings
   * @returns {Promise<Array>} Array of mapping suggestions
   */
  async suggestCustomerMappings(unmappedStripeCustomers, unmappedHaloPSAClients, unmappedQBCustomers) {
    if (!this.isReady()) {
      throw new Error('AI service not initialized. Configure Anthropic API key first.');
    }

    try {
      // Prepare data for AI analysis
      const prompt = this.buildCustomerMappingPrompt(
        unmappedStripeCustomers,
        unmappedHaloPSAClients,
        unmappedQBCustomers
      );

      console.log(`AIService: Sending customer mapping request to ${this.provider}...`);
      const response = await this.callAI(prompt, this.getSystemPrompt());

      // Parse AI response
      const suggestions = this.parseCustomerMappingResponse(response);
      console.log(`AIService: Generated ${suggestions.length} mapping suggestions`);

      return suggestions;
    } catch (error) {
      console.error('AIService: Error generating suggestions:', error.message);
      throw new Error(`AI mapping failed: ${error.message}`);
    }
  }

  /**
   * Build system prompt for customer mapping
   * @returns {string}
   */
  getSystemPrompt() {
    return `You are an expert financial data analyst specializing in customer data matching across different business systems (Stripe, HaloPSA, QuickBooks).

Your task is to intelligently match customers across these three systems based on:
- Company/customer names (handling variations, abbreviations, typos)
- Email addresses (exact match is strongest signal)
- Contact information (phone numbers)
- Address data (if available)
- Business context clues

You must return ONLY valid JSON in this exact format:
{
  "suggestions": [
    {
      "source_system": "stripe|halopsa|quickbooks",
      "source_id": "id_or_stripe_id",
      "target_system": "stripe|halopsa|quickbooks",
      "target_id": "id_or_stripe_id",
      "confidence": 0.95,
      "reasoning": "Brief explanation of why this is a match"
    }
  ]
}

Confidence scoring:
- 1.0: Exact email match across systems
- 0.9-0.99: Very strong match (email domain + name similarity)
- 0.8-0.89: Strong match (name + contact info)
- 0.7-0.79: Good match (name similarity with supporting data)
- 0.6-0.69: Moderate match (name similarity only)
- Below 0.6: Do not suggest (too uncertain)

IMPORTANT: Only suggest mappings with confidence >= 0.7. Return empty suggestions array if no good matches found.`;
  }

  /**
   * Build customer mapping prompt with data
   * @param {Array} stripeCustomers
   * @param {Array} haloPSAClients
   * @param {Array} qbCustomers
   * @returns {string}
   */
  buildCustomerMappingPrompt(stripeCustomers, haloPSAClients, qbCustomers) {
    // Limit data to prevent token overflow (max 50 customers per system)
    const maxCustomers = 50;
    const limitedStripe = stripeCustomers.slice(0, maxCustomers);
    const limitedHalo = haloPSAClients.slice(0, maxCustomers);
    const limitedQB = qbCustomers.slice(0, maxCustomers);

    return `Analyze these customer records and suggest mappings between systems.

**STRIPE CUSTOMERS:**
${limitedStripe.map((c, idx) => `${idx + 1}. ID: ${c.stripe_id}
   Name: ${c.name || 'N/A'}
   Email: ${c.email || 'N/A'}
   Phone: ${c.phone || 'N/A'}
   Address: ${[c.address_line1, c.address_city, c.address_state].filter(Boolean).join(', ') || 'N/A'}
`).join('\n')}

**HALOPSA CLIENTS:**
${limitedHalo.map((c, idx) => `${idx + 1}. ID: ${c.halopsa_id}
   Name: ${c.name}
   Email: ${c.email || 'N/A'}
   Phone: ${c.phone || 'N/A'}
   Address: ${[c.address_line1, c.address_city, c.address_state].filter(Boolean).join(', ') || 'N/A'}
`).join('\n')}

**QUICKBOOKS CUSTOMERS:**
${limitedQB.map((c, idx) => `${idx + 1}. ID: ${c.qb_list_id}
   Full Name: ${c.qb_full_name}
   Company: ${c.company_name || 'N/A'}
   Email: ${c.email || 'N/A'}
   Phone: ${c.phone || 'N/A'}
   Address: ${[c.billing_address_line1, c.billing_address_city, c.billing_address_state].filter(Boolean).join(', ') || 'N/A'}
`).join('\n')}

Find all possible customer matches across these three systems. Focus on:
1. Email matches (strongest signal)
2. Name similarity with supporting contact data
3. Address or phone number matches
4. Common business name variations (Inc, LLC, Corp, etc.)

Return only confident matches (confidence >= 0.7) in the specified JSON format.`;
  }

  /**
   * Parse AI response into structured suggestions
   * @param {Object} response - Anthropic API response
   * @returns {Array}
   */
  parseCustomerMappingResponse(response) {
    try {
      const content = response.content[0].text;

      // Try to extract JSON from response (handle markdown code blocks)
      let jsonContent = content;
      const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/);
      if (jsonMatch) {
        jsonContent = jsonMatch[1];
      }

      const parsed = JSON.parse(jsonContent);

      if (!parsed.suggestions || !Array.isArray(parsed.suggestions)) {
        throw new Error('Invalid response format: missing suggestions array');
      }

      // Validate and normalize suggestions
      return parsed.suggestions
        .filter(s => s.confidence >= 0.7) // Only keep high-confidence suggestions
        .map(s => ({
          source_system: String(s.source_system).toLowerCase(),
          source_id: String(s.source_id),
          target_system: String(s.target_system).toLowerCase(),
          target_id: String(s.target_id),
          confidence: Math.min(1.0, Math.max(0.0, parseFloat(s.confidence) || 0)),
          reasoning: String(s.reasoning || 'No reasoning provided').substring(0, 500)
        }));
    } catch (error) {
      console.error('AIService: Failed to parse response:', error.message);
      console.error('Raw response:', response.content[0].text);
      throw new Error(`Failed to parse AI response: ${error.message}`);
    }
  }

  /**
   * Extract invoice number from transaction description using regex patterns
   */
  extractInvoiceNumber(description) {
    if (!description) return null;

    // Common invoice number patterns
    const patterns = [
      /invoice\s*#?\s*([A-Z0-9-]+)/i,           // "Invoice #INV-1234"
      /inv\s*#?\s*([A-Z0-9-]+)/i,               // "INV-1234"
      /#\s*([A-Z0-9-]+)/i,                       // "#1234"
      /\b([A-Z]{2,5}-\d{3,})\b/i,                // "INV-1234", "INVOICE-001"
      /invoice\s+(\d{4,})/i,                     // "Invoice 123456"
      /payment\s+for\s+([A-Z0-9-]+)/i            // "Payment for INV-1234"
    ];

    for (const pattern of patterns) {
      const match = description.match(pattern);
      if (match && match[1]) {
        return match[1].toUpperCase();
      }
    }

    return null;
  }

  /**
   * Calculate date proximity score (0-1)
   * Higher score = closer dates
   */
  calculateDateProximity(date1, date2) {
    if (!date1 || !date2) return 0;

    const d1 = new Date(date1);
    const d2 = new Date(date2);

    if (isNaN(d1.getTime()) || isNaN(d2.getTime())) return 0;

    // Calculate days difference
    const diffMs = Math.abs(d1 - d2);
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    // Score based on proximity
    if (diffDays === 0) return 1.0;
    if (diffDays <= 3) return 0.9;
    if (diffDays <= 7) return 0.7;
    if (diffDays <= 14) return 0.5;
    if (diffDays <= 30) return 0.3;
    return 0.1;
  }

  /**
   * Calculate amount match score (0-1)
   * Stripe stores in cents, invoices in dollars
   */
  calculateAmountMatch(transactionAmount, invoiceAmount) {
    if (!transactionAmount || !invoiceAmount) return 0;

    // Convert Stripe amount from cents to dollars
    const txAmount = transactionAmount / 100;
    const invAmount = parseFloat(invoiceAmount);

    if (isNaN(txAmount) || isNaN(invAmount)) return 0;

    // Exact match
    if (Math.abs(txAmount - invAmount) < 0.01) return 1.0;

    // Calculate percentage difference
    const diff = Math.abs(txAmount - invAmount);
    const avg = (txAmount + invAmount) / 2;
    const percentDiff = (diff / avg) * 100;

    // Score based on percentage difference
    if (percentDiff < 1) return 0.95;
    if (percentDiff < 5) return 0.85;
    if (percentDiff < 10) return 0.6;
    return 0.2;
  }

  /**
   * Calculate customer match score (0-1)
   */
  calculateCustomerMatch(transactionCustomerId, invoiceClientId) {
    if (!transactionCustomerId || !invoiceClientId || !this.db) return 0;

    // Check if customer mapping exists
    const mapping = this.db.get(
      `SELECT * FROM customer_mappings
       WHERE stripe_customer_id = ?
       AND halopsa_client_id = ?
       AND mapping_confirmed = 1`,
      [transactionCustomerId, invoiceClientId]
    );

    return mapping ? 1.0 : 0;
  }

  /**
   * Generate AI-powered transaction-to-invoice mapping suggestions
   * Uses pattern matching, amount comparison, and date proximity
   */
  async suggestTransactionMappings() {
    if (!this.db) {
      throw new Error('Database not provided to AI service');
    }

    try {
      // Fetch unmapped Stripe transactions
      const transactions = this.db.all(`
        SELECT st.*, sc.name as customer_name, sc.email as customer_email
        FROM stripe_transactions st
        LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
        WHERE NOT EXISTS (
          SELECT 1 FROM transaction_invoice_mappings tim
          WHERE tim.stripe_transaction_id = st.id
        )
        ORDER BY st.created DESC
        LIMIT 1000
      `);

      // Fetch all HaloPSA invoices
      const invoices = this.db.all(`
        SELECT hi.*, hc.name as client_name
        FROM halopsa_invoices hi
        LEFT JOIN halopsa_clients hc ON hi.halopsa_client_id = hc.halopsa_id
        ORDER BY hi.invoice_date DESC
        LIMIT 1000
      `);

      if (!transactions || transactions.length === 0) {
        return { success: true, count: 0, suggestions: [], message: 'No unmapped transactions found' };
      }

      if (!invoices || invoices.length === 0) {
        return { success: true, count: 0, suggestions: [], message: 'No invoices found' };
      }

      const suggestions = [];

      // For each unmapped transaction
      for (const tx of transactions) {
        let bestMatch = null;
        let bestConfidence = 0;
        let bestReasoning = '';

        // Extract invoice number from description
        const extractedInvoiceNum = this.extractInvoiceNumber(tx.description);

        // Match against all invoices
        for (const inv of invoices) {
          let confidence = 0;
          const reasons = [];

          // 1. Invoice number extraction (highest weight: 50%)
          if (extractedInvoiceNum && inv.invoice_number.toUpperCase().includes(extractedInvoiceNum)) {
            confidence += 0.5;
            reasons.push(`Invoice #${extractedInvoiceNum} found in description`);
          }

          // 2. Amount matching (weight: 30%)
          const amountScore = this.calculateAmountMatch(tx.amount, inv.total_amount);
          if (amountScore > 0.5) {
            confidence += amountScore * 0.3;
            const txAmount = (tx.amount / 100).toFixed(2);
            const invAmount = parseFloat(inv.total_amount).toFixed(2);
            if (amountScore === 1.0) {
              reasons.push(`Exact amount match: $${txAmount}`);
            } else {
              reasons.push(`Amount match: $${txAmount} ≈ $${invAmount}`);
            }
          }

          // 3. Date proximity (weight: 10%)
          const txDate = new Date(tx.created * 1000).toISOString().split('T')[0];
          const dateScore = this.calculateDateProximity(txDate, inv.invoice_date);
          if (dateScore > 0.3) {
            confidence += dateScore * 0.1;
            reasons.push(`Date proximity: ${dateScore > 0.7 ? 'very close' : 'within range'}`);
          }

          // 4. Customer mapping (weight: 10%)
          const customerScore = this.calculateCustomerMatch(tx.customer_id, inv.halopsa_client_id);
          if (customerScore > 0) {
            confidence += customerScore * 0.1;
            reasons.push(`Customer mapping confirmed`);
          }

          // Track best match for this transaction
          if (confidence > bestConfidence && confidence >= 0.6) {
            bestConfidence = confidence;
            bestMatch = inv;
            bestReasoning = reasons.join(' • ');
          }
        }

        // Store suggestion if confidence is high enough
        if (bestMatch && bestConfidence >= 0.6) {
          suggestions.push({
            stripe_transaction_id: tx.id,
            halopsa_invoice_id: bestMatch.id,
            confidence: bestConfidence,
            reasoning: bestReasoning,
            transaction_data: {
              stripe_id: tx.stripe_id,
              amount: tx.amount,
              description: tx.description,
              created: tx.created,
              customer_name: tx.customer_name || tx.customer_email
            },
            invoice_data: {
              invoice_number: bestMatch.invoice_number,
              total_amount: bestMatch.total_amount,
              invoice_date: bestMatch.invoice_date,
              client_name: bestMatch.client_name
            }
          });
        }
      }

      // Save suggestions to database
      const insertStmt = this.db.db.prepare(`
        INSERT INTO ai_mapping_suggestions (
          suggestion_type, source_type, source_id, target_type, target_id,
          confidence, reasoning, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (const suggestion of suggestions) {
        insertStmt.run([
          'transaction',
          'stripe_transaction',
          suggestion.stripe_transaction_id,
          'halopsa_invoice',
          suggestion.halopsa_invoice_id,
          suggestion.confidence,
          suggestion.reasoning,
          'pending'
        ]);
      }

      return {
        success: true,
        count: suggestions.length,
        suggestions: suggestions
      };

    } catch (error) {
      console.error('Error generating AI transaction mapping suggestions:', error);
      return {
        success: false,
        error: error.message,
        count: 0,
        suggestions: []
      };
    }
  }

  /**
   * Get pending transaction mapping suggestions
   */
  async getPendingTransactionSuggestions() {
    if (!this.db) {
      throw new Error('Database not provided to AI service');
    }

    try {
      const suggestions = this.db.all(`
        SELECT
          s.*,
          st.stripe_id, st.amount as transaction_amount, st.currency as transaction_currency,
          st.description as transaction_description, st.created as transaction_created,
          sc.name as customer_name, sc.email as customer_email,
          hi.invoice_number, hi.total_amount as invoice_amount, hi.invoice_date,
          hi.client_name
        FROM ai_mapping_suggestions s
        INNER JOIN stripe_transactions st ON s.source_id = st.id
        INNER JOIN halopsa_invoices hi ON s.target_id = hi.id
        LEFT JOIN stripe_customers sc ON st.customer_id = sc.stripe_id
        WHERE s.suggestion_type = 'transaction' AND s.status = 'pending'
        ORDER BY s.confidence DESC, s.created_at DESC
      `);

      return {
        success: true,
        suggestions: suggestions || []
      };

    } catch (error) {
      console.error('Error fetching pending transaction suggestions:', error);
      return {
        success: false,
        error: error.message,
        suggestions: []
      };
    }
  }

  /**
   * Approve a transaction mapping suggestion
   */
  async approveTransactionMapping(suggestionId) {
    if (!this.db) {
      throw new Error('Database not provided to AI service');
    }

    try {
      // Get suggestion details
      const suggestion = this.db.get(
        'SELECT * FROM ai_mapping_suggestions WHERE id = ?',
        [suggestionId]
      );

      if (!suggestion) {
        throw new Error('Suggestion not found');
      }

      // Create the actual mapping in transaction_invoice_mappings
      this.db.run(`
        INSERT INTO transaction_invoice_mappings (
          stripe_transaction_id, halopsa_invoice_id, invoice_amount,
          auto_mapped, mapping_confidence
        ) VALUES (?, ?, (
          SELECT total_amount FROM halopsa_invoices WHERE id = ?
        ), 1, ?)
      `, [
        suggestion.source_id,
        suggestion.target_id,
        suggestion.target_id,
        suggestion.confidence
      ]);

      // Update suggestion status
      this.db.run(
        'UPDATE ai_mapping_suggestions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        ['approved', suggestionId]
      );

      return {
        success: true,
        message: 'Mapping created successfully'
      };

    } catch (error) {
      console.error('Error approving transaction mapping:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Reject a transaction mapping suggestion
   */
  async rejectTransactionMapping(suggestionId) {
    if (!this.db) {
      throw new Error('Database not provided to AI service');
    }

    try {
      this.db.run(
        'UPDATE ai_mapping_suggestions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
        ['rejected', suggestionId]
      );

      return {
        success: true,
        message: 'Suggestion rejected'
      };

    } catch (error) {
      console.error('Error rejecting transaction mapping:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }
}

module.exports = AIService;
