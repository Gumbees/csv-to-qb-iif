const { shell } = require('electron');
const crypto = require('crypto');
const Store = require('electron-store');
const StripeCallbackServer = require('./stripe-callback-server');

class StripeOAuth {
  constructor(database) {
    this.db = database;
    this.store = new Store({ name: 'stripe-oauth' });

    // Stripe OAuth configuration
    this.clientId = process.env.STRIPE_CLIENT_ID || 'ca_your_stripe_client_id';
    this.baseUrl = 'https://connect.stripe.com';

    // OAuth state management
    this.pendingStates = new Map();
    this.callbackServer = new StripeCallbackServer();
  }

  /**
   * Generate OAuth authorization URL
   * @param {string[]} scopes - Requested scopes (e.g., ['read_only'])
   * @returns {string} Authorization URL
   */
  generateAuthUrl(scopes = ['read_only']) {
    const state = this.generateState();
    const redirectUri = this.callbackServer.getCallbackUrl();

    const params = new URLSearchParams({
      response_type: 'code',
      client_id: this.clientId,
      scope: scopes.join(' '),
      redirect_uri: redirectUri,
      state: state
    });

    // Store state for validation
    this.pendingStates.set(state, {
      timestamp: Date.now(),
      scopes: scopes
    });

    return `${this.baseUrl}/oauth/authorize?${params.toString()}`;
  }

  /**
   * Generate secure random state parameter
   * @returns {string} Random state string
   */
  generateState() {
    return crypto.randomBytes(32).toString('hex');
  }

  /**
   * Validate OAuth state parameter
   * @param {string} state - State parameter to validate
   * @returns {boolean} True if valid
   */
  validateState(state) {
    const stateData = this.pendingStates.get(state);
    if (!stateData) return false;

    // Check if state is not older than 10 minutes
    const maxAge = 10 * 60 * 1000; // 10 minutes
    if (Date.now() - stateData.timestamp > maxAge) {
      this.pendingStates.delete(state);
      return false;
    }

    return true;
  }

  /**
   * Exchange authorization code for access token
   * @param {string} code - Authorization code from Stripe
   * @param {string} state - State parameter for validation
   * @returns {Promise<Object>} Token response
   */
  async exchangeCodeForToken(code, state) {
    if (!this.validateState(state)) {
      throw new Error('Invalid or expired state parameter');
    }

    const stateData = this.pendingStates.get(state);
    this.pendingStates.delete(state);

    try {
      const response = await fetch('https://connect.stripe.com/oauth/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': `Bearer ${process.env.STRIPE_SECRET_KEY}`
        },
        body: new URLSearchParams({
          client_secret: process.env.STRIPE_SECRET_KEY,
          code: code,
          grant_type: 'authorization_code'
        })
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`OAuth token exchange failed: ${errorData.error_description || errorData.error}`);
      }

      const tokenData = await response.json();

      // Store tokens in database
      await this.storeTokens(tokenData);

      return tokenData;
    } catch (error) {
      console.error('Error exchanging code for token:', error);
      throw error;
    }
  }

  /**
   * Store OAuth tokens in database
   * @param {Object} tokenData - Token response from Stripe
   */
  async storeTokens(tokenData) {
    return new Promise((resolve, reject) => {
      const {
        access_token,
        refresh_token,
        stripe_user_id,
        stripe_publishable_key,
        scope,
        livemode
      } = tokenData;

      // Calculate expiration (Stripe tokens typically don't expire, but we'll set a far future date)
      const expiresAt = new Date();
      expiresAt.setFullYear(expiresAt.getFullYear() + 10); // 10 years from now

      this.db.db.run(`
        INSERT OR REPLACE INTO stripe_oauth_tokens
        (stripe_user_id, access_token, refresh_token, stripe_publishable_key, scope, livemode, expires_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      `, [
        stripe_user_id,
        access_token,
        refresh_token || null,
        stripe_publishable_key,
        scope,
        livemode ? 1 : 0,
        expiresAt.toISOString()
      ], function(err) {
        if (err) {
          console.error('Error storing Stripe tokens:', err);
          reject(err);
        } else {
          console.log('Stripe tokens stored successfully');
          resolve({ id: this.lastID });
        }
      });
    });
  }

  /**
   * Get stored tokens for a Stripe user
   * @param {string} stripeUserId - Stripe user ID
   * @returns {Promise<Object|null>} Token data or null
   */
  async getTokens(stripeUserId = null) {
    return new Promise((resolve, reject) => {
      let query, params;

      if (stripeUserId) {
        query = 'SELECT * FROM stripe_oauth_tokens WHERE stripe_user_id = ? ORDER BY updated_at DESC LIMIT 1';
        params = [stripeUserId];
      } else {
        // Get most recent token if no specific user ID
        query = 'SELECT * FROM stripe_oauth_tokens ORDER BY updated_at DESC LIMIT 1';
        params = [];
      }

      this.db.db.get(query, params, (err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve(row || null);
        }
      });
    });
  }

  /**
   * Check if we have valid tokens
   * @param {string} stripeUserId - Optional Stripe user ID
   * @returns {Promise<boolean>} True if authenticated
   */
  async isAuthenticated(stripeUserId = null) {
    try {
      const tokens = await this.getTokens(stripeUserId);
      if (!tokens) return false;

      // Check if token is expired
      const expiresAt = new Date(tokens.expires_at);
      if (expiresAt <= new Date()) {
        return false;
      }

      return true;
    } catch (error) {
      console.error('Error checking authentication status:', error);
      return false;
    }
  }

  /**
   * Revoke stored tokens
   * @param {string} stripeUserId - Stripe user ID
   */
  async revokeTokens(stripeUserId) {
    try {
      const tokens = await this.getTokens(stripeUserId);
      if (!tokens) {
        throw new Error('No tokens found for user');
      }

      // Revoke token with Stripe
      const response = await fetch('https://connect.stripe.com/oauth/deauthorize', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': `Bearer ${process.env.STRIPE_SECRET_KEY}`
        },
        body: new URLSearchParams({
          client_secret: process.env.STRIPE_SECRET_KEY,
          stripe_user_id: stripeUserId
        })
      });

      if (!response.ok) {
        console.warn('Failed to revoke token with Stripe, removing locally anyway');
      }

      // Remove from database
      return new Promise((resolve, reject) => {
        this.db.db.run(
          'DELETE FROM stripe_oauth_tokens WHERE stripe_user_id = ?',
          [stripeUserId],
          function(err) {
            if (err) {
              reject(err);
            } else {
              resolve({ deleted: this.changes });
            }
          }
        );
      });
    } catch (error) {
      console.error('Error revoking tokens:', error);
      throw error;
    }
  }

  /**
   * Start OAuth flow by opening browser
   * @param {string[]} scopes - Requested scopes
   * @returns {Promise<Object>} Promise that resolves when OAuth completes
   */
  async startAuthFlow(scopes = ['read_only']) {
    try {
      // Start the callback server
      const callbackPromise = this.callbackServer.startCallbackServer();

      // Generate auth URL and open browser
      const authUrl = this.generateAuthUrl(scopes);
      shell.openExternal(authUrl);

      // Wait for callback
      const { code, state } = await callbackPromise;

      // Exchange code for token
      const tokenData = await this.exchangeCodeForToken(code, state);

      return { success: true, tokenData, authUrl };
    } catch (error) {
      // Stop callback server on error
      this.callbackServer.stop();
      throw error;
    }
  }

  /**
   * Get all stored Stripe accounts
   * @returns {Promise<Array>} Array of stored accounts
   */
  async getAllAccounts() {
    return new Promise((resolve, reject) => {
      this.db.db.all(
        'SELECT stripe_user_id, stripe_publishable_key, scope, livemode, created_at, updated_at FROM stripe_oauth_tokens ORDER BY updated_at DESC',
        [],
        (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows || []);
          }
        }
      );
    });
  }

  /**
   * Clean up expired states
   */
  cleanupExpiredStates() {
    const maxAge = 10 * 60 * 1000; // 10 minutes
    const now = Date.now();

    for (const [state, data] of this.pendingStates.entries()) {
      if (now - data.timestamp > maxAge) {
        this.pendingStates.delete(state);
      }
    }
  }
}

module.exports = StripeOAuth;