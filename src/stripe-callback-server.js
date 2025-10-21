const http = require('http');
const url = require('url');

class StripeCallbackServer {
  constructor() {
    this.server = null;
    this.port = 3000;
  }

  /**
   * Start temporary HTTP server to handle OAuth callback
   * @returns {Promise<Object>} Promise that resolves with auth code and state
   */
  async startCallbackServer() {
    return new Promise((resolve, reject) => {
      // Close any existing server
      if (this.server) {
        this.server.close();
      }

      this.server = http.createServer((req, res) => {
        const parsedUrl = url.parse(req.url, true);

        if (parsedUrl.pathname === '/stripe/callback') {
          const { code, state, error, error_description } = parsedUrl.query;

          // Send response to browser
          res.writeHead(200, { 'Content-Type': 'text/html' });

          if (error) {
            res.end(`
              <html>
                <body style="font-family: system-ui; text-align: center; padding: 50px;">
                  <h2>❌ Stripe Connection Failed</h2>
                  <p>Error: ${error}</p>
                  <p>${error_description || ''}</p>
                  <p>You can close this window.</p>
                  <script>setTimeout(() => window.close(), 3000);</script>
                </body>
              </html>
            `);

            this.server.close();
            reject(new Error(`OAuth error: ${error} - ${error_description}`));
            return;
          }

          if (code && state) {
            res.end(`
              <html>
                <body style="font-family: system-ui; text-align: center; padding: 50px;">
                  <h2>✅ Stripe Connected Successfully!</h2>
                  <p>Your Stripe account has been connected.</p>
                  <p>You can close this window.</p>
                  <script>setTimeout(() => window.close(), 3000);</script>
                </body>
              </html>
            `);

            this.server.close();
            resolve({ code, state });
            return;
          }

          // Invalid callback
          res.end(`
            <html>
              <body style="font-family: system-ui; text-align: center; padding: 50px;">
                <h2>⚠️ Invalid Callback</h2>
                <p>Missing required parameters.</p>
                <p>You can close this window.</p>
                <script>setTimeout(() => window.close(), 3000);</script>
              </body>
            </html>
          `);

          this.server.close();
          reject(new Error('Invalid callback: missing code or state'));
        } else {
          // 404 for other paths
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          res.end('Not Found');
        }
      });

      // Start server
      this.server.listen(this.port, 'localhost', () => {
        console.log(`OAuth callback server started on http://localhost:${this.port}`);
      });

      // Handle server errors
      this.server.on('error', (err) => {
        if (err.code === 'EADDRINUSE') {
          // Try next port
          this.port++;
          if (this.port > 3010) {
            reject(new Error('Could not find available port for OAuth callback'));
            return;
          }

          this.server.listen(this.port, 'localhost');
        } else {
          reject(err);
        }
      });

      // Timeout after 5 minutes
      setTimeout(() => {
        if (this.server) {
          this.server.close();
          reject(new Error('OAuth callback timeout'));
        }
      }, 5 * 60 * 1000);
    });
  }

  /**
   * Stop the callback server
   */
  stop() {
    if (this.server) {
      this.server.close();
      this.server = null;
    }
  }

  /**
   * Get the current callback URL
   */
  getCallbackUrl() {
    return `http://localhost:${this.port}/stripe/callback`;
  }
}

module.exports = StripeCallbackServer;