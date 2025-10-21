const Stripe = require('stripe');

class StripeAPI {
    constructor(apiKey) {
        this.stripe = new Stripe(apiKey || process.env.STRIPE_SECRET_KEY);
        this.webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
    }

    // Test connection to Stripe
    async testConnection() {
        try {
            const balance = await this.stripe.balance.retrieve();
            return {
                success: true,
                balance: balance,
                message: 'Stripe connection successful'
            };
        } catch (error) {
            return {
                success: false,
                error: error.message,
                message: 'Stripe connection failed'
            };
        }
    }

    // Get all customers with pagination
    async getAllCustomers(limit = 100) {
        try {
            const customers = await this.stripe.customers.list({
                limit: limit,
                expand: ['data.subscriptions']
            });
            
            return customers.data.map(customer => ({
                id: customer.id,
                name: customer.name,
                email: customer.email,
                phone: customer.phone,
                description: customer.description,
                created: new Date(customer.created * 1000).toISOString(),
                metadata: customer.metadata,
                balance: customer.balance,
                currency: customer.currency,
                subscriptions: customer.subscriptions?.data.map(sub => ({
                    id: sub.id,
                    status: sub.status,
                    current_period_start: new Date(sub.current_period_start * 1000).toISOString(),
                    current_period_end: new Date(sub.current_period_end * 1000).toISOString(),
                    items: sub.items.data.map(item => ({
                        price: item.price.id,
                        product: item.price.product,
                        quantity: item.quantity
                    }))
                })) || []
            }));
        } catch (error) {
            throw new Error(`Failed to fetch customers: ${error.message}`);
        }
    }

    // Get all charges/payments with pagination (for cash-based accounting)
    async getAllTransactions(limit = 100, startingAfter = null) {
        try {
            const params = {
                limit: limit,
                expand: ['data.customer', 'data.invoice']
            };
            
            if (startingAfter) {
                params.starting_after = startingAfter;
            }

            const charges = await this.stripe.charges.list(params);
            
            return charges.data.map(charge => ({
                charge_id: charge.id,
                payment_intent_id: charge.payment_intent,
                customer_id: charge.customer?.id,
                customer_email: charge.customer?.email,
                customer_name: charge.customer?.name,
                amount: charge.amount / 100, // Convert from cents to dollars
                currency: charge.currency,
                fee_amount: charge.application_fee_amount ? charge.application_fee_amount / 100 : 0,
                net_amount: (charge.amount - (charge.application_fee_amount || 0)) / 100,
                description: charge.description,
                invoice_id: charge.invoice?.id,
                created: new Date(charge.created * 1000).toISOString(),
                paid_date: charge.paid ? new Date(charge.created * 1000).toISOString() : null,
                status: charge.status,
                metadata: charge.metadata,
                receipt_url: charge.receipt_url,
                billing_details: charge.billing_details
            }));
        } catch (error) {
            throw new Error(`Failed to fetch transactions: ${error.message}`);
        }
    }

    // Get all transactions for a specific time period (cash accounting)
    async getTransactionsByDateRange(startDate, endDate, limit = 100) {
        try {
            const startTimestamp = Math.floor(new Date(startDate).getTime() / 1000);
            const endTimestamp = Math.floor(new Date(endDate).getTime() / 1000);

            const charges = await this.stripe.charges.list({
                limit: limit,
                created: {
                    gte: startTimestamp,
                    lte: endTimestamp
                },
                expand: ['data.customer', 'data.invoice']
            });

            return charges.data.map(charge => this.formatChargeForAccounting(charge));
        } catch (error) {
            throw new Error(`Failed to fetch transactions by date range: ${error.message}`);
        }
    }

    // Format charge for cash-based accounting
    formatChargeForAccounting(charge) {
        return {
            charge_id: charge.id,
            payment_intent_id: charge.payment_intent,
            customer_id: charge.customer?.id,
            customer_email: charge.customer?.email,
            customer_name: charge.customer?.name || charge.billing_details?.name,
            amount: charge.amount / 100,
            currency: charge.currency,
            fee_amount: this.calculateStripeFees(charge),
            net_amount: (charge.amount - this.calculateStripeFees(charge) * 100) / 100,
            description: charge.description || `Payment from ${charge.billing_details?.name || 'Customer'}`,
            invoice_id: charge.invoice?.id,
            created: new Date(charge.created * 1000).toISOString(),
            paid_date: charge.paid ? new Date(charge.created * 1000).toISOString() : null,
            status: charge.status,
            metadata: charge.metadata,
            receipt_url: charge.receipt_url,
            // Additional fields for MSP accounting
            accounting_date: new Date(charge.created * 1000).toISOString().split('T')[0], // Date only for cash accounting
            payment_method: charge.payment_method_details?.type || 'card'
        };
    }

    // Calculate Stripe fees (simplified - in practice, use actual fee breakdown)
    calculateStripeFees(charge) {
        // Standard Stripe fee: 2.9% + $0.30
        const percentageFee = charge.amount * 0.029;
        const fixedFee = 30; // $0.30 in cents
        return (percentageFee + fixedFee) / 100; // Convert to dollars
    }

    // Get subscription information for recurring payments
    async getSubscriptions(limit = 100) {
        try {
            const subscriptions = await this.stripe.subscriptions.list({
                limit: limit,
                status: 'all',
                expand: ['data.customer', 'data.items.data.price.product']
            });

            return subscriptions.data.map(sub => ({
                id: sub.id,
                customer_id: sub.customer.id,
                status: sub.status,
                current_period_start: new Date(sub.current_period_start * 1000).toISOString(),
                current_period_end: new Date(sub.current_period_end * 1000).toISOString(),
                cancel_at_period_end: sub.cancel_at_period_end,
                items: sub.items.data.map(item => ({
                    id: item.id,
                    price: item.price.id,
                    product: item.price.product,
                    product_name: item.price.product?.name,
                    quantity: item.quantity,
                    amount: item.price.unit_amount / 100
                })),
                total_amount: sub.items.data.reduce((sum, item) => 
                    sum + (item.price.unit_amount * item.quantity / 100), 0)
            }));
        } catch (error) {
            throw new Error(`Failed to fetch subscriptions: ${error.message}`);
        }
    }

    // Sync all data from Stripe for cash-based accounting
    async fullSync(options = {}) {
        const {
            syncCustomers = true,
            syncTransactions = true,
            syncSubscriptions = true,
            dateRange = null,
            limit = 100
        } = options;

        const results = {
            customers: [],
            transactions: [],
            subscriptions: [],
            summary: {}
        };

        try {
            // Sync customers
            if (syncCustomers) {
                results.customers = await this.getAllCustomers(limit);
                results.summary.totalCustomers = results.customers.length;
            }

            // Sync transactions
            if (syncTransactions) {
                if (dateRange) {
                    results.transactions = await this.getTransactionsByDateRange(
                        dateRange.start, 
                        dateRange.end, 
                        limit
                    );
                } else {
                    results.transactions = await this.getAllTransactions(limit);
                }
                results.summary.totalTransactions = results.transactions.length;
                results.summary.totalAmount = results.transactions.reduce((sum, t) => sum + t.net_amount, 0);
            }

            // Sync subscriptions
            if (syncSubscriptions) {
                results.subscriptions = await this.getSubscriptions(limit);
                results.summary.totalSubscriptions = results.subscriptions.length;
            }

            results.summary.success = true;
            return results;

        } catch (error) {
            results.summary.success = false;
            results.summary.error = error.message;
            return results;
        }
    }

    // Webhook handling for real-time updates
    async handleWebhook(event) {
        try {
            switch (event.type) {
                case 'charge.succeeded':
                    return await this.handleChargeSucceeded(event.data.object);
                
                case 'customer.subscription.created':
                case 'customer.subscription.updated':
                case 'customer.subscription.deleted':
                    return await this.handleSubscriptionEvent(event);
                
                case 'invoice.payment_succeeded':
                    return await this.handleInvoicePayment(event.data.object);
                
                default:
                    return { handled: false, type: event.type };
            }
        } catch (error) {
            throw new Error(`Webhook handling failed: ${error.message}`);
        }
    }

    // Handle successful charge (cash accounting: record when payment is received)
    async handleChargeSucceeded(charge) {
        const transaction = this.formatChargeForAccounting(charge);
        
        // For MSP cash accounting, this is when we recognize revenue
        return {
            type: 'charge.succeeded',
            transaction: transaction,
            accounting_date: transaction.accounting_date,
            message: 'Payment received and ready for cash accounting'
        };
    }

    // Handle subscription events for recurring revenue
    async handleSubscriptionEvent(event) {
        const subscription = event.data.object;
        
        return {
            type: event.type,
            subscription_id: subscription.id,
            customer_id: subscription.customer,
            status: subscription.status,
            message: `Subscription ${event.type.split('.')[2]}`
        };
    }

    // Handle invoice payments (links Stripe invoices to HaloPSA invoices)
    async handleInvoicePayment(invoice) {
        return {
            type: 'invoice.payment_succeeded',
            invoice_id: invoice.id,
            charge_id: invoice.charge,
            customer_id: invoice.customer,
            amount: invoice.amount_paid / 100,
            message: 'Invoice payment completed'
        };
    }

    // Generate QuickBooks compatible data for cash basis accounting
    generateQBDataForCashAccounting(transactions, startDate, endDate) {
        // Filter transactions for the accounting period
        const periodTransactions = transactions.filter(t => {
            const tDate = new Date(t.accounting_date);
            const start = new Date(startDate);
            const end = new Date(endDate);
            return tDate >= start && tDate <= end;
        });

        // Group by customer for summary
        const customerSummary = periodTransactions.reduce((acc, transaction) => {
            const customerId = transaction.customer_id || 'unknown';
            if (!acc[customerId]) {
                acc[customerId] = {
                    customer_name: transaction.customer_name || 'Unknown Customer',
                    total_amount: 0,
                    transactions: []
                };
            }
            acc[customerId].total_amount += transaction.net_amount;
            acc[customerId].transactions.push(transaction);
            return acc;
        }, {});

        return {
            period: { start: startDate, end: endDate },
            transactions: periodTransactions,
            customerSummary: customerSummary,
            totalRevenue: periodTransactions.reduce((sum, t) => sum + t.net_amount, 0),
            totalFees: periodTransactions.reduce((sum, t) => sum + t.fee_amount, 0)
        };
    }

    // Validate webhook signature
    validateWebhookSignature(payload, signature) {
        if (!this.webhookSecret) {
            throw new Error('STRIPE_WEBHOOK_SECRET not configured');
        }

        try {
            const event = this.stripe.webhooks.constructEvent(
                payload,
                signature,
                this.webhookSecret
            );
            return { valid: true, event };
        } catch (error) {
            return { valid: false, error: error.message };
        }
    }
}

module.exports = StripeAPI;