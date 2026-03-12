// k6 performance test: high_contention_marketplace scenario
// This test simulates a realistic marketplace with high contention on hot accounts
//
// Scenario:
// - Multiple sellers deposit funds to a central @platform account (many -> one)
// - Platform pays out to multiple sellers from @platform account (one -> many)
// - Buyers pay sellers through escrow accounts with platform fees
// - All operations contend on @platform and @fees accounts
//
// This creates realistic contention patterns seen in payment/marketplace systems.

import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, scriptRequest } from './shared/utils.js';
import exec from 'k6/execution';

// Configuration
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');
const HOT_ACCOUNT_COUNT = parseInt(__ENV.HOT_ACCOUNT_COUNT || '3'); // Number of hot accounts
const SELLER_POOL_SIZE = parseInt(__ENV.SELLER_POOL_SIZE || '100'); // Pool of seller accounts
const BUYER_POOL_SIZE = parseInt(__ENV.BUYER_POOL_SIZE || '100'); // Pool of buyer accounts

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');
const depositsCreated = new Counter('deposits_created');
const withdrawalsCreated = new Counter('withdrawals_created');
const paymentsCreated = new Counter('payments_created');
const contentionErrors = new Counter('contention_errors');

export const options = buildOptions(config);

let client;

// Hot accounts that all VUs contend for
const hotAccounts = {
  platform: 'platform:main', // Main platform account (receives all deposits)
  fees: 'platform:fees', // Fee collection account
  escrow: 'platform:escrow', // Escrow holding account
};

// Generate a seller account address from a pool
function getSellerAccount(id) {
  return `seller:${id % SELLER_POOL_SIZE}`;
}

// Generate a buyer account address from a pool
function getBuyerAccount(id) {
  return `buyer:${id % BUYER_POOL_SIZE}`;
}

// Generate hot accounts for distribution (simulate multiple merchants)
function getHotAccount(id) {
  return `merchant:${id % HOT_ACCOUNT_COUNT}`;
}

// Scenario 1: Seller deposits to platform (many -> one contention)
function generateSellerDeposit(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  return scriptRequest(config.ledgerName,
    `vars {
            account $seller
            monetary $amount
        }
        send $amount (
            source = @world
            destination = $seller
        )
        send $amount (
            source = $seller allowing unbounded overdraft
            destination = @${hotAccounts.platform}
        )`,
    { seller, amount: 'USD/2 1000' },
    { type: 'seller_deposit', seller },
  );
}

// Scenario 2: Platform pays out to seller (one -> many contention)
function generatePayout(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  const amount = 500 + Math.floor(Math.random() * 500);
  return scriptRequest(config.ledgerName,
    `vars {
            account $seller
            monetary $amount
        }
        send $amount (
            source = @${hotAccounts.platform} allowing unbounded overdraft
            destination = $seller
        )`,
    { seller, amount: `USD/2 ${amount}` },
    { type: 'seller_payout', seller },
  );
}

// Scenario 3: Buyer payment with fees (complex multi-account contention)
function generatePaymentWithFees(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const seller = getSellerAccount(uniqueId);
  const merchant = getHotAccount(uniqueId);
  const amount = 1000 + Math.floor(Math.random() * 9000);
  const feePercent = 3;

  return scriptRequest(config.ledgerName,
    `vars {
            account $buyer
            account $seller
            account $merchant
            monetary $amount
            portion $fee_percent
        }
        send $amount (
            source = $buyer allowing unbounded overdraft
            destination = {
                $fee_percent to @${hotAccounts.fees}
                remaining to $merchant
            }
        )`,
    {
      buyer,
      seller,
      merchant,
      amount: `USD/2 ${amount}`,
      fee_percent: `${feePercent}/100`,
    },
    { type: 'payment_with_fees', buyer, seller },
  );
}

// Scenario 4: Escrow funding (high contention on escrow account)
function generateEscrowFunding(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const orderId = uniqueId;
  const amount = 2000 + Math.floor(Math.random() * 8000);

  return scriptRequest(config.ledgerName,
    `vars {
            account $buyer
            string $order_id
            monetary $amount
        }
        send $amount (
            source = $buyer allowing unbounded overdraft
            destination = @escrow:$order_id
        )`,
    { buyer, order_id: orderId.toString(), amount: `USD/2 ${amount}` },
    { type: 'escrow_funding', buyer, order_id: orderId.toString() },
  );
}

// Scenario 5: Escrow release with fees
function generateEscrowRelease(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  const orderId = uniqueId;

  return scriptRequest(config.ledgerName,
    `vars {
            account $seller
            string $order_id
            portion $fee_percent
        }
        send [USD/2 *] (
            source = @escrow:$order_id
            destination = {
                $fee_percent to @${hotAccounts.fees}
                remaining to $seller
            }
        )`,
    { seller, order_id: orderId.toString(), fee_percent: '3/100' },
    { type: 'escrow_release', seller, order_id: orderId.toString() },
  );
}

// Scenario 6: High-frequency micropayments to merchant (extreme contention)
function generateMicropayment(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const merchant = getHotAccount(0); // Always same merchant = maximum contention
  const amount = 10 + Math.floor(Math.random() * 90);

  return scriptRequest(config.ledgerName,
    `vars {
            account $buyer
            account $merchant
            monetary $amount
        }
        send $amount (
            source = $buyer allowing unbounded overdraft
            destination = $merchant
        )`,
    { buyer, merchant, amount: `USD/2 ${amount}` },
    { type: 'micropayment', buyer, merchant },
  );
}

// Distribution of scenarios (weighted random selection)
const scenarioWeights = {
  sellerDeposit: 15,      // 15% - Seller deposits
  payout: 10,             // 10% - Platform payouts
  paymentWithFees: 25,    // 25% - Payments with fees
  escrowFunding: 15,      // 15% - Escrow funding
  escrowRelease: 10,      // 10% - Escrow releases
  micropayment: 25,       // 25% - High-frequency micropayments
};

function selectScenario() {
  const total = Object.values(scenarioWeights).reduce((a, b) => a + b, 0);
  let random = Math.floor(Math.random() * total);

  for (const [scenario, weight] of Object.entries(scenarioWeights)) {
    random -= weight;
    if (random < 0) {
      return scenario;
    }
  }
  return 'micropayment'; // Default fallback
}

function generateTransaction(uniqueId) {
  const scenario = selectScenario();

  switch (scenario) {
    case 'sellerDeposit':
      depositsCreated.add(1);
      return generateSellerDeposit(uniqueId);
    case 'payout':
      withdrawalsCreated.add(1);
      return generatePayout(uniqueId);
    case 'paymentWithFees':
      paymentsCreated.add(1);
      return generatePaymentWithFees(uniqueId);
    case 'escrowFunding':
      return generateEscrowFunding(uniqueId);
    case 'escrowRelease':
      return generateEscrowRelease(uniqueId);
    case 'micropayment':
      paymentsCreated.add(1);
      return generateMicropayment(uniqueId);
    default:
      return generateMicropayment(uniqueId);
  }
}

function generateRequests(iteration) {
  const requests = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    const uniqueId = iteration * BULK_SIZE + i;
    requests.push(generateTransaction(uniqueId));
  }
  return requests;
}

export default function () {
  if (!client) client = connectClient(config.grpcAddr);

  const requests = generateRequests(exec.scenario.iterationInTest);

  const startTime = Date.now();
  const response = apply(client, requests);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  const success = check(response, {
    'bulk operation successful': (r) => r && r.status === grpc.StatusOK,
  });

  if (!success) {
    errorRate.add(1);
    if (response && response.error && response.error.message && response.error.message.includes('conflict')) {
      contentionErrors.add(1);
    }
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}

// Setup function to initialize platform accounts
export function setup() {
  const setupClient = connectClient(config.grpcAddr);

  // Initialize platform accounts with initial balance from @world
  const initRequests = [
    scriptRequest(config.ledgerName,
      `send [USD/2 10000000] (
              source = @world
              destination = @${hotAccounts.platform}
          )`,
      {},
      { type: 'platform_init' },
    ),
    scriptRequest(config.ledgerName,
      `send [USD/2 1000000] (
              source = @world
              destination = @${hotAccounts.escrow}
          )`,
      {},
      { type: 'escrow_init' },
    ),
  ];

  // Initialize hot merchant accounts
  for (let i = 0; i < HOT_ACCOUNT_COUNT; i++) {
    initRequests.push(scriptRequest(config.ledgerName,
      `vars {
              account $merchant
          }
          send [USD/2 1000000] (
              source = @world
              destination = $merchant
          )`,
      { merchant: `merchant:${i}` },
      { type: 'merchant_init', merchant_id: i.toString() },
    ));
  }

  const response = apply(setupClient, initRequests);
  check(response, {
    'setup: platform accounts initialized': (r) => r && r.status === grpc.StatusOK,
  });

  setupClient.close();

  console.log(`Setup complete: initialized ${HOT_ACCOUNT_COUNT} hot merchant accounts`);
  return { hotAccounts, merchantCount: HOT_ACCOUNT_COUNT };
}

export function handleSummary(data) {
  return {
    stdout: JSON.stringify({
      test: 'high_contention_marketplace',
      metrics: {
        transactions_created: data.metrics.transactions_created?.values?.count || 0,
        deposits_created: data.metrics.deposits_created?.values?.count || 0,
        withdrawals_created: data.metrics.withdrawals_created?.values?.count || 0,
        payments_created: data.metrics.payments_created?.values?.count || 0,
        contention_errors: data.metrics.contention_errors?.values?.count || 0,
        error_rate: data.metrics.errors?.values?.rate || 0,
        p95_latency: data.metrics.bulk_latency?.values?.['p(95)'] || 0,
        p99_latency: data.metrics.bulk_latency?.values?.['p(99)'] || 0,
        avg_latency: data.metrics.bulk_latency?.values?.avg || 0,
        rps: data.metrics.grpc_reqs?.values?.rate || 0,
      },
    }, null, 2),
  };
}
