// k6 performance test: high_contention_marketplace scenario
// This test simulates a realistic marketplace with high contention on hot accounts
//
// Scenario:
// - Multiple sellers deposit funds to a central @platform account (many → one)
// - Platform pays out to multiple sellers from @platform account (one → many)  
// - Buyers pay sellers through escrow accounts with platform fees
// - All operations contend on @platform and @fees accounts
//
// This creates realistic contention patterns seen in payment/marketplace systems.

import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkOperation } from './shared/utils.js';
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

// Scenario 1: Seller deposits to platform (many → one contention)
// Simulates sellers adding funds to their platform wallet
function generateSellerDeposit(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
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
        vars: {
          seller: seller,
          amount: 'USD/2 1000', // $10.00 deposit
        },
      },
      metadata: {
        type: 'seller_deposit',
        seller: seller,
      },
    },
  };
}

// Scenario 2: Platform pays out to seller (one → many contention)
// Simulates platform paying out earnings to sellers
function generatePayout(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  const amount = 500 + Math.floor(Math.random() * 500); // $5.00-$10.00
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
            account $seller
            monetary $amount
        }
        send $amount (
            source = @${hotAccounts.platform} allowing unbounded overdraft
            destination = $seller
        )`,
        vars: {
          seller: seller,
          amount: `USD/2 ${amount}`,
        },
      },
      metadata: {
        type: 'seller_payout',
        seller: seller,
      },
    },
  };
}

// Scenario 3: Buyer payment with fees (complex multi-account contention)
// Simulates a buyer paying a seller with platform fees
function generatePaymentWithFees(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const seller = getSellerAccount(uniqueId);
  const merchant = getHotAccount(uniqueId);
  const amount = 1000 + Math.floor(Math.random() * 9000); // $10.00-$100.00
  const feePercent = 3; // 3% platform fee

  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
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
        vars: {
          buyer: buyer,
          seller: seller,
          merchant: merchant,
          amount: `USD/2 ${amount}`,
          fee_percent: `${feePercent}/100`,
        },
      },
      metadata: {
        type: 'payment_with_fees',
        buyer: buyer,
        seller: seller,
      },
    },
  };
}

// Scenario 4: Escrow funding (high contention on escrow account)
// Simulates buyers funding escrow for pending orders
function generateEscrowFunding(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const orderId = uniqueId;
  const amount = 2000 + Math.floor(Math.random() * 8000); // $20.00-$100.00

  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
            account $buyer
            string $order_id
            monetary $amount
        }
        send $amount (
            source = $buyer allowing unbounded overdraft
            destination = @escrow:$order_id
        )`,
        vars: {
          buyer: buyer,
          order_id: orderId.toString(),
          amount: `USD/2 ${amount}`,
        },
      },
      metadata: {
        type: 'escrow_funding',
        buyer: buyer,
        order_id: orderId.toString(),
      },
    },
  };
}

// Scenario 5: Escrow release with fees (releases escrowed funds)
function generateEscrowRelease(uniqueId) {
  const seller = getSellerAccount(uniqueId);
  const orderId = uniqueId;

  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
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
        vars: {
          seller: seller,
          order_id: orderId.toString(),
          fee_percent: '3/100',
        },
      },
      metadata: {
        type: 'escrow_release',
        seller: seller,
        order_id: orderId.toString(),
      },
    },
  };
}

// Scenario 6: High-frequency micropayments to merchant (extreme contention)
// Simulates many small payments to a single merchant
function generateMicropayment(uniqueId) {
  const buyer = getBuyerAccount(uniqueId);
  const merchant = getHotAccount(0); // Always same merchant = maximum contention
  const amount = 10 + Math.floor(Math.random() * 90); // $0.10-$1.00

  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
            account $buyer
            account $merchant
            monetary $amount
        }
        send $amount (
            source = $buyer allowing unbounded overdraft
            destination = $merchant
        )`,
        vars: {
          buyer: buyer,
          merchant: merchant,
          amount: `USD/2 ${amount}`,
        },
      },
      metadata: {
        type: 'micropayment',
        buyer: buyer,
        merchant: merchant,
      },
    },
  };
}

// Distribution of scenarios (weighted random selection)
// Adjust these weights to simulate different traffic patterns
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

function generateBulkElements(iteration) {
  const elements = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    const uniqueId = iteration * BULK_SIZE + i;
    elements.push(generateTransaction(uniqueId));
  }
  return elements;
}

export default function () {
  const ledgerName = config.ledgerName;
  const elements = generateBulkElements(exec.scenario.iterationInTest);
  
  const startTime = Date.now();
  const response = bulkOperation(config, ledgerName, elements);
  const latency = Date.now() - startTime;
  
  bulkLatency.add(latency);
  
  const success = check(response, {
    'bulk operation successful': (r) => r.status === 200,
  });

  if (!success) {
    errorRate.add(1);
    // Check if it's a contention-related error
    if (response.body && response.body.includes('conflict')) {
      contentionErrors.add(1);
    }
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}

// Setup function to initialize platform accounts
export function setup() {
  const ledgerName = config.ledgerName;
  
  // Initialize platform accounts with initial balance from @world
  const initElements = [
    {
      action: 'CREATE_TRANSACTION',
      data: {
        script: {
          plain: `send [USD/2 10000000] (
              source = @world
              destination = @${hotAccounts.platform}
          )`,
          vars: {},
        },
        metadata: {
          type: 'platform_init',
        },
      },
    },
    {
      action: 'CREATE_TRANSACTION',
      data: {
        script: {
          plain: `send [USD/2 1000000] (
              source = @world
              destination = @${hotAccounts.escrow}
          )`,
          vars: {},
        },
        metadata: {
          type: 'escrow_init',
        },
      },
    },
  ];
  
  // Initialize hot merchant accounts
  for (let i = 0; i < HOT_ACCOUNT_COUNT; i++) {
    initElements.push({
      action: 'CREATE_TRANSACTION',
      data: {
        script: {
          plain: `vars {
              account $merchant
          }
          send [USD/2 1000000] (
              source = @world
              destination = $merchant
          )`,
          vars: {
            merchant: `merchant:${i}`,
          },
        },
        metadata: {
          type: 'merchant_init',
          merchant_id: i.toString(),
        },
      },
    });
  }
  
  const response = bulkOperation(config, ledgerName, initElements);
  check(response, {
    'setup: platform accounts initialized': (r) => r.status === 200,
  });
  
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
        rps: data.metrics.http_reqs?.values?.rate || 0,
      },
    }, null, 2),
  };
}
