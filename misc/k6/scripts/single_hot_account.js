// k6 performance test: single_hot_account scenario
// This test creates EXTREME contention on a single account
//
// Scenario:
// - ALL virtual users send to/from the SAME account simultaneously
// - This is the worst-case scenario for contention
// - Useful for stress testing the system's conflict resolution
//
// Use cases tested:
// - Payment processor receiving millions of deposits
// - Popular merchant receiving many payments
// - Central treasury account
//
// Environment variables:
// - HOT_ACCOUNT: The hot account name (default: treasury:main)
// - CONTENTION_MODE: 'deposit' (many->one), 'withdraw' (one->many), 'mixed' (both)
// - BULK_SIZE: Number of transactions per request

import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, scriptRequest } from './shared/utils.js';
import exec from 'k6/execution';

// Configuration
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');
const HOT_ACCOUNT = __ENV.HOT_ACCOUNT || 'treasury:main';
const CONTENTION_MODE = __ENV.CONTENTION_MODE || 'mixed'; // 'deposit', 'withdraw', 'mixed'
const SENDER_POOL_SIZE = parseInt(__ENV.SENDER_POOL_SIZE || '1000');

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');
const depositOps = new Counter('deposit_ops');
const withdrawOps = new Counter('withdraw_ops');

export const options = buildOptions(config);

let client;

// Generate a unique sender account
function getSenderAccount(uniqueId) {
  return `user:${uniqueId % SENDER_POOL_SIZE}`;
}

// Deposit: sender -> hot account (many -> one)
function generateDeposit(uniqueId) {
  const sender = getSenderAccount(uniqueId);
  const amount = 100 + Math.floor(Math.random() * 900);

  depositOps.add(1);

  return scriptRequest(config.ledgerName,
    `vars {
            account $sender
            monetary $amount
        }
        send $amount (
            source = $sender allowing unbounded overdraft
            destination = @${HOT_ACCOUNT}
        )`,
    { sender, amount: `USD/2 ${amount}` },
    { type: 'deposit', sender },
  );
}

// Withdraw: hot account -> recipient (one -> many)
function generateWithdraw(uniqueId) {
  const recipient = getSenderAccount(uniqueId);
  const amount = 50 + Math.floor(Math.random() * 450);

  withdrawOps.add(1);

  return scriptRequest(config.ledgerName,
    `vars {
            account $recipient
            monetary $amount
        }
        send $amount (
            source = @${HOT_ACCOUNT} allowing unbounded overdraft
            destination = $recipient
        )`,
    { recipient, amount: `USD/2 ${amount}` },
    { type: 'withdraw', recipient },
  );
}

// Transfer through hot account: sender -> hot -> recipient
function generateTransfer(uniqueId) {
  const sender = getSenderAccount(uniqueId);
  const recipient = getSenderAccount(uniqueId + 1);
  const amount = 100;

  depositOps.add(1);
  withdrawOps.add(1);

  return scriptRequest(config.ledgerName,
    `vars {
            account $sender
            account $recipient
            monetary $amount
        }
        // Deposit to hot account
        send $amount (
            source = $sender allowing unbounded overdraft
            destination = @${HOT_ACCOUNT}
        )
        // Withdraw from hot account
        send $amount (
            source = @${HOT_ACCOUNT} allowing unbounded overdraft
            destination = $recipient
        )`,
    { sender, recipient, amount: `USD/2 ${amount}` },
    { type: 'transfer_through_hot', sender, recipient },
  );
}

function generateTransaction(uniqueId) {
  switch (CONTENTION_MODE) {
    case 'deposit':
      return generateDeposit(uniqueId);
    case 'withdraw':
      return generateWithdraw(uniqueId);
    case 'transfer':
      return generateTransfer(uniqueId);
    case 'mixed':
    default:
      // 50% deposit, 50% withdraw
      if (uniqueId % 2 === 0) {
        return generateDeposit(uniqueId);
      } else {
        return generateWithdraw(uniqueId);
      }
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
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}

// Setup: Initialize the hot account with funds
export function setup() {
  const setupClient = connectClient(config.grpcAddr);

  // Initialize hot account with large balance to handle withdrawals
  const initRequests = [
    scriptRequest(config.ledgerName,
      `send [USD/2 100000000] (
              source = @world
              destination = @${HOT_ACCOUNT}
          )`,
      {},
      { type: 'hot_account_init', hot_account: HOT_ACCOUNT },
    ),
  ];

  const response = apply(setupClient, initRequests);
  check(response, {
    'setup: hot account initialized': (r) => r && r.status === grpc.StatusOK,
  });

  setupClient.close();

  console.log(`Setup complete: hot account @${HOT_ACCOUNT} initialized`);
  console.log(`Contention mode: ${CONTENTION_MODE}`);
  console.log(`Bulk size: ${BULK_SIZE}`);

  return { hotAccount: HOT_ACCOUNT, mode: CONTENTION_MODE };
}

export function handleSummary(data) {
  const txCreated = data.metrics.transactions_created?.values?.count || 0;
  const duration = data.state.testRunDurationMs / 1000;
  const tps = txCreated / duration;

  return {
    stdout: JSON.stringify({
      test: 'single_hot_account',
      config: {
        hot_account: HOT_ACCOUNT,
        contention_mode: CONTENTION_MODE,
        bulk_size: BULK_SIZE,
        sender_pool_size: SENDER_POOL_SIZE,
      },
      metrics: {
        transactions_created: txCreated,
        deposit_ops: data.metrics.deposit_ops?.values?.count || 0,
        withdraw_ops: data.metrics.withdraw_ops?.values?.count || 0,
        error_rate: data.metrics.errors?.values?.rate || 0,
        p50_latency: data.metrics.bulk_latency?.values?.['p(50)'] || 0,
        p95_latency: data.metrics.bulk_latency?.values?.['p(95)'] || 0,
        p99_latency: data.metrics.bulk_latency?.values?.['p(99)'] || 0,
        avg_latency: data.metrics.bulk_latency?.values?.avg || 0,
        max_latency: data.metrics.bulk_latency?.values?.max || 0,
        rps: data.metrics.grpc_reqs?.values?.rate || 0,
        tps: tps,
      },
    }, null, 2),
  };
}
