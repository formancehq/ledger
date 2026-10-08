// Compiled together with the freshly generated SDK: operation header and amount
// fields must be expressible through the public generated types.
import { SDK } from "./sdk/sdk.js";
import { HTTPClient } from "./lib/http.js";

declare const process: { argv: string[] };
const exact = "340282366920938463463374607431768211457";
function equal(actual: unknown, expected: unknown, description: string): void {
  if (actual !== expected) {
    throw new Error(`${description}: expected ${String(expected)}, received ${String(actual)}`);
  }
}

const transport = new HTTPClient();
transport.addHook("beforeRequest", async (request) => {
  if (request.method === "POST" && new URL(request.url).pathname.endsWith("/transactions")) {
    const body = await request.clone().json();
    equal(body.postings[0].amount, request.headers.get("Formance-Bigint-As-String") || body.reference === "string-default" ? exact : 42,
      "SDK preserves the input JSON amount type");
  }
  if (new URL(request.url).pathname.endsWith("/bulk")) {
    const body = await request.clone().json();
    for (const element of body) equal(element.data.postings[0].amount, exact, "SDK preserves bulk string input");
  }
});
const sdk = new SDK({ serverURL: process.argv[2], httpClient: transport });

const numeric = await sdk.transactions.createTransaction({
  ledgerName: "ledger1",
  body: { postings: [{ source: "world", destination: "alice", amount: 42, asset: "USD" }] },
});
equal(numeric.result.data.transaction?.postings[0]?.amount, 42, "default numeric amount");

const created = await sdk.transactions.createTransaction({
  ledgerName: "ledger1",
  formanceBigintAsString: "true",
  body: { postings: [{ source: "world", destination: "alice", amount: exact, asset: "USD" }] },
});
equal(created.result.data.transaction?.postings[0]?.amount, exact, "exact create response");

const stringDefault = await sdk.transactions.createTransaction({
  ledgerName: "ledger1",
  body: { reference: "string-default", postings: [{ source: "world", destination: "alice", amount: exact, asset: "USD" }] },
});
equal(typeof stringDefault.result.data.transaction?.postings[0]?.amount, "number", "string input without negotiation");

const retrieved = await sdk.transactions.getTransaction({
  ledgerName: "ledger1", transactionId: 2, formanceBigintAsString: "true",
});
equal(retrieved.result.data.transaction.postings[0]?.amount, exact, "exact get response");

const account = await sdk.accounts.getAccount({
  ledgerName: "ledger1", address: "alice", formanceBigintAsString: "true",
});
equal(account.result.data.volumes?.[0]?.volumes.input, "0", "exact volume input");
equal(account.result.data.volumes?.[0]?.volumes.output, exact, "exact volume output");
equal(account.result.data.volumes?.[0]?.volumes.balance, `-${exact}`, "exact signed balance");

// The numeric default intentionally exposes JavaScript's normal numeric type;
// callers requiring precision opt into strings through the generated header.
const largeDefault = await sdk.transactions.getTransaction({ ledgerName: "ledger1", transactionId: 2 });
equal(typeof largeDefault.result.data.transaction.postings[0]?.amount, "number", "large default JSON type");
const numericAccount = await sdk.accounts.getAccount({ ledgerName: "ledger1", address: "numeric" });
equal(numericAccount.result.data.volumes?.[0]?.volumes.input, 0, "default volume input");
equal(numericAccount.result.data.volumes?.[0]?.volumes.output, 42, "default volume output");
equal(numericAccount.result.data.volumes?.[0]?.volumes.balance, -42, "default signed balance");

// Inspect the decoded operation result so the same assertions cover nested
// transaction/cursor/log variants without casts that bypass generated types.
function monetaryValues(value: unknown): unknown[] {
  if (typeof value !== "object" || value === null) return [];
  return Object.entries(value).flatMap(([key, field]) =>
    ["amount", "input", "output", "balance"].includes(key) ? [field] : monetaryValues(field));
}
function exactMoney(value: unknown, operation: string): void {
  const values = monetaryValues(value);
  if (!values.includes(exact)) throw new Error(`${operation}: exact monetary value missing`);
  for (const value of values) equal(typeof value, "string", `${operation}: monetary JSON type`);
}
exactMoney(created, "create with post-commit volumes");
const transactionPage = await sdk.transactions.listLedgerTransactions({
  ledgerName: "ledger1", pageSize: 1, formanceBigintAsString: "true",
});
exactMoney(transactionPage, "transaction cursor");
equal(transactionPage.result.data?.length, 1, "transaction page size");
equal(transactionPage.result.hasMore, true, "transaction next page retained");
if (!transactionPage.result.next) throw new Error("transaction next cursor missing");
const nextTransactionPage = await sdk.transactions.listLedgerTransactions({
  ledgerName: "ledger1", pageSize: 1, cursor: transactionPage.result.next, formanceBigintAsString: "true",
});
exactMoney(nextTransactionPage, "next transaction cursor");
equal(nextTransactionPage.result.data?.[0]?.id, 2, "transaction next cursor applied");
equal(nextTransactionPage.result.hasMore, false, "last transaction page");
if (!nextTransactionPage.result.previous) throw new Error("transaction previous cursor missing");
const previousTransactionPage = await sdk.transactions.listLedgerTransactions({
  ledgerName: "ledger1", pageSize: 1, cursor: nextTransactionPage.result.previous, formanceBigintAsString: "true",
});
exactMoney(previousTransactionPage, "previous transaction cursor");
equal(previousTransactionPage.result.data?.[0]?.id, 3, "transaction previous cursor applied");
exactMoney(await sdk.transactions.revertTransaction({
  ledgerName: "ledger1", transactionId: 2, formanceBigintAsString: "true",
}), "revert transaction");
const bulk = await sdk.transactions.bulkOperations({
  ledgerName: "ledger1", continueOnFailure: true, formanceBigintAsString: "true",
  body: [
    { action: "CREATE_TRANSACTION", data: { postings: [{ source: "world", destination: "alice", amount: exact, asset: "USD" }] } },
    { action: "CREATE_TRANSACTION", data: { reference: "fail", postings: [{ source: "world", destination: "alice", amount: exact, asset: "USD" }] } },
  ],
});
equal(bulk.result.data?.length, 2, "bulk partial results preserved");
equal(bulk.result.data?.[1]?.responseType, "ERROR", "bulk failure discriminator");
exactMoney(bulk.result.data?.[0], "bulk success beside failure");
const logPage = await sdk.logs.listLedgerLogs({ ledgerName: "ledger1", pageSize: 1, formanceBigintAsString: "true" });
exactMoney(logPage, "ledger logs");
equal(logPage.result.data?.length, 1, "log page size");
equal(logPage.result.hasMore, true, "log next page retained");
if (!logPage.result.next) throw new Error("log next cursor missing");
const nextLogPage = await sdk.logs.listLedgerLogs({
  ledgerName: "ledger1", pageSize: 1, cursor: logPage.result.next, formanceBigintAsString: "true",
});
exactMoney(nextLogPage, "next ledger log cursor");
equal(nextLogPage.result.hasMore, false, "last log page");
if (!nextLogPage.result.previous) throw new Error("log previous cursor missing");
exactMoney(await sdk.logs.getLog({ sequence: 7, formanceBigintAsString: "true" }), "system log");
for (const queryName of ["transactions", "accounts", "logs"]) {
  exactMoney(await sdk.preparedQueries.executePreparedQuery({
    ledgerName: "ledger1", queryName, formanceBigintAsString: "true",
  }), `prepared query ${queryName} cursor`);
}

exactMoney(await sdk.volumes.aggregateVolumes({
  ledgerName: "ledger1", formanceBigintAsString: "true",
}), "aggregate volumes");
exactMoney(await sdk.preparedQueries.executePreparedQuery({
  ledgerName: "ledger1", queryName: "aggregate", formanceBigintAsString: "true", body: { mode: "AGGREGATE_VOLUMES" },
}), "prepared query aggregate");

const accountPage = await sdk.accounts.listAccounts({
  ledgerName: "ledger1", pageSize: 1, formanceBigintAsString: "true",
});
exactMoney(accountPage, "account cursor");
equal(accountPage.result.data?.length, 1, "account page size");
equal(accountPage.result.hasMore, true, "account next page retained");
if (!accountPage.result.next) throw new Error("account next cursor missing");
const nextAccountPage = await sdk.accounts.listAccounts({
  ledgerName: "ledger1", pageSize: 1, cursor: accountPage.result.next, formanceBigintAsString: "true",
});
exactMoney(nextAccountPage, "next account cursor");
equal(nextAccountPage.result.data?.[0]?.address, "bob", "account next cursor applied");
equal(nextAccountPage.result.hasMore, false, "last account page");
if (!nextAccountPage.result.previous) throw new Error("account previous cursor missing");

for (const negotiated of [false, true]) {
  const analysis = await sdk.transactions.analyzeTransactions({
    ledgerName: "ledger1", ...(negotiated ? { formanceBigintAsString: "true" } : {}),
  });
  const pattern = analysis.result.data.flowPatterns?.[0];
  const statistics = pattern?.volumeStats?.[0];
  if (!statistics) throw new Error("transaction analysis volume statistics missing");
  for (const value of [statistics.totalVolume, statistics.averageVolume, statistics.minVolume, statistics.maxVolume]) {
    if (negotiated) equal(value, exact, "exact transaction analysis statistic");
    else equal(typeof value, "number", "default transaction analysis numeric type");
  }
  equal(analysis.result.data.totalTransactions, 1, "analysis total count stays numeric");
  equal(analysis.result.data.totalReverted, 0, "analysis reverted count stays numeric");
  equal(pattern?.transactionCount, 1, "analysis flow count stays numeric");
  equal(statistics.transactionCount, 1, "analysis volume count stays numeric");
}
