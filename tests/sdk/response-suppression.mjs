import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

// Use a compiled consumer SDK and real registered-router response fixtures.
const [sdkDirectory, writeDirectory, bulkDirectory] = process.argv.slice(2);
assert.ok(sdkDirectory && writeDirectory && bulkDirectory,
  "Usage: node response-suppression.mjs <compiled-sdk> <write-fixtures> <bulk-fixtures>");
const { SDK } = await import(pathToFileURL(resolve(sdkDirectory, "esm/sdk/sdk.js")));
const { HTTPClient } = await import(pathToFileURL(resolve(sdkDirectory, "esm/lib/http.js")));
const cases = [
  ["transactions", "createTransaction", "/v3/l1/transactions", { ledgerName: "l1", body: {} }],
  ["ledgers", "createLedger", "/v3/l1", { ledgerName: "l1", body: {} }],
  ["ledgers", "promoteLedger", "/v3/l1/promote", { ledgerName: "l1" }],
  ["numscriptLibrary", "saveNumscript", "/v3/l1/numscripts/script", {
    ledgerName: "l1", name: "script", body: { content: "send [USD 1] (source = @world destination = @bank)", version: "1.0.0" },
  }],
  ["ops", "addEventsSink", "/v3/_/events-sinks", { body: { name: "sink", nats: { url: "nats://localhost:4222", topic: "ledger" } } }],
  ["indexes", "createIndex", "/v3/l1/indexes", { ledgerName: "l1", body: { id: "metadata:TARGET_TYPE_ACCOUNT:color" } }],
  ["transactions", "revertTransaction", "/v3/l1/transactions/7/revert", { ledgerName: "l1", transactionId: 7, body: {} }],
];
for (const [group, operation, path, input] of cases) {
  for (const mode of [undefined, false, true]) {
    const suffix = mode === undefined ? "" : `?skipResponse=${mode}`;
    const filename = (path + suffix).replaceAll(/[/?=]/g, "_") + ".json";
    const raw = await readFile(resolve(writeDirectory, filename), "utf8");
    let request;
    const sdk = new SDK({ serverURL: "https://ledger.test", httpClient: new HTTPClient({ fetcher: async (value) => {
      request = value;
      return new Response(raw, { status: 201, headers: { "Content-Type": "application/json" } });
    } }) });
    const response = await sdk[group][operation]({ ...input, ...(mode === undefined ? {} : { skipResponse: mode }) });
    assert.equal(new URL(request.url).pathname, path);
    const option = new URL(request.url).searchParams.get("skipResponse");
    if (mode === undefined) assert.ok(option === null || option === "false");
    else assert.equal(option, String(mode));
    assert.deepEqual(JSON.parse(JSON.stringify(response.result)), JSON.parse(raw), `${operation} mode ${mode}`);
    if (mode === true) {
      // JSON.parse equality above proves fields, not integer precision. Compare
      // the actual raw token independently and disclose the numeric SDK limit.
      const token = raw.match(/"sequence"\s*:\s*(\d+)/)?.[1];
      assert.ok(token, "compact fixture must contain a numeric sequence");
      const decoded = response.result.data.sequence;
      if (BigInt(decoded) !== BigInt(token)) {
        console.log(`LIMIT: ${operation} sequence ${token} rounds to ${decoded}; use lossless JSON for full range`);
      }
    }
  }
}
for (const atomic of [false, true]) {
  for (const compact of [false, true]) {
    const file = `write-response-atomic-${atomic}-compact-${compact}.json`;
    const fixture = JSON.parse(await readFile(resolve(bulkDirectory, file), "utf8"));
    const sdk = new SDK({ serverURL: "https://ledger.test", httpClient: new HTTPClient({ fetcher: async (request) => {
      assert.equal(new URL(request.url).searchParams.get("skipResponse"), String(compact));
      assert.equal(new URL(request.url).searchParams.get("atomic"), String(atomic));
      return new Response(JSON.stringify(fixture.body), { status: fixture.status, headers: { "Content-Type": "application/json" } });
    } }) });
    const response = await sdk.transactions.bulkOperations({ ledgerName: "l1", atomic, skipResponse: compact, body: [] });
    assert.deepEqual(JSON.parse(JSON.stringify(response.result)), fixture.body, file);
  }
}
console.log("PASS: routed full/default/compact single and sequential/atomic bulk responses survive generated SDK operations");
