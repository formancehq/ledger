import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

// Accept an already generated, compiled SDK so this probe uses the consumer's
// real generator configuration without adding a second generator to Ledger.
const [sdkDirectory, fixtureDirectory] = process.argv.slice(2);
assert.ok(sdkDirectory && fixtureDirectory, "Usage: node bulk-contract.mjs <sdk-directory> <fixture-directory>");
const { SDK } = await import(pathToFileURL(resolve(sdkDirectory, "esm/sdk/sdk.js")));
const { HTTPClient } = await import(pathToFileURL(resolve(sdkDirectory, "esm/lib/http.js")));
const files = (await readdir(fixtureDirectory)).filter((name) => name.endsWith(".json"));
for (const required of [
  "internal-continue-false", "internal-continue-true",
  "unavailable-continue-false", "unavailable-continue-true",
  "not-found-continue-false", "conflict-continue-false",
  "unauthenticated-continue-false", "permission-denied-continue-false",
  "resource-exhausted-continue-false", "resource-exhausted-continue-true",
  "malformed", "body-byte-limit", "element-count-limit", "no-token",
  "invalid-token", "insufficient-scope", "panic-recovery", "empty-success",
  "atomic-continue-false", "atomic-continue-true",
  "no-leader-continue-false", "no-leader-continue-true",
  "cache-horizon-continue-false", "cache-horizon-continue-true",
  "unknown-error-continue-false", "unknown-error-continue-true",
]) {
  assert.ok(files.includes(`${required}.json`), `Missing real handler fixture: ${required}`);
}

for (const file of files) {
  const fixture = JSON.parse(await readFile(resolve(fixtureDirectory, file), "utf8"));
  let attempts = 0;
  const httpClient = new HTTPClient({ fetcher: async (request) => {
    attempts++;
    assert.equal(request.method, "POST");
    assert.equal(new URL(request.url).pathname, "/v3/ledger1/bulk");
    const headers = new Headers();
    for (const [key, values] of Object.entries(fixture.headers)) {
      for (const value of values) headers.append(key, value);
    }
    const body = typeof fixture.body === "string" ? fixture.body : JSON.stringify(fixture.body);
    return new Response(body, { status: fixture.status, headers });
  }});
  const sdk = new SDK({ serverURL: "https://ledger.test", httpClient });
  let observed;
  try {
    const response = await sdk.transactions.bulkOperations({ ledgerName: "ledger1", body: [] });
    assert.equal(fixture.status, 200, `${file}: non-2xx must be an SDK error`);
    observed = response.result;
  } catch (error) {
    assert.notEqual(fixture.status, 200, `${file}: successful response must decode`);
    assert.equal(error.statusCode, fixture.status, `${file}: status must survive decoding`);
    assert.notEqual(error.name, "ResponseValidationError", `${file}: schema must accept the real body`);
    observed = typeof fixture.body === "string"
      ? error.body
      : { data: error.data, errorCode: error.errorCode, errorMessage: error.errorMessage };
    if (fixture.status === 503) {
      assert.equal(error.headers.get("Retry-After"), "1", `${file}: caller must receive retry hint`);
    }
  }
  assert.deepEqual(JSON.parse(JSON.stringify(observed)), fixture.body, `${file}: all element and error fields must survive`);
  assert.equal(attempts, 1, `${file}: default SDK retry behavior must remain unchanged`);
}

for (const idempotencyKey of ["batch-key", undefined]) {
  let observed;
  const httpClient = new HTTPClient({ fetcher: async (request) => {
    observed = request;
    return new Response("{}", { headers: { "Content-Type": "application/json" } });
  }});
  const sdk = new SDK({ serverURL: "https://ledger.test", httpClient });
  await sdk.transactions.bulkOperations({ ledgerName: "ledger1", atomic: true, idempotencyKey, body: [] });
  assert.equal(observed.headers.get("Idempotency-Key"), idempotencyKey ?? null);
  assert.equal(new URL(observed.url).searchParams.get("atomic"), "true");
}

let sequentialRequest;
const sequentialSDK = new SDK({
  serverURL: "https://ledger.test",
  httpClient: new HTTPClient({ fetcher: async (request) => {
    sequentialRequest = request;
    return new Response("{}", { headers: { "Content-Type": "application/json" } });
  }}),
});
await sequentialSDK.transactions.bulkOperations({
  ledgerName: "ledger1", atomic: false,
  body: [
    { action: "REVERT_TRANSACTION", ik: "element-one", data: { id: 1 } },
    { action: "REVERT_TRANSACTION", ik: "element-two", data: { id: 2 } },
  ],
});
assert.deepEqual((await sequentialRequest.json()).map((element) => element.ik), ["element-one", "element-two"]);

console.log(`PASS: ${files.length} real bulk responses, batch/element identities, retry hints and default retry behavior`);
